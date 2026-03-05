// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::config::BallistaConfig;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Optimizer rule that collapses a small resolved [`ExchangeExec`] to a single
/// partition when it feeds an unresolved (about-to-run) exchange.
///
/// # How it works
///
/// The rule is applied **top-down**.  For every unresolved [`ExchangeExec`]
/// (one whose shuffle has not been created yet) the rule walks its input chain
/// following only **single-child** nodes.  The walk stops at the **first
/// [`ExchangeExec`]** found in that chain:
///
/// - If that exchange is **resolved** (`shuffle_created() == true`) *and*
///   reports fewer than `row_threshold` rows **or** fewer than `byte_threshold`
///   bytes, it is replaced with its single-partition equivalent via
///   [`ExchangeExec::to_single_partition`].  All intermediate single-child
///   nodes between the outer and the found exchange are rebuilt with the new
///   child.  The outer unresolved exchange itself is left unchanged.
/// - If the first exchange found is **unresolved**, or if any node in the chain
///   has more than one child (e.g. a join), the rule does nothing.
///
/// # Why top-down?
///
/// Processing parents before children ensures that the optimisation decision
/// for each unresolved exchange is based on the actual statistics of its
/// nearest resolved ancestor, without interference from already-optimised
/// deeper nodes.
#[derive(Debug, Clone, Default)]
pub struct CoalesceExchangeRule {}

impl CoalesceExchangeRule {
    fn transform(
        plan: Arc<dyn ExecutionPlan>,
        row_threshold: usize,
        byte_threshold: usize,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() else {
            return Ok(Transformed::no(plan));
        };

        // Only consider exchanges that have not run yet.
        if exchange.shuffle_created() {
            return Ok(Transformed::no(plan));
        }

        // Walk the single-child input chain for the first resolved exchange.
        // If it is small, rebuild the current plan with the coalesced inner exchange.
        let input = exchange.input().clone();
        match Self::coalesce_first_resolved_in_chain(
            input,
            row_threshold,
            byte_threshold,
        )? {
            Some(new_input) => {
                Ok(Transformed::yes(plan.with_new_children(vec![new_input])?))
            }
            None => Ok(Transformed::no(plan)),
        }
    }

    /// Recursively walks `plan` following only single-child nodes, looking for
    /// the first [`ExchangeExec`].
    ///
    /// - If the first exchange found is **resolved** and a **majority of its
    ///   partitions** are individually below both `row_threshold` rows *and*
    ///   `byte_threshold` bytes, it is returned coalesced via
    ///   [`ExchangeExec::to_coalesced_partitions`], which groups neighboring
    ///   small partitions into larger ones while leaving large partitions
    ///   intact.
    /// - If the majority check does not apply (no per-partition stats, or fewer
    ///   than half the partitions are small), the rule falls back to the
    ///   original behavior: coalesce to a **single** partition when the
    ///   *total* stats are below either threshold.
    /// - If the first exchange found is **unresolved**, or any node has ≠ 1
    ///   child, `None` is returned and the caller leaves the plan unchanged.
    fn coalesce_first_resolved_in_chain(
        plan: Arc<dyn ExecutionPlan>,
        row_threshold: usize,
        byte_threshold: usize,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(inner) = plan.as_any().downcast_ref::<ExchangeExec>() {
            // First exchange found in the chain – it must be resolved.
            if !inner.shuffle_created() {
                return Ok(None);
            }

            // If per-partition stats are available, check whether the majority
            // of partitions are individually small (under both thresholds).
            // When that is the case, coalesce neighboring small partitions
            // together instead of forcing everything into a single partition.
            if let Some(partitions) = inner.shuffle_partitions() {
                let total = partitions.len();
                let small_count = partitions
                    .iter()
                    .filter(|partition| {
                        // If any location is missing stats, treat the partition as large.
                        let rows: Option<u64> = partition
                            .iter()
                            .map(|loc| loc.partition_stats.num_rows())
                            .sum();
                        let bytes: Option<u64> = partition
                            .iter()
                            .map(|loc| loc.partition_stats.num_bytes())
                            .sum();
                        match (rows, bytes) {
                            (Some(r), Some(b)) => {
                                r < row_threshold as u64 && b < byte_threshold as u64
                            }
                            _ => false,
                        }
                    })
                    .count();

                if total > 0 && small_count * 2 > total {
                    return Ok(Some(Arc::new(
                        inner.to_coalesced_partitions(row_threshold, byte_threshold),
                    )));
                }
            }

            // Fallback: check total stats and collapse to a single partition.
            let stats = inner.partition_statistics(None)?;
            let small_by_rows = match stats.num_rows {
                Precision::Exact(n) | Precision::Inexact(n) => n < row_threshold,
                Precision::Absent => false,
            };
            let small_by_bytes = match stats.total_byte_size {
                Precision::Exact(n) | Precision::Inexact(n) => n < byte_threshold,
                Precision::Absent => false,
            };
            return if small_by_rows || small_by_bytes {
                Ok(Some(Arc::new(inner.to_single_partition())))
            } else {
                Ok(None)
            };
        }

        // Only follow single-child paths.
        let children = plan.children();
        if children.len() != 1 {
            return Ok(None);
        }

        let child = children[0].clone();
        match Self::coalesce_first_resolved_in_chain(
            child,
            row_threshold,
            byte_threshold,
        )? {
            Some(new_child) => Ok(Some(plan.with_new_children(vec![new_child])?)),
            None => Ok(None),
        }
    }
}

impl PhysicalOptimizerRule for CoalesceExchangeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let c = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        let row_threshold = c.adaptive_query_coalesce_exchange_rows();
        let byte_threshold = c.adaptive_query_coalesce_exchange_bytes();

        Ok(plan
            .transform_down(|p| Self::transform(p, row_threshold, byte_threshold))?
            .data)
    }

    fn name(&self) -> &str {
        "CoalesceExchangeRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_plan;
    use crate::state::aqe::execution_plan::ExchangeExec;
    use crate::state::aqe::planner::AdaptivePlanner;
    use crate::state::aqe::test::{
        mock_context, mock_memory_table, mock_partitions_with_statistics_provided,
    };
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
        PartitionStats,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    /// Builds a multi-partition exchange in the partition locations that
    /// reports `num_rows` rows total across all partitions.
    fn partitions_with_rows(num_rows: u64) -> Vec<Vec<PartitionLocation>> {
        let location = PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: "job".to_string(),
                stage_id: 0,
                partition_id: 0,
            },
            executor_meta: ExecutorMetadata {
                id: "e".to_string(),
                host: "localhost".to_string(),
                port: 50050,
                grpc_port: 50051,
                specification: ExecutorSpecification { task_slots: 4 },
            },
            path: "/tmp/p0".to_string(),
            partition_stats: PartitionStats::new(
                Some(num_rows),
                None,
                Some(num_rows * 8),
            ),
        };
        // Two output partitions, first carries all rows, second is empty.
        vec![vec![location], vec![]]
    }

    /// Verify that after the first stage (ExchangeExec input) resolves with a
    /// small row count the planner collapses the next exchange to a single
    /// partition via the CoalesceSmallInputRule.
    #[tokio::test]
    async fn small_resolved_exchange_input_is_collapsed() -> datafusion::error::Result<()>
    {
        let ctx = mock_context();
        ctx.register_table("t", mock_memory_table())?;

        // Two-stage plan: stage 0 hashes into 2 partitions, stage 1 reads them.
        let plan = ctx
            .sql("select min(a) as a from t group by b")
            .await?
            .create_physical_plan()
            .await?;

        let mut planner = AdaptivePlanner::try_new_with_optimizers(
            plan,
            ctx.state().config(),
            vec![
                Arc::new(crate::state::aqe::optimizer_rule::DistributedExchangeRule::default()),
                Arc::new(crate::state::aqe::optimizer_rule::EliminateEmptyExchangeRule::default()),
                Arc::new(CoalesceExchangeRule::default()),
            ],
            "test".to_string(),
        )?;

        // Stage 0 runs and resolves with fewer than 1 000 rows.
        let stages = planner.runnable_stages()?.unwrap();
        assert_eq!(1, stages.len(), "expected one runnable stage");

        planner.finalise_stage_internal(0, partitions_with_rows(42))?;

        // After replanning, stage 1's exchange should have been collapsed to
        // a single partition because its resolved-exchange input is small.
        let stages = planner.runnable_stages()?.unwrap();
        assert_eq!(1, stages.len());

        // The ShuffleWriterExec wrapping should show no partitioning (single
        // partition) on the inner exchange.
        assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
        ShuffleWriterExec: partitioning: None
          ProjectionExec: expr=[min(t.a)@1 as a]
            AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[min(t.a)]
              ShuffleReaderExec: partitioning: Hash([b@0], 2)
        ");

        Ok(())
    }

    /// A large input (≥ 1000 rows) must NOT be collapsed.
    #[tokio::test]
    async fn large_resolved_exchange_input_is_not_collapsed()
    -> datafusion::error::Result<()> {
        let ctx = mock_context();
        ctx.register_table("t", mock_memory_table())?;

        let plan = ctx
            .sql("select min(a) as a from t group by b")
            .await?
            .create_physical_plan()
            .await?;

        let mut planner = AdaptivePlanner::try_new_with_optimizers(
            plan,
            ctx.state().config(),
            vec![
                Arc::new(crate::state::aqe::optimizer_rule::DistributedExchangeRule::default()),
                Arc::new(crate::state::aqe::optimizer_rule::EliminateEmptyExchangeRule::default()),
                Arc::new(CoalesceExchangeRule::default()),
            ],
            "test".to_string(),
        )?;

        let stages = planner.runnable_stages()?.unwrap();
        assert_eq!(1, stages.len());

        // Resolve stage 0 with 200 000 rows – above both default thresholds
        // (131 072 rows and 1 048 576 bytes).
        planner.finalise_stage_internal(0, partitions_with_rows(200_000))?;

        let stages = planner.runnable_stages()?.unwrap();
        assert_eq!(1, stages.len());

        // large input should NOT be collapsed to single partition,
        assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
        ShuffleWriterExec: partitioning: None
          ProjectionExec: expr=[min(t.a)@1 as a]
            AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[min(t.a)]
              ShuffleReaderExec: partitioning: Hash([b@0], 2)
        ");

        Ok(())
    }

    /// Unit test: outer unresolved exchange → inner resolved small exchange.
    /// The inner exchange is coalesced to 1 partition; the outer keeps its own.
    #[test]
    fn rule_coalesces_inner_small_resolved_exchange() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ));

        // Resolved inner exchange with 42 rows (below threshold).
        let inner =
            ExchangeExec::new(leaf, Some(Partitioning::UnknownPartitioning(2)), 0);
        inner
            .resolve_shuffle_partitions(mock_partitions_with_statistics_provided(10, 20));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(inner);

        // Outer unresolved exchange, 4 partitions.
        let outer =
            ExchangeExec::new(inner, Some(Partitioning::UnknownPartitioning(4)), 1);
        let outer: Arc<dyn ExecutionPlan> = Arc::new(outer);

        let result = CoalesceExchangeRule::transform(outer, 1_000, 8_000).unwrap();

        assert!(result.transformed, "expected the plan to be transformed");
        // Outer exchange keeps its own partitioning unchanged.
        assert_eq!(
            result.data.output_partitioning().partition_count(),
            4,
            "outer exchange must keep its original partition count"
        );
        // Inner (direct child) exchange is coalesced to a single partition.
        assert_eq!(
            result.data.children()[0]
                .output_partitioning()
                .partition_count(),
            1,
            "inner resolved exchange must be coalesced to 1 partition"
        );
    }

    /// Unit test: outer unresolved exchange → inner resolved large exchange.
    /// Nothing is transformed.
    #[test]
    fn rule_does_not_transform_large_resolved_exchange() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ));

        let inner =
            ExchangeExec::new(leaf, Some(Partitioning::UnknownPartitioning(2)), 0);
        inner.resolve_shuffle_partitions(partitions_with_rows(5_000));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(inner);

        let outer =
            ExchangeExec::new(inner, Some(Partitioning::UnknownPartitioning(2)), 1);
        let outer: Arc<dyn ExecutionPlan> = Arc::new(outer);

        // Thresholds: 1 000 rows / 8 000 bytes. 5 000 rows * 8 = 40 000 bytes –
        // both exceed their respective thresholds, so nothing is coalesced.
        let result = CoalesceExchangeRule::transform(outer, 1_000, 8_000).unwrap();

        assert!(!result.transformed, "large input must NOT be transformed");
        assert_eq!(
            result.data.output_partitioning().partition_count(),
            2,
            "partition count must remain unchanged"
        );
    }

    /// Unit test: the first exchange in the chain is unresolved → no transformation.
    #[test]
    fn rule_does_not_transform_when_first_exchange_is_unresolved() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ));

        // Inner exchange is also unresolved (not yet run).
        let inner =
            ExchangeExec::new(leaf, Some(Partitioning::UnknownPartitioning(2)), 0);
        let inner: Arc<dyn ExecutionPlan> = Arc::new(inner);

        let outer =
            ExchangeExec::new(inner, Some(Partitioning::UnknownPartitioning(2)), 1);
        let outer: Arc<dyn ExecutionPlan> = Arc::new(outer);

        let result = CoalesceExchangeRule::transform(outer, 1_000, 8_000).unwrap();
        assert!(
            !result.transformed,
            "unresolved inner exchange must not be transformed"
        );
    }
}
