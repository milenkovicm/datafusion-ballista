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

//! Adaptive query execution (AQE) execution plan wrappers used by the
//! scheduler.
//!
//! This module provides lightweight ExecutionPlan implementations used by the
//! scheduler's adaptive query execution logic. They do not perform actual
//! execution themselves; instead they act as placeholders/markers for
//! shuffle/exchange boundaries and carry metadata the scheduler uses to
//! resolve shuffle locations, stage ids, and finalization state.
//!
//! Types:
//! - `ExchangeExec`: Represents an unresolved/resolved shuffle exchange. It
//!   stores the child plan, optional target partitioning, and (when
//!   available) the resolved `shuffle_partitions` describing where each
//!   partition's data lives.
//! - `AdaptiveDatafusionExec`: Wrapper used by AQE to mark a plan as
//!   adaptive and to carry mutable state such as `is_final` and resolved
//!   shuffle metadata.

use ballista_core::execution_plans::{stats_for_partition, stats_for_partitions};
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::Statistics;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties,
    },
};
use log::trace;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::AtomicI64};

/// Execution plan representing an exchange/shuffle boundary used by the
/// scheduler during adaptive query execution (AQE).
///
/// `ExchangeExec` acts as a placeholder for a shuffle: it holds the child
/// `input` plan and, when available, the resolved shuffle metadata in
/// `shuffle_partitions`. The scheduler uses the information stored here to
/// decide stage execution and to compute partition statistics without
/// executing the plan directly.
///
/// Note: this type implements DataFusion's `ExecutionPlan` trait but returns
/// an error from `execute` because it is not directly runnable.
#[derive(Debug)]
pub(crate) struct ExchangeExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    pub(crate) partitioning: Option<Partitioning>,
    pub(crate) plan_id: usize,
    stage_id: Arc<AtomicI64>,

    /// first vector is target representing target partitioning
    /// (to be called on shuffle read side,  fn execute( partition: usize ...)
    /// will be used as key.
    /// second vector represents exchange files, their locations,
    ///
    /// the so the len of `shuffle_partitions` vector is equal to number
    /// partitions after partitioning, the len of each vector item
    /// can not be assumed.
    shuffle_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,

    /// this disables stage from running even it would be suitable to run.
    ///
    /// the main reason for this property this is to allow rules to override
    /// stage execution logic, and to support making more complex
    /// stage run decisions.
    pub(crate) inactive_stage: bool,
}

impl ExchangeExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
    ) -> Self {
        Self::new_with_details(
            input,
            partitioning,
            plan_id,
            Arc::new(AtomicI64::new(-1)),
            Arc::new(Mutex::new(None)),
        )
    }

    pub fn new_with_details(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
        stage_id: Arc<AtomicI64>,
        stage_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,
    ) -> Self {
        let plan_partitioning = match partitioning.as_ref() {
            Some(partitioning) => partitioning.clone(),
            None => input.output_partitioning().clone(),
        };
        let eq_properties = input.properties().eq_properties.clone();
        let properties = PlanProperties::new(
            eq_properties,
            plan_partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            input,
            properties,
            plan_id,
            stage_id,
            shuffle_partitions: stage_partitions,
            partitioning,
            inactive_stage: false,
        }
    }

    /// Indicates that partitions have been resolved
    ///
    /// If partitions has been resolved, current stage has
    /// finished and new one could be started.
    ///
    /// Unresolved shuffle could be  replaced with shuffle read.
    pub fn shuffle_created(&self) -> bool {
        self.shuffle_partitions.lock().is_some()
    }

    /// Resolves and stores the shuffle partitions for this exchange operation.
    ///
    /// This method should be called once the partitions for the shuffle have been determined.
    /// After calling this method, `shuffle_created()` will return `true` and the stored
    /// partitions can be retrieved via `shuffle_partitions()`.
    ///
    /// # Arguments
    ///
    /// * `partitions` - A vector of partition vectors, where each inner vector contains
    ///   the `PartitionLocation`s for a shuffle partition.
    pub fn resolve_shuffle_partitions(&self, partitions: Vec<Vec<PartitionLocation>>) {
        self.shuffle_partitions.lock().replace(partitions);
    }

    /// Checks whether the shuffle partitions have been resolved.
    ///
    /// Returns `true` if partitions have been resolved, indicating that the current stage
    /// has finished and a new stage can be started. An unresolved shuffle can be replaced
    /// with a shuffle read operation.
    ///
    /// # Returns
    ///
    /// `true` if `shuffle_partitions` contains a value, `false` otherwise.
    pub fn shuffle_partitions(&self) -> Option<Vec<Vec<PartitionLocation>>> {
        self.shuffle_partitions.lock().clone()
    }

    /// sets the stage id running this exchange
    pub fn set_stage_id(&self, id: usize) {
        self.stage_id
            .store(id as i64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stage_id(&self) -> Option<usize> {
        let stage_id = self.stage_id.load(std::sync::atomic::Ordering::Relaxed);

        if stage_id >= 0 {
            Some(stage_id as usize)
        } else {
            None
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Converts this `ExchangeExec` into an equivalent single-partition plan.
    ///
    /// All resolved shuffle partitions are merged (flattened) into a single
    /// partition vector, the output partitioning is set to
    /// `UnknownPartitioning(1)`, and the plan properties are updated
    /// accordingly.  If no shuffle partitions have been resolved yet the
    /// new plan's `shuffle_partitions` remain `None`.
    pub fn to_single_partition(&self) -> Self {
        let merged = self.shuffle_partitions.lock().as_ref().map(|partitions| {
            let flat: Vec<PartitionLocation> =
                partitions.iter().flatten().cloned().collect();
            vec![flat]
        });

        let mut new_exec = Self::new_with_details(
            self.input.clone(),
            Some(Partitioning::UnknownPartitioning(1)),
            self.plan_id,
            self.stage_id.clone(),
            Arc::new(Mutex::new(merged)),
        );
        new_exec.inactive_stage = self.inactive_stage;
        new_exec
    }

    /// Creates new [ExchangeExec] with partitions grouped together, so each new partition
    /// is bigger than provided row_threshold or byte_threshold.
    ///
    /// Only neighboring partitions could be coalesced. For example if we have partitions
    /// [0,1,2,3,4,5,6] resulting partitioning can be [[0], [1,2,3], [4,5,6]]. resulting
    /// partition should be bigger than provided thresholds
    pub fn to_coalesced_partitions(
        &self,
        row_threshold: usize,
        byte_threshold: usize,
    ) -> Self {
        let coalesced = self.shuffle_partitions.lock().as_ref().map(|partitions| {
            let mut result: Vec<Vec<PartitionLocation>> = Vec::new();

            let mut current_group: Vec<PartitionLocation> = Vec::new();
            let mut current_rows: u64 = 0;
            let mut current_bytes: u64 = 0;

            for partition in partitions {
                let partition_rows: Option<u64> = partition
                    .iter()
                    .map(|loc| loc.partition_stats.num_rows())
                    .sum();
                let partition_bytes: Option<u64> = partition
                    .iter()
                    .map(|loc| loc.partition_stats.num_bytes())
                    .sum();

                match (partition_rows, partition_bytes) {
                    (Some(rows), Some(bytes)) => {
                        current_group.extend(partition.iter().cloned());
                        current_rows += rows;
                        current_bytes += bytes;

                        if current_rows >= row_threshold as u64
                            || current_bytes >= byte_threshold as u64
                        {
                            result.push(std::mem::take(&mut current_group));
                            current_rows = 0;
                            current_bytes = 0;
                        }
                    }
                    _ => {
                        // Missing stats: close the current group and isolate this partition.
                        if !current_group.is_empty() {
                            result.push(std::mem::take(&mut current_group));
                            current_rows = 0;
                            current_bytes = 0;
                        }
                        result.push(partition.iter().cloned().collect());
                    }
                }
            }

            if !current_group.is_empty() {
                result.push(current_group);
            }

            result
        });

        let partition_count = coalesced.as_ref().map(|v| v.len()).unwrap_or(1);
        let mut new_exec = Self::new_with_details(
            self.input.clone(),
            Some(Partitioning::UnknownPartitioning(partition_count)),
            self.plan_id,
            self.stage_id.clone(),
            Arc::new(Mutex::new(coalesced)),
        );
        new_exec.inactive_stage = self.inactive_stage;
        new_exec
    }
}

impl DisplayAs for ExchangeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ExchangeExec: partitioning={}, plan_id={}, stage_id={}, stage_resolved={}",
                    self.partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                    self.plan_id,
                    self.stage_id()
                        .map(|stage_id| format!("{}", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                    self.shuffle_partitions.lock().is_some()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "partitioning={}",
                    self.partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                )?;
                writeln!(f, "plan_id={}", self.plan_id)?;
                writeln!(
                    f,
                    "stage_id={}",
                    self.stage_id()
                        .map(|stage_id| format!("({})", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )?;
                writeln!(
                    f,
                    "stage_resolved={}",
                    self.shuffle_partitions.lock().is_some()
                )
            }
        }
    }
}

impl ExecutionPlan for ExchangeExec {
    fn name(&self) -> &str {
        "ExchangeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        match self.partitioning {
            Some(_) => vec![false; self.children().len()],
            None => vec![true; self.children().len()],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let mut new_exec = Self::new_with_details(
                children[0].clone(),
                self.partitioning.clone(),
                self.plan_id,
                self.stage_id.clone(),
                self.shuffle_partitions.clone(),
            );
            new_exec.inactive_stage = self.inactive_stage;

            Ok(Arc::new(new_exec))
        } else {
            Err(DataFusionError::Plan(
                "ExchangeExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "ExchangeExec does not support execution".to_owned(),
        ))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let schema = self.input.schema();
        match self.shuffle_partitions.lock().deref() {
            //
            Some(partition_locations) => {
                if let Some(idx) = partition {
                    let partition_count =
                        self.properties().partitioning.partition_count();
                    if idx >= partition_count {
                        return datafusion::common::internal_err!(
                            "Invalid partition index: {}, the partition count is {}",
                            idx,
                            partition_count
                        );
                    }
                    let stat_for_partition = stats_for_partition(
                        idx,
                        schema.fields().len(),
                        partition_locations,
                    );

                    trace!(
                        "shuffle reader at stage: {:?} and partition {} returned statistics: {:?}",
                        self.stage_id, idx, stat_for_partition
                    );
                    stat_for_partition
                } else {
                    let stats_for_partitions = stats_for_partitions(
                        schema.fields().len(),
                        partition_locations
                            .iter()
                            .flatten()
                            .map(|loc| loc.partition_stats),
                    );
                    trace!(
                        "shuffle reader at stage: {:?} returned statistics for all partitions: {:?}",
                        self.stage_id, stats_for_partitions
                    );
                    Ok(stats_for_partitions)
                }
            }
            None => Ok(Statistics::new_unknown(&schema)),
        }
    }
}

/// Wrapper execution plan used by the scheduler to represent an adaptive
/// DataFusion plan.
///
/// `AdaptiveDatafusionExec` is a lightweight wrapper that carries AQE-specific
/// mutable state.  Like `ExchangeExec`, this type implements `ExecutionPlan` for
/// integration but does not support direct execution.
#[derive(Debug)]
pub(crate) struct AdaptiveDatafusionExec {
    input: Arc<dyn ExecutionPlan>,
    shuffle_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,
    stage_id: Arc<AtomicI64>,
    plan_id: usize,
    pub(crate) is_final: Arc<AtomicBool>,
}

impl AdaptiveDatafusionExec {
    pub fn new(plan_id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            is_final: AtomicBool::new(false).into(),
            plan_id,
            input,
            stage_id: Arc::new(AtomicI64::new(-1)),
            shuffle_partitions: Arc::new(Mutex::new(None)),
        }
    }

    pub fn shuffle_created(&self) -> bool {
        self.shuffle_partitions.lock().is_some()
    }

    /// Changes shuffle from unresolved to resolved
    /// providing list of available partitions
    ///
    pub fn resolve_shuffle_partitions(&self, partitions: Vec<Vec<PartitionLocation>>) {
        self.shuffle_partitions.lock().replace(partitions);
    }

    /// sets the stage id running this exchange
    pub fn set_stage_id(&self, id: usize) {
        self.stage_id
            .store(id as i64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stage_id(&self) -> Option<usize> {
        let stage_id = self.stage_id.load(std::sync::atomic::Ordering::Relaxed);

        if stage_id >= 0 {
            Some(stage_id as usize)
        } else {
            None
        }
    }

    pub fn set_final_plan(&self) {
        self.is_final
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for AdaptiveDatafusionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "AdaptiveDatafusionExec: is_final={:?}, plan_id={}, stage_id={}",
                    self.is_final,
                    self.plan_id,
                    self.stage_id()
                        .map(|stage_id| format!("{}", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "is_final={:?}", self.is_final)?;
                writeln!(f, "plan_id={}", self.plan_id)?;
                writeln!(
                    f,
                    "stage_id={}",
                    self.stage_id()
                        .map(|stage_id| format!("({})", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )
            }
        }
    }
}

impl ExecutionPlan for AdaptiveDatafusionExec {
    fn name(&self) -> &str {
        "AdaptiveDatafusionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let new_exec = Self {
                is_final: self.is_final.clone(),
                plan_id: self.plan_id,
                input: children[0].clone(),
                stage_id: self.stage_id.clone(),
                shuffle_partitions: Arc::clone(&self.shuffle_partitions),
            };

            Ok(Arc::new(new_exec))
        } else {
            Err(DataFusionError::Plan(
                "AdaptiveDatafusionExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "AdaptiveDatafusionExec does not support execution".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
        PartitionStats,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    fn mock_location(partition_id: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition_id,
            partition_id: PartitionId {
                job_id: "job".to_string(),
                stage_id: 0,
                partition_id,
            },
            executor_meta: ExecutorMetadata {
                id: "exec".to_string(),
                host: "localhost".to_string(),
                port: 50050,
                grpc_port: 50051,
                specification: ExecutorSpecification { task_slots: 4 },
            },
            path: format!("/tmp/partition_{partition_id}"),
            partition_stats: PartitionStats::new(Some(10), None, Some(100)),
        }
    }

    fn mock_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ))
    }

    #[test]
    fn to_single_partition_merges_all_partitions() {
        // Build an ExchangeExec with Hash(3) partitioning and resolve it with
        // three partition vectors (2 locations each → 6 total).
        let input = mock_input();
        let exchange =
            ExchangeExec::new(input, Some(Partitioning::UnknownPartitioning(3)), 0);

        let partitions = vec![
            vec![mock_location(0), mock_location(1)],
            vec![mock_location(2), mock_location(3)],
            vec![mock_location(4), mock_location(5)],
        ];
        exchange.resolve_shuffle_partitions(partitions);

        let single = exchange.to_single_partition();

        // Partitioning must be single-partition.
        assert_eq!(
            single.properties().partitioning.partition_count(),
            1,
            "expected 1 output partition"
        );
        assert!(matches!(
            single.properties().partitioning,
            Partitioning::UnknownPartitioning(1)
        ));

        // All 6 locations must be in the single merged partition.
        let resolved = single
            .shuffle_partitions()
            .expect("partitions should be resolved");

        assert_eq!(resolved.len(), 1, "expected a single partition vector");
        assert_eq!(
            resolved[0].len(),
            6,
            "expected all 6 locations merged into one partition"
        );

        let paths: Vec<&str> = resolved[0].iter().map(|l| l.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "/tmp/partition_0",
                "/tmp/partition_1",
                "/tmp/partition_2",
                "/tmp/partition_3",
                "/tmp/partition_4",
                "/tmp/partition_5",
            ]
        );
    }

    #[test]
    fn to_single_partition_unresolved_stays_none() {
        // If shuffle partitions have not been resolved yet the result should
        // also be unresolved.
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(4)),
            1,
        );
        assert!(!exchange.shuffle_created());

        let single = exchange.to_single_partition();

        assert_eq!(single.properties().partitioning.partition_count(), 1);
        assert!(
            single.shuffle_partitions().is_none(),
            "unresolved source → result must also be unresolved"
        );
    }

    #[test]
    fn to_single_partition_preserves_inactive_stage_flag() {
        let mut exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(2)),
            2,
        );
        exchange.inactive_stage = true;

        let single = exchange.to_single_partition();
        assert!(
            single.inactive_stage,
            "inactive_stage flag must be preserved"
        );
    }

    /// Build a mock location with explicit row/byte stats.
    fn mock_location_with_stats(
        partition_id: usize,
        num_rows: u64,
        num_bytes: u64,
    ) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition_id,
            partition_id: PartitionId {
                job_id: "job".to_string(),
                stage_id: 0,
                partition_id,
            },
            executor_meta: ExecutorMetadata {
                id: "exec".to_string(),
                host: "localhost".to_string(),
                port: 50050,
                grpc_port: 50051,
                specification: ExecutorSpecification { task_slots: 4 },
            },
            path: format!("/tmp/partition_{partition_id}"),
            partition_stats: PartitionStats::new(Some(num_rows), None, Some(num_bytes)),
        }
    }

    #[test]
    fn to_coalesced_partitions_by_row_threshold() {
        // 6 partitions, each with 10 rows. Row threshold = 25 → groups: [0,1,2], [3,4,5]
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(6)),
            0,
        );
        let partitions: Vec<Vec<PartitionLocation>> = (0..6)
            .map(|i| vec![mock_location_with_stats(i, 10, 5)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(25, usize::MAX);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");

        // Groups: [0,1,2] (30 rows ≥ 25), [3,4,5] remainder
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].len(), 3);
        assert_eq!(resolved[1].len(), 3);
        assert_eq!(coalesced.properties().partitioning.partition_count(), 2);
    }

    #[test]
    fn to_coalesced_partitions_by_byte_threshold() {
        // 4 partitions, each with 100 bytes. Byte threshold = 150 → groups: [0,1], [2,3]
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(4)),
            0,
        );
        let partitions: Vec<Vec<PartitionLocation>> = (0..4)
            .map(|i| vec![mock_location_with_stats(i, 1, 100)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(usize::MAX, 150);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].len(), 2);
        assert_eq!(resolved[1].len(), 2);
    }

    #[test]
    fn to_coalesced_partitions_all_merge_when_threshold_high() {
        // Threshold higher than total → all partitions end up in one group
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(4)),
            0,
        );
        let partitions: Vec<Vec<PartitionLocation>> = (0..4)
            .map(|i| vec![mock_location_with_stats(i, 10, 10)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(usize::MAX, usize::MAX);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].len(), 4);
    }

    #[test]
    fn to_coalesced_partitions_each_partition_own_group_when_threshold_zero() {
        // Threshold = 0 → every partition immediately exceeds it → each is its own group
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(3)),
            0,
        );
        let partitions: Vec<Vec<PartitionLocation>> = (0..3)
            .map(|i| vec![mock_location_with_stats(i, 10, 10)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(0, 0);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");
        assert_eq!(resolved.len(), 3);
    }

    #[test]
    fn to_coalesced_partitions_unresolved_stays_none() {
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(4)),
            0,
        );
        assert!(!exchange.shuffle_created());

        let coalesced = exchange.to_coalesced_partitions(100, 1000);

        assert!(
            coalesced.shuffle_partitions().is_none(),
            "unresolved source → result must also be unresolved"
        );
    }

    #[test]
    fn to_coalesced_partitions_preserves_inactive_stage_flag() {
        let mut exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(2)),
            0,
        );
        exchange.inactive_stage = true;
        let partitions: Vec<Vec<PartitionLocation>> = (0..2)
            .map(|i| vec![mock_location_with_stats(i, 10, 10)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(100, 100);
        assert!(
            coalesced.inactive_stage,
            "inactive_stage flag must be preserved"
        );
    }

    #[test]
    fn to_coalesced_partitions_all_under_threshold_coalesce_to_single() {
        // All partitions are below both thresholds individually and in total,
        // so nothing triggers the emit inside the loop. They all fall through
        // to the trailing remainder push, producing exactly 1 output partition.
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(5)),
            0,
        );
        let partitions: Vec<Vec<PartitionLocation>> = (0..5)
            .map(|i| vec![mock_location_with_stats(i, 10, 10)])
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        // Thresholds well above the total (50 rows / 50 bytes combined).
        let coalesced = exchange.to_coalesced_partitions(1000, 1000);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");

        assert_eq!(
            resolved.len(),
            1,
            "all under-threshold partitions must coalesce into one"
        );
        assert_eq!(resolved[0].len(), 5, "all 5 locations must be present");
        assert_eq!(coalesced.properties().partitioning.partition_count(), 1);
    }

    #[test]
    fn to_coalesced_partitions_never_splits_a_partition_across_groups() {
        // Each source partition has 2 locations. Row threshold = 50.
        // Partition 0: 30 rows, Partition 1: 30 rows → emitted as group 0 after partition 1 tips over 50
        // Partition 2: 30 rows → group 1 (remainder, both its locations)
        //
        // A buggy implementation splitting a partition mid-way would put some locations
        // from partition 1 in group 0 and others in group 1. This test catches that.
        let exchange = ExchangeExec::new(
            mock_input(),
            Some(Partitioning::UnknownPartitioning(3)),
            0,
        );

        // Each partition has 2 locations tagged with unique paths.
        let partitions: Vec<Vec<PartitionLocation>> = (0..3usize)
            .map(|p| {
                vec![
                    mock_location_with_stats(p * 2, 15, 10),
                    mock_location_with_stats(p * 2 + 1, 15, 10),
                ]
            })
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        let coalesced = exchange.to_coalesced_partitions(50, usize::MAX);

        let resolved = coalesced
            .shuffle_partitions()
            .expect("partitions should be resolved");

        assert_eq!(resolved.len(), 2, "expected 2 output groups");

        // Group 0 contains all 4 locations from source partitions 0 and 1.
        let group0_paths: Vec<&str> =
            resolved[0].iter().map(|l| l.path.as_str()).collect();
        assert_eq!(group0_paths.len(), 4);
        assert!(group0_paths.contains(&"/tmp/partition_0"));
        assert!(group0_paths.contains(&"/tmp/partition_1"));
        assert!(group0_paths.contains(&"/tmp/partition_2"));
        assert!(group0_paths.contains(&"/tmp/partition_3"));

        // Group 1 contains both locations from source partition 2 — none leaked into group 0.
        let group1_paths: Vec<&str> =
            resolved[1].iter().map(|l| l.path.as_str()).collect();
        assert_eq!(group1_paths.len(), 2);
        assert!(group1_paths.contains(&"/tmp/partition_4"));
        assert!(group1_paths.contains(&"/tmp/partition_5"));
    }
}
