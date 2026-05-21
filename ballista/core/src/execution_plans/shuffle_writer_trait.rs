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

//! Common trait for shuffle writer execution plans.
//!
//! This trait provides a common interface for both standard hash-based shuffle
//! (`ShuffleWriterExec`) and sort-based shuffle (`SortShuffleWriterExec`).

use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Trait for shuffle writer execution plans.
///
/// This trait defines the common interface needed by the distributed planner
/// and execution graph to work with different shuffle implementations.
pub trait ShuffleWriter: Any + ExecutionPlan + Debug + Send + Sync {
    /// Get the Job ID for this query stage.
    fn job_id(&self) -> &str;

    /// Get the Stage ID for this query stage.
    fn stage_id(&self) -> usize;

    /// Get the shuffle output partitioning, if any.
    ///
    /// Returns `Some(partitioning)` for repartitioning stages,
    /// `None` for stages that preserve the input partitioning.
    fn shuffle_output_partitioning(&self) -> Option<&Partitioning>;

    /// Get the number of input partitions.
    fn input_partition_count(&self) -> usize;

    /// Clone this shuffle writer as an Arc'd trait object.
    fn clone_box(&self) -> Arc<dyn ShuffleWriter>;
}

impl dyn ShuffleWriter {
    /// Returns `true` if the plan is of type `T`.
    ///
    /// Prefer this over `downcast_ref::<T>().is_some()`. Works correctly when
    /// called on `Arc<dyn ExecutionPlan>` via auto-deref.
    pub fn is<T: ExecutionPlan>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Attempts to downcast this plan to a concrete type `T`, returning `None`
    /// if the plan is not of that type.
    ///
    /// Works correctly when called on `Arc<dyn ExecutionPlan>` via auto-deref,
    /// unlike `(&arc as &dyn Any).downcast_ref::<T>()` which would attempt to
    /// downcast the `Arc` itself.
    pub fn downcast_ref<T: ExecutionPlan>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }
}
