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

// sort join
//
// ```
// [2025-08-24T19:03:40Z INFO  ballista_scheduler::state::task_manager] Submitting execution graph: ExecutionGraph[job_id=sHhXnRZ, session_id=ecbfec72-49c9-4f12-a21c-8abe998ef77b, available_tasks=0, is_successful=false]
//     =========ResolvedStage[stage_id=2.0, partitions=16]=========
//     ShuffleWriterExec: partitions:Some(Hash([Column { name: "c_custkey", index: 0 }], 16))
//       DataSourceExec: file_groups={16 groups: [[Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:0..5766018], [Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:5766018..6639728, Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:0..4892308], [Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:4892308..6597204, Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:0..4061122], [Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:4061122..6569978, Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:0..3257162], [Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:3257162..6568206, Users/marko/TMP/tpch-data-sf10/customer/part-12.parquet:0..2454974], ...]}, projection=[c_custkey], file_type=parquet
//     =========UnResolvedStage[stage_id=3.0, children=2]=========
//     Inputs{1: StageOutput { partition_locations: {}, complete: false }, 2: StageOutput { partition_locations: {}, complete: false }}
//     ShuffleWriterExec: partitions:None
//       SortMergeJoin: join_type=Inner, on=[(c_custkey@0, c_custkey@0)]
//         SortExec: expr=[c_custkey@0 ASC], preserve_partitioning=[true]
//           CoalesceBatchesExec: target_batch_size=8192
//             UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 0 }], 16)
//         SortExec: expr=[c_custkey@0 ASC], preserve_partitioning=[true]
//           CoalesceBatchesExec: target_batch_size=8192
//             UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 0 }], 16)
//     =========UnResolvedStage[stage_id=4.0, children=1]=========
//     Inputs{3: StageOutput { partition_locations: {}, complete: false }}
//     ShuffleWriterExec: partitions:None
//       CoalescePartitionsExec: fetch=10
//         UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 1 }], 16)
//     =========ResolvedStage[stage_id=1.0, partitions=16]=========
//     ShuffleWriterExec: partitions:Some(Hash([Column { name: "c_custkey", index: 0 }], 16))
//       DataSourceExec: file_groups={16 groups: [[Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:0..5766018], [Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:5766018..6639728, Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:0..4892308], [Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:4892308..6597204, Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:0..4061122], [Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:4061122..6569978, Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:0..3257162], [Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:3257162..6568206, Users/marko/TMP/tpch-data-sf10/customer/part-12.parquet:0..2454974], ...]}, projection=[c_custkey], file_type=parquet
// ```
// Hash Join
//
// ```
//    Submitting execution graph: ExecutionGraph[job_id=2ZKA2Hj, session_id=626d11e4-d3a9-48d6-8219-ecf08d376bb7, available_tasks=0, is_successful=false]
//     =========ResolvedStage[stage_id=1.0, partitions=16]=========
//     ShuffleWriterExec: partitions:Some(Hash([Column { name: "c_custkey", index: 0 }], 16))
//       DataSourceExec: file_groups={16 groups: [[Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:0..5766018], [Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:5766018..6639728, Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:0..4892308], [Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:4892308..6597204, Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:0..4061122], [Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:4061122..6569978, Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:0..3257162], [Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:3257162..6568206, Users/marko/TMP/tpch-data-sf10/customer/part-12.parquet:0..2454974], ...]}, projection=[c_custkey], file_type=parquet
//     =========ResolvedStage[stage_id=2.0, partitions=16]=========
//     ShuffleWriterExec: partitions:Some(Hash([Column { name: "c_custkey", index: 0 }], 16))
//       DataSourceExec: file_groups={16 groups: [[Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:0..5766018], [Users/marko/TMP/tpch-data-sf10/customer/part-0.parquet:5766018..6639728, Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:0..4892308], [Users/marko/TMP/tpch-data-sf10/customer/part-1.parquet:4892308..6597204, Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:0..4061122], [Users/marko/TMP/tpch-data-sf10/customer/part-10.parquet:4061122..6569978, Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:0..3257162], [Users/marko/TMP/tpch-data-sf10/customer/part-11.parquet:3257162..6568206, Users/marko/TMP/tpch-data-sf10/customer/part-12.parquet:0..2454974], ...]}, projection=[c_custkey], file_type=parquet
//     =========UnResolvedStage[stage_id=4.0, children=1]=========
//     Inputs{3: StageOutput { partition_locations: {}, complete: false }}
//     ShuffleWriterExec: partitions:None
//       CoalescePartitionsExec: fetch=10
//         UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 1 }], 16)
//     =========UnResolvedStage[stage_id=3.0, children=2]=========
//     Inputs{1: StageOutput { partition_locations: {}, complete: false }, 2: StageOutput { partition_locations: {}, complete: false }}
//     ShuffleWriterExec: partitions:None
//       CoalesceBatchesExec: target_batch_size=8192, fetch=10
//         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c_custkey@0, c_custkey@0)]
//           CoalesceBatchesExec: target_batch_size=8192
//             UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 0 }], 16)
//           CoalesceBatchesExec: target_batch_size=8192
//             UnresolvedShuffleExec: partitions=Hash([Column { name: "c_custkey", index: 0 }], 16)
// ```

use ballista::datafusion::{
    common::Result,
    execution::{options::ParquetReadOptions, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        //.parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        //.parse_filters("ballista=debug,ballista_scheduler-rs=debug,ballista_executor=debug,datafusion=debug")
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        //.with_target_partitions(1)
        //.with_ballista_standalone_parallelism(2)
        ;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    ctx.sql("SET datafusion.optimizer.prefer_hash_join = false")
        .await?
        .show()
        .await?;

    //let test_data = test_util::examples_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "t0",
        "/Users/marko/TMP/tpch-data-sf10/customer/",
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet(
        "t1",
        "/Users/marko/TMP/tpch-data-sf10/customer",
        ParquetReadOptions::default(),
    )
    .await?;

    //let df = ctx.sql("select * from t0 limit 1").await?;

    //df.show().await?;

    let df = ctx
        .sql("select t0.c_custkey, t1.c_custkey from t0 join t1 on t0.c_custkey = t1.c_custkey limit 10")
        .await?;

    df.show().await?;
    Ok(())
}
