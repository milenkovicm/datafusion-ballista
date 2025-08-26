use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_scheduler::display::DisplayableBallistaExecutionPlan;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{collect, displayable, DisplayAs};
use datafusion::prelude::*;
use std::error::Error;
use std::fs;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .try_init();
    // create temp dir to store csv table.
    let temp_dir = "/Users/marko/TMP/t/";
    //fs::create_dir_all(temp_dir)?;

    // generate csv demo data.
    create_date_dim_csv(temp_dir)?;
    create_store_csv(temp_dir)?;
    create_item_csv(temp_dir)?;
    create_store_sales_csv(temp_dir)?;

    // let config = SessionConfig::new_with_ballista()
    //     .with_target_partitions(2)
    //     .with_ballista_standalone_parallelism(2);

    // let state = SessionStateBuilder::new()
    //     .with_config(config)
    //     .with_default_features()
    //     .build();

    // let ctx = SessionContext::standalone_with_state(state).await?;

    // let ctx = SessionContext::standalone().await?;
    let ctx = SessionContext::new();
    // let ctx = SessionContext::remote("df://localhost:50050").await?;

    // Just register a few table which will be used.
    ctx.register_csv(
        "date_dim",
        &format!("{}/date_dim.csv", temp_dir),
        CsvReadOptions::new().has_header(true),
    )
    .await?;

    ctx.register_csv(
        "store",
        &format!("{}/store.csv", temp_dir),
        CsvReadOptions::new().has_header(true),
    )
    .await?;

    ctx.register_csv(
        "item",
        &format!("{}/item.csv", temp_dir),
        CsvReadOptions::new().has_header(true),
    )
    .await?;

    ctx.register_csv(
        "store_sales",
        &format!("{}/store_sales.csv", temp_dir),
        CsvReadOptions::new().has_header(true),
    )
    .await?;

    let query = r#"
with v1 as(
    select i_category,
           i_brand,
           s_store_name,
           s_company_name,
           d_year,
           d_moy,
           sum(ss_sales_price) sum_sales,
           avg(sum(ss_sales_price)) over
        (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
            rank() over
        (partition by i_category, i_brand,
                   s_store_name, s_company_name
         order by d_year, d_moy) rn
    from item, store_sales, date_dim, store
    where ss_item_sk = i_item_sk and
            ss_sold_date_sk = d_date_sk
      and ss_store_sk = s_store_sk and
        (
                    d_year = 1999 or
                    ( d_year = 1999-1 and d_moy =12) or
                    ( d_year = 1999+1 and d_moy =1)
            )
    group by i_category, i_brand,
             s_store_name, s_company_name,
             d_year, d_moy),
     v2 as(
         select v1.i_category,
                v1.i_brand,
                v1.s_store_name,
                v1.s_company_name,
                v1.d_year,
                v1.d_moy,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales psum,
                v1_lead.sum_sales nsum
         from v1, v1 v1_lag, v1 v1_lead
         where v1.i_category = v1_lag.i_category and
                 v1.i_category = v1_lead.i_category and
                 v1.i_brand = v1_lag.i_brand and
                 v1.i_brand = v1_lead.i_brand and
                 v1.s_store_name = v1_lag.s_store_name and
                 v1.s_store_name = v1_lead.s_store_name and
                 v1.s_company_name = v1_lag.s_company_name and
                 v1.s_company_name = v1_lead.s_company_name and
                 v1.rn = v1_lag.rn + 1 and
                 v1.rn = v1_lead.rn - 1)
select  *
from v2
where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
    limit 100;
    "#;

    //ctx.sql(query).await?.show().await?;
    let df = ctx.sql(query).await?;
    let plan = df.create_physical_plan().await?;

    // println!("{}", displayable(plan.as_ref()).indent(false));

    let mut planner = ballista_scheduler::planner::DefaultDistributedPlanner::new();
    let job_uuid = "abcds";
    let stages = ballista_scheduler::planner::DistributedPlanner::plan_query_stages(
        &mut planner,
        &job_uuid.to_string(),
        plan,
        ctx.state().config(),
    )?;
    let es = EnforceSorting::default();
    println!("==========================================================================================================");
    for stage in stages {
        println!(
            "------------------------------------------------------------------------"
        );
        let orig = displayable(stage.as_ref()).indent(false).to_string();
        println!("{}", orig);

        let p = es.optimize(stage, ctx.state().config().options())?;
        println!("***");
        let new = displayable(p.as_ref()).indent(false).to_string();
        println!("{}", new);

        if new != orig {
            println!("========> DIFF")
        }
    }

    // let plan = ctx.sql(query).await?.create_physical_plan().await?;
    // let bytes = datafusion_proto::bytes::physical_plan_to_bytes(plan.clone())?;
    // let physical_round_trip =
    //     datafusion_proto::bytes::physical_plan_from_bytes(&bytes, &ctx)?;

    // collect(physical_round_trip, ctx.task_ctx()).await?;

    Ok(())
}

// demo file create: date_dim.csv
fn create_date_dim_csv(dir: &str) -> Result<(), Box<dyn Error>> {
    let content = r#"d_date_sk,d_date_id,d_date,d_year,d_moy,d_month_name
1,19981201,1998-12-01,1998,12,December
2,19981215,1998-12-15,1998,12,December
3,19990105,1999-01-05,1999,1,January
4,19990210,1999-02-10,1999,2,February
5,19990315,1999-03-15,1999,3,March
6,19990420,1999-04-20,1999,4,April
7,19990525,1999-05-25,1999,5,May
8,19990630,1999-06-30,1999,6,June
9,20000105,2000-01-05,2000,1,January
10,20000120,2000-01-20,2000,1,January
"#;
    let path = Path::new(dir).join("date_dim.csv");
    fs::write(path, content)?;
    Ok(())
}

// demo file create: store.csv
fn create_store_csv(dir: &str) -> Result<(), Box<dyn Error>> {
    let content = r#"s_store_sk,s_store_id,s_store_name,s_company_name,s_city,s_state
10,ST001,Downtown Store,Retail Corp,New York,NY
11,ST002,Mall Store,Retail Corp,Los Angeles,CA
12,ST003,Uptown Market,Market Group,Chicago,IL
"#;
    let path = Path::new(dir).join("store.csv");
    fs::write(path, content)?;
    Ok(())
}

// demo file create: item.csv
fn create_item_csv(dir: &str) -> Result<(), Box<dyn Error>> {
    let content = r#"i_item_sk,i_item_id,i_item_name,i_category,i_brand,i_price
20,ITM001,Laptop Pro,Electronics,TechBrand,999.99
21,ITM002,Wireless Headphones,Electronics,SoundTech,199.99
22,ITM003,Cotton T-Shirt,Clothing,FashionNow,19.99
23,ITM004,Running Shoes,Footwear,SpeedWay,89.99
24,ITM005,Coffee Maker,Appliances,HomeGoods,79.99
"#;
    let path = Path::new(dir).join("item.csv");
    fs::write(path, content)?;
    Ok(())
}

// demo file create: store_sales.csv
fn create_store_sales_csv(dir: &str) -> Result<(), Box<dyn Error>> {
    let content = r#"ss_item_sk,ss_sold_date_sk,ss_store_sk,ss_quantity,ss_sales_price
20,1,10,2,999.99
20,2,10,3,999.99
22,2,11,10,19.99
20,3,10,4,999.99
20,3,10,1,999.99
22,3,11,8,19.99
20,4,10,9,999.99
20,4,10,1,999.99
22,4,11,15,19.99
20,5,10,5,999.99
22,5,11,9,19.99
20,6,10,4,999.99
22,6,11,8,19.99
20,9,10,3,999.99
22,9,11,10,19.99
"#;
    let path = Path::new(dir).join("store_sales.csv");
    fs::write(path, content)?;
    Ok(())
}
