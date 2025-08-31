use airflow_task_sdk::{
    bases::operator::Operator,
    definitions::{Context, Dag, DagBag, TaskError},
    execution::TaskRuntime,
};
use alloc::{
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::f32;
use embassy_sync::lazy_lock::LazyLock;
use embassy_time::{Duration, Timer};
use log::{error, info, warn};
use serde::Serialize;

use crate::airflow::EmbassyTaskRuntime;

#[derive(Debug, Clone, Serialize)]
pub struct MultipleOutputs {
    pi: f32,
    list: Vec<u32>,
    map: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ExampleOperator {
    sleep_secs: u64,
}

impl ExampleOperator {
    pub fn new(sleep_secs: u64) -> Self {
        Self { sleep_secs }
    }
}

impl<R: TaskRuntime> Operator<R> for ExampleOperator {
    type Output = MultipleOutputs;

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
        info!(
            "I am task instance {} {} {} {} {}",
            ctx.dag_id(),
            ctx.task_id(),
            ctx.run_id(),
            ctx.try_number(),
            ctx.map_index()
        );
        Timer::after(Duration::from_secs(self.sleep_secs)).await;
        warn!("This feels very fast! 😎");
        info!("I am done");
        let mut map = BTreeMap::new();
        map.insert("hello".to_string(), "world".to_string());
        let output = MultipleOutputs {
            pi: f32::consts::PI,
            list: vec![1, 2, 3],
            map,
        };
        Ok(output)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PrintXComOperator {
    task_id: String,
}

impl PrintXComOperator {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
        }
    }
}

impl<R: TaskRuntime> Operator<R> for PrintXComOperator {
    type Output = ();

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
        let ti = ctx.task_instance();
        match ti
            .xcom()
            .task_id(&self.task_id)
            .key("pi")
            .pull::<f32>()
            .await
        {
            Ok(xcom_value) => {
                info!("XCom value: {}", xcom_value);
            }
            Err(e) => {
                error!("Failed to pull XCom value: {}", e);
            }
        };
        Ok(())
    }
}

static DAG_BAG: LazyLock<DagBag<EmbassyTaskRuntime>> = LazyLock::new(|| {
    let task = ExampleOperator::new(5)
        .into_task("run")
        .with_multiple_outputs(true);
    let print_xcom = PrintXComOperator::new("run").into_task("print_xcom");
    let mut dag = Dag::new("example_dag");
    dag.add_task(task);
    dag.add_task(print_xcom);
    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(dag);
    dag_bag
});

pub fn get_dag_bag() -> &'static DagBag<EmbassyTaskRuntime> {
    DAG_BAG.get()
}
