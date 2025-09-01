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
use log::{info, warn};
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

        ctx.task_instance().xcom_push("example_key", &42).await?;

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

        let pi: f32 = ti
            .xcom_pull()
            .task_id(&self.task_id)
            .key("pi")
            .one()
            .await?;
        info!("PI is {}", pi);

        // Use Option to handle missing XCom values
        let non_existing: Option<String> = ti
            .xcom_pull()
            .task_id(&self.task_id)
            .key("non_existing")
            .one()
            .await?;
        info!("Non existing is {:?}", non_existing);

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RetryOperator;

impl<R: TaskRuntime> Operator<R> for RetryOperator {
    type Output = ();

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
        if ctx.try_number() < 2 {
            info!("Failing task, attempt {}", ctx.try_number());
            Err(TaskError::Unknown("Failing task".to_string()))
        } else {
            info!("Succeeding task, attempt {}", ctx.try_number());
            Ok(())
        }
    }
}

static DAG_BAG: LazyLock<DagBag<EmbassyTaskRuntime>> = LazyLock::new(|| {
    let task = ExampleOperator::new(5)
        .into_task("run")
        .with_multiple_outputs(true);
    let print_xcom = PrintXComOperator::new("run").into_task("print_xcom");
    let retry = RetryOperator.into_task("retry");

    let mut dag = Dag::new("example_dag");
    dag.add_task(task);
    dag.add_task(print_xcom);
    dag.add_task(retry);

    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(dag);
    dag_bag
});

pub fn get_dag_bag() -> &'static DagBag<EmbassyTaskRuntime> {
    DAG_BAG.get()
}
