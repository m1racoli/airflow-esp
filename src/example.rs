use airflow_task_sdk::prelude::*;
use alloc::string::{String, ToString};
use core::{f32, time::Duration};
use serde_json::{Value, json};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct ExampleOperator {
    sleep: Duration,
}

impl ExampleOperator {
    pub fn new(sleep_secs: u64) -> Self {
        Self {
            sleep: Duration::from_secs(sleep_secs),
        }
    }
}

impl<R: TaskRuntime> Operator<R> for ExampleOperator {
    type Output = Value;

    fn set_opts(&self, opts: &mut OperatorOpts) {
        opts.multiple_outputs = true;
    }

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
        info!(
            dag_id = %ctx.dag_id(),
            task_id = %ctx.task_id(),
            run_id = %ctx.run_id(),
            try_number = %ctx.try_number(),
            map_index = %ctx.map_index(),
            "I am a task instance!",
        );

        ctx.task_instance().xcom_push("example_key", &42).await?;

        R::sleep(self.sleep).await;

        warn!("This feels very fast! 😎");
        info!("I am done");

        let output = json!({
            "pi": f32::consts::PI,
            "list": [1, 2, 3],
            "map": { "hello": "world" }
        });
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

pub fn get_dag_bag<R: TaskRuntime>() -> DagBag<R> {
    let run = ExampleOperator::new(5).into_task("run");
    let print_xcom = PrintXComOperator::new("run").into_task("print_xcom");
    let retry = RetryOperator.into_task("retry");

    let mut dag = Dag::new("example_dag");
    dag.add_task(run);
    dag.add_task(print_xcom);
    dag.add_task(retry);

    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(dag);
    dag_bag
}
