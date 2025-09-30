use airflow_task_sdk::prelude::*;
use tracing::info;

use crate::{
    button::{Button, next_button_pressed},
    led::{Led, set_led},
};

#[derive(Debug, Clone, Default)]
pub struct ButtonSensor;

impl<R: TaskRuntime> Operator<R> for ButtonSensor {
    // Button implements serde::Serialize and serde::Deserialize
    type Output = Button;

    async fn execute<'t>(
        &'t mut self,
        _ctx: &'t Context<'t, R>,
    ) -> Result<Self::Output, TaskError> {
        info!("Waiting for button press...");
        let button = next_button_pressed().await;
        info!("Button {:?} pressed", button);
        Ok(button)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LedOperator;

impl<R: TaskRuntime> Operator<R> for LedOperator {
    type Output = ();

    async fn execute<'t>(&'t mut self, ctx: &'t Context<'t, R>) -> Result<Self::Output, TaskError> {
        let button: Button = ctx
            .task_instance()
            .xcom_pull()
            .task_id("wait_button")
            .one()
            .await?;

        info!("Got button {:?} from upstream task", button);

        let led = match button {
            Button::Green => Led::Green,
            Button::Yellow => Led::Yellow,
            Button::Red => Led::Red,
            Button::Boot => Led::None,
        };

        info!("Setting LED to {:?}", led);
        set_led(led);

        Ok(())
    }
}

pub fn get_dag_bag<R: TaskRuntime>() -> DagBag<R> {
    let wait_button = ButtonSensor.into_task("wait_button");
    let set_led = LedOperator.into_task("set_led");

    let mut airflow_summit_demo = Dag::new("airflow_summit_demo");
    airflow_summit_demo.add_task(wait_button);
    airflow_summit_demo.add_task(set_led);

    let mut dag_bag = DagBag::default();
    dag_bag.add_dag(airflow_summit_demo);
    dag_bag
}
