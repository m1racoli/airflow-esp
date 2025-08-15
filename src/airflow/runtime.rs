use core::{convert::Infallible, time};

use airflow_common::{
    executors::ExecuteTask,
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom, LocalRuntime, WorkerState},
};
use airflow_task_sdk::definitions::DagBag;
use embassy_executor::Spawner;
use embassy_futures::select::{Either, select};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{Channel, Receiver, Sender},
    pubsub::PubSubBehavior,
    signal::Signal,
};
use embassy_time::{Duration, Timer};
use log::{debug, info};

use crate::{EVENTS, Event};

const INTERCOM_BUFFER_SIZE: usize = 8;
static INTERCOM: Channel<CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE> =
    Channel::new();
static RESULT_SIGNAL: Signal<CriticalSectionRawMutex, bool> = Signal::new();
static ABORT_SIGNAL: Signal<CriticalSectionRawMutex, ()> = Signal::new();

pub struct EmbassyEdgeJob {
    ti_key: TaskInstanceKey,
    concurrency_slots: usize,
    result_signal: &'static Signal<CriticalSectionRawMutex, bool>,
    abort_signal: &'static Signal<CriticalSectionRawMutex, ()>,
    result: Option<bool>,
}

impl LocalEdgeJob for EmbassyEdgeJob {
    fn ti_key(&self) -> &TaskInstanceKey {
        &self.ti_key
    }

    fn concurrency_slots(&self) -> usize {
        self.concurrency_slots
    }

    fn is_running(&self) -> bool {
        match self.result {
            Some(_) => false,
            None => !self.abort_signal.signaled() && !self.result_signal.signaled(),
        }
    }

    fn abort(&mut self) {
        self.abort_signal.signal(());
    }

    async fn is_success(&mut self) -> bool {
        match self.result {
            Some(r) => r,
            None => {
                let r = !self.abort_signal.signaled() && self.result_signal.wait().await;
                self.result = Some(r);
                r
            }
        }
    }
}

#[derive(Clone)]
pub struct EmbassyIntercom(
    Sender<'static, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
);

impl LocalIntercom for EmbassyIntercom {
    type SendError = Infallible;

    async fn send(&self, msg: IntercomMessage) -> Result<(), Self::SendError> {
        self.0.send(msg).await;
        Ok(())
    }
}

#[embassy_executor::task(pool_size = 1)]
async fn launch(
    task: ExecuteTask,
    intercom: EmbassyIntercom,
    dag_bag: &'static DagBag,
    result_signal: &'static Signal<CriticalSectionRawMutex, bool>,
    abort_signal: &'static Signal<CriticalSectionRawMutex, ()>,
) {
    let closure = async {
        let key = task.ti().ti_key();
        info!("Worker task launched for {}", key);

        // let client = match StdExecutionApiClient::new(EXECUTION_API_SERVER_URL, task.token()) {
        //     Ok(client) => client,
        //     Err(e) => {
        //         error!("Failed to create execution API client: {}", e);
        //         intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
        //         return false;
        //     }
        // };
        // TODO use supervisor
        let _ = dag_bag;
        // supervise(task, client, dag_bag).await;
        Timer::after(Duration::from_secs(30)).await; // Simulate task execution delay

        intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
        true
    };

    match select(closure, abort_signal.wait()).await {
        Either::First(success) => {
            result_signal.signal(success);
        }
        Either::Second(_) => {
            // TODO signal to supervised activity task
        }
    }
}

pub struct EmbassyRuntime {
    spawner: Spawner,
    recv: Receiver<'static, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
    send: Sender<'static, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
}

impl EmbassyRuntime {
    pub fn init(spawner: Spawner) -> Self {
        EmbassyRuntime {
            spawner,
            recv: INTERCOM.receiver(),
            send: INTERCOM.sender(),
        }
    }
}

impl LocalRuntime<'static> for EmbassyRuntime {
    type Job = EmbassyEdgeJob;
    type Intercom = EmbassyIntercom;

    async fn sleep(&mut self, duration: time::Duration) -> Option<IntercomMessage> {
        debug!("Sleeping for {} seconds", duration.as_secs());
        match select(
            self.recv.receive(),
            Timer::after(Duration::from_secs(duration.as_secs())),
        )
        .await
        {
            Either::First(v) => Some(v),
            Either::Second(_) => None,
        }
    }

    fn intercom(&self) -> Self::Intercom {
        EmbassyIntercom(self.send)
    }

    fn launch(&self, job: EdgeJobFetched, dag_bag: &'static DagBag) -> Self::Job {
        let ti_key = job.ti_key();
        let intercom = self.intercom();

        RESULT_SIGNAL.reset();
        ABORT_SIGNAL.reset();
        self.spawner
            .spawn(launch(
                job.command,
                intercom,
                dag_bag,
                &RESULT_SIGNAL,
                &ABORT_SIGNAL,
            ))
            .ok();

        EmbassyEdgeJob {
            ti_key,
            result_signal: &RESULT_SIGNAL,
            abort_signal: &ABORT_SIGNAL,
            concurrency_slots: job.concurrency_slots,
            result: None,
        }
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn on_update(&mut self, state: &WorkerState) {
        debug!("Worker state updated: {:?}", state);
        EVENTS.publish_immediate(Event::WorkerState(state.clone()));
    }
}
