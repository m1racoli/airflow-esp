use core::{convert::Infallible, time};

use airflow_common::{
    executors::ExecuteTask,
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom, LocalRuntime},
};
use embassy_executor::Spawner;
use embassy_futures::select::{Either, select};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{Channel, Receiver, Sender},
    signal::Signal,
};
use embassy_time::{Duration, Timer};
use log::{debug, info};

const INTERCOM_BUFFER_SIZE: usize = 4;
static INTERCOM: Channel<CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE> =
    Channel::new();
static LAUNCH_RESULT: Signal<CriticalSectionRawMutex, bool> = Signal::new();

pub struct EmbassyEdgeJob {
    ti_key: TaskInstanceKey,
    concurrency_slots: usize,
    abort: bool,
    signal: &'static Signal<CriticalSectionRawMutex, bool>,
}

impl LocalEdgeJob for EmbassyEdgeJob {
    fn ti_key(&self) -> &TaskInstanceKey {
        &self.ti_key
    }

    fn concurrency_slots(&self) -> usize {
        self.concurrency_slots
    }

    fn is_running(&self) -> bool {
        !self.abort && !self.signal.signaled()
    }

    fn abort(&mut self) {
        // TODO how to actually abort the embassy task?
        // maybe we need select the future with another signal from the job
        self.abort = true;
    }

    async fn is_success(&mut self) -> bool {
        !self.abort && self.signal.wait().await
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
    signal: &'static Signal<CriticalSectionRawMutex, bool>,
) {
    let key = task.ti().ti_key();
    info!("Worker task launched for {key}");

    // let client = match StdExecutionApiClient::new(EXECUTION_API_SERVER_URL, task.token()) {
    //     Ok(client) => client,
    //     Err(e) => {
    //         error!("Failed to create execution API client: {}", e);
    //         intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
    //         return false;
    //     }
    // };

    // TODO: dag bag should coming from self or so
    // let dag_bag = ExampleDagBag {};
    // supervise::<ExampleDagBag>(task, client, dag_bag).await;
    Timer::after(Duration::from_secs(9)).await; // Simulate task execution delay

    intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
    signal.signal(true);
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

impl LocalRuntime for EmbassyRuntime {
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

    fn launch(&self, job: EdgeJobFetched) -> Self::Job {
        let ti_key = job.ti_key();
        let intercom = self.intercom();

        LAUNCH_RESULT.reset();
        self.spawner
            .spawn(launch(job.command, intercom, &LAUNCH_RESULT))
            .ok();

        EmbassyEdgeJob {
            ti_key,
            signal: &LAUNCH_RESULT,
            concurrency_slots: job.concurrency_slots,
            abort: false,
        }
    }

    fn concurrency(&self) -> usize {
        1
    }
}
