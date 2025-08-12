use core::{convert::Infallible, time};

use airflow_common::{
    executors::ExecuteTask,
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom, LocalRuntime},
};
use embassy_futures::select::{Either, select};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{Channel, Receiver, Sender},
};
use embassy_time::{Duration, Instant, Timer};
use log::{debug, info};

const INTERCOM_BUFFER_SIZE: usize = 4;
static INTERCOM: Channel<CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE> =
    Channel::new();

#[derive(Debug)]
pub struct EmbassyEdgeJob {
    ti_key: TaskInstanceKey,
    instant: Instant,
    // handle: Option<JoinHandle<bool>>,
    // result: bool,
    concurrency_slots: usize,
    abort: bool,
}

impl LocalEdgeJob for EmbassyEdgeJob {
    fn ti_key(&self) -> &TaskInstanceKey {
        &self.ti_key
    }

    fn concurrency_slots(&self) -> usize {
        self.concurrency_slots
    }

    fn is_running(&self) -> bool {
        // TODO
        // match &self.handle {
        //     Some(h) => !h.is_finished(),
        //     None => false,
        // }
        !self.abort && self.instant + Duration::from_secs(15) > Instant::now()
    }

    fn abort(&mut self) {
        // TODO
        // if let Some(h) = self.handle.take() {
        //     h.abort();
        // };
        self.abort = true;
    }

    async fn is_success(&mut self) -> bool {
        // TODO
        // if let Some(h) = self.handle.take() {
        //     self.result = h.await.unwrap_or_default()
        // };
        // self.result
        !self.abort
    }
}

#[derive(Clone)]
pub struct EmbassyIntercom<'intercom>(
    Sender<'intercom, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
);

impl<'intercom> LocalIntercom for EmbassyIntercom<'intercom> {
    type SendError = Infallible;

    async fn send(&self, msg: IntercomMessage) -> Result<(), Self::SendError> {
        self.0.send(msg).await;
        Ok(())
    }
}

pub struct EmbassyRuntime<'intercom> {
    recv: Receiver<'intercom, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
    send: Sender<'intercom, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
}

impl<'intercom> Default for EmbassyRuntime<'intercom> {
    fn default() -> Self {
        EmbassyRuntime {
            recv: INTERCOM.receiver(),
            send: INTERCOM.sender(),
        }
    }
}

impl<'intercom> LocalRuntime for EmbassyRuntime<'intercom> {
    type Job = EmbassyEdgeJob;
    type Intercom = EmbassyIntercom<'intercom>;

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
        async fn _launch<'intercom>(
            task: ExecuteTask,
            intercom: EmbassyIntercom<'intercom>,
        ) -> bool {
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
            intercom.send(IntercomMessage::JobCompleted(key)).await.ok();
            true
        }
        let ti_key = job.ti_key();
        // let intercom = self.send.clone();
        // let spawner = SendSpawner::for_current_executor().await;
        // let handle = tokio::spawn(_launch(job.command, intercom));
        EmbassyEdgeJob {
            ti_key,
            // handle: Some(handle),
            // result: false,
            concurrency_slots: job.concurrency_slots,
            abort: false,
            instant: Instant::now(),
        }
    }
}
