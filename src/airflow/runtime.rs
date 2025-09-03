use core::{convert::Infallible, time};

use airflow_common::{
    executors::ExecuteTask,
    models::{TaskInstanceKey, TaskInstanceLike},
};
use airflow_edge_sdk::{
    api::EdgeJobFetched,
    worker::{IntercomMessage, LocalEdgeJob, LocalIntercom, LocalWorkerRuntime, WorkerState},
};
use airflow_task_sdk::{
    definitions::DagBag,
    execution::{
        ExecutionError, ServiceResult, StartupDetails, SupervisorComms, SupervisorCommsError,
        TaskHandle, TaskRunner, TaskRuntime, ToSupervisor, ToTask, supervise,
    },
};
use embassy_executor::Spawner;
use embassy_futures::select::{Either, Either3, select, select3};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{Channel, Receiver, Sender},
    pubsub::PubSubBehavior,
    signal::Signal,
};
use embassy_time::{Duration, Instant, Timer};
use tracing::{debug, info};

use crate::{CONFIG, EVENTS, EspExecutionApiClientFactory, EspTimeProvider, Event, TIME_PROVIDER};

const INTERCOM_BUFFER_SIZE: usize = 8;
static INTERCOM: Channel<CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE> =
    Channel::new();
static RESULT_SIGNAL: Signal<CriticalSectionRawMutex, bool> = Signal::new();
static ABORT_SIGNAL: Signal<CriticalSectionRawMutex, ()> = Signal::new();
static TASK_RESULT_SIGNAL: Signal<CriticalSectionRawMutex, Result<(), ExecutionError>> =
    Signal::new();
static TASK_ABORT_SIGNAL: Signal<CriticalSectionRawMutex, ()> = Signal::new();
static TO_SUPERVISOR: Signal<CriticalSectionRawMutex, ToSupervisor> = Signal::new();
static TO_TASK: Signal<CriticalSectionRawMutex, Result<ToTask, SupervisorCommsError>> =
    Signal::new();

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
#[allow(clippy::too_many_arguments)]
async fn launch(
    task: ExecuteTask,
    intercom: EmbassyIntercom,
    dag_bag: &'static DagBag<EmbassyTaskRuntime>,
    client_factory: EspExecutionApiClientFactory,
    runtime: EmbassyTaskRuntime,
    result_signal: &'static Signal<CriticalSectionRawMutex, bool>,
    abort_signal: &'static Signal<CriticalSectionRawMutex, ()>,
) {
    let closure = async {
        let key = task.ti().ti_key();
        info!("Worker task launched for {}", key);

        supervise(
            task,
            client_factory,
            dag_bag,
            &runtime,
            CONFIG.airflow.core.execution_api_server_url, // TODO this should be an argument?
        )
        .await;

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

#[embassy_executor::task(pool_size = 1)]
async fn task_runner(
    details: StartupDetails,
    dag_bag: &'static DagBag<EmbassyTaskRuntime>,
    comms: EmbassySupervisorComms,
    time_provider: EspTimeProvider,
    result_signal: &'static Signal<CriticalSectionRawMutex, Result<(), ExecutionError>>,
    abort_signal: &'static Signal<CriticalSectionRawMutex, ()>,
) {
    let closure = async {
        let task_runner: TaskRunner<EmbassyTaskRuntime> = TaskRunner::new(comms, time_provider);
        task_runner.main(details, dag_bag).await
    };

    match select(closure, abort_signal.wait()).await {
        Either::First(result) => {
            result_signal.signal(result);
        }
        Either::Second(_) => {
            // TODO signal to supervised activity task
        }
    }
}

pub struct EmbassyRuntime<'r> {
    spawner: Spawner,
    recv: Receiver<'static, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
    send: Sender<'static, CriticalSectionRawMutex, IntercomMessage, INTERCOM_BUFFER_SIZE>,
    hostname: &'r str,
    client_factory: EspExecutionApiClientFactory,
}

impl<'r> EmbassyRuntime<'r> {
    pub fn init(
        spawner: Spawner,
        hostname: &'r str,
        client_factory: EspExecutionApiClientFactory,
    ) -> Self {
        EmbassyRuntime {
            spawner,
            recv: INTERCOM.receiver(),
            send: INTERCOM.sender(),
            hostname,
            client_factory,
        }
    }
}

impl LocalWorkerRuntime for EmbassyRuntime<'static> {
    type Job = EmbassyEdgeJob;
    type Intercom = EmbassyIntercom;
    type TaskRuntime = EmbassyTaskRuntime;

    async fn sleep(&mut self, duration: time::Duration) -> Option<IntercomMessage> {
        debug!("Sleeping for {} seconds", duration.as_secs());
        let duration = Duration::from_micros(duration.as_micros() as u64);
        match select(self.recv.receive(), Timer::after(duration)).await {
            Either::First(v) => Some(v),
            Either::Second(_) => None,
        }
    }

    fn intercom(&self) -> Self::Intercom {
        EmbassyIntercom(self.send)
    }

    fn launch(
        &self,
        job: EdgeJobFetched,
        dag_bag: &'static DagBag<EmbassyTaskRuntime>,
    ) -> Self::Job {
        let ti_key = job.ti_key();
        let intercom = self.intercom();

        let runtime = EmbassyTaskRuntime {
            spawner: self.spawner,
            hostname: self.hostname,
        };

        RESULT_SIGNAL.reset();
        ABORT_SIGNAL.reset();
        self.spawner
            .spawn(launch(
                job.command,
                intercom,
                dag_bag,
                self.client_factory.clone(),
                runtime,
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

    fn hostname(&self) -> &str {
        self.hostname
    }
}

pub struct EmbassyTaskHandle {
    result_signal: &'static Signal<CriticalSectionRawMutex, Result<(), ExecutionError>>,
    abort_signal: &'static Signal<CriticalSectionRawMutex, ()>,
    to_supervisor: &'static Signal<CriticalSectionRawMutex, ToSupervisor>,
    to_task: &'static Signal<CriticalSectionRawMutex, Result<ToTask, SupervisorCommsError>>,
}

impl TaskHandle for EmbassyTaskHandle {
    fn abort(&self) {
        self.abort_signal.signal(());
    }

    async fn service(&mut self, timeout: time::Duration) -> ServiceResult {
        debug!("Sleeping for {} seconds", timeout.as_secs());
        let timeout = Duration::from_micros(timeout.as_micros() as u64);
        match select3(
            Timer::after(timeout),
            self.result_signal.wait(),
            self.to_supervisor.wait(),
        )
        .await
        {
            Either3::First(_) => ServiceResult::None,
            Either3::Second(r) => ServiceResult::Terminated(r),
            Either3::Third(r) => ServiceResult::Comms(r),
        }
    }

    async fn respond(&mut self, msg: Result<ToTask, SupervisorCommsError>) {
        self.to_task.signal(msg);
    }
}

pub struct EmbassyTaskRuntime {
    spawner: Spawner,
    hostname: &'static str,
}

impl TaskRuntime for EmbassyTaskRuntime {
    type TaskHandle = EmbassyTaskHandle;
    type Instant = Instant;
    type TimeProvider = EspTimeProvider;
    type Comms = EmbassySupervisorComms;

    fn now(&self) -> Self::Instant {
        Instant::now()
    }

    fn elapsed(&self, start: Self::Instant) -> time::Duration {
        Instant::now().duration_since(start).into()
    }

    fn hostname(&self) -> &str {
        self.hostname
    }

    fn unixname(&self) -> &str {
        "airflow"
    }

    fn pid(&self) -> u32 {
        0
    }

    fn start(&self, details: StartupDetails, dag_bag: &'static DagBag<Self>) -> Self::TaskHandle {
        TASK_RESULT_SIGNAL.reset();
        TASK_ABORT_SIGNAL.reset();
        TO_SUPERVISOR.reset();
        TO_TASK.reset();
        let comms = EmbassySupervisorComms {
            send: &TO_SUPERVISOR,
            recv: &TO_TASK,
        };
        self.spawner
            .spawn(task_runner(
                details,
                dag_bag,
                comms,
                self.time_provider().clone(),
                &TASK_RESULT_SIGNAL,
                &TASK_ABORT_SIGNAL,
            ))
            .ok();
        EmbassyTaskHandle {
            result_signal: &TASK_RESULT_SIGNAL,
            abort_signal: &TASK_ABORT_SIGNAL,
            to_supervisor: &TO_SUPERVISOR,
            to_task: &TO_TASK,
        }
    }

    fn time_provider(&self) -> &Self::TimeProvider {
        TIME_PROVIDER.get()
    }

    fn sleep(duration: time::Duration) -> impl Future<Output = ()> + Send + Sync {
        let duration = Duration::from_micros(duration.as_micros() as u64);
        Timer::after(duration)
    }
}

pub struct EmbassySupervisorComms {
    send: &'static Signal<CriticalSectionRawMutex, ToSupervisor>,
    recv: &'static Signal<CriticalSectionRawMutex, Result<ToTask, SupervisorCommsError>>,
}

impl SupervisorComms for EmbassySupervisorComms {
    async fn send(&self, msg: ToSupervisor) -> Result<ToTask, SupervisorCommsError> {
        self.send.signal(msg);
        self.recv.wait().await
    }
}
