#![no_std]

extern crate alloc;

// When you are okay with using a nightly compiler it's better to use https://docs.rs/static_cell/2.1.0/static_cell/macro.make_static.html
#[macro_export]
macro_rules! mk_static {
    ($t:ty,$val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        #[deny(unused_attributes)]
        let x = STATIC_CELL.uninit().write(($val));
        x
    }};
}

use airflow_edge_sdk::worker::WorkerState;
use core::net::Ipv4Addr;
use embassy_net::{dns::DnsSocket, tcp::client::TcpClient};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex, lazy_lock::LazyLock, pubsub::PubSubChannel,
    watch::Watch,
};

use crate::airflow::{
    OffsetWatchTimeProvider, ReqwlessExecutionApiClient, ReqwlessExecutionApiClientFactory,
};

pub mod airflow;
pub mod button;
pub mod display;
pub mod example;
pub mod time;
pub mod tracing;
pub mod wifi;

pub const HTTP_RX_BUF_SIZE: usize = RESOURCES.http.rx_buf_size as usize;
pub const NUM_TCP_CONNECTIONS: usize = RESOURCES.tcp.num_connections as usize;
pub const STACK_NUM_SOCKETS: usize = RESOURCES.stack.num_sockets as usize;
pub const TCP_RX_BUF_SIZE: usize = RESOURCES.tcp.rx_buf_size as usize;
pub const TCP_TIMEOUT: u64 = RESOURCES.tcp.timeout as u64;
pub const TCP_TX_BUF_SIZE: usize = RESOURCES.tcp.tx_buf_size as usize;

pub type EspExecutionApiClientFactory = ReqwlessExecutionApiClientFactory<
    'static,
    TcpClient<'static, NUM_TCP_CONNECTIONS, TCP_TX_BUF_SIZE, TCP_RX_BUF_SIZE>,
    DnsSocket<'static>,
>;
pub type EspExecutionApiClient = ReqwlessExecutionApiClient<
    'static,
    TcpClient<'static, NUM_TCP_CONNECTIONS, TCP_TX_BUF_SIZE, TCP_RX_BUF_SIZE>,
    DnsSocket<'static>,
>;
pub type EspTimeProvider = OffsetWatchTimeProvider<'static, CriticalSectionRawMutex, 1>;

pub static EVENTS: PubSubChannel<CriticalSectionRawMutex, Event, 8, 2, 3> = PubSubChannel::new();
pub static STATE: Watch<CriticalSectionRawMutex, State, 1> = Watch::new();
pub static OFFSET: Watch<CriticalSectionRawMutex, i64, 1> = Watch::new();
pub static TIME_PROVIDER: LazyLock<OffsetWatchTimeProvider<'static, CriticalSectionRawMutex, 1>> =
    LazyLock::new(|| OffsetWatchTimeProvider::new(&OFFSET));

#[cfg(not(feature = "wokwi"))]
pub static HOSTNAME: &str = "airflow-esp";
#[cfg(feature = "wokwi")]
pub static HOSTNAME: &str = "airflow-esp-wokwi";

#[cfg(not(feature = "wokwi"))]
static_toml::static_toml! {
    pub static CONFIG = include_toml!("config.toml");
    const RESOURCES = include_toml!("resources.toml");
}
#[cfg(feature = "wokwi")]
static_toml::static_toml! {
    pub static CONFIG = include_toml!("config.wokwi.toml");
    const RESOURCES = include_toml!("resources.toml");
}

#[derive(Debug, Clone)]
pub enum Event {
    Wifi(WifiStatus),
    Ip(Option<Ipv4Addr>),
    ButtonPressed(button::Button),
    WorkerState(WorkerState),
}

#[derive(Debug, Default, Clone)]
pub struct State {
    pub wifi: WifiStatus,
    pub ip: Option<core::net::Ipv4Addr>,
    pub worker_state: Option<WorkerState>,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum WifiStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected,
}
