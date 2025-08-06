#![no_std]

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

use core::net::Ipv4Addr;
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex, channel::Channel, lazy_lock::LazyLock,
    watch::Watch,
};

use crate::airflow::OffsetTimeProvider;

pub mod airflow;
pub mod display;
pub mod time;
pub mod wifi;

pub static EVENTS: Channel<CriticalSectionRawMutex, Event, 8> = Channel::new();
pub static STATE: Watch<CriticalSectionRawMutex, State, 1> = Watch::new();
pub static OFFSET: Watch<CriticalSectionRawMutex, i64, 1> = Watch::new();
pub static TIME_PROVIDER: LazyLock<OffsetTimeProvider<'static, CriticalSectionRawMutex, 1>> =
    LazyLock::new(|| OffsetTimeProvider::new(&OFFSET));

static_toml::static_toml! {
    pub const CONFIG = include_toml!("config.toml");
}

#[derive(Debug, Clone, Copy)]
pub enum Event {
    Connection(bool),
    Ip(Option<Ipv4Addr>),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct State {
    pub connected: bool,
    pub ip: Option<core::net::Ipv4Addr>,
}
