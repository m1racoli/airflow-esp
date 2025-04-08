#![no_std]

// When you are okay with using a nightly compiler it's better to use https://docs.rs/static_cell/2.1.0/static_cell/macro.make_static.html
macro_rules! mk_static {
    ($t:ty,$val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        #[deny(unused_attributes)]
        let x = STATIC_CELL.uninit().write(($val));
        x
    }};
}

use core::net::Ipv4Addr;
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, channel::Channel, watch::Watch};
pub(crate) use mk_static;

pub mod display;
pub mod wifi;

pub static EVENTS: Channel<CriticalSectionRawMutex, Event, 8> = Channel::new();
pub static STATE: Watch<CriticalSectionRawMutex, State, 1> = Watch::new();

#[cfg(not(feature = "wokwi"))]
static_toml::static_toml! {
    pub const CONFIG = include_toml!("config.toml");
}
#[cfg(feature = "wokwi")]
static_toml::static_toml! {
    pub const CONFIG = include_toml!("wokwi.toml");
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
