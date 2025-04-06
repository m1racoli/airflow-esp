#![no_std]
pub mod display;

#[derive(Debug, Default)]
pub struct State {
    pub connected: bool,
    pub ip: Option<core::net::IpAddr>,
}
