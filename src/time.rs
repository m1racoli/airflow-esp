use core::net::{IpAddr, SocketAddr};

use embassy_net::{
    udp::{PacketMetadata, UdpSocket},
    Stack,
};
use embassy_time::{Duration, Instant, Timer};
use log::info;
use sntpc::{get_time, NtpContext, NtpResult, NtpTimestampGenerator};

use crate::{Event, EVENTS};

#[embassy_executor::task]
pub async fn measure_time(stack: Stack<'static>) {
    // Create UDP socket
    let mut rx_meta = [PacketMetadata::EMPTY; 16];
    let mut rx_buffer = [0; 4096];
    let mut tx_meta = [PacketMetadata::EMPTY; 16];
    let mut tx_buffer = [0; 4096];

    let mut socket = UdpSocket::new(
        stack,
        &mut rx_meta,
        &mut rx_buffer,
        &mut tx_meta,
        &mut tx_buffer,
    );
    socket.bind(123).unwrap();

    info!("Socket bound!");

    let ntp_address = stack
        .dns_query("pool.ntp.org", smoltcp::wire::DnsQueryType::A)
        .await
        .expect("Failed to resolve DNS");

    if ntp_address.is_empty() {
        panic!("Empty DNS response");
    }

    info!("NTP address: {:?}", ntp_address);

    let addr: IpAddr = ntp_address[0].into();
    let socket_addr = SocketAddr::new(addr, 123);
    let mut first: Option<NtpResult> = None;
    let context = NtpContext::new(Timestamp::default());

    loop {
        match get_time(socket_addr, &socket, context).await {
            Ok(result) => {
                EVENTS.send(Event::Ntp(result)).await;
                info!("NTP result: {:?}", result);
                match first {
                    Some(first) => {
                        let offset_diff = result.offset - first.offset;
                        let ts_diff = result.seconds as i64 - first.seconds as i64;
                        let relative_diff = offset_diff as f64 / (ts_diff * 1_000_000) as f64;
                        info!("Drift: {offset_diff} micros after {ts_diff} seconds ({relative_diff:e} relative)");
                    }
                    None => {
                        first = Some(result);
                    }
                }

                Timer::after(Duration::from_secs(600)).await;
            }
            Err(e) => {
                info!("Failed to get NTP time: {:?}", e);
                Timer::after(Duration::from_secs(5)).await;
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct Timestamp {
    instant: Instant,
}

impl NtpTimestampGenerator for Timestamp {
    fn init(&mut self) {
        self.instant = Instant::now();
    }

    fn timestamp_sec(&self) -> u64 {
        self.instant.as_secs()
    }

    fn timestamp_subsec_micros(&self) -> u32 {
        self.instant.as_micros().wrapping_rem(1_000_000) as u32
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self {
            instant: Instant::now(),
        }
    }
}

// Scenario 1:
// Timestamp is always 0
// offset is relative to 0
// current time = 0 + offset + time since measurement
// problem: offset only applies to time of invocation

// Scenario 2:
// Timestamp is instant now
// offset is relative to instant now
// current time = now + offset
// problem: offset only works if instant does not drift
