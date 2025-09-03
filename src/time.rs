use core::net::{IpAddr, SocketAddr};

use crate::{CONFIG, OFFSET};
use embassy_net::{
    Stack,
    dns::DnsQueryType,
    udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Instant, Timer};
#[cfg(feature = "stats")]
use sntpc::NtpResult;
use sntpc::{NtpContext, NtpTimestampGenerator, get_time};
use tracing::info;

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

    // TODO: query DNS on every loop iteration
    let ntp_address = stack
        .dns_query(CONFIG.ntp.server, DnsQueryType::A)
        .await
        .expect("Failed to resolve DNS");

    if ntp_address.is_empty() {
        panic!("Empty DNS response");
    }

    let addr: IpAddr = ntp_address[0].into();
    info!("NTP server resolved to: {}", addr);

    let socket_addr = SocketAddr::new(addr, 123);
    #[cfg(feature = "stats")]
    let mut first: Option<NtpResult> = None;
    let context = NtpContext::new(Timestamp::default());
    let offset = OFFSET.sender();

    // the first request has some delay (around 500 ms)
    // warming up the socket
    if let Ok(result) = get_time(socket_addr, &socket, context).await {
        info!("{result:?}");
    }

    loop {
        match get_time(socket_addr, &socket, context).await {
            Ok(result) => {
                offset.send(result.offset);
                info!("{result:?}");
                #[cfg(feature = "stats")]
                match first {
                    Some(first) => {
                        let offset_diff = result.offset - first.offset;
                        let ts_diff = result.seconds as i64 - first.seconds as i64;
                        let relative_diff = offset_diff as f64 / (ts_diff * 1_000_000) as f64;
                        info!(
                            "Drift: {offset_diff} micros after {ts_diff} seconds ({relative_diff:e} relative)"
                        );
                    }
                    None => {
                        first = Some(result);
                    }
                }

                Timer::after(Duration::from_secs(3600)).await;
            }
            Err(e) => {
                info!("Failed to get NTP time: {e:?}");
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
