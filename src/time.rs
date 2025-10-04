use core::net::{IpAddr, SocketAddr};

use crate::{CONFIG, OFFSET};
use embassy_net::{
    Stack,
    dns::DnsQueryType,
    udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Instant, Timer, WithTimeout};
use sntpc::NtpResult;
use sntpc::{NtpContext, NtpTimestampGenerator, get_time};
use tracing::{debug, error, info};

#[embassy_executor::task]
pub async fn measure_time(stack: Stack<'static>) {
    let ntp_client = NtpClient::new(stack, CONFIG.ntp.server);
    let offset = OFFSET.sender();

    loop {
        match ntp_client.get_time().await {
            Ok(result) => {
                offset.send(result.offset);
                info!("{result:?}");
                Timer::after(Duration::from_secs(3600)).await;
            }
            Err(e) => {
                error!("Failed to get NTP time: {e:?}");
                Timer::after(Duration::from_secs(5)).await;
            }
        }
    }
}

pub struct NtpClient {
    stack: Stack<'static>,
    hostname: &'static str,
    port: u16,
    timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum NtpClientError {
    #[error("Timeout: {0:?}")]
    Timeout(embassy_time::TimeoutError),
    #[error("DNS: {0:?}")]
    Dns(embassy_net::dns::Error),
    #[error("Empty DNS response")]
    EmptyDns,
    #[error("NTP: {0:?}")]
    Ntp(sntpc::Error),
    #[error("UDP Bind: {0:?}")]
    Bind(embassy_net::udp::BindError),
}

impl NtpClient {
    pub fn new(stack: Stack<'static>, hostname: &'static str) -> Self {
        Self {
            stack,
            hostname,
            port: 123,
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn resolve(&self) -> Result<IpAddr, NtpClientError> {
        let addresses = self
            .stack
            .dns_query(self.hostname, DnsQueryType::A)
            .with_timeout(self.timeout)
            .await
            .map_err(NtpClientError::Timeout)?
            .map_err(NtpClientError::Dns)?;

        if addresses.is_empty() {
            return Err(NtpClientError::EmptyDns);
        }

        let addr: IpAddr = addresses[0].into();
        debug!("NTP server resolved to: {}", addr);
        Ok(addr)
    }

    pub async fn get_time(&self) -> Result<NtpResult, NtpClientError> {
        let addr = SocketAddr::new(self.resolve().await?, self.port);

        let mut rx_meta = [PacketMetadata::EMPTY; 16];
        let mut rx_buffer = [0; 512];
        let mut tx_meta = [PacketMetadata::EMPTY; 16];
        let mut tx_buffer = [0; 512];

        let mut socket = UdpSocket::new(
            self.stack,
            &mut rx_meta,
            &mut rx_buffer,
            &mut tx_meta,
            &mut tx_buffer,
        );
        socket.bind(self.port).map_err(NtpClientError::Bind)?;
        let context = NtpContext::new(Timestamp::default());

        let result = get_time(addr, &socket, context)
            .with_timeout(self.timeout)
            .await
            .map_err(NtpClientError::Timeout)?
            .map_err(NtpClientError::Ntp)?;

        Ok(result)
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
