use core::error::Error;
use core::fmt::Debug;

use airflow_common::api::JWTGenerator;
use airflow_common::datetime::UtcDateTime;
use airflow_edge_sdk::api::{EdgeApiError, HealthReturn, LocalEdgeApiClient};
use airflow_edge_sdk::models::{EdgeWorkerState, SysInfo};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use embedded_nal_async::{Dns, TcpConnect};
use reqwless::client::HttpClient;
use reqwless::headers::ContentType;
use reqwless::request::{Method, RequestBody, RequestBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::HTTP_RX_BUF_SIZE;

#[derive(thiserror::Error, Debug)]
pub enum ReqwlessEdgeApiError<J: Error> {
    #[error("{0:?}")]
    Reqwless(reqwless::Error),
    #[error(transparent)]
    JWT(J),
    #[error(transparent)]
    Serde(serde_json::Error),
    #[error("Http error {0}: {1}")]
    Http(u16, String),
}

pub struct ReqwlessEdgeApiClient<'a, J: JWTGenerator, T: TcpConnect + 'a, D: Dns + 'a> {
    client: HttpClient<'a, T, D>,
    base_url: String,
    jwt_generator: J,
    tcp: &'a T,
    dns: &'a D,
}

impl<'a, J: JWTGenerator, T: TcpConnect + 'a, D: Dns + 'a> ReqwlessEdgeApiClient<'a, J, T, D> {
    pub fn new(tcp: &'a T, dns: &'a D, base_url: &str, jwt_generator: J) -> Self {
        let client = HttpClient::new(tcp, dns);

        Self {
            client,
            base_url: base_url.into(),
            jwt_generator,
            tcp,
            dns,
        }
    }

    fn token(&self, path: &str) -> Result<String, ReqwlessEdgeApiError<J::Error>> {
        self.jwt_generator
            .generate(path)
            .map_err(ReqwlessEdgeApiError::JWT)
    }

    async fn request<'buf, B: RequestBody>(
        &mut self,
        rx_buf: &'buf mut [u8],
        method: Method,
        path: &str,
        body: Option<B>,
    ) -> Result<&'buf [u8], EdgeApiError<ReqwlessEdgeApiError<J::Error>>> {
        let url = format!("{}/{}", self.base_url, path);
        let token = self.token(path)?;
        let headers = [("accept", "application/json"), ("authorization", &token)];

        let mut handle = self
            .client
            .request(method, &url)
            .await
            .map_err(ReqwlessEdgeApiError::Reqwless)?
            .headers(&headers);

        if body.is_some() {
            handle = handle.content_type(ContentType::ApplicationJson);
        }

        let mut handle = handle.body(body);

        let response = handle
            .send(rx_buf)
            .await
            .map_err(ReqwlessEdgeApiError::Reqwless)?;

        if !response.status.is_successful() {
            let code = response.status.0;
            let msg = match response.body().read_to_end().await {
                Ok(v) => core::str::from_utf8(v).unwrap_or("").to_string(),
                Err(e) => Err(ReqwlessEdgeApiError::Reqwless(e))?,
            };
            return match code {
                404 => Err(EdgeApiError::EdgeNotEnabled)?,
                400 => Err(EdgeApiError::VersionMismatch(msg))?,
                code => Err(ReqwlessEdgeApiError::Http(code, msg))?,
            };
        }

        let body = response
            .body()
            .read_to_end()
            .await
            .map_err(ReqwlessEdgeApiError::Reqwless)?;

        Ok(body)
    }

    fn serialize<B: Serialize>(
        data: &B,
    ) -> Result<Vec<u8>, EdgeApiError<ReqwlessEdgeApiError<J::Error>>> {
        Ok(serde_json::to_vec(data).map_err(ReqwlessEdgeApiError::Serde)?)
    }

    fn deserialize<B: DeserializeOwned>(
        data: &[u8],
    ) -> Result<B, EdgeApiError<ReqwlessEdgeApiError<J::Error>>> {
        Ok(serde_json::from_slice(data).map_err(ReqwlessEdgeApiError::Serde)?)
    }
}

impl<'a, J: JWTGenerator, T: TcpConnect + 'a, D: Dns + 'a> LocalEdgeApiClient
    for ReqwlessEdgeApiClient<'a, J, T, D>
{
    type Error = ReqwlessEdgeApiError<J::Error>;

    async fn health(&mut self) -> Result<HealthReturn, EdgeApiError<Self::Error>> {
        let path = "health";
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<()>(&mut rx_buf, Method::GET, path, None)
            .await?;
        Self::deserialize(response_body)
    }

    async fn worker_register(
        &mut self,
        hostname: &str,
        state: airflow_edge_sdk::models::EdgeWorkerState,
        queues: Option<&alloc::vec::Vec<String>>,
        sysinfo: &airflow_edge_sdk::models::SysInfo,
    ) -> Result<airflow_edge_sdk::api::WorkerRegistrationReturn, EdgeApiError<Self::Error>> {
        let path = format!("worker/{hostname}");
        let body = WorkerStateBody {
            state,
            jobs_active: 0,
            queues,
            sysinfo,
            maintenance_comments: None,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<&[u8]>(&mut rx_buf, Method::POST, &path, Some(&body))
            .await?;
        Self::deserialize(response_body)
    }

    async fn worker_set_state(
        &mut self,
        hostname: &str,
        state: airflow_edge_sdk::models::EdgeWorkerState,
        jobs_active: usize,
        queues: Option<&alloc::vec::Vec<String>>,
        sysinfo: &airflow_edge_sdk::models::SysInfo,
        maintenance_comments: Option<&str>,
    ) -> Result<airflow_edge_sdk::api::WorkerSetStateReturn, EdgeApiError<Self::Error>> {
        let path = format!("worker/{hostname}");
        let body = WorkerStateBody {
            state,
            jobs_active,
            queues,
            sysinfo,
            maintenance_comments,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<&[u8]>(&mut rx_buf, Method::PATCH, &path, Some(&body))
            .await?;
        Self::deserialize(response_body)
    }

    async fn jobs_fetch(
        &mut self,
        hostname: &str,
        queues: Option<&alloc::vec::Vec<String>>,
        free_concurrency: usize,
    ) -> Result<Option<airflow_edge_sdk::api::EdgeJobFetched>, EdgeApiError<Self::Error>> {
        let path = format!("jobs/fetch/{hostname}");
        let body = WorkerQueuesBody {
            queues,
            free_concurrency,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<&[u8]>(&mut rx_buf, Method::POST, &path, Some(&body))
            .await?;
        Self::deserialize(response_body)
    }

    async fn jobs_set_state(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
        state: airflow_common::utils::TaskInstanceState,
    ) -> Result<(), EdgeApiError<Self::Error>> {
        let path = format!(
            "jobs/state/{}/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
            state
        );
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<()>(&mut rx_buf, Method::PATCH, &path, None)
            .await?;
        Ok(())
    }

    async fn logs_logfile_path(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
    ) -> Result<String, EdgeApiError<Self::Error>> {
        let path = format!(
            "logs/logfile_path/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
        );
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<()>(&mut rx_buf, Method::GET, &path, None)
            .await?;
        Self::deserialize(response_body)
    }

    async fn logs_push(
        &mut self,
        key: &airflow_common::models::TaskInstanceKey,
        log_chunk_time: &airflow_common::datetime::UtcDateTime,
        log_chunk_data: &str,
    ) -> Result<(), EdgeApiError<Self::Error>> {
        let path = format!(
            "logs/push/{}/{}/{}/{}/{}",
            key.dag_id(),
            key.task_id(),
            key.run_id(),
            key.try_number(),
            key.map_index(),
        );
        let body = PushLogsBody {
            log_chunk_time,
            log_chunk_data,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::POST, &path, Some(&body))
            .await?;
        Ok(())
    }
}

/// Incremental new log content from worker.
#[derive(Debug, Serialize)]
struct PushLogsBody<'a> {
    /// Time of the log chunk at point of sending.
    log_chunk_time: &'a UtcDateTime,
    /// Log chunk data as incremental log text.
    log_chunk_data: &'a str,
}

/// Queues that a worker supports to run jobs on.
#[derive(Debug, Serialize)]
struct WorkerQueuesBody<'a> {
    /// List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.
    queues: Option<&'a Vec<String>>,
    /// Number of free concurrency slots on the worker.
    free_concurrency: usize,
}

/// Details of the worker state sent to the scheduler.
#[derive(Debug, Serialize)]
struct WorkerStateBody<'a> {
    /// State of the worker from the view of the worker.
    state: EdgeWorkerState,
    /// Number of active jobs the worker is running.
    jobs_active: usize,
    /// List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.
    queues: Option<&'a Vec<String>>,
    /// System information of the worker.
    sysinfo: &'a SysInfo,
    /// Comments about the maintenance state of the worker.
    maintenance_comments: Option<&'a str>,
}

impl<'a, J: JWTGenerator + Clone, T: TcpConnect + 'a, D: Dns + 'a> Clone
    for ReqwlessEdgeApiClient<'a, J, T, D>
{
    fn clone(&self) -> Self {
        Self::new(
            self.tcp,
            self.dns,
            &self.base_url,
            self.jwt_generator.clone(),
        )
    }
}
