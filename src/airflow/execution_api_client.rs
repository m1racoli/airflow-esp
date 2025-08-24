use airflow_common::datetime::UtcDateTime;
use airflow_common::executors::UniqueTaskInstanceId;
use airflow_common::utils::{MapIndex, SecretString, TaskInstanceState, TerminalTIStateNonSuccess};
use airflow_task_sdk::api::{
    ExecutionApiError, LocalExecutionApiClient, LocalExecutionApiClientFactory, datamodels::*,
};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::convert::Infallible;
use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::TcpClient;
use reqwless::client::HttpClient;
use reqwless::headers::ContentType;
use reqwless::request::{Method, RequestBody, RequestBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::HTTP_RX_BUF_SIZE;

#[derive(Clone)]
pub struct ReqwlessExecutionApiClientFactory<'a, const N: usize, const TX: usize, const RX: usize> {
    tcp_client: &'a TcpClient<'a, N, TX, RX>,
    dns_socket: &'a DnsSocket<'a>,
}

impl<'a, const N: usize, const TX: usize, const RX: usize>
    ReqwlessExecutionApiClientFactory<'a, N, TX, RX>
{
    pub fn new(tcp_client: &'a TcpClient<'a, N, TX, RX>, dns_socket: &'a DnsSocket<'a>) -> Self {
        Self {
            tcp_client,
            dns_socket,
        }
    }
}

impl<'a, const N: usize, const TX: usize, const RX: usize> LocalExecutionApiClientFactory
    for ReqwlessExecutionApiClientFactory<'a, N, TX, RX>
{
    type Error = Infallible;
    type Client = ReqwlessExecutionApiClient<'a, N, TX, RX>;

    fn create(
        &self,
        base_url: &str,
        token: &SecretString,
    ) -> Result<ReqwlessExecutionApiClient<'a, N, TX, RX>, Infallible> {
        Ok(ReqwlessExecutionApiClient::new(
            self.tcp_client,
            self.dns_socket,
            base_url,
            token,
        ))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReqwlessExecutionApiError {
    #[error("{0:?}")]
    Reqwless(reqwless::Error),
    #[error(transparent)]
    Serde(serde_json::Error),
    #[error("Http error {0}: {1}")]
    Http(u16, String),
}

pub struct ReqwlessExecutionApiClient<'a, const N: usize, const TX: usize, const RX: usize> {
    client: HttpClient<'a, TcpClient<'a, N, TX, RX>, DnsSocket<'a>>,
    base_url: String,
    token: SecretString,
}

impl<'a, const N: usize, const TX: usize, const RX: usize>
    ReqwlessExecutionApiClient<'a, N, TX, RX>
{
    pub fn new(
        tcp_client: &'a TcpClient<'a, N, TX, RX>,
        dns_socket: &'a DnsSocket<'a>,
        base_url: &str,
        token: &SecretString,
    ) -> Self {
        let client = HttpClient::new(tcp_client, dns_socket);

        Self {
            client,
            base_url: base_url.into(),
            token: token.clone(),
        }
    }

    async fn request<'buf, T: RequestBody>(
        &mut self,
        rx_buf: &'buf mut [u8],
        method: Method,
        path: &str,
        body: Option<T>,
    ) -> Result<&'buf [u8], ExecutionApiError<ReqwlessExecutionApiError>> {
        let url = format!("{}/{}", self.base_url, path);
        let auth = format!("Bearer {}", self.token.secret());
        let headers = [
            ("accept", "application/json"),
            ("authorization", auth.as_str()),
        ];

        let mut handle = self
            .client
            .request(method, &url)
            .await
            .map_err(ReqwlessExecutionApiError::Reqwless)?
            .headers(&headers);

        if body.is_some() {
            handle = handle.content_type(ContentType::ApplicationJson);
        }

        let mut handle = handle.body(body);

        let response = handle
            .send(rx_buf)
            .await
            .map_err(ReqwlessExecutionApiError::Reqwless)?;

        if !response.status.is_successful() {
            let code = response.status.0;
            let msg = match response.body().read_to_end().await {
                Ok(v) => core::str::from_utf8(v).unwrap_or("").to_string(),
                Err(e) => Err(ReqwlessExecutionApiError::Reqwless(e))?,
            };
            return match code {
                404 => Err(ExecutionApiError::NotFound(msg))?,
                409 => Err(ExecutionApiError::Conflict(msg))?,
                code => Err(ReqwlessExecutionApiError::Http(code, msg))?,
            };
        }

        let body = response
            .body()
            .read_to_end()
            .await
            .map_err(ReqwlessExecutionApiError::Reqwless)?;

        Ok(body)
    }

    fn serialize<T: Serialize>(
        data: &T,
    ) -> Result<Vec<u8>, ExecutionApiError<ReqwlessExecutionApiError>> {
        Ok(serde_json::to_vec(data).map_err(ReqwlessExecutionApiError::Serde)?)
    }

    fn deserialize<T: DeserializeOwned>(
        data: &[u8],
    ) -> Result<T, ExecutionApiError<ReqwlessExecutionApiError>> {
        Ok(serde_json::from_slice(data).map_err(ReqwlessExecutionApiError::Serde)?)
    }
}

impl<'a, const N: usize, const TX: usize, const RX: usize> LocalExecutionApiClient
    for ReqwlessExecutionApiClient<'a, N, TX, RX>
{
    type Error = ReqwlessExecutionApiError;

    #[doc = " Tell the API server that this TI has started running."]
    async fn task_instances_start(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        unixname: &str,
        pid: u32,
        when: &UtcDateTime,
    ) -> Result<TIRunContext, ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/run");
        let body = TIEnterRunningPayloadBody {
            state: TaskInstanceState::Running,
            hostname,
            unixname,
            pid,
            start_date: when,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        let response_body = self
            .request::<&[u8]>(&mut rx_buf, Method::PATCH, &path, Some(&body))
            .await?;
        Self::deserialize(response_body)
    }

    #[doc = " Tell the API server that this TI has reached a terminal state."]
    async fn task_instances_finish(
        &mut self,
        id: &UniqueTaskInstanceId,
        state: TerminalTIStateNonSuccess,
        when: &UtcDateTime,
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TITerminalStatePayloadBody {
            state,
            end_date: when,
            rendered_map_index,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::PATCH, &path, Some(&body))
            .await?;
        Ok(())
    }

    #[doc = " Tell the API server that this TI has failed and reached a up_for_retry state."]
    async fn task_instances_retry(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _when: &UtcDateTime,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Tell the API server that this TI has succeeded."]
    async fn task_instances_succeed(
        &mut self,
        id: &UniqueTaskInstanceId,
        when: &UtcDateTime,
        task_outlets: &[AssetProfile],
        outlet_events: &[()],
        rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/state");
        let body = TISuccessStatePayloadBody {
            state: TaskInstanceState::Success,
            end_date: when,
            task_outlets,
            outlet_events,
            rendered_map_index,
        };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::PATCH, &path, Some(&body))
            .await?;
        Ok(())
    }

    #[doc = " Tell the API server that this TI has been deferred."]
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_defer<T: Serialize + Sync, NK: Serialize + Sync>(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _classpath: &str,
        _trigger_kwargs: &T,
        _trigger_timeout: u64,
        _next_method: &str,
        _next_kwargs: &NK,
        _rendered_map_index: Option<&str>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Tell the API server that this TI has been rescheduled."]
    async fn task_instances_reschedule(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _reschedule_date: &UtcDateTime,
        _end_date: &UtcDateTime,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Tell the API server that this TI is still running and send a heartbeat."]
    #[doc = " Also, updates the auth token if the server returns a new one."]
    async fn task_instances_heartbeat(
        &mut self,
        id: &UniqueTaskInstanceId,
        hostname: &str,
        pid: u32,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let path = format!("task-instances/{id}/heartbeat");
        let body = TIHeartbeatInfoBody { hostname, pid };
        let body = Self::serialize(&body)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::PUT, &path, Some(&body))
            .await?;
        // TODO handle token renewal
        Ok(())
    }

    #[doc = " Tell the API server to skip the downstream tasks of this TI."]
    async fn task_instances_skip_downstream_tasks(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _tasks: &[(String, MapIndex)],
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Set Rendered Task Instance Fields via the API server."]
    async fn task_instances_set_rtif<F: Serialize + Sync>(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _fields: &F,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Get the previous successful dag run for a given task instance."]
    #[doc = ""]
    #[doc = " The data from it is used to get values for Task Context."]
    async fn task_instances_get_previous_successful_dagrun(
        &mut self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<PrevSuccessfulDagRunResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Get the start date of a task reschedule via the API server."]
    async fn task_instances_get_reschedule_start_date(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _try_number: usize,
    ) -> Result<TaskRescheduleStartDate, ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Get count of task instances matching the given criteria."]
    #[allow(clippy::too_many_arguments)]
    async fn task_instances_get_count(
        &mut self,
        _dag_id: &str,
        _map_index: Option<MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
        _states: Option<&Vec<TaskInstanceState>>,
    ) -> Result<TICount, ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Get task states given criteria."]
    async fn task_instances_get_task_states(
        &mut self,
        _dag_id: &str,
        _map_index: Option<MapIndex>,
        _task_ids: Option<&Vec<String>>,
        _task_group_id: Option<&str>,
        _logical_dates: Option<&Vec<UtcDateTime>>,
        _run_ids: Option<&Vec<String>>,
    ) -> Result<TaskStatesResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }

    #[doc = " Validate whether there\'re inactive assets in inlets and outlets of a given task instance."]
    async fn task_instances_validate_inlets_and_outlets(
        &mut self,
        _id: &UniqueTaskInstanceId,
    ) -> Result<InactiveAssetsResponse, ExecutionApiError<Self::Error>> {
        todo!()
    }
}
