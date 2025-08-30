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
use core::fmt::Display;
use embedded_nal_async::{Dns, TcpConnect};
use reqwless::client::HttpClient;
use reqwless::headers::ContentType;
use reqwless::request::{Method, RequestBody, RequestBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::HTTP_RX_BUF_SIZE;

pub struct ReqwlessExecutionApiClientFactory<'a, T: TcpConnect + 'a, D: Dns + 'a> {
    tcp: &'a T,
    dns: &'a D,
}

impl<'a, T: TcpConnect + 'a, D: Dns + 'a> Clone for ReqwlessExecutionApiClientFactory<'a, T, D> {
    fn clone(&self) -> Self {
        Self {
            tcp: self.tcp,
            dns: self.dns,
        }
    }
}

impl<'a, T: TcpConnect + 'a, D: Dns + 'a> ReqwlessExecutionApiClientFactory<'a, T, D> {
    pub fn new(tcp: &'a T, dns: &'a D) -> Self {
        Self { tcp, dns }
    }
}

impl<'a, T: TcpConnect + 'a, D: Dns + 'a> LocalExecutionApiClientFactory
    for ReqwlessExecutionApiClientFactory<'a, T, D>
{
    type Error = Infallible;
    type Client = ReqwlessExecutionApiClient<'a, T, D>;

    fn create(
        &self,
        base_url: &str,
        token: &SecretString,
    ) -> Result<ReqwlessExecutionApiClient<'a, T, D>, Infallible> {
        Ok(ReqwlessExecutionApiClient::new(
            self.tcp, self.dns, base_url, token,
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

pub struct ReqwlessExecutionApiClient<'a, T: TcpConnect + 'a, D: Dns + 'a> {
    client: HttpClient<'a, T, D>,
    base_url: String,
    token: SecretString,
}

impl<'a, T: TcpConnect + 'a, D: Dns + 'a> ReqwlessExecutionApiClient<'a, T, D> {
    pub fn new(tcp: &'a T, dns: &'a D, base_url: &str, token: &SecretString) -> Self {
        let client = HttpClient::new(tcp, dns);

        Self {
            client,
            base_url: base_url.into(),
            token: token.clone(),
        }
    }

    async fn request<'buf, B: RequestBody>(
        &mut self,
        rx_buf: &'buf mut [u8],
        method: Method,
        path: &str,
        body: Option<B>,
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

    fn query<K: Display, V: Display>(path: String, query: &[(K, V)]) -> String {
        // TODO use serde_urlencoded
        if query.is_empty() {
            path
        } else {
            let mut path = path;
            path.push('?');
            path.push_str(
                &query
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&"),
            );
            path
        }
    }

    fn serialize<B: Serialize>(
        data: &B,
    ) -> Result<Vec<u8>, ExecutionApiError<ReqwlessExecutionApiError>> {
        Ok(serde_json::to_vec(data).map_err(ReqwlessExecutionApiError::Serde)?)
    }

    fn deserialize<B: DeserializeOwned>(
        data: &[u8],
    ) -> Result<B, ExecutionApiError<ReqwlessExecutionApiError>> {
        Ok(serde_json::from_slice(data).map_err(ReqwlessExecutionApiError::Serde)?)
    }
}

impl<'a, T: TcpConnect + 'a, D: Dns + 'a> LocalExecutionApiClient
    for ReqwlessExecutionApiClient<'a, T, D>
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
    async fn task_instances_defer<B: Serialize + Sync, NK: Serialize + Sync>(
        &mut self,
        _id: &UniqueTaskInstanceId,
        _classpath: &str,
        _trigger_kwargs: &B,
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

    async fn xcoms_head(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
    ) -> Result<usize, ExecutionApiError<Self::Error>> {
        let path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::HEAD, &path, None)
            .await?;
        todo!()
    }

    async fn xcoms_get(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
        include_prior_dates: Option<bool>,
    ) -> Result<XComResponse, ExecutionApiError<Self::Error>> {
        let mut path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        if let Some(include_prior_dates) = include_prior_dates {
            query.push(("include_prior_dates", include_prior_dates.to_string()));
        }
        path = Self::query(path, &query);
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::GET, &path, None)
            .await?;
        let response: XComResponse = Self::deserialize(&rx_buf)?;
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    async fn xcoms_set(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        value: &JsonValue,
        map_index: Option<MapIndex>,
        mapped_length: Option<usize>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let mut path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        if let Some(mapped_length) = mapped_length {
            query.push(("mapped_length", mapped_length.to_string()));
        }
        path = Self::query(path, &query);
        let body = Self::serialize(value)?;
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::POST, &path, Some(&body))
            .await?;
        Ok(())
    }

    async fn xcoms_delete(
        &mut self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        key: &str,
        map_index: Option<MapIndex>,
    ) -> Result<(), ExecutionApiError<Self::Error>> {
        let mut path = format!("xcoms/{dag_id}/{run_id}/{task_id}/{key}");
        let mut query = Vec::new();
        if let Some(map_index) = map_index {
            query.push(("map_index", map_index.to_string()));
        }
        path = Self::query(path, &query);
        let mut rx_buf = [0; HTTP_RX_BUF_SIZE];
        self.request::<&[u8]>(&mut rx_buf, Method::DELETE, &path, None)
            .await?;
        Ok(())
    }
}
