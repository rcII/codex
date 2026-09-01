use crate::app_server_session::CchHistoryThread;
use crate::app_server_session::StrictHistorySnapshot;
use crate::legacy_core::config::CchConfig;
use crate::legacy_core::config::CchEndpointConfig;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadTurnsListPaginatedParams;
use codex_app_server_protocol::ThreadTurnsListPaginatedResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_http_client::HttpClient;
use codex_http_client::HttpClientBuilder;
use rand::Rng as _;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
type Json = serde_json::Value;
static NEXT_STRICT_HISTORY_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
impl StrictHistorySnapshot {
    pub(crate) fn max_history_request_id(&self) -> u64 {
        self.source_high_water_ordinal
            .saturating_mul(2)
            .saturating_add(4)
    }

    pub(crate) fn snapshot_sha256(&self, prelude_sha256: &str) -> Result<String, CchError> {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "historyRevision": &self.revision,
            "preludeSha256": prelude_sha256,
            "sourceHighWaterOrdinal": self.source_high_water_ordinal,
        }))
        .map_err(|_| CchError::HistoryExchange)?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }
}
pub(crate) const CONTRACT_SHA256: &str =
    "40f1d07cf785ec0e057f1e5fd449cf91e9762396c09aeb4bb3a22fa7a104ebf8";
pub(crate) const NATIVE_PROTOCOL_VERSION: &str = "cch.codex-native-http.v1";
pub(crate) const NATIVE_PROTOCOL_SHA256: &str =
    "3e30c9d59276b786dd61ff16b044ac205ccb58b92e7e316341ec4cb658959663";
pub(crate) const NATIVE_CAPABILITY: &str =
    "cch-native/3e30c9d59276b786dd61ff16b044ac205ccb58b92e7e316341ec4cb658959663";
const NATIVE_CAPABILITY_HEADER: &str = "x-cch-native-capability";
const MIN_BEARER_TOKEN_CORE_BYTES: usize = 32;
const MIN_BEARER_TOKEN_DISTINCT_CORE_BYTES: usize = 8;
const MAX_BEARER_TOKEN_BYTES: usize = 4 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CchError {
    #[error("failed to configure the native CCH HTTP client")]
    ClientConfiguration,
    #[error("native CCH bearer token is missing or invalid")]
    BearerToken,
    #[error("native CCH request body exceeds its configured limit")]
    RequestTooLarge,
    #[error("native CCH request failed")]
    RequestFailed,
    #[error("native CCH rejected the request with HTTP {0}")]
    RequestRejected(u16),
    #[error("native CCH response exceeds its configured limit")]
    ResponseTooLarge,
    #[error("native CCH returned an invalid response")]
    InvalidResponse,
    #[error("native CCH contract does not match the configured contract")]
    ContractMismatch,
    #[error("native CCH history exchange failed closed")]
    HistoryExchange,
}
#[derive(Clone)]
pub(crate) struct CchTransport {
    client: HttpClient,
    pub(crate) endpoint: CchEndpointConfig,
    bearer_token: String,
}
impl CchTransport {
    fn new(
        endpoint: CchEndpointConfig,
        read_env: impl FnOnce(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Self, CchError> {
        let bearer_token = read_env(&endpoint.bearer_token_env_var)
            .map_err(|_| CchError::BearerToken)
            .and_then(Self::bearer_token)?;
        let client = HttpClientBuilder::new()
            .without_redirects()
            .without_request_logging()
            .build_direct()
            .map_err(|_| CchError::ClientConfiguration)?;
        Ok(Self {
            client,
            endpoint,
            bearer_token,
        })
    }
    async fn get<Response: DeserializeOwned>(&self, path: &str) -> Result<Response, CchError> {
        self.send(path, None).await
    }
    pub(crate) async fn post<Request: Serialize + ?Sized, Response: DeserializeOwned>(
        &self,
        path: &str,
        value: &Request,
    ) -> Result<Response, CchError> {
        let body = serde_json::to_vec(value).map_err(|_| CchError::InvalidResponse)?;
        if body.len() > self.endpoint.max_request_body_bytes {
            return Err(CchError::RequestTooLarge);
        }
        self.send(path, Some(body)).await
    }
    async fn send<Response: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Response, CchError> {
        let advertise_native = body.is_none() && path == "v1/runtime/contract";
        let url = self
            .endpoint
            .base_url
            .join(path)
            .map_err(|_| CchError::InvalidResponse)?;
        let request = match body {
            Some(body) => self
                .client
                .post(url)
                .header("content-type", "application/json")
                .body(body),
            None => self.client.get(url),
        }
        .bearer_auth(&self.bearer_token)
        .timeout(self.endpoint.timeout);
        let request = if advertise_native {
            request.header(NATIVE_CAPABILITY_HEADER, NATIVE_CAPABILITY)
        } else {
            request
        };
        let mut response = request.send().await.map_err(|_| CchError::RequestFailed)?;
        if !response.status().is_success() {
            return Err(CchError::RequestRejected(response.status().as_u16()));
        }
        let max_body = self.endpoint.max_response_body_bytes;
        if matches!(response.content_length(), Some(length) if length > max_body as u64) {
            return Err(CchError::ResponseTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CchError::RequestFailed)?
        {
            if chunk.len().saturating_add(body.len()) > max_body {
                return Err(CchError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| CchError::InvalidResponse)
    }
    fn bearer_token(value: String) -> Result<String, CchError> {
        let padding = value.find('=').unwrap_or(value.len());
        let (body, padding) = value.split_at(padding);
        if body.len() < MIN_BEARER_TOKEN_CORE_BYTES
            || value.len() > MAX_BEARER_TOKEN_BYTES
            || !padding.bytes().all(|byte| byte == b'=')
        {
            return Err(CchError::BearerToken);
        }
        let mut seen = 0_u128;
        for byte in body.bytes() {
            if !(byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/'))
            {
                return Err(CchError::BearerToken);
            }
            seen |= 1_u128 << byte;
        }
        if seen.count_ones() < MIN_BEARER_TOKEN_DISTINCT_CORE_BYTES as u32 {
            return Err(CchError::BearerToken);
        }
        Ok(value)
    }
}
#[derive(Clone)]
pub(crate) struct CchIntegration {
    pub(crate) transport: CchTransport,
    pub(crate) threads: Arc<Mutex<HashMap<String, CchHistoryThread>>>,
    pub(crate) shutdown: Arc<AtomicBool>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContractResponse {
    contract_sha256: String,
    namespace_canonical_json: String,
    #[serde(default)]
    native_capability: Option<String>,
    #[serde(default)]
    native_protocol_sha256: Option<String>,
    #[serde(default)]
    native_protocol_version: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventResponse {
    contract_sha256: String,
    event_id: String,
    history_revision: String,
    idempotency_key: String,
    receipt: Option<Json>,
    source_ordinal: u64,
    source_subordinal: u32,
}
struct SourceEvent<'a> {
    kind: &'a str,
    thread_id: &'a str,
    turn_id: Option<&'a str>,
    item_id: Option<&'a str>,
    snapshot: &'a StrictHistorySnapshot,
    source_ordinal: u64,
    source_subordinal: u32,
    idempotency_key: String,
}
impl CchIntegration {
    pub(crate) async fn connect(config: &CchConfig) -> Result<Option<Self>, CchError> {
        Self::connect_with_env(config, |name| std::env::var(name)).await
    }

    pub(crate) async fn connect_with_env(
        config: &CchConfig,
        read_env: impl FnOnce(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Option<Self>, CchError> {
        let CchConfig::Enabled(endpoint) = config else {
            return Ok(None);
        };
        let transport = CchTransport::new(endpoint.clone(), read_env)?;
        let contract: ContractResponse = transport.get("v1/runtime/contract").await?;
        let digest = format!(
            "{:x}",
            Sha256::digest(contract.namespace_canonical_json.as_bytes())
        );
        if contract.contract_sha256 != CONTRACT_SHA256
            || digest != CONTRACT_SHA256
            || !valid_native_protocol(&contract)
        {
            return Err(CchError::ContractMismatch);
        }
        Ok(Some(Self {
            transport,
            threads: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        }))
    }
    pub(crate) async fn record_child_continuity(
        &self,
        thread: &Thread,
        snapshot: &StrictHistorySnapshot,
    ) -> Result<Option<String>, CchError> {
        let Some(parent_thread_id) = thread
            .parent_thread_id
            .as_deref()
            .or(thread.forked_from_id.as_deref())
        else {
            return Ok(None);
        };
        let fork_mode = if thread.forked_from_id.is_some() {
            "full_history"
        } else {
            "empty"
        };
        self.record_event_until_ack(
            SourceEvent {
                kind: "agent.continuity",
                thread_id: &thread.id,
                turn_id: None,
                item_id: None,
                snapshot,
                source_ordinal: 0,
                source_subordinal: 1,
                idempotency_key: format!(
                    "agent-continuity:{}:{parent_thread_id}:{}",
                    snapshot.revision, thread.id
                ),
            },
            serde_json::json!({
                "parentThreadId": parent_thread_id,
                "runtimeKind": "multi_agent_v2",
                "forkMode": fork_mode,
                "degradedReason": Json::Null,
            }),
        )
        .await
        .map(Some)
    }
    pub(crate) async fn record_thread_settings(
        &self,
        thread: &Thread,
        source: &str,
        snapshot: &StrictHistorySnapshot,
    ) -> Result<String, CchError> {
        let payload = serde_json::json!({
            "cwd": thread.cwd.to_string_lossy(),
            "projectId": thread.project_id.as_deref(),
            "source": source,
        });
        let encoded = serde_json::to_vec(&payload).map_err(|_| CchError::InvalidResponse)?;
        let digest = Sha256::digest(encoded);
        self.record_event_until_ack(
            SourceEvent {
                kind: "thread.settings_updated",
                thread_id: &thread.id,
                turn_id: None,
                item_id: None,
                snapshot,
                source_ordinal: 0,
                source_subordinal: 0,
                idempotency_key: format!(
                    "thread-settings:{}:{}:{digest:x}",
                    snapshot.revision, thread.id
                ),
            },
            payload,
        )
        .await
    }
    async fn record_event_until_ack(
        &self,
        event: SourceEvent<'_>,
        payload: Json,
    ) -> Result<String, CchError> {
        if event.source_ordinal > event.snapshot.source_high_water_ordinal {
            return Err(CchError::HistoryExchange);
        }
        let event_id = format!(
            "{:x}",
            Sha256::digest(format!("cch:event:{}", event.idempotency_key).as_bytes())
        );
        let response: EventResponse = self
            .transport
            .post(
                "v1/runtime/events",
                &serde_json::json!({
                    "kind": event.kind,
                    "threadId": event.thread_id,
                    "turnId": event.turn_id,
                    "itemId": event.item_id,
                    "historyRevision": &event.snapshot.revision,
                    "sourceOrdinal": event.source_ordinal,
                    "sourceSubordinal": event.source_subordinal,
                    "idempotencyKey": &event.idempotency_key,
                    "payload": &payload,
                }),
            )
            .await?;
        if response.contract_sha256 == CONTRACT_SHA256
            && response.event_id == event_id
            && response.history_revision == event.snapshot.revision
            && response.idempotency_key == event.idempotency_key
            && response.source_ordinal == event.source_ordinal
            && response.source_subordinal == event.source_subordinal
            && valid_event_receipt(response.receipt.as_ref(), &payload)
        {
            Ok(event_id)
        } else {
            Err(CchError::InvalidResponse)
        }
    }
}
fn valid_native_protocol(contract: &ContractResponse) -> bool {
    match (
        contract.native_protocol_version.as_deref(),
        contract.native_protocol_sha256.as_deref(),
        contract.native_capability.as_deref(),
    ) {
        (Some(version), Some(protocol), Some(capability)) => {
            version == NATIVE_PROTOCOL_VERSION
                && protocol == NATIVE_PROTOCOL_SHA256
                && capability == NATIVE_CAPABILITY
        }
        _ => false,
    }
}
pub(crate) async fn fetch_strict_history_revision(
    app: &AppServerRequestHandle,
    thread_id: &str,
    timeout: std::time::Duration,
) -> Result<StrictHistorySnapshot, String> {
    let response = tokio::time::timeout(
        timeout,
        app.request_typed::<ThreadTurnsListPaginatedResponse>(
            ClientRequest::ThreadTurnsListPaginated {
                request_id: RequestId::String(format!(
                    "cch-history-revision:{}",
                    NEXT_STRICT_HISTORY_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
                )),
                params: ThreadTurnsListPaginatedParams(ThreadTurnsListParams {
                    thread_id: thread_id.to_string(),
                    cursor: None,
                    limit: Some(1),
                    sort_direction: Some(codex_app_server_protocol::SortDirection::Asc),
                    items_view: Some(TurnItemsView::NotLoaded),
                }),
            },
        ),
    )
    .await
    .map_err(|_| "strict history revision request timed out".to_string())?
    .map_err(|_| "strict history revision request failed".to_string())?;
    if response.history_revision.is_empty() {
        return Err("strict history revision is empty".to_string());
    }
    Ok(StrictHistorySnapshot {
        revision: response.history_revision,
        source_high_water_ordinal: response.source_high_water_ordinal,
    })
}
pub(crate) fn history_retry_delay(
    timeout: std::time::Duration,
    failures: u32,
) -> std::time::Duration {
    let cap_ms = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let exponent = 1_u64 << failures.saturating_sub(1).min(4);
    let delay_ms = (cap_ms / 16).max(1).saturating_mul(exponent).min(cap_ms);
    let jitter = rand::rng().random_range(80_u64..=120);
    std::time::Duration::from_millis(delay_ms.saturating_mul(jitter).div_ceil(100).min(cap_ms))
}
fn valid_event_receipt(receipt: Option<&Json>, payload: &Json) -> bool {
    receipt.is_none() && payload.get("success").and_then(Json::as_bool) != Some(true)
}
#[cfg(test)]
#[path = "cch_tests.rs"]
mod tests;
