//! Resume history policy for local rollouts that may migrate between formats.
use super::AppServerSession;
use super::AppServerStartedThread;
use super::HistoryHydrationScope;
use super::ResumeModelSettings;
use super::bootstrap_request_error;
use super::started_thread_from_resume_response;
use super::thread_resume_params_from_config;
use crate::cch::CONTRACT_SHA256;
use crate::cch::CchError;
use crate::cch::CchIntegration;
use crate::cch::fetch_strict_history_revision;
use crate::cch::history_retry_delay;
use crate::legacy_core::config::Config;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItemsListPaginatedParams;
use codex_app_server_protocol::ThreadItemsListPaginatedResponse;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadTurnsListPaginatedParams;
use codex_app_server_protocol::ThreadTurnsListPaginatedResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnItemsView;
use codex_features::Feature;
use codex_protocol::ThreadId;
use color_eyre::eyre::Result;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
type CchResult<T> = std::result::Result<T, CchError>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StrictHistorySnapshot {
    pub(crate) revision: String,
    pub(crate) source_high_water_ordinal: u64,
}
struct HistoryPrelude {
    event_ids: Vec<String>,
    sha256: String,
    snapshot_sha256: String,
}
struct HistoryCaptureContext<'a> {
    thread_id: &'a str,
    capture_id: &'a str,
    snapshot: &'a StrictHistorySnapshot,
    prelude: &'a HistoryPrelude,
}
pub(crate) struct CchHistoryThread {
    thread: Thread,
    source: String,
    running: bool,
    dirty: bool,
    cancelled: Arc<AtomicBool>,
    ready_waiters: Vec<tokio::sync::oneshot::Sender<bool>>,
}

impl AppServerSession {
    /// Captures the server's startup migration policy before workspace config can change.
    ///
    /// Sessions without a recorded startup config conservatively assume migration is enabled.
    pub(crate) fn with_startup_config(mut self, config: &Config) -> Self {
        self.background_rollout_migration_enabled = config
            .features
            .enabled(Feature::BackgroundPaginatedRolloutMigration);
        self.task_tool_capabilities_dir = (!self.uses_embedded_app_server())
            .then(|| config.codex_home.join("tui-thread-reference-capabilities"));
        self
    }

    /// Retains trusted embedded-server history metadata for a later resume.
    ///
    /// Remote hints are ignored because another process can migrate the thread before resume.
    pub(crate) fn remember_thread_history_mode(
        &mut self,
        thread_id: ThreadId,
        history_mode: ThreadHistoryMode,
    ) {
        if !self.uses_embedded_app_server() {
            return;
        }
        self.history_pagination
            .entry(thread_id)
            .or_default()
            .history_mode = history_mode;
    }
    pub(crate) async fn resume_thread(
        &mut self,
        config: Config,
        thread_id: ThreadId,
        model_settings: ResumeModelSettings,
    ) -> Result<AppServerStartedThread> {
        let session_config = if matches!(
            model_settings,
            ResumeModelSettings::RestoreFromThread | ResumeModelSettings::PreserveExistingThread
        ) {
            config.clone()
        } else {
            self.session_config_with_effective_service_tier(&config)
        };
        let mut params = thread_resume_params_from_config(
            session_config,
            thread_id,
            self.thread_params_mode(),
            self.remote_cwd_override.as_deref(),
            model_settings,
        );
        self.thread_tool_transport()
            .configure_mcp(&mut params.config);
        params.exclude_turns = true;
        let request_id = self.next_request_id();
        let mut response: ThreadResumeResponse = self
            .client
            .request_typed(ClientRequest::ThreadResume {
                request_id,
                params: params.clone(),
            })
            .await
            .map_err(|err| {
                bootstrap_request_error("thread/resume failed during TUI bootstrap", err)
            })?;
        if let Some(cch) = self.cch_integration() {
            cch.register_history_thread(
                self.request_handle(),
                response.thread.clone(),
                "thread/resume",
                /*capture_now*/ true,
            )
            .await?;
        }
        self.hydrate_initial_thread_history(
            &mut response.thread,
            response.turns_backwards_cursor.clone(),
            response.items_backwards_cursor.clone(),
            Some(&config),
            HistoryHydrationScope::Initial,
        )
        .await?;
        let fork_parent_title = self
            .fork_parent_title_from_app_server(response.thread.forked_from_id.as_deref())
            .await;
        let mut started =
            started_thread_from_resume_response(response, &config, self.thread_params_mode())
                .await?;
        started.session.fork_parent_title = fork_parent_title;
        if self.task_tools_available(thread_id) {
            self.remember_task_tool_thread(thread_id);
            started.task_tools_available = true;
        }
        Ok(started)
    }
}
impl CchIntegration {
    pub(crate) async fn register_history_thread(
        &self,
        app: AppServerRequestHandle,
        thread: Thread,
        source: &str,
        capture_now: bool,
    ) -> CchResult<()> {
        if thread.history_mode != ThreadHistoryMode::Paginated {
            return Err(CchError::HistoryExchange);
        }
        let thread_id = thread.id.clone();
        {
            let mut threads = self.threads.lock().await;
            let state = threads
                .entry(thread_id.clone())
                .or_insert_with(|| CchHistoryThread {
                    thread: thread.clone(),
                    source: source.to_string(),
                    running: false,
                    dirty: false,
                    cancelled: Arc::new(AtomicBool::new(false)),
                    ready_waiters: Vec::new(),
                });
            state.thread = thread;
            state.source = source.to_string();
        }
        if capture_now {
            self.ensure_history_captured(app, &thread_id).await?;
        }
        Ok(())
    }
    pub(crate) async fn ensure_history_captured(
        &self,
        app: AppServerRequestHandle,
        thread_id: &str,
    ) -> CchResult<()> {
        let (cancelled, ready) = {
            let mut threads = self.threads.lock().await;
            let state = threads
                .get_mut(thread_id)
                .ok_or(CchError::HistoryExchange)?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            state.ready_waiters.push(sender);
            (request_capture(state), receiver)
        };
        if let Some(cancelled) = cancelled {
            self.spawn_history_capture(app, thread_id.to_string(), cancelled);
        }
        if matches!(ready.await, Ok(true)) {
            Ok(())
        } else {
            Err(CchError::HistoryExchange)
        }
    }
    pub(crate) async fn history_turn_completed(
        &self,
        app: AppServerRequestHandle,
        thread_id: &str,
    ) -> CchResult<()> {
        let cancelled = {
            let mut threads = self.threads.lock().await;
            threads.get_mut(thread_id).and_then(request_capture)
        };
        if let Some(cancelled) = cancelled {
            self.spawn_history_capture(app, thread_id.to_string(), cancelled);
        }
        Ok(())
    }
    pub(crate) async fn reconcile_history_threads(&self, app: AppServerRequestHandle) {
        let captures = {
            let mut threads = self.threads.lock().await;
            threads
                .iter_mut()
                .filter_map(|(thread_id, state)| {
                    request_capture(state).map(|cancelled| (thread_id.clone(), cancelled))
                })
                .collect::<Vec<_>>()
        };
        for (thread_id, cancelled) in captures {
            self.spawn_history_capture(app.clone(), thread_id, cancelled);
        }
    }
    pub(crate) async fn forget_history_thread(&self, thread_id: &str) {
        if let Some(state) = self.threads.lock().await.remove(thread_id) {
            state.cancelled.store(true, Ordering::Release);
        }
    }
    pub(crate) async fn shutdown_history(&self) {
        self.shutdown.store(true, Ordering::Release);
        for (_, state) in self.threads.lock().await.drain() {
            state.cancelled.store(true, Ordering::Release);
        }
    }
    fn spawn_history_capture(
        &self,
        app: AppServerRequestHandle,
        thread_id: String,
        cancelled: Arc<AtomicBool>,
    ) {
        let integration = self.clone();
        tokio::spawn(async move {
            let _ = integration
                .capture_history_until_clean(app, thread_id, cancelled)
                .await;
        });
    }
    async fn capture_history_until_clean(
        &self,
        app: AppServerRequestHandle,
        thread_id: String,
        cancelled: Arc<AtomicBool>,
    ) -> bool {
        let timeout = self.transport.endpoint.timeout;
        let mut failures = 0_u32;
        let mut retry_delay_used = Duration::ZERO;
        let ready = loop {
            if self.shutdown.load(Ordering::Acquire) || cancelled.load(Ordering::Acquire) {
                break false;
            }
            let (thread, source) = {
                let threads = self.threads.lock().await;
                let Some(state) = threads.get(&thread_id) else {
                    break false;
                };
                if !Arc::ptr_eq(&state.cancelled, &cancelled) {
                    break false;
                }
                (state.thread.clone(), state.source.clone())
            };
            match run_history_capture(self, &app, &thread, &source, &cancelled).await {
                Ok(true) => {
                    failures = 0;
                    retry_delay_used = Duration::ZERO;
                    let mut threads = self.threads.lock().await;
                    let Some(state) = threads.get_mut(&thread_id) else {
                        break false;
                    };
                    if !Arc::ptr_eq(&state.cancelled, &cancelled) {
                        break false;
                    }
                    if state.dirty {
                        state.dirty = false;
                    } else {
                        break true;
                    }
                }
                Ok(false) => break false,
                Err(error) => {
                    failures = failures.saturating_add(1);
                    let delay = history_retry_delay(timeout, failures);
                    let Some(next_retry_delay_used) = retry_delay_used
                        .checked_add(delay)
                        .filter(|used| *used <= timeout)
                    else {
                        tracing::error!(
                            thread_id,
                            failures,
                            retry_budget_ms = timeout.as_millis(),
                            retry_delay_used_ms = retry_delay_used.as_millis(),
                            %error,
                            "native CCH history capture exhausted its bounded retry budget"
                        );
                        break false;
                    };
                    retry_delay_used = next_retry_delay_used;
                    tracing::warn!(
                        thread_id,
                        failures,
                        retry_delay_ms = delay.as_millis(),
                        %error,
                        "native CCH history capture will retry after a bounded failure"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        };
        let mut threads = self.threads.lock().await;
        if let Some(state) = threads.get_mut(&thread_id)
            && Arc::ptr_eq(&state.cancelled, &cancelled)
        {
            state.running = false;
            state.dirty |= !ready;
            for waiter in state.ready_waiters.drain(..) {
                let _ = waiter.send(ready);
            }
        }
        ready
    }
}
async fn prepare_history_prelude(
    integration: &CchIntegration,
    app: &AppServerRequestHandle,
    thread: &Thread,
    source: &str,
) -> CchResult<(StrictHistorySnapshot, HistoryPrelude)> {
    let timeout = integration.transport.endpoint.timeout;
    let snapshot = fetch_strict_history_revision(app, &thread.id, timeout)
        .await
        .map_err(|_| CchError::HistoryExchange)?;
    let mut event_ids = vec![
        integration
            .record_thread_settings(thread, source, &snapshot)
            .await?,
    ];
    if let Some(event_id) = integration
        .record_child_continuity(thread, &snapshot)
        .await?
    {
        event_ids.push(event_id);
    }
    let encoded = serde_json::to_vec(&event_ids).map_err(|_| CchError::HistoryExchange)?;
    let sha256 = format!("{:x}", Sha256::digest(encoded));
    let snapshot_sha256 = snapshot.snapshot_sha256(&sha256)?;
    Ok((
        snapshot,
        HistoryPrelude {
            event_ids,
            sha256,
            snapshot_sha256,
        },
    ))
}
fn request_capture(state: &mut CchHistoryThread) -> Option<Arc<AtomicBool>> {
    if state.running {
        state.dirty = true;
        None
    } else {
        state.running = true;
        state.dirty = false;
        Some(Arc::clone(&state.cancelled))
    }
}
async fn execute_strict_history_request(
    app: &AppServerRequestHandle,
    capture: &HistoryCaptureContext<'_>,
    request_id: u64,
    method: &str,
    params: &Value,
    timeout: std::time::Duration,
) -> Result<Value, Value> {
    if request_id == 0 {
        return Err(strict_history_error("history request id must be positive"));
    }
    let request_id = RequestId::String(format!("cch-history:{}:{request_id}", capture.capture_id));
    let response = match method {
        "thread/turns/listPaginated" => {
            let params: ThreadTurnsListParams =
                decode_strict_history_params(params, is_turn_history_param)?;
            if params.thread_id != capture.thread_id {
                return Err(strict_history_error("history owner mismatch"));
            }
            if !is_strict_turn_history_request(&params) {
                return Err(strict_history_error(
                    "strict turn history requires ascending pages or a descending unit probe and itemsView=notLoaded",
                ));
            }
            serialize_strict_history_response(
                tokio::time::timeout(
                    timeout,
                    app.request_typed::<ThreadTurnsListPaginatedResponse>(
                        ClientRequest::ThreadTurnsListPaginated {
                            request_id,
                            params: ThreadTurnsListPaginatedParams(params),
                        },
                    ),
                )
                .await,
            )
        }
        "thread/items/listPaginated" => {
            let params: ThreadItemsListParams =
                decode_strict_history_params(params, is_item_history_param)?;
            if params.thread_id != capture.thread_id {
                return Err(strict_history_error("history owner mismatch"));
            }
            if params.turn_id.is_none()
                || params.sort_direction != Some(codex_app_server_protocol::SortDirection::Asc)
            {
                return Err(strict_history_error(
                    "strict item pages require turnId and ascending source order",
                ));
            }
            serialize_strict_history_response(
                tokio::time::timeout(
                    timeout,
                    app.request_typed::<ThreadItemsListPaginatedResponse>(
                        ClientRequest::ThreadItemsListPaginated {
                            request_id,
                            params: ThreadItemsListPaginatedParams(params),
                        },
                    ),
                )
                .await,
            )
        }
        _ => Err(strict_history_error("unsupported native history method")),
    }?;
    validate_strict_page_response(&response, capture.snapshot)?;
    Ok(response)
}
fn is_strict_turn_history_request(params: &ThreadTurnsListParams) -> bool {
    if params.items_view != Some(TurnItemsView::NotLoaded) {
        return false;
    }
    match params.sort_direction {
        Some(codex_app_server_protocol::SortDirection::Asc) => true,
        Some(codex_app_server_protocol::SortDirection::Desc) => params.limit == Some(1),
        None => false,
    }
}
fn validate_strict_page_response(
    response: &Value,
    snapshot: &StrictHistorySnapshot,
) -> Result<(), Value> {
    if response.get("historyRevision").and_then(Value::as_str) != Some(snapshot.revision.as_str())
        || response
            .get("sourceHighWaterOrdinal")
            .and_then(Value::as_u64)
            != Some(snapshot.source_high_water_ordinal)
    {
        return Err(strict_history_error(
            "native paginated history snapshot changed during capture",
        ));
    }
    let entries = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| strict_history_error("strict history page data is missing"))?;
    let mut previous = None;
    for entry in entries {
        let revision = entry.get("historyRevision").and_then(Value::as_str);
        let ordinal = entry.get("sourceOrdinal").and_then(Value::as_u64);
        let subordinal = entry.get("sourceSubordinal").and_then(Value::as_u64);
        let (Some(ordinal), Some(subordinal)) = (ordinal, subordinal) else {
            return Err(strict_history_error(
                "strict history member has no native source position",
            ));
        };
        if revision != Some(snapshot.revision.as_str())
            || ordinal == 0
            || ordinal > snapshot.source_high_water_ordinal
            || subordinal > u32::MAX as u64
            || previous.is_some_and(|position| position >= (ordinal, subordinal))
        {
            return Err(strict_history_error(
                "strict history member source position is invalid",
            ));
        }
        previous = Some((ordinal, subordinal));
    }
    Ok(())
}
fn decode_strict_history_params<T>(
    value: &Value,
    allowed: impl Fn(&str) -> bool,
) -> Result<T, Value>
where
    T: DeserializeOwned,
{
    let fields = value
        .as_object()
        .ok_or_else(|| strict_history_error("native history params must be an object"))?;
    if fields.keys().any(|field| !allowed(field)) {
        return Err(strict_history_error(
            "native history params contain an unknown field",
        ));
    }
    serde_json::from_value(value.clone())
        .map_err(|_| strict_history_error("native history params do not match the typed API"))
}
fn is_turn_history_param(field: &str) -> bool {
    matches!(
        field,
        "threadId" | "cursor" | "limit" | "sortDirection" | "itemsView"
    )
}
fn is_item_history_param(field: &str) -> bool {
    matches!(
        field,
        "threadId" | "turnId" | "cursor" | "limit" | "sortDirection"
    )
}
fn serialize_strict_history_response<T>(
    response: Result<Result<T, TypedRequestError>, tokio::time::error::Elapsed>,
) -> Result<Value, Value>
where
    T: Serialize,
{
    let response = response
        .map_err(|_| strict_history_error("native app-server history request timed out"))?;
    match response {
        Ok(response) => serde_json::to_value(response)
            .map_err(|_| strict_history_error("native history response could not be encoded")),
        Err(TypedRequestError::Server { source, .. }) => serde_json::to_value(source)
            .map_err(|_| strict_history_error("native history error could not be encoded"))
            .and_then(Err),
        Err(TypedRequestError::Transport { .. }) => Err(strict_history_error(
            "native app-server history transport failed",
        )),
        Err(TypedRequestError::Deserialize { .. }) => Err(strict_history_error(
            "native app-server history response did not match the typed API",
        )),
    }
}
fn strict_history_error(message: &str) -> Value {
    serde_json::json!({"code": -32602, "message": message})
}
#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistoryOutcome {
    capture_id: String,
    contract_sha256: String,
    history_revision: String,
    prelude_event_ids: Vec<String>,
    prelude_sha256: String,
    snapshot_sha256: String,
    source_high_water_ordinal: u64,
    status: HistoryStatus,
    #[serde(default)]
    request: Option<NativeRequest>,
    #[serde(default)]
    progress: Option<Value>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    resource_admission: Option<Value>,
    thread_id: String,
}
#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
enum HistoryStatus {
    Request,
    Complete,
    Failed,
}
#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NativeRequest {
    id: u64,
    method: String,
    params: Value,
}
async fn run_history_capture(
    integration: &CchIntegration,
    app: &AppServerRequestHandle,
    thread: &Thread,
    source: &str,
    cancelled: &AtomicBool,
) -> CchResult<bool> {
    let transport = &integration.transport;
    let thread_id = thread.id.as_str();
    let (snapshot, prelude) = prepare_history_prelude(integration, app, thread, source).await?;
    let mut outcome: HistoryOutcome = transport
        .post(
            "v1/runtime/history/checkpoint",
            &serde_json::json!({
                "historyRevision": &snapshot.revision,
                "preludeEventIds": &prelude.event_ids,
                "preludeSha256": &prelude.sha256,
                "snapshotSha256": &prelude.snapshot_sha256,
                "sourceHighWaterOrdinal": snapshot.source_high_water_ordinal,
                "threadId": thread_id,
                "thread": thread,
            }),
        )
        .await?;
    let capture_id = outcome.capture_id.clone();
    let capture = HistoryCaptureContext {
        thread_id,
        capture_id: &capture_id,
        snapshot: &snapshot,
        prelude: &prelude,
    };
    let mut previous_request_id = 0;
    let mut request_count = 0_u64;
    let max_request_id = snapshot.max_history_request_id();
    loop {
        if integration.shutdown.load(Ordering::Acquire) || cancelled.load(Ordering::Acquire) {
            return Ok(false);
        }
        verify_history_outcome(&outcome, &capture)?;
        match outcome.status {
            HistoryStatus::Request => {
                let request = outcome.request.clone().ok_or(CchError::HistoryExchange)?;
                if request.id > max_request_id {
                    return Err(CchError::HistoryExchange);
                }
                outcome = exchange_history_page(
                    integration,
                    app,
                    &capture,
                    request,
                    &mut previous_request_id,
                )
                .await?;
                request_count = request_count.saturating_add(1);
                if request_count.is_power_of_two() {
                    tracing::debug!(thread_id, request_count, "CCH history advanced");
                }
            }
            HistoryStatus::Complete => {
                let progress = outcome.progress.clone();
                let resource_admission = outcome.resource_admission.clone();
                finalize_history_capture(transport, &capture, outcome).await?;
                tracing::info!(
                    thread_id,
                    capture_id = capture.capture_id,
                    request_count,
                    source_high_water_ordinal = snapshot.source_high_water_ordinal,
                    ?progress,
                    ?resource_admission,
                    "native CCH history capture completed and was acknowledged"
                );
                return Ok(true);
            }
            HistoryStatus::Failed => {
                let cch_error = outcome.error.as_deref().unwrap_or("missing CCH error");
                tracing::error!(thread_id, request_count, cch_error, "CCH history failed");
                finalize_history_capture(transport, &capture, outcome).await?;
                return Err(CchError::HistoryExchange);
            }
        }
    }
}
async fn exchange_history_page(
    integration: &CchIntegration,
    app: &AppServerRequestHandle,
    capture: &HistoryCaptureContext<'_>,
    request: NativeRequest,
    previous_request_id: &mut u64,
) -> CchResult<HistoryOutcome> {
    if request.id <= *previous_request_id {
        return Err(CchError::HistoryExchange);
    }
    *previous_request_id = request.id;
    let response = execute_strict_history_request(
        app,
        capture,
        request.id,
        &request.method,
        &request.params,
        integration.transport.endpoint.timeout,
    )
    .await;
    let (result, error) = match response {
        Ok(result) => (Some(result), None),
        Err(error) => (None, Some(error)),
    };
    integration
        .transport
        .post(
            "v1/runtime/history/pages",
            &serde_json::json!({
                "captureId": capture.capture_id,
                "error": error,
                "historyRevision": &capture.snapshot.revision,
                "method": request.method,
                "params": request.params,
                "preludeEventIds": &capture.prelude.event_ids,
                "preludeSha256": &capture.prelude.sha256,
                "snapshotSha256": &capture.prelude.snapshot_sha256,
                "requestId": request.id,
                "result": result,
                "sourceHighWaterOrdinal": capture.snapshot.source_high_water_ordinal,
                "threadId": capture.thread_id,
            }),
        )
        .await
}
async fn finalize_history_capture(
    transport: &crate::cch::CchTransport,
    capture: &HistoryCaptureContext<'_>,
    outcome: HistoryOutcome,
) -> CchResult<()> {
    let acknowledgement: HistoryOutcome = transport
        .post(
            "v1/runtime/history/finalize",
            &serde_json::json!({
                "captureId": &outcome.capture_id,
                "historyRevision": &capture.snapshot.revision,
                "preludeEventIds": &capture.prelude.event_ids,
                "preludeSha256": &capture.prelude.sha256,
                "snapshotSha256": &capture.prelude.snapshot_sha256,
                "sourceHighWaterOrdinal": capture.snapshot.source_high_water_ordinal,
                "threadId": capture.thread_id,
            }),
        )
        .await?;
    verify_history_outcome(&acknowledgement, capture)?;
    if outcome.status == HistoryStatus::Failed || outcome != acknowledgement {
        return Err(CchError::HistoryExchange);
    }
    Ok(())
}
fn verify_history_outcome(
    outcome: &HistoryOutcome,
    capture: &HistoryCaptureContext<'_>,
) -> CchResult<()> {
    if outcome.capture_id != capture.capture_id
        || outcome.capture_id.is_empty()
        || outcome.contract_sha256 != CONTRACT_SHA256
        || outcome.history_revision != capture.snapshot.revision
        || outcome.source_high_water_ordinal != capture.snapshot.source_high_water_ordinal
        || outcome.prelude_event_ids != capture.prelude.event_ids
        || outcome.prelude_sha256 != capture.prelude.sha256
        || outcome.snapshot_sha256 != capture.prelude.snapshot_sha256
        || outcome.thread_id != capture.thread_id
        || !matches!(
            (
                outcome.status,
                outcome.request.is_some(),
                outcome.progress.is_some(),
                outcome.error.is_some()
            ),
            (HistoryStatus::Request, true, false, false)
                | (HistoryStatus::Complete, false, true, false)
                | (HistoryStatus::Failed, false, false, true)
        )
    {
        return Err(CchError::HistoryExchange);
    }
    Ok(())
}
#[cfg(test)]
#[path = "rollout_history_tests.rs"]
mod tests;
