use super::*;
pub(crate) async fn request_thread_start(
    request_handle: &AppServerRequestHandle,
    request_id: RequestId,
    params: ThreadStartParams,
) -> std::result::Result<(ThreadStartResponse, bool), TypedRequestError> {
    let response: ThreadStartResponse = request_handle
        .request_typed(ClientRequest::ThreadStart {
            request_id,
            params: params.clone(),
        })
        .await?;
    let task_tools_available = params.dynamic_tools.is_some()
        || params
            .config
            .as_ref()
            .is_some_and(|config| config.contains_key("mcp_servers.codex_tui"));
    Ok((response, task_tools_available))
}

pub(crate) async fn start_thread_with_request_handle(
    request_handle: AppServerRequestHandle,
    config: Config,
    thread_params_mode: ThreadParamsMode,
    remote_cwd_override: Option<PathBuf>,
    thread_tool_transport: ThreadToolTransport,
    cch: Option<Arc<CchIntegration>>,
) -> Result<AppServerStartedThread> {
    let request_id = RequestId::String(format!("startup-thread-start-{}", Uuid::new_v4()));
    let mut params = thread_start_params_from_config(
        &config,
        thread_params_mode,
        remote_cwd_override.as_deref(),
        /*session_start_source*/ None,
    );
    thread_tool_transport.configure(&mut params);
    let (response, task_tools_available) =
        request_thread_start(&request_handle, request_id, params)
            .await
            .map_err(|err| {
                bootstrap_request_error("thread/start failed during TUI bootstrap", err)
            })?;
    if let Some(cch) = cch.as_ref() {
        cch.register_history_thread(
            request_handle.clone(),
            response.thread.clone(),
            "thread/start",
            /*capture_now*/ false,
        )
        .await?;
    }
    let mut started =
        started_thread_from_start_response(response, &config, thread_params_mode).await?;
    started.task_tools_available = task_tools_available;
    Ok(started)
}
