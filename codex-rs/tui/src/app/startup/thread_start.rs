use super::*;

pub(super) fn spawn_startup_thread_start(
    app_server: &AppServerSession,
    config: Config,
    app_event_tx: AppEventSender,
) {
    let request_handle = app_server.request_handle();
    let thread_params_mode = app_server.thread_params_mode();
    let remote_cwd_override = app_server.remote_cwd_override().map(Path::to_path_buf);
    let thread_tool_transport = app_server.thread_tool_transport();
    let cch = app_server.cch_integration();
    tokio::spawn(async move {
        let result = crate::app_server_session::start_thread_with_request_handle(
            request_handle,
            config,
            thread_params_mode,
            remote_cwd_override,
            thread_tool_transport,
            cch,
        )
        .await
        .map_err(|err| format!("{err:#}"));
        app_event_tx.send(AppEvent::StartupThreadStarted { result });
    });
}
