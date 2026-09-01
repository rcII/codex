use super::*;
use crate::extensions::send_thread_warning;
use crate::realtime_event_handling::apply_realtime_event_effects;
use crate::realtime_event_handling::persist_realtime_items;
use crate::realtime_history::RealtimeEventEffects;
use codex_app_server_protocol::ThreadQueueChangedNotification;
use codex_protocol::protocol::ThreadHistoryMode;

pub(super) const THREAD_UNLOADING_DELAY: Duration = Duration::from_secs(30 * 60);

#[derive(Clone)]
pub(super) struct ListenerTaskContext {
    pub(super) thread_manager: Arc<ThreadManager>,
    pub(super) thread_state_manager: ThreadStateManager,
    pub(super) outgoing: Arc<OutgoingMessageSender>,
    pub(super) pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
    pub(super) thread_watch_manager: ThreadWatchManager,
    pub(super) thread_list_state_permit: Arc<Semaphore>,
    pub(super) fallback_model_provider: String,
    pub(super) codex_home: PathBuf,
    pub(super) skills_watcher: Arc<SkillsWatcher>,
    pub(super) turn_cost_worker: Option<crate::turn_cost_worker::TurnCostWorkerHandle>,
}

struct UnloadingState {
    delay: Duration,
    has_subscribers_rx: watch::Receiver<bool>,
    has_subscribers: (bool, Instant),
    thread_status_rx: watch::Receiver<ThreadStatus>,
    is_active: (bool, Instant),
}

impl UnloadingState {
    async fn new(
        listener_task_context: &ListenerTaskContext,
        thread_id: ThreadId,
        delay: Duration,
    ) -> Option<Self> {
        let has_subscribers_rx = listener_task_context
            .thread_state_manager
            .subscribe_to_has_connections(thread_id)
            .await?;
        let thread_status_rx = listener_task_context
            .thread_watch_manager
            .subscribe(thread_id)
            .await?;
        let has_subscribers = (*has_subscribers_rx.borrow(), Instant::now());
        let is_active = (
            matches!(*thread_status_rx.borrow(), ThreadStatus::Active { .. }),
            Instant::now(),
        );
        Some(Self {
            delay,
            has_subscribers_rx,
            has_subscribers,
            thread_status_rx,
            is_active,
        })
    }

    fn unloading_target(&self) -> Option<Instant> {
        match (self.has_subscribers, self.is_active) {
            ((false, has_no_subscribers_since), (false, is_inactive_since)) => {
                Some(std::cmp::max(has_no_subscribers_since, is_inactive_since) + self.delay)
            }
            _ => None,
        }
    }

    fn sync_receiver_values(&mut self) {
        let has_subscribers = *self.has_subscribers_rx.borrow();
        if self.has_subscribers.0 != has_subscribers {
            self.has_subscribers = (has_subscribers, Instant::now());
        }

        let is_active = matches!(*self.thread_status_rx.borrow(), ThreadStatus::Active { .. });
        if self.is_active.0 != is_active {
            self.is_active = (is_active, Instant::now());
        }
    }

    fn should_unload_now(&mut self) -> bool {
        self.sync_receiver_values();
        self.unloading_target()
            .is_some_and(|target| target <= Instant::now())
    }

    fn note_thread_activity_observed(&mut self) {
        if !self.is_active.0 {
            self.is_active = (false, Instant::now());
        }
    }

    async fn wait_for_unloading_trigger(&mut self) -> bool {
        loop {
            self.sync_receiver_values();
            let unloading_target = self.unloading_target();
            if let Some(target) = unloading_target
                && target <= Instant::now()
            {
                return true;
            }
            let unloading_sleep = async {
                if let Some(target) = unloading_target {
                    tokio::time::sleep_until(target.into()).await;
                } else {
                    futures::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = unloading_sleep => return true,
                changed = self.has_subscribers_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                },
                changed = self.thread_status_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                },
            }
        }
    }
}

pub(super) enum ThreadShutdownResult {
    Complete,
    SubmitFailed,
    TimedOut,
}

pub(super) enum EnsureConversationListenerResult {
    Attached,
    ConnectionClosed,
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "listener subscription must be serialized against pending unloads"
)]
pub(super) async fn ensure_conversation_listener(
    listener_task_context: ListenerTaskContext,
    conversation_id: ThreadId,
    connection_id: ConnectionId,
    raw_events_enabled: bool,
) -> Result<EnsureConversationListenerResult, JSONRPCErrorError> {
    let conversation = match listener_task_context
        .thread_manager
        .get_thread(conversation_id)
        .await
    {
        Ok(conv) => conv,
        Err(_) => {
            return Err(invalid_request(format!(
                "thread not found: {conversation_id}"
            )));
        }
    };
    let thread_state = {
        let pending_thread_unloads = listener_task_context.pending_thread_unloads.lock().await;
        if pending_thread_unloads.contains(&conversation_id) {
            return Err(invalid_request(format!(
                "thread {conversation_id} is closing; retry after the thread is closed"
            )));
        }
        let Some(thread_state) = listener_task_context
            .thread_state_manager
            .try_ensure_connection_subscribed(conversation_id, connection_id, raw_events_enabled)
            .await
        else {
            return Ok(EnsureConversationListenerResult::ConnectionClosed);
        };
        thread_state
    };
    if let Err(error) = ensure_listener_task_running(
        listener_task_context.clone(),
        conversation_id,
        conversation,
        thread_state,
    )
    .await
    {
        let _ = listener_task_context
            .thread_state_manager
            .unsubscribe_connection_from_thread(conversation_id, connection_id)
            .await;
        return Err(error);
    }
    Ok(EnsureConversationListenerResult::Attached)
}

pub(super) fn log_listener_attach_result(
    result: Result<EnsureConversationListenerResult, JSONRPCErrorError>,
    thread_id: ThreadId,
    connection_id: ConnectionId,
    thread_kind: &'static str,
) {
    match result {
        Ok(EnsureConversationListenerResult::Attached) => {}
        Ok(EnsureConversationListenerResult::ConnectionClosed) => {
            tracing::debug!(
                thread_id = %thread_id,
                connection_id = ?connection_id,
                "skipping auto-attach for closed connection"
            );
        }
        Err(err) => {
            tracing::warn!(
                "failed to attach listener for {thread_kind} {thread_id}: {message}",
                message = err.message
            );
        }
    }
}

pub(super) async fn ensure_listener_task_running(
    listener_task_context: ListenerTaskContext,
    conversation_id: ThreadId,
    conversation: Arc<CodexThread>,
    thread_state: Arc<Mutex<ThreadState>>,
) -> Result<(), JSONRPCErrorError> {
    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    let Some(mut unloading_state) = UnloadingState::new(
        &listener_task_context,
        conversation_id,
        THREAD_UNLOADING_DELAY,
    )
    .await
    else {
        return Err(invalid_request(format!(
            "thread {conversation_id} is closing; retry after the thread is closed"
        )));
    };
    let config = conversation.config().await;
    let environments = conversation.environment_selections().await;
    let watch_registration = listener_task_context
        .skills_watcher
        .register_thread_config(
            config.as_ref(),
            listener_task_context.thread_manager.as_ref(),
            &environments,
        )
        .await;
    let config_snapshot = conversation.config_snapshot().await;
    let realtime_history_enabled =
        matches!(config_snapshot.history_mode, ThreadHistoryMode::Paginated);
    let thread_settings_baseline = thread_settings_from_config_snapshot(&config_snapshot);
    let (mut listener_command_rx, listener_generation) = {
        let mut thread_state = thread_state.lock().await;
        if thread_state.listener_matches(&conversation) {
            return Ok(());
        }
        let (listener_command_rx, listener_generation) = thread_state.set_listener(
            cancel_tx,
            &conversation,
            watch_registration,
            thread_settings_baseline,
        );
        let Some(listener_command_tx) = thread_state.listener_command_tx() else {
            tracing::warn!(
                "thread listener command sender missing immediately after listener registration"
            );
            return Ok(());
        };
        listener_task_context
            .thread_state_manager
            .register_listener_command_tx(conversation_id, listener_command_tx);
        (listener_command_rx, listener_generation)
    };
    let ListenerTaskContext {
        outgoing,
        thread_manager,
        thread_state_manager,
        pending_thread_unloads,
        thread_watch_manager,
        thread_list_state_permit,
        fallback_model_provider,
        codex_home,
        turn_cost_worker,
        ..
    } = listener_task_context;
    let outgoing_for_task = Arc::clone(&outgoing);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => {
                    // Listener was superseded or the thread is being torn down.
                    break;
                }
                listener_command = listener_command_rx.recv() => {
                    let Some(listener_command) = listener_command else {
                        break;
                    };
                    handle_thread_listener_command(
                        conversation_id,
                        &conversation,
                        codex_home.as_path(),
                        &thread_state_manager,
                        &thread_state,
                        &thread_watch_manager,
                        &outgoing_for_task,
                        &pending_thread_unloads,
                        listener_command,
                    )
                    .await;
                }
                event = conversation.next_event() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(err) => {
                            tracing::warn!("thread.next_event() failed with: {err}");
                            break;
                        }
                    };

                    if let Some(worker) = &turn_cost_worker {
                        worker.observe_event(
                            conversation_id,
                            config.as_ref(),
                            &event,
                            || conversation.session_telemetry(),
                        );
                    }

                    // Track the event before emitting any typed translations
                    // so thread-local state such as raw event opt-in stays
                    // synchronized with the conversation.
                    let (raw_events_enabled, realtime_effects) = {
                        let mut thread_state = thread_state.lock().await;
                        thread_state.track_current_turn_event(&event.id, &event.msg);
                        let realtime_effects = if realtime_history_enabled
                            && thread_state.realtime_history.should_observe(&event.msg)
                        {
                            let active_turn_id = thread_state.active_turn_snapshot().map(|turn| turn.id);
                            thread_state
                                .realtime_history
                                .observe(&event.msg, active_turn_id.as_deref())
                        } else {
                            RealtimeEventEffects::default()
                        };
                        (thread_state.experimental_raw_events, realtime_effects)
                    };
                    if matches!(
                        &event.msg,
                        EventMsg::RawResponseItem(_) | EventMsg::RawResponseCompleted(_)
                    ) && !raw_events_enabled
                    {
                        continue;
                    }
                    let subscribed_connection_ids = thread_state_manager
                        .subscribed_connection_ids(conversation_id)
                        .await;
                    let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
                        outgoing_for_task.clone(),
                        subscribed_connection_ids,
                        conversation_id,
                    );

                    apply_realtime_event_effects(
                        conversation.as_ref(),
                        &thread_outgoing,
                        conversation_id,
                        realtime_effects,
                    )
                    .await;

                    apply_bespoke_event_handling(
                        event.clone(),
                        conversation_id,
                        conversation.clone(),
                        thread_manager.clone(),
                        thread_outgoing,
                        thread_state.clone(),
                        thread_watch_manager.clone(),
                        thread_list_state_permit.clone(),
                        fallback_model_provider.clone(),
                    )
                    .await;
                    if matches!(event.msg, EventMsg::ShutdownComplete)
                        && let Some(completion_tx) = thread_state
                            .lock()
                            .await
                            .take_shutdown_drain_waiter()
                    {
                        let _ = completion_tx.send(());
                    }
                }
                unloading_watchers_open = unloading_state.wait_for_unloading_trigger() => {
                    if !unloading_watchers_open {
                        break;
                    }
                    if !unloading_state.should_unload_now() {
                        continue;
                    }
                    if matches!(conversation.agent_status().await, AgentStatus::Running) {
                        unloading_state.note_thread_activity_observed();
                        continue;
                    }
                    {
                        let mut pending_thread_unloads = pending_thread_unloads.lock().await;
                        if pending_thread_unloads.contains(&conversation_id) {
                            continue;
                        }
                        if !unloading_state.should_unload_now() {
                            continue;
                        }
                        pending_thread_unloads.insert(conversation_id);
                    }
                    unload_thread_without_subscribers(
                        thread_manager.clone(),
                        outgoing_for_task.clone(),
                        pending_thread_unloads.clone(),
                        thread_state_manager.clone(),
                        thread_watch_manager.clone(),
                        conversation_id,
                        conversation.clone(),
                    )
                    .await;
                    break;
                }
            }
        }

        let mut thread_state = thread_state.lock().await;
        if thread_state.listener_generation == listener_generation {
            thread_state_manager.unregister_listener_command_tx(conversation_id);
            thread_state.clear_listener();
        }
    });
    Ok(())
}

pub(super) async fn wait_for_thread_shutdown(thread: &Arc<CodexThread>) -> ThreadShutdownResult {
    match tokio::time::timeout(Duration::from_secs(10), thread.shutdown_and_wait()).await {
        Ok(Ok(())) => ThreadShutdownResult::Complete,
        Ok(Err(_)) => ThreadShutdownResult::SubmitFailed,
        Err(_) => ThreadShutdownResult::TimedOut,
    }
}

pub(super) async fn unload_thread_without_subscribers(
    thread_manager: Arc<ThreadManager>,
    outgoing: Arc<OutgoingMessageSender>,
    pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
    thread_state_manager: ThreadStateManager,
    thread_watch_manager: ThreadWatchManager,
    thread_id: ThreadId,
    thread: Arc<CodexThread>,
) {
    info!("thread {thread_id} has no subscribers and is idle; shutting down");

    // Any pending app-server -> client requests for this thread can no longer be
    // answered; cancel their callbacks before shutdown/unload.
    outgoing
        .cancel_requests_for_thread(thread_id, /*error*/ None)
        .await;
    thread_state_manager.remove_thread_state(thread_id).await;

    tokio::spawn(async move {
        match wait_for_thread_shutdown(&thread).await {
            ThreadShutdownResult::Complete => {
                // A delayed unload can finish after thread/revert replaces this runtime under
                // the same thread ID. Only the runtime that scheduled this unload may remove it.
                if thread_manager
                    .remove_thread_if_matches(&thread_id, &thread)
                    .await
                    .is_none()
                {
                    info!("thread {thread_id} was replaced or removed before teardown finalized");
                    pending_thread_unloads.lock().await.remove(&thread_id);
                    return;
                }
                thread_watch_manager
                    .remove_thread(&thread_id.to_string())
                    .await;
                let notification = ThreadClosedNotification {
                    thread_id: thread_id.to_string(),
                };
                outgoing
                    .send_server_notification(ServerNotification::ThreadClosed(notification))
                    .await;
                pending_thread_unloads.lock().await.remove(&thread_id);
            }
            ThreadShutdownResult::SubmitFailed => {
                pending_thread_unloads.lock().await.remove(&thread_id);
                warn!("failed to submit Shutdown to thread {thread_id}");
            }
            ThreadShutdownResult::TimedOut => {
                pending_thread_unloads.lock().await.remove(&thread_id);
                warn!("thread {thread_id} shutdown timed out; leaving thread loaded");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_thread_listener_command(
    conversation_id: ThreadId,
    conversation: &Arc<CodexThread>,
    codex_home: &Path,
    thread_state_manager: &ThreadStateManager,
    thread_state: &Arc<Mutex<ThreadState>>,
    thread_watch_manager: &ThreadWatchManager,
    outgoing: &Arc<OutgoingMessageSender>,
    pending_thread_unloads: &Arc<Mutex<HashSet<ThreadId>>>,
    listener_command: ThreadListenerCommand,
) {
    match listener_command {
        ThreadListenerCommand::SendThreadResumeResponse(resume_request) => {
            super::thread_history::handle_pending_thread_resume_request(
                conversation_id,
                conversation,
                codex_home,
                thread_state_manager,
                thread_state,
                thread_watch_manager,
                outgoing,
                pending_thread_unloads,
                *resume_request,
            )
            .await;
        }
        ThreadListenerCommand::EmitThreadGoalUpdated { turn_id, goal } => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalUpdated(
                    ThreadGoalUpdatedNotification {
                        thread_id: conversation_id.to_string(),
                        turn_id,
                        goal,
                    },
                ))
                .await;
        }
        ThreadListenerCommand::EmitThreadQueueChanged => {
            let subscribed_connection_ids = thread_state_manager
                .subscribed_connection_ids(conversation_id)
                .await;
            let outgoing = ThreadScopedOutgoingMessageSender::new(
                Arc::clone(outgoing),
                subscribed_connection_ids,
                conversation_id,
            );
            outgoing
                .send_server_notification(ServerNotification::ThreadQueueChanged(
                    ThreadQueueChangedNotification {
                        thread_id: conversation_id.to_string(),
                    },
                ))
                .await;
        }
        ThreadListenerCommand::EmitWarning { message } => {
            send_thread_warning(outgoing, thread_state_manager, conversation_id, message).await;
        }
        ThreadListenerCommand::EmitThreadGoalCleared => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalCleared(
                    ThreadGoalClearedNotification {
                        thread_id: conversation_id.to_string(),
                    },
                ))
                .await;
        }
        ThreadListenerCommand::EmitThreadGoalSnapshot { state_db } => {
            send_thread_goal_snapshot_notification(outgoing, conversation_id, &state_db).await;
        }
        ThreadListenerCommand::ResolveServerRequest {
            request_id,
            completion_tx,
        } => {
            resolve_pending_server_request(
                conversation_id,
                thread_state_manager,
                outgoing,
                request_id,
            )
            .await;
            let _ = completion_tx.send(());
        }
        ThreadListenerCommand::SealRealtimeUserInput {
            input,
            completion_tx,
        } => {
            let items = thread_state
                .lock()
                .await
                .realtime_history
                .seal_user_input(&input);
            let subscribed_connection_ids = thread_state_manager
                .subscribed_connection_ids(conversation_id)
                .await;
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
                outgoing.clone(),
                subscribed_connection_ids,
                conversation_id,
            );
            let result = persist_realtime_items(
                conversation.as_ref(),
                &thread_outgoing,
                &conversation_id.to_string(),
                items,
            )
            .await;
            let _ = completion_tx.send(result);
        }
    }
}

pub(super) async fn send_thread_goal_snapshot_notification(
    outgoing: &Arc<OutgoingMessageSender>,
    thread_id: ThreadId,
    state_db: &StateDbHandle,
) {
    match state_db.thread_goals().get_thread_goal(thread_id).await {
        Ok(Some(goal)) => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalUpdated(
                    ThreadGoalUpdatedNotification {
                        thread_id: thread_id.to_string(),
                        turn_id: None,
                        goal: api_thread_goal_from_state(goal),
                    },
                ))
                .await;
        }
        Ok(None) => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalCleared(
                    ThreadGoalClearedNotification {
                        thread_id: thread_id.to_string(),
                    },
                ))
                .await;
        }
        Err(err) => {
            tracing::warn!(
                thread_id = %thread_id,
                "failed to read thread goal for resume snapshot: {err}"
            );
        }
    }
}

pub(crate) fn populate_thread_turns_from_history(
    thread: &mut Thread,
    items: &[RolloutItem],
    active_turn: Option<&Turn>,
) {
    let mut turns = build_legacy_api_turns_from_rollout_items(items);
    if let Some(active_turn) = active_turn {
        merge_turn_history_with_active_turn(&mut turns, active_turn.clone());
    }
    thread.turns = turns;
}

pub(super) async fn resolve_pending_server_request(
    conversation_id: ThreadId,
    thread_state_manager: &ThreadStateManager,
    outgoing: &Arc<OutgoingMessageSender>,
    request_id: RequestId,
) {
    let thread_id = conversation_id.to_string();
    let subscribed_connection_ids = thread_state_manager
        .subscribed_connection_ids(conversation_id)
        .await;
    let outgoing = ThreadScopedOutgoingMessageSender::new(
        outgoing.clone(),
        subscribed_connection_ids,
        conversation_id,
    );
    outgoing
        .send_server_notification(ServerNotification::ServerRequestResolved(
            ServerRequestResolvedNotification {
                thread_id,
                request_id,
            },
        ))
        .await;
}

pub(super) fn merge_turn_history_with_active_turn(turns: &mut Vec<Turn>, active_turn: Turn) {
    turns.retain(|turn| turn.id != active_turn.id);
    turns.push(active_turn);
}

pub(super) fn merge_active_turn_into_page(
    page: &mut codex_app_server_protocol::TurnsPage,
    mut active_turn: Turn,
    params: &codex_app_server_protocol::ThreadResumeInitialTurnsPageParams,
) {
    super::thread_processor::apply_thread_turns_items_view(
        std::slice::from_mut(&mut active_turn),
        params.items_view.unwrap_or(TurnItemsView::Summary),
    );
    let sort_direction = params.sort_direction.unwrap_or(SortDirection::Desc);
    let page_size = super::thread_processor::thread_turns_page_size(params.limit);
    let active_turn_is_in_page = page.data.iter().any(|turn| turn.id == active_turn.id);
    page.data.retain(|turn| turn.id != active_turn.id);
    match sort_direction {
        SortDirection::Asc
            if active_turn_is_in_page
                || (page.data.len() < page_size && page.next_cursor.is_none()) =>
        {
            page.data.push(active_turn);
        }
        SortDirection::Asc => {}
        SortDirection::Desc => page.data.insert(0, active_turn),
    }
}

pub(super) fn set_thread_status_and_interrupt_stale_turns(
    thread: &mut Thread,
    loaded_status: ThreadStatus,
    has_live_in_progress_turn: bool,
) {
    let status = resolve_thread_status(loaded_status, has_live_in_progress_turn);
    if !matches!(status, ThreadStatus::Active { .. }) {
        for turn in &mut thread.turns {
            if matches!(turn.status, TurnStatus::InProgress) {
                turn.status = TurnStatus::Interrupted;
            }
        }
    }
    thread.status = status;
}
