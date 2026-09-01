use super::thread_lifecycle::merge_active_turn_into_page;
use super::thread_lifecycle::merge_turn_history_with_active_turn;
use super::thread_lifecycle::populate_thread_turns_from_history;
use super::thread_lifecycle::send_thread_goal_snapshot_notification;
use super::thread_lifecycle::set_thread_status_and_interrupt_stale_turns;
use super::thread_processor::THREAD_ITEMS_DEFAULT_LIMIT;
use super::thread_processor::THREAD_ITEMS_MAX_LIMIT;
use super::thread_processor::THREAD_TURNS_MAX_LIMIT;
use super::thread_processor::ThreadRequestProcessor;
use super::thread_processor::normalize_thread_turns_status;
use super::thread_processor::paginated_history_list_error;
use super::thread_processor::thread_turns_page_size;
use super::thread_processor::unsupported_thread_store_operation;
use super::*;
use crate::error_code::method_not_found;
use codex_extension_api::ThreadIdleCause;
use codex_protocol::config_types::MultiAgentMode;

impl ThreadRequestProcessor {
    pub(super) async fn thread_turns_list_paginated_response_inner(
        &self,
        params: ThreadTurnsListParams,
    ) -> Result<ThreadTurnsListPaginatedResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        if params.items_view != Some(TurnItemsView::NotLoaded) {
            return Err(invalid_request(
                "strict paginated turn reads require itemsView=notLoaded",
            ));
        }
        let snapshot = self.strict_paginated_history_revision(thread_id).await?;
        if snapshot.source_high_water_ordinal == 0 && params.cursor.is_none() {
            return Ok(ThreadTurnsListPaginatedResponse {
                data: Vec::new(),
                next_cursor: None,
                backwards_cursor: None,
                history_revision: snapshot.revision,
                source_high_water_ordinal: 0,
            });
        }
        let (page, source_ordinals) = self
            .paginated_thread_turns_list_response(
                thread_id,
                params.cursor,
                params.limit,
                params.sort_direction,
                params.items_view,
            )
            .await?;
        if self.strict_paginated_history_revision(thread_id).await? != snapshot {
            return Err(invalid_request(
                "paginated thread history changed while the page was being read",
            ));
        }
        let data = page
            .data
            .into_iter()
            .zip(source_ordinals)
            .map(|(turn, source_ordinal)| {
                if source_ordinal > snapshot.source_high_water_ordinal {
                    return Err(internal_error(
                        "strict turn occurrence exceeds its durable source high-water ordinal",
                    ));
                }
                Ok(codex_app_server_protocol::ThreadTurnHistoryEntry {
                    turn,
                    history_revision: snapshot.revision.clone(),
                    source_ordinal,
                    source_subordinal: 0,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ThreadTurnsListPaginatedResponse {
            data,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
            history_revision: snapshot.revision,
            source_high_water_ordinal: snapshot.source_high_water_ordinal,
        })
    }

    pub(super) async fn paginated_thread_turns_list_response(
        &self,
        thread_id: ThreadId,
        cursor: Option<String>,
        limit: Option<u32>,
        sort_direction: Option<SortDirection>,
        items_view: Option<TurnItemsView>,
    ) -> Result<(ThreadTurnsListResponse, Vec<u64>), JSONRPCErrorError> {
        let items_view = items_view.unwrap_or(TurnItemsView::Summary);
        let page_size = thread_turns_page_size(limit);
        let sort_direction = match sort_direction.unwrap_or(SortDirection::Desc) {
            SortDirection::Asc => StoreSortDirection::Asc,
            SortDirection::Desc => StoreSortDirection::Desc,
        };
        // `Full` is only a temporary compatibility path. Keep it out of ThreadStore's API:
        // load turn shells here, then hydrate their items below.
        let stored_items_view = match items_view {
            TurnItemsView::NotLoaded => StoredTurnItemsView::NotLoaded,
            TurnItemsView::Summary => StoredTurnItemsView::Summary,
            TurnItemsView::Full => StoredTurnItemsView::NotLoaded,
        };
        let page = self
            .thread_store
            .list_turns(StoreListTurnsParams {
                thread_id,
                include_archived: true,
                cursor,
                page_size,
                sort_direction,
                items_view: stored_items_view,
            })
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::Unsupported { operation } => {
                    unsupported_thread_store_operation(operation)
                }
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                err => internal_error(format!("failed to list thread history: {err}")),
            })?;
        let mut turns = Vec::with_capacity(page.turns.len());
        let mut source_ordinals = Vec::with_capacity(page.turns.len());
        for stored_turn in page.turns {
            let source_ordinal = stored_turn.source_ordinal;
            let mut turn = stored_turn_to_api_turn(stored_turn, items_view)?;
            if matches!(items_view, TurnItemsView::Full) {
                turn.items = self
                    .paginated_turn_full_items(thread_id, turn.id.as_str())
                    .await?;
            }
            turns.push(turn);
            source_ordinals.push(source_ordinal);
        }
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let has_live_running_thread = match loaded_thread.as_ref() {
            Some(thread) => matches!(thread.agent_status().await, AgentStatus::Running),
            None => false,
        };
        normalize_thread_turns_status(
            &mut turns,
            self.thread_watch_manager
                .loaded_status_for_thread(&thread_id.to_string())
                .await,
            has_live_running_thread,
        );
        Ok((
            ThreadTurnsListResponse {
                data: turns,
                next_cursor: page.next_cursor,
                backwards_cursor: page.backwards_cursor,
            },
            source_ordinals,
        ))
    }

    // Older clients still request `itemsView: "full"` from turn pages. Keep this
    // app-server-only hydration path until those clients use `thread/items/list`.
    pub(super) async fn paginated_turn_full_items(
        &self,
        thread_id: ThreadId,
        turn_id: &str,
    ) -> Result<Vec<ThreadItem>, JSONRPCErrorError> {
        let mut cursor = None;
        let mut items = Vec::new();
        loop {
            let page = self
                .thread_store
                .list_items(StoreListItemsParams {
                    thread_id,
                    turn_id: Some(turn_id.to_string()),
                    include_archived: true,
                    cursor: cursor.clone(),
                    page_size: THREAD_ITEMS_MAX_LIMIT,
                    sort_direction: StoreSortDirection::Asc,
                    sort_key: StoreItemSortKey::CreatedAtOrdinal,
                    after_updated_at_ordinal: None,
                })
                .await
                .map_err(paginated_history_list_error)?;
            for item in page.items {
                items.push(deserialize_stored_thread_item(&item)?);
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(items);
            };
            if cursor.as_ref() == Some(&next_cursor) {
                return Err(internal_error(format!(
                    "failed to load full turn items for {turn_id}: thread store returned a repeated cursor"
                )));
            }
            cursor = Some(next_cursor);
        }
    }

    // Older clients expect full `thread.turns` from resume and `thread/read(includeTurns=true)`.
    // Keep this slow compatibility path until all clients page history directly.
    pub(super) async fn paginated_thread_full_turns(
        &self,
        thread_id: ThreadId,
    ) -> Result<Vec<Turn>, JSONRPCErrorError> {
        let mut cursor = None;
        let mut turns = Vec::new();
        loop {
            let page = self
                .paginated_thread_turns_list_response(
                    thread_id,
                    cursor.clone(),
                    Some(THREAD_TURNS_MAX_LIMIT as u32),
                    Some(SortDirection::Asc),
                    Some(TurnItemsView::Full),
                )
                .await?;
            let (page, _) = page;
            turns.extend(page.data);
            let Some(next_cursor) = page.next_cursor else {
                return Ok(turns);
            };
            if cursor.as_ref() == Some(&next_cursor) {
                return Err(internal_error(format!(
                    "failed to load full thread turns for {thread_id}: thread store returned a repeated cursor"
                )));
            }
            cursor = Some(next_cursor);
        }
    }

    pub(super) async fn paginated_resume_initial_turns_page(
        &self,
        thread_id: ThreadId,
        params: &ThreadResumeInitialTurnsPageParams,
    ) -> Result<codex_app_server_protocol::TurnsPage, JSONRPCErrorError> {
        self.paginated_thread_turns_list_response(
            thread_id,
            /*cursor*/ None,
            params.limit,
            params.sort_direction,
            params.items_view,
        )
        .await
        .map(|(page, _)| page.into())
    }

    pub(super) async fn paginated_resume_initial_turns_page_with_active_slot(
        &self,
        thread_id: ThreadId,
        params: &ThreadResumeInitialTurnsPageParams,
    ) -> Result<codex_app_server_protocol::TurnsPage, JSONRPCErrorError> {
        // A running resume overlays the newest live turn on this durable page.
        // Reserve one row so the overlay keeps the requested limit and the
        // durable next cursor still starts after the last returned stored turn.
        let page_size = thread_turns_page_size(params.limit);
        if page_size == 1 {
            // ThreadStore does not accept an empty page. Use its backwards cursor as
            // the next cursor so the omitted durable row is returned next.
            let mut page = self
                .paginated_resume_initial_turns_page(thread_id, params)
                .await?;
            page.next_cursor = page.backwards_cursor.clone();
            page.data.clear();
            return Ok(page);
        }

        let mut params = params.clone();
        params.limit = Some((page_size - 1) as u32);
        self.paginated_resume_initial_turns_page(thread_id, &params)
            .await
    }

    pub(super) async fn paginated_resume_backwards_cursors(
        thread_store: &dyn ThreadStore,
        thread_id: ThreadId,
    ) -> Result<(Option<String>, Option<String>), JSONRPCErrorError> {
        let turns_page = thread_store
            .list_turns(StoreListTurnsParams {
                thread_id,
                include_archived: true,
                cursor: None,
                page_size: 1,
                sort_direction: StoreSortDirection::Desc,
                items_view: StoredTurnItemsView::NotLoaded,
            })
            .await
            .map_err(paginated_history_list_error)?;
        let items_page = thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id: None,
                include_archived: true,
                cursor: None,
                page_size: 1,
                sort_direction: StoreSortDirection::Desc,
                sort_key: StoreItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .map_err(paginated_history_list_error)?;
        Ok((turns_page.backwards_cursor, items_page.backwards_cursor))
    }

    pub(super) async fn thread_items_list_response_inner(
        &self,
        params: ThreadItemsListParams,
    ) -> Result<ThreadItemsListResponse, JSONRPCErrorError> {
        let ThreadItemsListParams {
            thread_id,
            turn_id,
            cursor,
            limit,
            sort_direction,
        } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_ITEMS_DEFAULT_LIMIT)
            .clamp(1, THREAD_ITEMS_MAX_LIMIT);
        let page = self
            .thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id,
                include_archived: true,
                cursor,
                page_size,
                sort_direction: match sort_direction.unwrap_or(SortDirection::Asc) {
                    SortDirection::Asc => StoreSortDirection::Asc,
                    SortDirection::Desc => StoreSortDirection::Desc,
                },
                sort_key: StoreItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::Unsupported { .. } => {
                    method_not_found("thread/items/list is not supported yet")
                }
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                err => internal_error(format!("failed to list thread items: {err}")),
            })?;
        let data = page
            .items
            .into_iter()
            .map(|stored_item| {
                let turn_id = stored_item.turn_id.clone();
                let item = deserialize_stored_thread_item(&stored_item)?;
                Ok(ThreadItemEntry { turn_id, item })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ThreadItemsListResponse {
            data,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
        })
    }

    pub(super) async fn thread_items_list_paginated_response_inner(
        &self,
        params: ThreadItemsListParams,
    ) -> Result<ThreadItemsListPaginatedResponse, JSONRPCErrorError> {
        let ThreadItemsListParams {
            thread_id,
            turn_id,
            cursor,
            limit,
            sort_direction,
        } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let turn_id = turn_id
            .ok_or_else(|| invalid_request("strict paginated item reads require a turnId"))?;
        if matches!(sort_direction, Some(SortDirection::Desc)) {
            return Err(invalid_request(
                "strict paginated item reads require ascending source order",
            ));
        }
        let snapshot = self.strict_paginated_history_revision(thread_id).await?;
        let page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_ITEMS_DEFAULT_LIMIT)
            .clamp(1, THREAD_ITEMS_MAX_LIMIT);
        let page = self
            .thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id: Some(turn_id),
                include_archived: true,
                cursor,
                page_size,
                sort_direction: StoreSortDirection::Asc,
                sort_key: StoreItemSortKey::UpdatedAtOrdinal,
                after_updated_at_ordinal: Some(0),
            })
            .await
            .map_err(paginated_history_list_error)?;
        if self.strict_paginated_history_revision(thread_id).await? != snapshot {
            return Err(invalid_request(
                "paginated thread history changed while the page was being read",
            ));
        }
        let data = page
            .items
            .into_iter()
            .map(|stored_item| {
                let source_ordinal = stored_item.updated_at_ordinal;
                if source_ordinal > snapshot.source_high_water_ordinal {
                    return Err(internal_error(
                        "strict item occurrence exceeds its durable source high-water ordinal",
                    ));
                }
                let turn_id = stored_item.turn_id.clone();
                let completed_at_ms = stored_item.completed_at_ms;
                let item = deserialize_stored_thread_item(&stored_item)?;
                Ok(codex_app_server_protocol::ThreadItemHistoryEntry {
                    entry: ThreadItemEntry { turn_id, item },
                    completed_at_ms,
                    history_revision: snapshot.revision.clone(),
                    source_ordinal,
                    source_subordinal: 0,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ThreadItemsListPaginatedResponse {
            data,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
            history_revision: snapshot.revision,
            source_high_water_ordinal: snapshot.source_high_water_ordinal,
        })
    }

    pub(super) async fn strict_paginated_history_revision(
        &self,
        thread_id: ThreadId,
    ) -> Result<codex_thread_store::StrictHistorySnapshot, JSONRPCErrorError> {
        self.thread_store
            .strict_paginated_history_revision(thread_id)
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                ThreadStoreError::Unsupported { operation } => {
                    unsupported_thread_store_operation(operation)
                }
                err => internal_error(format!("failed to read thread: {err}")),
            })
    }
}
fn deserialize_stored_thread_item(
    item: &codex_thread_store::StoredThreadItem,
) -> Result<ThreadItem, JSONRPCErrorError> {
    serde_json::from_slice::<ThreadItem>(&item.item_json).map_err(|err| {
        internal_error(format!(
            "failed to deserialize stored thread item {}: {err}",
            item.item_id
        ))
    })
}

fn stored_turn_to_api_turn(
    turn: StoredTurn,
    items_view: TurnItemsView,
) -> Result<Turn, JSONRPCErrorError> {
    let status = match turn.status {
        StoredTurnStatus::Completed => TurnStatus::Completed,
        StoredTurnStatus::Interrupted => TurnStatus::Interrupted,
        StoredTurnStatus::Failed => TurnStatus::Failed,
        StoredTurnStatus::InProgress => TurnStatus::InProgress,
    };
    let error = turn.error.map(|error| TurnError {
        misalignment: None,
        message: error.message,
        codex_error_info: error.codex_error_info,
        additional_details: error.additional_details,
    });
    let items = turn
        .items
        .into_iter()
        .map(|item| deserialize_stored_thread_item(&item))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Turn {
        id: turn.turn_id,
        items,
        items_view,
        status,
        error,
        started_at: turn.started_at,
        completed_at: turn.completed_at,
        duration_ms: turn.duration_ms,
    })
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "running-thread resume subscription must be serialized against pending unloads"
)]
pub(super) async fn handle_pending_thread_resume_request(
    conversation_id: ThreadId,
    conversation: &Arc<CodexThread>,
    _codex_home: &Path,
    thread_state_manager: &ThreadStateManager,
    thread_state: &Arc<Mutex<ThreadState>>,
    thread_watch_manager: &ThreadWatchManager,
    outgoing: &Arc<OutgoingMessageSender>,
    pending_thread_unloads: &Arc<Mutex<HashSet<ThreadId>>>,
    mut pending: crate::thread_state::PendingThreadResumeRequest,
) {
    let active_turn = {
        let state = thread_state.lock().await;
        state.active_turn_snapshot()
    };
    tracing::debug!(
        thread_id = %conversation_id,
        request_id = ?pending.request_id,
        active_turn_present = active_turn.is_some(),
        active_turn_id = ?active_turn.as_ref().map(|turn| turn.id.as_str()),
        active_turn_status = ?active_turn.as_ref().map(|turn| &turn.status),
        "composing running thread resume response"
    );
    let has_live_in_progress_turn =
        matches!(conversation.agent_status().await, AgentStatus::Running)
            || active_turn
                .as_ref()
                .is_some_and(|turn| matches!(turn.status, TurnStatus::InProgress));

    let request_id = pending.request_id;
    let connection_id = request_id.connection_id;
    let mut thread = pending.thread_summary;
    if pending.include_turns {
        if let Some(turns) = pending.paginated_turns.take() {
            thread.turns = turns;
        } else {
            populate_thread_turns_from_history(
                &mut thread,
                &pending.history_items,
                /*active_turn*/ None,
            );
        }
        if let Some(active_turn) = active_turn.as_ref() {
            merge_turn_history_with_active_turn(&mut thread.turns, active_turn.clone());
        }
    }

    let thread_status = thread_watch_manager
        .loaded_status_for_thread(&thread.id)
        .await;

    set_thread_status_and_interrupt_stale_turns(
        &mut thread,
        thread_status.clone(),
        has_live_in_progress_turn,
    );
    let mut initial_turns_page = if let Some(mut page) = pending.paginated_initial_turns_page.take()
    {
        if let (Some(active_turn), Some(params)) =
            (active_turn, pending.initial_turns_page.as_ref())
        {
            let sort_direction = params.sort_direction.unwrap_or(SortDirection::Desc);
            let active_turn_is_in_page = page.data.iter().any(|turn| turn.id == active_turn.id);
            if matches!(sort_direction, SortDirection::Desc)
                && !active_turn_is_in_page
                && let Some(page_with_active_slot) =
                    pending.paginated_initial_turns_page_with_active_slot.take()
            {
                page = page_with_active_slot;
            }
            merge_active_turn_into_page(&mut page, active_turn, params);
        }
        super::thread_processor::normalize_thread_turns_status(
            &mut page.data,
            thread_status,
            has_live_in_progress_turn,
        );
        Some(page)
    } else if let Some(params) = pending.initial_turns_page.as_ref() {
        match super::thread_processor::build_thread_resume_initial_turns_page(
            &pending.history_items,
            thread.status.clone(),
            has_live_in_progress_turn,
            active_turn,
            params,
        ) {
            Ok(page) => Some(page),
            Err(error) => {
                outgoing.send_error(request_id, error).await;
                return;
            }
        }
    } else {
        None
    };
    let token_usage_turn_id = pending.cold_resume_token_usage_turn_id.or_else(|| {
        pending
            .include_turns
            .then(|| restored_token_usage_turn_id(&pending.history_items, thread.turns.as_slice()))
    });
    if pending.initial_turns_page.is_none() {
        initial_turns_page = None;
    }
    if pending.redact_resume_payloads {
        redact_thread_resume_payloads(&mut thread.turns);
        if let Some(initial_turns_page) = initial_turns_page.as_mut() {
            redact_thread_resume_payloads(&mut initial_turns_page.data);
        }
    }

    {
        let pending_thread_unloads = pending_thread_unloads.lock().await;
        if pending_thread_unloads.contains(&conversation_id) {
            drop(pending_thread_unloads);
            outgoing
                .send_error(
                    request_id,
                    invalid_request(format!(
                        "thread {conversation_id} is closing; retry thread/resume after the thread is closed"
                    )),
                )
                .await;
            return;
        }
        if !thread_state_manager
            .try_add_connection_to_thread(conversation_id, connection_id)
            .await
        {
            tracing::debug!(
                thread_id = %conversation_id,
                connection_id = ?connection_id,
                "skipping running thread resume for closed connection"
            );
            return;
        }
    }

    let (turns_backwards_cursor, items_backwards_cursor) = if let Some(thread_store) =
        pending.resume_cursor_store.as_ref()
    {
        match super::thread_processor::ThreadRequestProcessor::paginated_resume_backwards_cursors(
            thread_store.as_ref(),
            conversation_id,
        )
        .await
        {
            Ok(cursors) => cursors,
            Err(error) => {
                outgoing.send_error(request_id, error).await;
                return;
            }
        }
    } else {
        (None, None)
    };

    let config_snapshot = pending.config_snapshot;
    let sandbox = config_snapshot.sandbox_policy().into();
    let cwd = config_snapshot.cwd().clone();
    let ThreadConfigSnapshot {
        model,
        model_provider_id,
        service_tier,
        approval_policy,
        approvals_reviewer,
        active_permission_profile,
        workspace_roots,
        reasoning_effort,
        originator,
        ..
    } = config_snapshot;
    let instruction_sources = pending.instruction_sources;
    let active_permission_profile =
        thread_response_active_permission_profile(active_permission_profile);
    let session_id = conversation.session_configured().session_id.to_string();
    thread.session_id = session_id;

    let response = ThreadResumeResponse {
        thread,
        model,
        model_provider: model_provider_id,
        service_tier,
        cwd,
        runtime_workspace_roots: workspace_roots,
        instruction_sources,
        approval_policy: approval_policy.into(),
        approvals_reviewer: approvals_reviewer.into(),
        sandbox,
        active_permission_profile,
        reasoning_effort,
        multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        initial_turns_page,
        turns_backwards_cursor,
        items_backwards_cursor,
    };
    outgoing
        .send_response_with_thread_originator(request_id, response, originator)
        .await;
    // Warm metadata-only resumes skip history reconstruction. Cold paginated children can
    // replay usage using attribution captured before the listener was attached.
    if let Some(token_usage_turn_id) = token_usage_turn_id {
        // Rejoining a loaded thread has the same UI contract as a cold resume, but
        // uses the live conversation state instead of reconstructing a new session.
        send_thread_token_usage_update_to_connection(
            outgoing,
            connection_id,
            conversation_id,
            conversation.as_ref(),
            token_usage_turn_id,
        )
        .await;
    }
    if pending.emit_thread_goal_update {
        if let Some(state_db) = pending.thread_goal_state_db {
            send_thread_goal_snapshot_notification(outgoing, conversation_id, &state_db).await;
        } else {
            tracing::warn!(
                thread_id = %conversation_id,
                "state db unavailable when reading thread goal for running thread resume"
            );
        }
    }
    outgoing
        .replay_requests_to_connection_for_thread(connection_id, conversation_id)
        .await;
    // App-server owns resume response and snapshot ordering, so wait until
    // replay completes before letting extensions react to the idle thread.
    conversation
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
}
