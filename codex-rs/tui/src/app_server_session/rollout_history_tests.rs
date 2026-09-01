use super::super::ResumeModelSettings;
use super::HistoryCaptureContext;
use super::HistoryOutcome;
use super::HistoryPrelude;
use super::HistoryStatus;
use super::StrictHistorySnapshot;
use super::verify_history_outcome;
use crate::legacy_core::config::Config;
use crate::legacy_core::config::ConfigBuilder;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_features::Feature;
use codex_protocol::ThreadId;
use color_eyre::eyre::Result;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

async fn build_config(temp_dir: &TempDir) -> Config {
    ConfigBuilder::default()
        .codex_home(temp_dir.path().to_path_buf())
        .build()
        .await
        .expect("config should build")
}

#[tokio::test]
async fn legacy_resume_fails_closed_after_picker_server_replacement() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut picker_app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let history_mode = picker_app_server
        .thread_read(thread_id, /*include_turns*/ false)
        .await?
        .history_mode;
    picker_app_server.shutdown().await?;

    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, history_mode);
    let next_request_id = app_server.next_request_id;
    let error = app_server
        .resume_thread(config, thread_id, ResumeModelSettings::RestoreFromThread)
        .await
        .expect_err("legacy history must fail closed");

    assert_eq!(app_server.next_request_id, next_request_id + 1);
    assert!(error.to_string().contains("native paginated history"));
    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn background_migration_never_reenables_legacy_resume() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let legacy_thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved legacy user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create legacy rollout"),
    )?;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-01",
            "2025-01-05T12:00:01Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut migration_config = config.clone();
    migration_config
        .features
        .enable(Feature::BackgroundPaginatedRolloutMigration)?;

    for (startup_config, resume_config) in
        [(&config, &migration_config), (&migration_config, &config)]
    {
        // Keep the real worker disabled while exercising both startup/request mismatches.
        let mut app_server = crate::start_embedded_app_server_for_picker(&config)
            .await?
            .with_startup_config(startup_config);
        app_server.remember_thread_history_mode(legacy_thread_id, ThreadHistoryMode::Legacy);
        let next_request_id = app_server.next_request_id;
        let error = app_server
            .resume_thread(
                resume_config.clone(),
                legacy_thread_id,
                ResumeModelSettings::RestoreFromThread,
            )
            .await
            .expect_err("legacy history must remain unsupported");
        assert_eq!(app_server.next_request_id, next_request_id + 1);
        assert!(error.to_string().contains("native paginated history"));

        app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
        let next_request_id = app_server.next_request_id;

        let resumed = app_server
            .resume_thread(
                resume_config.clone(),
                thread_id,
                ResumeModelSettings::RestoreFromThread,
            )
            .await?;

        assert!(app_server.next_request_id > next_request_id + 1);
        assert_eq!(resumed.session.thread_id, thread_id);
        assert_eq!(
            app_server
                .history_pagination
                .get(&thread_id)
                .map(|state| state.history_mode),
            Some(ThreadHistoryMode::Paginated)
        );
        app_server.shutdown().await?;
    }

    Ok(())
}

#[tokio::test]
async fn rollout_maintenance_contention_disables_cached_legacy_resume_shortcut() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let _maintenance_guard =
        codex_rollout::try_acquire_rollout_maintenance_lock(codex_home.path())?
            .expect("acquire rollout maintenance lock");
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(config, thread_id, ResumeModelSettings::RestoreFromThread)
        .await?;

    assert_eq!(app_server.next_request_id, next_request_id + 3);
    assert_eq!(resumed.session.thread_id, thread_id);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stale_legacy_history_mode_is_revalidated_before_resume() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(
            config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    assert_eq!(resumed.session.thread_id, thread_id);
    assert_eq!(app_server.next_request_id, next_request_id + 3);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    let missing_thread_id = ThreadId::new();
    app_server.remember_thread_history_mode(missing_thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;
    assert!(
        app_server
            .resume_thread(
                config,
                missing_thread_id,
                ResumeModelSettings::RestoreFromThread
            )
            .await
            .is_err()
    );
    assert_eq!(app_server.next_request_id, next_request_id + 1);

    app_server.shutdown().await?;
    Ok(())
}

#[test]
fn history_outcome_requires_the_exact_source_snapshot_epoch() {
    let snapshot = StrictHistorySnapshot {
        revision: "rollout-1".to_string(),
        source_high_water_ordinal: 42,
    };
    let sha256 = "abc".to_string();
    let prelude = HistoryPrelude {
        event_ids: vec!["settings".to_string()],
        snapshot_sha256: snapshot.snapshot_sha256(&sha256).expect("snapshot digest"),
        sha256,
    };
    let capture_id = "capture-1".to_string();
    let outcome = HistoryOutcome {
        capture_id: capture_id.clone(),
        contract_sha256: crate::cch::CONTRACT_SHA256.to_string(),
        history_revision: snapshot.revision.clone(),
        prelude_event_ids: prelude.event_ids.clone(),
        prelude_sha256: prelude.sha256.clone(),
        snapshot_sha256: prelude.snapshot_sha256.clone(),
        source_high_water_ordinal: snapshot.source_high_water_ordinal,
        status: HistoryStatus::Complete,
        request: None,
        progress: Some(serde_json::json!({})),
        error: None,
        resource_admission: None,
        thread_id: "thread-1".to_string(),
    };
    let capture = HistoryCaptureContext {
        thread_id: "thread-1",
        capture_id: &capture_id,
        snapshot: &snapshot,
        prelude: &prelude,
    };
    assert!(verify_history_outcome(&outcome, &capture).is_ok());

    let mut failed = outcome.clone();
    failed.status = HistoryStatus::Failed;
    failed.progress = None;
    failed.error = Some("no history facts".to_string());
    assert!(verify_history_outcome(&failed, &capture).is_ok());

    let corruptions: [fn(&mut HistoryOutcome); 8] = [
        |value| value.contract_sha256 = "0".repeat(64),
        |value| value.capture_id = "capture-2".to_string(),
        |value| value.thread_id = "thread-2".to_string(),
        |value| value.history_revision = "rollout-2".to_string(),
        |value| value.source_high_water_ordinal += 1,
        |value| value.prelude_event_ids.push("extra".to_string()),
        |value| value.prelude_sha256 = "0".repeat(64),
        |value| value.snapshot_sha256 = "0".repeat(64),
    ];
    for corrupt in corruptions {
        let mut invalid = outcome.clone();
        corrupt(&mut invalid);
        assert!(verify_history_outcome(&invalid, &capture).is_err());
    }
}
