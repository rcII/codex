use super::*;
use std::sync::Mutex as StdMutex;
use tokio::sync::watch;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::Request;
use wiremock::ResponseTemplate;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

fn endpoint(base_url: &str) -> CchEndpointConfig {
    CchEndpointConfig {
        base_url: url::Url::parse(base_url).expect("valid test endpoint"),
        bearer_token_env_var: "CCH_BEARER_TOKEN".to_string(),
        timeout: std::time::Duration::from_secs(5),
        max_request_body_bytes: 1024,
        max_response_body_bytes: 1024,
    }
}

async fn wait_for_count(receiver: &mut watch::Receiver<usize>, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while *receiver.borrow() < expected {
            receiver.changed().await.expect("history count sender");
        }
    })
    .await
    .expect("history exchange count deadline");
}

#[tokio::test]
async fn disabled_cch_connect_is_a_transport_noop() {
    let integration = CchIntegration::connect_with_env(&CchConfig::Disabled, |_| {
        panic!("disabled CCH must not inspect the environment")
    })
    .await
    .expect("disabled CCH");
    assert!(integration.is_none());
}

#[test]
fn history_snapshot_digest_binds_revision_prelude_and_high_water() {
    let snapshot = StrictHistorySnapshot {
        revision: "rollout-1".to_string(),
        source_high_water_ordinal: 42,
    };
    assert_eq!(
        snapshot.snapshot_sha256("abc").expect("snapshot digest"),
        "c87a44c7af69e2c73892931fe8c63eca84fb57d12742a6294217bf9c29e445dc"
    );
    assert_ne!(
        snapshot.snapshot_sha256("abc").expect("snapshot digest"),
        StrictHistorySnapshot {
            source_high_water_ordinal: 43,
            ..snapshot
        }
        .snapshot_sha256("abc")
        .expect("advanced snapshot digest")
    );
}

#[test]
fn bearer_token_validation_matches_cch_contract_and_redacts_secrets() {
    let missing = CchTransport::new(endpoint("http://127.0.0.1:1/"), |_| {
        Err(std::env::VarError::NotPresent)
    });
    assert!(matches!(missing, Err(CchError::BearerToken)));

    for secret in [
        "0123456".repeat(4),
        "01234567abcdefghijklmnopqrstuvwx=bad".to_string(),
        "a".repeat(32),
        "01234567abcdefghijklmnopqrstuvwxyz".repeat(121),
    ] {
        let invalid = CchTransport::new(endpoint("http://127.0.0.1:1/"), |_| Ok(secret.clone()));
        let Err(error) = invalid else {
            panic!("invalid bearer token must fail closed")
        };
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains(&secret));
    }

    for token in [
        "01234567abcdefghijklmnopqrstuvwx".to_string(),
        format!("{}0123456=", "01234567".repeat(511)),
    ] {
        CchTransport::bearer_token(token).expect("boundary token must be accepted");
    }
}

#[tokio::test]
async fn bearer_token_is_attached_to_every_get_and_post() {
    let server = MockServer::start().await;
    let token = "01234567abcdefghijklmnopqrstuvwxyz=";
    Mock::given(method("GET"))
        .and(path("/v1/runtime/contract"))
        .and(header("authorization", format!("Bearer {token}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/runtime/events"))
        .and(header("authorization", format!("Bearer {token}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .expect(1)
        .mount(&server)
        .await;
    let transport = CchTransport::new(endpoint(&format!("{}/", server.uri())), |_| {
        Ok(token.to_string())
    })
    .expect("valid bearer transport");

    let _: serde_json::Value = transport
        .get("v1/runtime/contract")
        .await
        .expect("authorized contract GET");
    let _: serde_json::Value = transport
        .post("v1/runtime/events", &serde_json::json!({"kind": "test"}))
        .await
        .expect("authorized event POST");
    server.verify().await;
}

#[tokio::test]
async fn transport_enforces_request_and_response_byte_limits() {
    let token = "01234567abcdefghijklmnopqrstuvwxyz=";
    let transport = CchTransport::new(endpoint("http://127.0.0.1:1/"), |_| Ok(token.to_string()))
        .expect("valid bearer transport");
    let request: Result<Json, CchError> = transport
        .post(
            "v1/runtime/events",
            &serde_json::json!({"payload": "x".repeat(2048)}),
        )
        .await;
    assert!(matches!(request, Err(CchError::RequestTooLarge)));

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/oversized"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(1025)))
        .expect(1)
        .mount(&server)
        .await;
    let transport = CchTransport::new(endpoint(&format!("{}/", server.uri())), |_| {
        Ok(token.to_string())
    })
    .expect("valid bearer transport");
    let response: Result<Json, CchError> = transport.get("oversized").await;
    assert!(matches!(response, Err(CchError::ResponseTooLarge)));
    server.verify().await;
}

#[tokio::test]
async fn settings_only_empty_complete_unblocks_safe_boundary_and_terminal_capture() {
    let server = MockServer::start().await;
    let token = "01234567abcdefghijklmnopqrstuvwxyz=";
    let (checkpoint_tx, mut checkpoint_rx) = watch::channel(0_usize);
    let (finalize_tx, mut finalize_rx) = watch::channel(0_usize);
    let last_outcome = Arc::new(StdMutex::new(None::<Json>));

    Mock::given(method("POST"))
        .and(path("/v1/runtime/events"))
        .and(header("authorization", format!("Bearer {token}")))
        .respond_with(|request: &Request| {
            let body: Json = request.body_json().expect("event body");
            assert_eq!(body["kind"], "thread.settings_updated");
            let key = body["idempotencyKey"].as_str().expect("idempotency key");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "contractSha256": CONTRACT_SHA256,
                "eventId": format!("{:x}", Sha256::digest(format!("cch:event:{key}"))),
                "historyRevision": body["historyRevision"],
                "idempotencyKey": key,
                "receipt": Json::Null,
                "sourceOrdinal": body["sourceOrdinal"],
                "sourceSubordinal": body["sourceSubordinal"],
            }))
        })
        .expect(2)
        .mount(&server)
        .await;
    let checkpoint_outcome = Arc::clone(&last_outcome);
    Mock::given(method("POST"))
        .and(path("/v1/runtime/history/checkpoint"))
        .and(header("authorization", format!("Bearer {token}")))
        .respond_with(move |request: &Request| {
            checkpoint_tx.send_modify(|count| *count += 1);
            let count = *checkpoint_tx.borrow();
            let body: Json = request.body_json().expect("checkpoint body");
            assert_eq!(
                body["preludeEventIds"]
                    .as_array()
                    .expect("settings-only prelude")
                    .len(),
                1
            );
            let outcome = serde_json::json!({
                "captureId": format!("capture-{count}"),
                "contractSha256": CONTRACT_SHA256,
                "historyRevision": body["historyRevision"],
                "preludeEventIds": body["preludeEventIds"],
                "preludeSha256": body["preludeSha256"],
                "snapshotSha256": body["snapshotSha256"],
                "sourceHighWaterOrdinal": body["sourceHighWaterOrdinal"],
                "status": "complete",
                "progress": {
                    "eligible": false,
                    "reason": "no_terminal_history",
                    "threadId": body["threadId"],
                    "repositoryState": "none",
                    "repositoryIds": [],
                    "historySha256": format!("{:x}", Sha256::digest(b"")),
                    "sourceRevisionSha256": "0".repeat(64),
                    "snapshotSha256": body["snapshotSha256"],
                    "pageCount": 0,
                    "turns": 0,
                    "pairedTurns": 0,
                    "sourceEvents": 0,
                },
                "threadId": body["threadId"],
            });
            *checkpoint_outcome.lock().expect("outcome lock") = Some(outcome.clone());
            ResponseTemplate::new(200).set_body_json(outcome)
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/runtime/history/finalize"))
        .and(header("authorization", format!("Bearer {token}")))
        .respond_with(move |_request: &Request| {
            finalize_tx.send_modify(|count| *count += 1);
            ResponseTemplate::new(200).set_body_json(
                last_outcome
                    .lock()
                    .expect("outcome lock")
                    .clone()
                    .expect("checkpoint outcome"),
            )
        })
        .expect(2)
        .mount(&server)
        .await;

    let codex_home = tempfile::tempdir().expect("codex home");
    let mut config = crate::legacy_core::config::ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await
        .expect("config");
    config.sqlite = codex_state::SqliteConfig::new_for_testing(config.codex_home.clone());
    let mut endpoint = endpoint(&format!("{}/", server.uri()));
    endpoint.max_request_body_bytes = 64 * 1024;
    endpoint.max_response_body_bytes = 64 * 1024;
    let integration = CchIntegration {
        transport: CchTransport::new(endpoint, |_| Ok(token.to_string())).expect("transport"),
        threads: Arc::new(Mutex::new(HashMap::new())),
        shutdown: Arc::new(AtomicBool::new(false)),
    };
    let capture = integration.clone();
    let mut app_server = crate::start_embedded_app_server_for_picker(&config)
        .await
        .expect("app-server");
    app_server.install_cch_integration(Some(integration));
    let started = app_server
        .start_thread(&config)
        .await
        .expect("fresh thread");
    let thread_id = started.session.thread_id.to_string();
    let handle = app_server.request_handle();

    let first_capture = capture
        .ensure_history_captured(handle.clone(), &thread_id)
        .await;
    if let Err(error) = first_capture {
        let requests = server
            .received_requests()
            .await
            .expect("recorded requests")
            .iter()
            .map(|request| request.url.path().to_string())
            .collect::<Vec<_>>();
        panic!(
            "zero-fact Complete must release the first-turn safe boundary: {error}; {requests:?}"
        );
    }
    wait_for_count(&mut checkpoint_rx, 1).await;
    wait_for_count(&mut finalize_rx, 1).await;
    capture
        .history_turn_completed(handle, &thread_id)
        .await
        .expect("terminal capture trigger");
    wait_for_count(&mut checkpoint_rx, 2).await;
    wait_for_count(&mut finalize_rx, 2).await;
    let paths = server
        .received_requests()
        .await
        .expect("recorded requests")
        .into_iter()
        .map(|request| request.url.path().to_string())
        .collect::<Vec<_>>();
    assert!(
        !paths
            .iter()
            .any(|path| matches!(path.as_str(), "/v1/runtime/enroll" | "/v1/runtime/recall"))
    );
    server.verify().await;
    app_server.shutdown().await.expect("shutdown");
}
