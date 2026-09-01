use super::*;
use pretty_assertions::assert_eq;

#[test]
fn serialize_client_response() -> Result<()> {
    let cwd = absolute_path("/tmp");
    let response = ClientResponse::ThreadStart {
        request_id: RequestId::Integer(7),
        response: v2::ThreadStartResponse {
            thread: v2::Thread {
                id: "67e55044-10b1-426f-9247-bb680e5fe0c8".to_string(),
                extra: None,
                session_id: "67e55044-10b1-426f-9247-bb680e5fe0c7".to_string(),
                forked_from_id: None,
                parent_thread_id: None,
                preview: "first prompt".to_string(),
                ephemeral: true,
                section: None,
                section_entered_at: None,
                project_id: None,
                history_mode: Default::default(),
                model_provider: "openai".to_string(),
                created_at: 1,
                updated_at: 2,
                recency_at: Some(3),
                status: v2::ThreadStatus::Idle,
                path: None,
                cwd: cwd.clone(),
                cli_version: "0.0.0".to_string(),
                source: v2::SessionSource::Exec,
                can_accept_direct_input: None,
                thread_source: None,
                agent_nickname: None,
                agent_role: None,
                git_info: None,
                name: None,
                turns: Vec::new(),
            },
            model: "gpt-5".to_string(),
            model_provider: "openai".to_string(),
            service_tier: None,
            cwd,
            runtime_workspace_roots: Vec::new(),
            instruction_sources: vec![codex_utils_path_uri::LegacyAppPathString::from_abs_path(
                &absolute_path("/tmp/AGENTS.md"),
            )],
            approval_policy: v2::AskForApproval::OnRequest,
            approvals_reviewer: v2::ApprovalsReviewer::User,
            sandbox: v2::SandboxPolicy::DangerFullAccess,
            active_permission_profile: None,
            reasoning_effort: None,
            multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        },
    };

    assert_eq!(response.id(), &RequestId::Integer(7));
    assert_eq!(response.method(), "thread/start");
    assert_eq!(
        json!({
            "method": "thread/start",
            "id": 7,
            "response": {
                "thread": {
                    "id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                    "extra": null,
                    "sessionId": "67e55044-10b1-426f-9247-bb680e5fe0c7",
                    "forkedFromId": null,
                    "parentThreadId": null,
                    "preview": "first prompt",
                    "ephemeral": true,
                    "section": null,
                    "sectionEnteredAt": null,
                    "projectId": null,
                    "historyMode": "legacy",
                    "modelProvider": "openai",
                    "createdAt": 1,
                    "updatedAt": 2,
                    "recencyAt": 3,
                    "status": {
                        "type": "idle"
                    },
                    "path": null,
                    "cwd": absolute_path_string("tmp"),
                    "cliVersion": "0.0.0",
                    "source": "exec",
                    "canAcceptDirectInput": null,
                    "threadSource": null,
                    "agentNickname": null,
                    "agentRole": null,
                    "gitInfo": null,
                    "name": null,
                    "turns": []
                },
                "model": "gpt-5",
                "modelProvider": "openai",
                "serviceTier": null,
                "cwd": absolute_path_string("tmp"),
                "runtimeWorkspaceRoots": [],
                "instructionSources": [absolute_path_string("tmp/AGENTS.md")],
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": {
                    "type": "dangerFullAccess"
                },
                "activePermissionProfile": null,
                "reasoningEffort": null,
                "multiAgentMode": "explicitRequestOnly"
            }
        }),
        serde_json::to_value(&response)?,
    );
    Ok(())
}
