// Project-local config comes from repository contents, so it must not choose
// where a user's credentials are sent or which local commands are run.
pub(super) const PROJECT_LOCAL_CONFIG_DENYLIST: &[&str] = &[
    "cch",
    "openai_base_url",
    "chatgpt_base_url",
    "apps_mcp_product_sku",
    "responses_api_metadata",
    "model_provider",
    "model_providers",
    "notify",
    "profile",
    "profiles",
    "experimental_realtime_webrtc_call_base_url",
    "experimental_realtime_ws_base_url",
    "otel",
];
