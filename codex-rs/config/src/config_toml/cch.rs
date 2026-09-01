use super::*;

/// Native Contextual Conversation History settings for a loopback endpoint.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CchToml {
    pub enabled: Option<bool>,
    pub base_url: Option<String>,
    /// Name of the process environment variable containing the CCH bearer token.
    pub bearer_token_env_var: Option<String>,
    pub timeout_ms: Option<NonZeroU64>,
    pub max_request_body_bytes: Option<NonZeroU64>,
    pub max_response_body_bytes: Option<NonZeroU64>,
}
