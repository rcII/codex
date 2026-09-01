use codex_config::config_toml::CchToml;
use std::num::NonZeroU64;
use std::time::Duration;
use url::Host;
use url::Url;

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(120_000);
const DEFAULT_MAX_REQUEST_BODY_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BODY_BYTES: u64 = 64 * 1024;
const MIN_TIMEOUT_MS: u64 = 100;
const MAX_TIMEOUT_MS: u64 = 300_000;
const MIN_BODY_BYTES: u64 = 1024;
const MAX_REQUEST_BODY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESPONSE_BODY_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CchConfig {
    #[default]
    Disabled,
    Enabled(CchEndpointConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CchEndpointConfig {
    pub base_url: Url,
    pub bearer_token_env_var: String,
    pub timeout: Duration,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
}

pub(super) fn resolve_cch_config(config: Option<&CchToml>) -> std::io::Result<CchConfig> {
    let Some(config) = config.filter(|config| config.enabled.unwrap_or(false)) else {
        return Ok(CchConfig::Disabled);
    };
    let base_url = config
        .base_url
        .as_deref()
        .ok_or_else(|| invalid("cch.base_url is required when CCH is enabled"))?;
    let bearer_token_env_var = config
        .bearer_token_env_var
        .as_deref()
        .ok_or_else(|| invalid("cch.bearer_token_env_var is required when CCH is enabled"))?;
    let timeout_ms = config
        .timeout_ms
        .map(NonZeroU64::get)
        .unwrap_or(DEFAULT_TIMEOUT.as_millis() as u64);
    validate_range("cch.timeout_ms", timeout_ms, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS)?;
    let max_request_body_bytes = config
        .max_request_body_bytes
        .map(NonZeroU64::get)
        .unwrap_or(DEFAULT_MAX_REQUEST_BODY_BYTES);
    validate_range(
        "cch.max_request_body_bytes",
        max_request_body_bytes,
        MIN_BODY_BYTES,
        MAX_REQUEST_BODY_BYTES,
    )?;
    let max_response_body_bytes = config
        .max_response_body_bytes
        .map(NonZeroU64::get)
        .unwrap_or(DEFAULT_MAX_RESPONSE_BODY_BYTES);
    validate_range(
        "cch.max_response_body_bytes",
        max_response_body_bytes,
        MIN_BODY_BYTES,
        MAX_RESPONSE_BODY_BYTES,
    )?;
    Ok(CchConfig::Enabled(CchEndpointConfig {
        base_url: parse_base_url(base_url)?,
        bearer_token_env_var: parse_bearer_token_env_var(bearer_token_env_var)?,
        timeout: Duration::from_millis(timeout_ms),
        max_request_body_bytes: usize::try_from(max_request_body_bytes)
            .map_err(|_| invalid("cch.max_request_body_bytes exceeds platform limits"))?,
        max_response_body_bytes: usize::try_from(max_response_body_bytes)
            .map_err(|_| invalid("cch.max_response_body_bytes exceeds platform limits"))?,
    }))
}

fn parse_bearer_token_env_var(value: &str) -> std::io::Result<String> {
    let mut bytes = value.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(invalid(
            "cch.bearer_token_env_var must be a portable environment variable name",
        ));
    }
    Ok(value.to_string())
}

fn validate_range<T>(name: &str, value: T, min: T, max: T) -> std::io::Result<()>
where
    T: Copy + PartialOrd + std::fmt::Display,
{
    if value < min || value > max {
        return Err(invalid(format!(
            "{name} must be in the inclusive range {min}..={max}"
        )));
    }
    Ok(())
}

fn parse_base_url(value: &str) -> std::io::Result<Url> {
    let mut url = Url::parse(value.trim())
        .map_err(|error| invalid(format!("invalid cch.base_url: {error}")))?;
    if url.cannot_be_a_base() || url.host().is_none() {
        return Err(invalid("cch.base_url must be an absolute hierarchical URL"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid("cch.base_url must not contain user information"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(invalid("cch.base_url must not contain a query or fragment"));
    }
    let is_loopback = match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(host)) => host.is_loopback(),
        Some(Host::Ipv6(host)) => host.is_loopback(),
        None => false,
    };
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid("cch.base_url must use HTTP or HTTPS"));
    }
    if !is_loopback {
        return Err(invalid("cch.base_url must use a loopback host"));
    }
    let normalized_path = format!("{}/", url.path().trim_end_matches('/'));
    url.set_path(&normalized_path);
    Ok(url)
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}
