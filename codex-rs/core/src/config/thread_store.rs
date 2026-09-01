use codex_config::config_toml::ThreadStoreToml;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ThreadStoreConfig {
    #[default]
    Local,
    InMemory {
        id: String,
    },
}

pub(super) fn resolve(thread_store: Option<ThreadStoreToml>) -> ThreadStoreConfig {
    match thread_store {
        Some(ThreadStoreToml::Local {}) => ThreadStoreConfig::Local,
        Some(ThreadStoreToml::InMemory { id }) => ThreadStoreConfig::InMemory { id },
        None => ThreadStoreConfig::Local,
    }
}
