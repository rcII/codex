use serde::Deserialize;
use serde::Serialize;

/// Exact durable prefix selected for strict paginated replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictHistorySnapshot {
    pub revision: String,
    pub source_high_water_ordinal: u64,
}

/// A projected app-server `ThreadItem` snapshot within a turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredThreadItem {
    /// Turn containing this item.
    pub turn_id: String,
    /// Stable item identifier within the turn.
    pub item_id: String,
    /// Rollout ordinal of the latest persisted update to this item.
    pub updated_at_ordinal: u64,
    /// Unix timestamp (milliseconds) when this logical item was first projected.
    pub created_at_ms: i64,
    /// Unix timestamp (milliseconds) from the native completed-item occurrence.
    ///
    /// Legacy rows remain unknown rather than receiving a read-time timestamp.
    pub completed_at_ms: Option<i64>,
    /// Serialized app-server ThreadItem snapshot.
    pub item_json: Vec<u8>,
}
