use super::ThreadItem;
use super::Turn;
use super::TurnItemsView;
use super::thread::SortDirection;
use super::thread::TurnsPage;
use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadInjectItemsParams {
    pub thread_id: String,
    /// Raw Responses API items to append to the thread's model-visible history.
    pub items: Vec<JsonValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadInjectItemsResponse {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadTurnsListParams {
    pub thread_id: String,
    /// Opaque cursor to pass to the next call to continue after the last turn.
    #[ts(optional = nullable)]
    pub cursor: Option<String>,
    /// Optional turn page size.
    #[ts(optional = nullable)]
    pub limit: Option<u32>,
    /// Optional turn pagination direction; defaults to descending.
    #[ts(optional = nullable)]
    pub sort_direction: Option<SortDirection>,
    /// How much item detail to include for each returned turn; defaults to summary.
    #[ts(optional = nullable)]
    pub items_view: Option<TurnItemsView>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(transparent)]
#[ts(type = "ThreadTurnsListParams", export_to = "v2/")]
pub struct ThreadTurnsListPaginatedParams(pub ThreadTurnsListParams);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadTurnsListResponse {
    pub data: Vec<Turn>,
    /// Opaque cursor to pass to the next call to continue after the last turn.
    /// if None, there are no more turns to return.
    pub next_cursor: Option<String>,
    /// Opaque cursor to pass as `cursor` when reversing `sortDirection`.
    /// This is only populated when the page contains at least one turn.
    /// Use it with the opposite `sortDirection` to include the anchor turn again
    /// and catch updates to that turn.
    pub backwards_cursor: Option<String>,
}

impl From<ThreadTurnsListResponse> for TurnsPage {
    fn from(response: ThreadTurnsListResponse) -> Self {
        Self {
            data: response.data,
            next_cursor: response.next_cursor,
            backwards_cursor: response.backwards_cursor,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadTurnHistoryEntry {
    #[serde(flatten)]
    pub turn: Turn,
    pub history_revision: String,
    #[ts(type = "number")]
    pub source_ordinal: u64,
    pub source_subordinal: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadTurnsListPaginatedResponse {
    pub data: Vec<ThreadTurnHistoryEntry>,
    pub next_cursor: Option<String>,
    pub backwards_cursor: Option<String>,
    pub history_revision: String,
    #[ts(type = "number")]
    pub source_high_water_ordinal: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadItemsListParams {
    pub thread_id: String,
    /// Optional turn id to filter by. When omitted, returns items across the thread.
    #[ts(optional = nullable)]
    pub turn_id: Option<String>,
    /// Opaque cursor to pass to the next call to continue after the last item.
    #[ts(optional = nullable)]
    pub cursor: Option<String>,
    /// Optional item page size.
    #[ts(optional = nullable)]
    pub limit: Option<u32>,
    /// Optional item pagination direction; defaults to ascending.
    #[ts(optional = nullable)]
    pub sort_direction: Option<SortDirection>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(transparent)]
#[ts(type = "ThreadItemsListParams", export_to = "v2/")]
pub struct ThreadItemsListPaginatedParams(pub ThreadItemsListParams);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadItemEntry {
    /// Turn containing this item.
    pub turn_id: String,
    pub item: ThreadItem,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadItemHistoryEntry {
    #[serde(flatten)]
    pub entry: ThreadItemEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub completed_at_ms: Option<i64>,
    pub history_revision: String,
    #[ts(type = "number")]
    pub source_ordinal: u64,
    pub source_subordinal: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadItemsListResponse {
    pub data: Vec<ThreadItemEntry>,
    /// Opaque cursor to pass to the next call to continue after the last item.
    /// if None, there are no more items to return.
    pub next_cursor: Option<String>,
    /// Opaque cursor to pass as `cursor` when reversing `sortDirection`.
    /// This is only populated when the page contains at least one item.
    pub backwards_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadItemsListPaginatedResponse {
    pub data: Vec<ThreadItemHistoryEntry>,
    pub next_cursor: Option<String>,
    pub backwards_cursor: Option<String>,
    pub history_revision: String,
    #[ts(type = "number")]
    pub source_high_water_ordinal: u64,
}
