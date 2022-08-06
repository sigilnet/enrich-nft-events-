use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct EmitInfo {
    pub receipt_id: String,
    pub block_timestamp: u64,
    pub block_height: u64,
    pub shard_id: u64,
    pub contract_account_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "standard")]
#[serde(rename_all = "snake_case")]
pub(crate) enum NearEvent {
    Nep171(Nep171Event),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Nep171Event {
    pub version: String,
    #[serde(flatten)]
    pub event_kind: Nep171EventKind,
    pub emit_info: Option<EmitInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "event", content = "data")]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Nep171EventKind {
    NftMint(Vec<NftMintData>),
    NftTransfer(Vec<NftTransferData>),
    NftBurn(Vec<NftBurnData>),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct NftMintData {
    pub owner_id: String,
    pub token_ids: Vec<String>,
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct NftTransferData {
    pub authorized_id: Option<String>,
    pub old_owner_id: String,
    pub new_owner_id: String,
    pub token_ids: Vec<String>,
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct NftBurnData {
    pub authorized_id: Option<String>,
    pub owner_id: String,
    pub token_ids: Vec<String>,
    pub memo: Option<String>,
}
