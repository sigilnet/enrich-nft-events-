use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Token {
    pub _id: Option<String>,
    pub token_id: String,
    pub owner_id: String,
    pub metadata: Option<TokenMetadata>,
    pub metadata_extra: Option<serde_json::Value>,
    pub approved_account_ids: Option<HashMap<String, u64>>,
    pub contract_account_id: Option<String>,
}

impl Token {
    pub fn build_id(contract_id: &str, token_id: &str) -> String {
        format!("{}:{}", contract_id, token_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub media: Option<String>,
    pub media_hash: Option<String>,
    pub copies: Option<u64>,
    pub issued_at: Option<String>,
    pub expires_at: Option<String>,
    pub starts_at: Option<String>,
    pub updated_at: Option<String>,
    pub extra: Option<String>,
    pub reference: Option<String>,
    pub reference_hash: Option<String>,
    pub collection_id: Option<String>,
}
