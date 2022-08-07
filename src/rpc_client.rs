use moka::future::Cache;
use near_jsonrpc_client::{methods::query::RpcQueryRequest, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::types::{BlockReference, Finality, FunctionArgs};
use near_primitives::views::QueryRequest;
use serde_json::{from_slice, json};
use tracing::info;

use crate::token::Token;

pub struct RpcClient {
    pub client: JsonRpcClient,
    pub cache: Cache<String, String>,
}

impl RpcClient {
    pub fn new(node_url: &str, cache: Cache<String, String>) -> Self {
        Self {
            client: JsonRpcClient::connect(node_url),
            cache,
        }
    }

    pub async fn update_cache(&self, key: String, value: String) {
        self.cache.insert(key, value).await;
    }

    pub async fn update_nft_cache(&self, token: &Token) -> anyhow::Result<()> {
        let key = token.get_id();
        if let Some(key) = key {
            let value = serde_json::to_string(&token)?;
            self.update_cache(key, value).await;
        }

        Ok(())
    }

    pub fn get_nft_cache(
        &self,
        contract_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        let key = Token::build_id(contract_id, token_id);
        let value = self.cache.get(&key);
        if let Some(value) = value {
            info!("cache hit {}", &key);
            let token: Token = serde_json::from_str(&value)?;
            return Ok(Some(token));
        }

        Ok(None)
    }

    pub async fn get_nft_token(
        &self,
        contract_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        let cache_token = self.get_nft_cache(contract_id, token_id)?;
        if cache_token.is_some() {
            return Ok(cache_token);
        }

        let request = RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: QueryRequest::CallFunction {
                account_id: contract_id.parse()?,
                method_name: "nft_token".to_string(),
                args: FunctionArgs::from(
                    json!({
                        "token_id": token_id,
                    })
                    .to_string()
                    .into_bytes(),
                ),
            },
        };

        let response = self.client.call(request).await?;

        if let QueryResponseKind::CallResult(result) = response.kind {
            let token = from_slice::<Token>(&result.result)?;
            return Ok(Some(token));
        }

        Ok(None)
    }
}
