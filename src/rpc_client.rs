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

    pub async fn get_nft_token(
        &self,
        contract_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        let cache_key = Token::build_id(contract_id, token_id);
        let cache_value = self.cache.get(&cache_key);
        if let Some(val) = cache_value {
            info!("cache hit {}", &cache_key);
            let token = serde_json::from_str::<'_, Token>(&val)?;
            return Ok(Some(token));
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
