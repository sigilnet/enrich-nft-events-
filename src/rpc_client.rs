use near_jsonrpc_client::{methods::query::RpcQueryRequest, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::types::{BlockReference, Finality, FunctionArgs};
use near_primitives::views::QueryRequest;
use serde_json::{from_slice, json};

use crate::configs::AppConfig;
use crate::token::Token;

pub fn init_client(config: &AppConfig) -> JsonRpcClient {
    JsonRpcClient::connect(&config.near_node_url)
}

pub async fn get_nft_token(
    client: &JsonRpcClient,
    contract_id: &str,
    token_id: &str,
) -> anyhow::Result<Option<Token>> {
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

    let response = client.call(request).await?;

    if let QueryResponseKind::CallResult(result) = response.kind {
        let token = from_slice::<Token>(&result.result)?;
        return Ok(Some(token));
    }

    Ok(None)
}
