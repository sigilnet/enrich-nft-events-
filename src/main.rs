use std::path::PathBuf;

use clap::Parser;
use configs::{AppConfig, Opts};
use futures::StreamExt;
use moka::future::Cache;
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use streamer::init_streamer;
use streamer::StreamerMessage;
use tracing::info;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

use crate::rpc_client::RpcClient;
use crate::token::Token;

mod configs;
mod rpc_client;
mod sender;
mod streamer;
mod token;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_ssl_cert_env_vars();

    let opts: Opts = Opts::parse();
    let config = AppConfig::new(PathBuf::from(opts.home_dir))?;
    let config_box = Box::new(config);
    let config_ref: &'static mut AppConfig = Box::leak(config_box);
    let cache = Cache::new(100_000);

    init_tracer(config_ref);

    let rpc_client = RpcClient::new(&config_ref.near_node_url, cache.clone());
    let (sender, stream) = init_streamer(config_ref)?;

    info!("Start streamer...");

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|m| handle_message(m, &rpc_client, cache.clone(), config_ref))
        .buffer_unordered(usize::from(opts.concurrency.get()));

    while let Some(handle_message) = handlers.next().await {
        handle_message?;
    }

    drop(handlers);

    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}

fn init_tracer(config: &AppConfig) {
    let mut env_filter = EnvFilter::new(&config.log_level);

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    FmtSubscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

fn parse_event(message: &BorrowedMessage) -> Option<Token> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) => {
            let token = serde_json::from_str::<'_, Token>(payload);
            match token {
                Ok(token) => Some(token),
                Err(err) => {
                    warn!(
                        "Payload does not correspond to NFT standard. \n {:?} \n{:?}",
                        err, payload,
                    );
                    None
                }
            }
        }
        Some(Err(_)) => {
            warn!("Message payload is not a string");
            None
        }
        None => {
            warn!("Message has no payload");
            None
        }
    }
}

async fn handle_message(
    streamer_message: StreamerMessage<'static>,
    rpc_client: &RpcClient,
    cache: Cache<String, String>,
    config: &'static AppConfig,
) -> anyhow::Result<()> {
    let token = parse_event(&streamer_message.message);
    if let Some(mut token) = token {
        if let Some(ref contract_id) = token.contract_account_id {
            let full_token = rpc_client
                .get_nft_token(contract_id, &token.token_id)
                .await?;
            if let Some(full_token) = full_token {
                token.metadata = full_token.metadata;
                if let Some(ref metadata) = token.metadata {
                    if let Some(ref extra_str) = metadata.extra {
                        let extra = serde_json::from_str::<'_, serde_json::Value>(extra_str).ok();
                        token.metadata_extra = extra;
                    }
                }
                let _id = Token::build_id(contract_id, &token.token_id);
                token._id = Some(_id.clone());
                let event_payload = serde_json::to_string(&token)?;
                let event_topic = format!(
                    "{}_{}",
                    streamer_message.message.topic(),
                    config.topic_output_suffix
                );
                info!("Token after enrich: {}", event_payload);
                cache.insert(_id.clone(), event_payload.clone()).await;
                streamer_message
                    .send(config, &event_topic, &_id, &event_payload)
                    .await?;
            } else {
                warn!(
                    "Could not fetch token for: {:?} -> ${:?}",
                    &contract_id, &token.token_id
                );
            }
        } else {
            warn!("Token don't have contract_account_id: {:?}", token);
        }
    }
    streamer_message.commit()?;
    Ok(())
}
