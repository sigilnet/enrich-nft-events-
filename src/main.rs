use std::path::PathBuf;

use clap::Parser;
use configs::{AppConfig, Opts};
use futures::StreamExt;
use moka::future::Cache;
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;
use streamer::init_streamer;
use streamer::StreamerMessage;
use tracing::info;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

use crate::rpc_client::RpcClient;
use crate::sender::send_event;
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
    let cache = Cache::new(100_000);

    init_tracer(&config);

    let rpc_client = RpcClient::new("", cache.clone());
    let (sender, stream) = init_streamer(&config)?;

    let admin_client: AdminClient<DefaultClientContext> = config.kafka_config.create()?;
    let producer: FutureProducer = config.kafka_config.create()?;

    info!("Start streamer...");

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|m| handle_message(m, &rpc_client, &producer, &admin_client, &config))
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

fn parse_token(message: &BorrowedMessage) -> Option<Token> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) => {
            let token = serde_json::from_str::<Token>(payload);
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

async fn enrich_metadata(
    rpc_client: &RpcClient,
    token: &Token,
    contract_id: &str,
) -> anyhow::Result<Option<Token>> {
    let full_token = rpc_client
        .get_nft_token(contract_id, &token.token_id)
        .await?;

    if let Some(full_token) = full_token {
        let mut enriched_token = token.clone();
        enriched_token.set_id();
        enriched_token.metadata = full_token.metadata;
        if let Some(ref metadata) = token.metadata {
            if let Some(ref extra) = metadata.extra {
                enriched_token.metadata_extra = serde_json::from_str(extra).ok();
            }
        }
        return Ok(Some(enriched_token));
    } else {
        warn!("Could not fetch full token for: {:?}", &token);
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
async fn send_enriched_token(
    rpc_client: &RpcClient,
    producer: &FutureProducer,
    admin_client: &AdminClient<DefaultClientContext>,
    streamer_message: &StreamerMessage<'static>,
    config: &AppConfig,
    enriched_token: &Option<Token>,
    topic_input: &str,
    topic_output_suffix: &str,
) -> anyhow::Result<()> {
    if let Some(token) = enriched_token {
        let event_payload = serde_json::to_string(token)?;
        let event_topic = format!("{}_{}", topic_input, topic_output_suffix);
        let event_id = token.get_id().unwrap();
        info!("Token after enriched: {}", event_payload);
        rpc_client.update_nft_cache(token).await?;
        send_event(
            producer,
            admin_client,
            streamer_message,
            config,
            &event_topic,
            &event_id,
            &event_payload,
        )
        .await?;
    }

    Ok(())
}

async fn handle_message<'a>(
    streamer_message: StreamerMessage<'static>,
    rpc_client: &'a RpcClient,
    producer: &'a FutureProducer,
    admin_client: &'a AdminClient<DefaultClientContext>,
    config: &'a AppConfig,
) -> anyhow::Result<()> {
    let token = parse_token(&streamer_message.message);
    if let Some(ref token) = token {
        if let Some(ref contract_id) = token.contract_account_id {
            let enriched_token = enrich_metadata(rpc_client, token, contract_id).await?;
            send_enriched_token(
                rpc_client,
                producer,
                admin_client,
                &streamer_message,
                config,
                &enriched_token,
                streamer_message.message.topic(),
                &config.topic_output_suffix,
            )
            .await?;
        } else {
            warn!("Token doesn't have contract_account_id: {:?}", &token);
        }
    }
    streamer_message.commit()?;
    Ok(())
}
