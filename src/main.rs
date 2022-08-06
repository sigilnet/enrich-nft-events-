use std::path::PathBuf;

use clap::Parser;
use configs::{AppConfig, Opts};
use event_types::NearEvent;
use event_types::Nep171EventKind;
use futures::StreamExt;
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use streamer::init_streamer;
use streamer::StreamerMessage;
use tracing::info;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

mod configs;
mod event_types;
mod streamer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_ssl_cert_env_vars();

    let opts: Opts = Opts::parse();
    let config = AppConfig::new(PathBuf::from(opts.home_dir))?;

    init_tracer(&config);

    let (sender, stream) = init_streamer(config)?;
    info!("Start streamer...");

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(handle_message)
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

fn parse_event(message: &BorrowedMessage) -> Option<NearEvent> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) => {
            let event = serde_json::from_str::<'_, NearEvent>(payload);
            match event {
                Ok(event) => Some(event),
                Err(err) => {
                    warn!(
                    "Payload does not correspond to any of formats defined in NEP. Will ignore this event. \n {:#?} \n{:#?}",
                    err,
                    payload,
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

async fn handle_message(streamer_message: StreamerMessage<'static>) -> anyhow::Result<()> {
    let event = parse_event(&streamer_message.message);
    if let Some(event) = event {
        match event {
            NearEvent::Nep171(nep171) => match nep171.event_kind {
                Nep171EventKind::NftMint(v) => {
                    for data in v {
                        info!("[nft_mint] fetching metadata for: {:?}", data.token_ids);
                    }
                }
                Nep171EventKind::NftTransfer(v) => {
                    for data in v {
                        info!("[nft_transfer] fetching metadata for: {:?}", data.token_ids);
                    }
                }
                Nep171EventKind::NftBurn(data) => {
                    warn!("[nft_burn] unhandled: {:?}", data);
                }
            },
        }
    }
    streamer_message.commit()?;
    Ok(())
}
