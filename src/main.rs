use anyhow::Context;
use clap::Parser;
use unicodex::config::Config;

mod cli;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = cli::Cli::parse();
    unicodex::logging::init();
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(_) => {
            // YAML and transport error chains may contain credentials or payloads.
            tracing::error!("server terminated with an error");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: cli::Cli) -> anyhow::Result<()> {
    let config = Config::read(&cli.config).inspect_err(|_| {
        tracing::error!(operation = "load_config", "startup failed");
    })?;
    let listen = config.server.listen;
    let app = config.build().await.inspect_err(|_| {
        tracing::error!(operation = "build_app", "startup failed");
    })?;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .inspect_err(|error| {
            tracing::error!(operation = "bind_listener", error_kind = ?error.kind(), "startup failed");
        })
        .context("bind listener")?;
    tracing::info!(listen = %listener.local_addr()?, "server listening");
    axum::serve(listener, app.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown requested");
        })
        .await
        .inspect_err(|error| {
            tracing::error!(error_kind = ?error.kind(), "server failed");
        })?;
    tracing::info!("server stopped");
    Ok(())
}
