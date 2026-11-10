use clap::Parser;
use codex_claude_bridge::BridgeOptions;
use codex_claude_bridge::run_mcp_stdio;

/// MCP stdio server that talks to Claude Code sessions via a headless bridge session.
#[derive(Parser)]
#[command(name = "codex-claude-bridge")]
struct Cli {
    /// Claude Code binary (default: env CODEX_CLAUDE_BRIDGE_BIN or `claude`).
    #[arg(long)]
    claude_bin: Option<String>,
    /// Bridge model (default: env CODEX_CLAUDE_BRIDGE_MODEL or `haiku`).
    #[arg(long)]
    model: Option<String>,
    /// Bridge permission mode; must match the peers' mode for delivery.
    #[arg(long)]
    permission_mode: Option<String>,
    /// Bridge session name shown to peers.
    #[arg(long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut options = BridgeOptions::from_env();
    if let Some(bin) = cli.claude_bin {
        options.claude_bin = bin;
    }
    if let Some(model) = cli.model {
        options.model = model;
    }
    if cli.permission_mode.is_some() {
        options.permission_mode = cli.permission_mode;
    }
    if cli.name.is_some() {
        options.name = cli.name;
    }
    run_mcp_stdio(options).await
}
