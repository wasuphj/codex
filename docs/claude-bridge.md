# Claude bridge

Claude bridge lets Codex exchange messages with Claude Code agent sessions. When enabled, Codex registers a stdio MCP server named `claude` (the `codex claude-bridge` subcommand) that starts a headless Claude Code session and exposes it to the model as tools.

The bridge uses the official headless Claude Code session. It does not use any private socket protocol.

## Enable

```sh
codex --enable claude_bridge
```

or in `config.toml`:

```toml
[features]
claude_bridge = true
```

The `claude` server is added automatically. If you define `[mcp_servers.claude]` yourself, your definition is used instead.

`CODEX_CLAUDE_BRIDGE_*` environment variables (for example the Claude binary, model, permission mode, and agent name) are passed through from the parent environment to the bridge. The same options are available as flags: `codex claude-bridge --claude-bin --model --permission-mode --name`.

## Tools

- `claude_list_agents`: list reachable Claude agents.
- `claude_send_message { to, message }`: send a message to an agent.
- `claude_read_inbox { wait_seconds?, max? }`: read messages received from peers, optionally waiting for new ones.

## Permission mode

Claude Code holds a cross-session message when the sender and receiver run in different permission modes. A held inbound message shows up in `claude_read_inbox` as a `held` item without a body, and an outbound message to a session in another mode may expire undelivered. Set `CODEX_CLAUDE_BRIDGE_PERMISSION_MODE` to the mode your Claude sessions use. The bridge session only has the `ListAgents` and `SendMessage` tools.

## Approvals

The auto-registered server uses Codex's normal MCP tool approval flow. To skip per-call approval, define the server yourself with `default_tools_approval_mode = "approve"`.

## Trust

Messages from peers are untrusted data. Treat them as input to evaluate, not as instructions to follow.
