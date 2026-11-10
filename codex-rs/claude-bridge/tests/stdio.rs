#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end tests: drive the real `codex-claude-bridge` binary over MCP
//! stdio with a fake `claude` that mimics the stream-json protocol.

use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::process::Child;
use std::process::ChildStdin;
use std::process::ChildStdout;
use std::process::Command;
use std::process::Stdio;

use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Client {
    fn start(claude_bin: &str, envs: &[(&str, &str)]) -> Self {
        let bin = codex_utils_cargo_bin::cargo_bin("codex-claude-bridge").unwrap();
        let mut command = Command::new(bin);
        command
            .env("CODEX_CLAUDE_BRIDGE_BIN", claude_bin)
            .env("CODEX_CLAUDE_BRIDGE_TIMEOUT_SECS", "15")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in envs {
            command.env(k, v);
        }
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let init = client.request(
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
        );
        assert_eq!(init["result"]["serverInfo"]["name"], "claude");
        client.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        client
    }

    fn send(&mut self, value: &Value) {
        writeln!(self.stdin, "{value}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let mut line = String::new();
            assert!(
                self.stdout.read_line(&mut line).unwrap() > 0,
                "server closed"
            );
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id {
                return value;
            }
        }
    }

    /// Returns (is_error, text).
    fn call(&mut self, name: &str, arguments: Value) -> (bool, String) {
        let response = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        let result = &response["result"];
        (
            result["isError"].as_bool().unwrap_or(false),
            result["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        )
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fake_claude() -> String {
    format!("{}/tests/fake_claude.py", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn lists_three_tools_with_instructions() {
    let mut client = Client::start(&fake_claude(), &[]);
    let response = client.request("tools/list", json!({}));
    let mut names: Vec<_> = response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "claude_list_agents",
            "claude_read_inbox",
            "claude_send_message"
        ]
    );
}

#[test]
fn list_agents_returns_parsed_peers_and_raw() {
    let mut client = Client::start(&fake_claude(), &[]);
    let (is_error, text) = client.call("claude_list_agents", json!({}));
    assert_eq!(is_error, false);
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["self_name"], "fake-bridge");
    assert_eq!(value["peers"][0]["name"], "peer-one");
    assert_eq!(value["peers"][0]["ref"], "def456");
    assert_eq!(value["peers"][0]["status"], "busy");
    assert_eq!(
        value["raw"].as_str().unwrap().contains("Peer sessions (1)"),
        true
    );
}

#[test]
fn send_message_verifies_arguments() {
    let mut client = Client::start(&fake_claude(), &[]);
    let (is_error, text) = client.call(
        "claude_send_message",
        json!({"to": "peer-one", "message": "안녕 \"quoted\"\nline2"}),
    );
    assert_eq!((is_error, text.as_str()), (false, "{\"success\":true}"));
}

#[test]
fn send_message_rejects_tampered_arguments() {
    let mut client = Client::start(&fake_claude(), &[("FAKE_TAMPER", "1")]);
    let (is_error, text) = client.call(
        "claude_send_message",
        json!({"to": "peer-one", "message": "original"}),
    );
    assert_eq!(is_error, true);
    assert_eq!(text.contains("altered the SendMessage arguments"), true);
}

#[test]
fn send_message_rejects_missing_tool_call() {
    let mut client = Client::start(&fake_claude(), &[("FAKE_NOCALL", "1")]);
    let (is_error, text) = client.call(
        "claude_send_message",
        json!({"to": "peer-one", "message": "x"}),
    );
    assert_eq!(is_error, true);
    assert_eq!(text.contains("did not call SendMessage"), true);
}

#[test]
fn inbox_returns_peer_and_held_entries_then_empties() {
    let mut client = Client::start(&fake_claude(), &[("FAKE_PEER", "1")]);
    let (_, text) = client.call("claude_read_inbox", json!({"wait_seconds": 5}));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["bridge"], "running");
    let messages = value["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["kind"], "held");
    assert_eq!(messages[0]["cause"], "mode-mismatch");
    assert_eq!(messages[1]["kind"], "message");
    assert_eq!(messages[1]["from"], "uds:/p.sock");
    assert_eq!(messages[1]["from_name"], "peer-one");
    assert_eq!(messages[1]["body"], "hello from peer");
    assert_eq!(
        value["notice"].as_str().unwrap().contains("untrusted"),
        true
    );

    let (_, text) = client.call("claude_read_inbox", json!({}));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["messages"].as_array().unwrap().len(), 0);
}

#[test]
fn bridge_crash_reports_reason_then_respawns() {
    let dir = tempfile::tempdir().unwrap();
    let flag = dir.path().join("died");
    let mut client = Client::start(&fake_claude(), &[("FAKE_DIE_FLAG", flag.to_str().unwrap())]);
    let (is_error, text) = client.call(
        "claude_send_message",
        json!({"to": "peer-one", "message": "x"}),
    );
    assert_eq!(is_error, true);
    assert_eq!(text.contains("exited"), true, "{text}");
    assert_eq!(text.contains("crashed on purpose"), true, "{text}");

    let (is_error, text) = client.call(
        "claude_send_message",
        json!({"to": "peer-one", "message": "x"}),
    );
    assert_eq!((is_error, text.as_str()), (false, "{\"success\":true}"));
}

#[test]
fn missing_claude_binary_gives_clear_error() {
    let mut client = Client::start("/nonexistent/claude-bin", &[]);
    let (is_error, text) = client.call("claude_list_agents", json!({}));
    assert_eq!(is_error, true);
    assert_eq!(text.contains("claude binary not found"), true, "{text}");
}
