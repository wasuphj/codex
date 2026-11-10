//! Pure parsers for the `claude -p --output-format stream-json` JSONL stream
//! and for the human-readable `ListAgents` tool result.

use serde::Serialize;
use serde_json::Value;

/// A cross-session message delivered to the bridge session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerMessage {
    pub from: String,
    pub from_name: Option<String>,
    pub body: String,
}

/// One semantic event extracted from a stream line.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Event {
    /// Replay of a plain user prompt written on stdin (needs `--replay-user-messages`).
    UserText(String),
    Peer(PeerMessage),
    ToolUse {
        name: String,
        input: Value,
    },
    ToolResult {
        text: String,
        is_error: bool,
    },
    Result {
        is_error: bool,
        text: String,
    },
    /// `peer_message_hold`: the message was held (mode mismatch) or dropped.
    Hold {
        state: String,
        from: String,
        from_name: Option<String>,
        cause: String,
    },
}

/// Parses one stdout line into zero or more events. Unknown or malformed
/// lines yield no events.
pub(crate) fn parse_line(line: &str) -> Vec<Event> {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return Vec::new();
    };
    match value.get("type").and_then(Value::as_str) {
        Some("user") => parse_user(&value),
        Some("assistant") => parse_assistant(&value),
        Some("result") => vec![Event::Result {
            is_error: value
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            text: str_field(&value, "result").unwrap_or_default(),
        }],
        Some("system") if str_field(&value, "subtype").as_deref() == Some("peer_message_hold") => {
            let cause = str_field(&value, "cause")
                .or_else(|| str_field(&value, "outcome"))
                .unwrap_or_default();
            vec![Event::Hold {
                state: str_field(&value, "state").unwrap_or_default(),
                from: str_field(&value, "from").unwrap_or_default(),
                from_name: str_field(&value, "from_name"),
                cause,
            }]
        }
        _ => Vec::new(),
    }
}

fn str_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn parse_user(value: &Value) -> Vec<Event> {
    if let Some(origin) = value.get("origin")
        && origin.get("kind").and_then(Value::as_str) == Some("peer")
    {
        return vec![Event::Peer(PeerMessage {
            from: str_field(origin, "from").unwrap_or_else(|| "unknown".to_string()),
            from_name: str_field(origin, "name"),
            body: str_field(origin, "body").unwrap_or_default(),
        })];
    }
    let content = value.pointer("/message/content");
    match content {
        Some(Value::String(text)) => match parse_cross_session_tag(text) {
            Some(peer) => vec![Event::Peer(peer)],
            None => vec![Event::UserText(text.clone())],
        },
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
            .map(|block| Event::ToolResult {
                text: tool_result_text(block.get("content")),
                is_error: block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn tool_result_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn parse_assistant(value: &Value) -> Vec<Event> {
    let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .map(|block| Event::ToolUse {
            name: str_field(block, "name").unwrap_or_default(),
            input: block.get("input").cloned().unwrap_or(Value::Null),
        })
        .collect()
}

/// Fallback for streams without `origin`: extracts the sender and body from
/// the `<cross-session-message from="..." from-name="...">` wrapper.
fn parse_cross_session_tag(text: &str) -> Option<PeerMessage> {
    const OPEN: &str = "<cross-session-message ";
    const CLOSE: &str = "</cross-session-message>";
    let start = text.find(OPEN)? + OPEN.len();
    let tag_end = start + text[start..].find('>')?;
    let attrs = &text[start..tag_end];
    let body_start = tag_end + 1;
    let body_end = body_start + text[body_start..].find(CLOSE)?;
    let body = text[body_start..body_end]
        .strip_prefix('\n')
        .unwrap_or(&text[body_start..body_end]);
    let body = body.strip_suffix('\n').unwrap_or(body);
    Some(PeerMessage {
        from: attr(attrs, "from").unwrap_or_else(|| "unknown".to_string()),
        from_name: attr(attrs, "from-name"),
        body: body.to_string(),
    })
}

fn attr(attrs: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    let found = if attrs.starts_with(&needle) {
        0
    } else {
        attrs.find(&format!(" {needle}"))? + 1
    };
    let value_start = found + needle.len();
    let value_end = value_start + attrs[value_start..].find('"')?;
    Some(attrs[value_start..value_end].to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AgentPeer {
    pub name: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub kind: String,
    pub status: String,
    pub started: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AgentList {
    pub self_name: String,
    pub peers: Vec<AgentPeer>,
}

/// Parses the `ListAgents` tool result text. Returns `None` when the header
/// line is not recognised so that callers can fall back to the raw text.
pub(crate) fn parse_list_agents(raw: &str) -> Option<AgentList> {
    let mut lines = raw.lines();
    let header = lines.find(|line| line.starts_with("This session is "))?;
    let self_name = header
        .strip_prefix("This session is ")?
        .split(" [")
        .next()?
        .trim()
        .to_string();
    let peers = lines
        .skip_while(|line| !line.starts_with("Peer sessions"))
        .skip(1)
        .filter_map(parse_peer_line)
        .collect();
    Some(AgentList { self_name, peers })
}

fn parse_peer_line(line: &str) -> Option<AgentPeer> {
    let mut parts = line.trim().split('·').map(str::trim);
    let ident = parts.next()?;
    let (name, rest) = ident.split_once(" [")?;
    let reference = rest.strip_suffix(']')?;
    let kind = parts.next().unwrap_or_default();
    let status = parts.next().unwrap_or_default();
    let started = parts.next().unwrap_or_default();
    Some(AgentPeer {
        name: name.trim().to_string(),
        reference: reference.to_string(),
        kind: kind.to_string(),
        status: status.to_string(),
        started: started
            .strip_prefix("started ")
            .unwrap_or(started)
            .to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn fixture_events(name: &str) -> Vec<Event> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .flat_map(parse_line)
            .collect()
    }

    #[test]
    fn probe_stream_without_replay_yields_results_only() {
        let events = fixture_events("probe-stream.jsonl");
        let results = events
            .iter()
            .filter(|event| matches!(event, Event::Result { .. }))
            .count();
        assert_eq!(results, 2);
        // Without --replay-user-messages the inbound text is only visible as
        // assistant text, which the parser deliberately ignores.
        assert_eq!(events.iter().any(|e| matches!(e, Event::Peer(_))), false);
    }

    #[test]
    fn held_stream_yields_hold_events() {
        let holds: Vec<_> = fixture_events("probe-held.jsonl")
            .into_iter()
            .filter(|event| matches!(event, Event::Hold { .. }))
            .collect();
        assert_eq!(
            holds,
            vec![
                Event::Hold {
                    state: "held".into(),
                    from: "uds:/run/user/1000/cc-socks/844943.sock".into(),
                    from_name: Some("alice-1e".into()),
                    cause: "mode-mismatch".into(),
                },
                Event::Hold {
                    state: "dropped".into(),
                    from: "uds:/run/user/1000/cc-socks/844943.sock".into(),
                    from_name: Some("alice-1e".into()),
                    cause: "discarded".into(),
                },
            ]
        );
    }

    #[test]
    fn replay_stream_yields_peer_message_from_origin() {
        let events = fixture_events("replay-recv.jsonl");
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    Event::Peer(peer) => Some(peer.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![PeerMessage {
                from: "uds:/run/user/1000/cc-socks/1025476.sock".into(),
                from_name: Some("replay-send".into()),
                body: "HELLO-REPLAY-42".into(),
            }]
        );
        assert_eq!(
            events.first(),
            Some(&Event::UserText("ready? reply ok".into()))
        );
    }

    #[test]
    fn replay_send_stream_yields_tool_use_and_result() {
        let events = fixture_events("replay-send.jsonl");
        let tool_use = events.iter().find_map(|event| match event {
            Event::ToolUse { name, input } => Some((name.clone(), input.clone())),
            _ => None,
        });
        let (name, input) = tool_use.unwrap();
        assert_eq!(name, "SendMessage");
        assert_eq!(input["to"], "replay-recv");
        assert_eq!(input["message"], "HELLO-REPLAY-42");
        let result = events.iter().find_map(|event| match event {
            Event::ToolResult { text, is_error } => Some((text.clone(), *is_error)),
            _ => None,
        });
        let (text, is_error) = result.unwrap();
        assert_eq!(is_error, false);
        assert_eq!(text.contains("\"success\":true"), true);
    }

    #[test]
    fn cross_session_tag_fallback_without_origin() {
        let line = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "Another Claude session sent a message:\n<cross-session-message from=\"uds:/x.sock\" from-name=\"peer-a\" from-mode=\"bypass\">\nline1\nline2\n</cross-session-message>\n\ntrailer"}
        })
        .to_string();
        assert_eq!(
            parse_line(&line),
            vec![Event::Peer(PeerMessage {
                from: "uds:/x.sock".into(),
                from_name: Some("peer-a".into()),
                body: "line1\nline2".into(),
            })]
        );
    }

    #[test]
    fn garbage_lines_are_ignored() {
        assert_eq!(parse_line("not json"), Vec::new());
        assert_eq!(parse_line("{\"type\":\"rate_limit_event\"}"), Vec::new());
    }

    #[test]
    fn list_agents_text_is_parsed() {
        let raw = "This session is probe-be [663ac0] — the name other sessions use to message it ...\n\nPeer sessions (2):\n  alice-6f [27a820]  ·  interactive  ·  busy  ·  started 26m ago\n  bg-worker [aa11bb]  ·  headless  ·  idle  ·  started 3h ago\n";
        let list = parse_list_agents(raw).unwrap();
        assert_eq!(list.self_name, "probe-be");
        assert_eq!(
            list.peers,
            vec![
                AgentPeer {
                    name: "alice-6f".into(),
                    reference: "27a820".into(),
                    kind: "interactive".into(),
                    status: "busy".into(),
                    started: "26m ago".into(),
                },
                AgentPeer {
                    name: "bg-worker".into(),
                    reference: "aa11bb".into(),
                    kind: "headless".into(),
                    status: "idle".into(),
                    started: "3h ago".into(),
                },
            ]
        );
        assert_eq!(parse_list_agents("unexpected text"), None);
    }
}
