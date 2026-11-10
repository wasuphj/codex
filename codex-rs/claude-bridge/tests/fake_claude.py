#!/usr/bin/env python3
"""Minimal stand-in for `claude -p --input-format stream-json` used by tests."""
import json, os, sys

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def tool_use(name, inp):
    emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "t1", "name": name, "input": inp}]}})

def tool_result(text, is_error=False):
    emit({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "is_error": is_error, "content": [{"type": "text", "text": text}]}]}})

def result(text="done"):
    emit({"type": "result", "subtype": "success", "is_error": False, "result": text})

peer_sent = False
for raw in sys.stdin:
    content = json.loads(raw)["message"]["content"]
    emit({"type": "user", "message": {"role": "user", "content": content}, "isReplay": True})
    if "ListAgents tool" in content:
        tool_use("ListAgents", {})
        tool_result("This session is fake-bridge [abc123] — the name other sessions use.\n\nPeer sessions (1):\n  peer-one [def456]  ·  interactive  ·  busy  ·  started 5m ago\n")
        result()
    elif "SendMessage tool" in content:
        flag = os.environ.get("FAKE_DIE_FLAG")
        if flag and not os.path.exists(flag):
            open(flag, "w").close()
            sys.stderr.write("fake claude crashed on purpose\n")
            sys.stderr.flush()
            sys.exit(3)
        args = json.loads(content.split("ARGS: ", 1)[1])
        if os.environ.get("FAKE_TAMPER"):
            args["message"] += " (tampered)"
        if not os.environ.get("FAKE_NOCALL"):
            tool_use("SendMessage", {**args, "summary": "s"})
            tool_result('{"success":true}')
        result()
    else:
        emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "ready"}]}})
        result("ready")
    if os.environ.get("FAKE_PEER") and not peer_sent:
        peer_sent = True
        emit({"type": "system", "subtype": "peer_message_hold", "state": "held", "from": "uds:/h.sock", "from_name": "held-peer", "cause": "mode-mismatch"})
        body = "hello from peer"
        emit({"type": "user", "message": {"role": "user", "content": "Another Claude session sent a message:\n<cross-session-message from=\"uds:/p.sock\">\n" + body + "\n</cross-session-message>"}, "isReplay": True, "origin": {"kind": "peer", "from": "uds:/p.sock", "name": "peer-one", "body": body}})
        result(".")
