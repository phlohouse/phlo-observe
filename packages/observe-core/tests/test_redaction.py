"""Redaction: keys, patterns, paths, value patterns, URLs, end-to-end."""

from __future__ import annotations

import json
from pathlib import Path

from observe_core import ObserveSettings, configure, event, flush, observe, shutdown
from observe_core.drains.jsonl import JsonlDrain
from observe_core.drains.memory import MemoryDrain
from observe_core.redaction import REDACTED, Redactor, sanitize_url
from observe_core.runtime import Runtime


def _data(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


def test_default_secret_keys_redacted():
    redactor = Redactor()
    data = {
        "attributes": {
            "password": "hunter2",
            "api_key": "abc123",
            "nested": {"authorization": "Bearer x", "safe": "ok"},
            "list": [{"token": "t"}, "plain"],
        }
    }
    redactor.redact_event(data)
    attrs = data["attributes"]
    assert attrs["password"] == REDACTED
    assert attrs["api_key"] == REDACTED
    assert attrs["nested"]["authorization"] == REDACTED
    assert attrs["nested"]["safe"] == "ok"
    assert attrs["list"][0]["token"] == REDACTED
    assert attrs["list"][1] == "plain"


def test_case_insensitive():
    data = {"attributes": {"PASSWORD": "x", "Secret": "y", "API_KEY": "z"}}
    Redactor().redact_event(data)
    assert all(v == REDACTED for v in data["attributes"].values())


def test_extra_keys_and_regex():
    redactor = Redactor(extra_keys=["ssn"], key_patterns=[r"^db_.*cred"])
    data = {"attributes": {"ssn": "123", "db_admin_cred": "x", "other": "keep"}}
    redactor.redact_event(data)
    assert data["attributes"]["ssn"] == REDACTED
    assert data["attributes"]["db_admin_cred"] == REDACTED
    assert data["attributes"]["other"] == "keep"


def test_dotted_path_rules():
    redactor = Redactor(path_rules=["attributes.db.password", "connection.dsn"])
    data = {
        "attributes": {"db": {"password": "x", "host": "ok"}},
        "source": {"connection": {"dsn": "postgres://u:p@h"}},
    }
    redactor.redact_event(data)
    assert data["attributes"]["db"]["password"] == REDACTED
    assert data["attributes"]["db"]["host"] == "ok"
    assert data["source"]["connection"]["dsn"] == REDACTED


def test_value_patterns():
    redactor = Redactor()
    jwt = (
        "eyJhbGciOiJIUzI1NiJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )
    data = {
        "attributes": {
            "note": "used Bearer abcdef1234567890 token",
            "jwt": jwt,
            "plain": "nothing here",
        }
    }
    redactor.redact_event(data)
    assert data["attributes"]["note"] == REDACTED
    assert data["attributes"]["jwt"] == REDACTED
    assert data["attributes"]["plain"] == "nothing here"


def test_redaction_reaches_drain(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with observe("ingestion.load") as evt:
        evt.set(password="hunter2", rows=10)
    (ev,) = _data(drain)
    assert ev["attributes"]["password"] == REDACTED
    assert ev["attributes"]["rows"] == 10
    assert "hunter2" not in json.dumps(ev)


def test_secrets_never_reach_jsonl_file(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    rt = configure(
        ObserveSettings(
            service_name="test",
            drains=[{"type": "jsonl", "path": str(path)}],
            spool_enabled=False,
        )
    )
    try:
        event("application.log", attributes={"token": "secret-token-value", "ok": 1})
        flush(2.0)
    finally:
        shutdown(2.0)
    content = path.read_text()
    assert "secret-token-value" not in content
    assert REDACTED in content
    assert isinstance(rt.drains[0], JsonlDrain)


def test_sanitize_url():
    assert sanitize_url("postgresql://user:p4ss@db:5432/app") == "postgresql://user:***@db:5432/app"
    out = sanitize_url("https://api.example.com/x?token=abc&q=1")
    assert "token=%5BREDACTED%5D" in out or "token=[REDACTED]" in out.replace("%5B", "[").replace(
        "%5D", "]"
    )
    assert "q=1" in out
    assert sanitize_url("not a url") is not None
