"""``observe`` CLI: config output must never leak secrets; replay is remote-only."""

from __future__ import annotations

import json

from observe_core.cli import main


def test_config_masks_drain_secrets(monkeypatch, capsys):
    """Regression: ``observe config`` previously printed tokens in cleartext."""
    monkeypatch.setenv(
        "OBSERVE_DRAINS",
        json.dumps(
            [
                {
                    "type": "http",
                    "endpoint": "https://observer.test/v1/events",
                    "token": "SUPERSECRET-TOKEN",
                    "api_key": "SUPERSECRET-KEY",
                    "headers": {"X-Proxy-Auth": "SUPERSECRET-HDR"},
                }
            ]
        ),
    )
    assert main(["config"]) == 0
    out = capsys.readouterr().out
    for secret in ("SUPERSECRET-TOKEN", "SUPERSECRET-KEY", "SUPERSECRET-HDR"):
        assert secret not in out
    data = json.loads(out)
    drain = data["drains"][0]
    assert drain["token"] == "***"
    assert drain["api_key"] == "***"
    assert drain["headers"] == {"X-Proxy-Auth": "***"}
    assert drain["endpoint"] == "https://observer.test/v1/events"  # not masked


def test_replay_spool_requires_remote_drain(monkeypatch, capsys, tmp_path):
    """Console/jsonl drains only → refuse rather than print-then-delete spool."""
    monkeypatch.setenv("OBSERVE_DRAINS", "console")
    code = main(["replay-spool", "--dir", str(tmp_path / "spool")])
    assert code == 1
    assert "no remote drain" in capsys.readouterr().err


def test_replay_spool_replays_to_remote(monkeypatch, tmp_path):
    """A configured remote drain receives the spooled payloads."""
    from observe_core.spool import Spool

    spool_dir = tmp_path / "spool"
    spool = Spool(spool_dir)
    assert spool.append(b'{"event": "wap.promote", "delivery": "critical"}')

    received: list[bytes] = []

    class _FakeRemote:
        name = "http"
        is_remote = True

        def emit_raw(self, payloads):
            received.extend(payloads)

        def emit_batch(self, events): ...

        def flush(self): ...

        def close(self): ...

    import observe_core.runtime as runtime_mod

    monkeypatch.setenv("OBSERVE_DRAINS", "http")
    monkeypatch.setenv("OBSERVE_HTTP_ENDPOINT", "https://o.test/v1/events")
    monkeypatch.setattr(
        runtime_mod.Runtime, "_build_drain", staticmethod(lambda cfg: _FakeRemote())
    )
    assert main(["replay-spool", "--dir", str(spool_dir)]) == 0
    assert len(received) == 1
    assert b"wap.promote" in received[0]
