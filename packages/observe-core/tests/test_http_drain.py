"""HTTP drain: retries, backoff, gzip, 413 split, auth, keepalive."""

from __future__ import annotations

import gzip
import json

import httpx
import pytest
import respx
from observe_core.drains.base import CanonicalEvent, DrainFailure
from observe_core.drains.http import HttpDrain
from observe_core.models import EventEnvelope
from observe_core.serialization import dumps
from observe_core.timestamps import utcnow

URL = "https://observer.test/v1/events:batch"


def _env(**overrides) -> EventEnvelope:
    base = {
        "event_id": "01JTEST0000000000000000000",
        "event": "asset.materialize",
        "category": "data",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": utcnow(),
        "service": {"name": "svc"},
    }
    base.update(overrides)
    return EventEnvelope(**base)


def _canon(env: EventEnvelope) -> CanonicalEvent:
    data = env.to_canonical_dict()
    return CanonicalEvent(data=data, payload=dumps(data), delivery=env.delivery)


def _drain(**overrides) -> HttpDrain:
    opts = {"endpoint": URL, "read_timeout_s": 2.0, "backoff_base_ms": 1.0}
    opts.update(overrides)
    return HttpDrain(**opts)


def _body(request: httpx.Request) -> list:
    content = request.content
    if request.headers.get("content-encoding") == "gzip":
        content = gzip.decompress(content)
    return json.loads(content)


class TestHttpPost:
    @respx.mock
    def test_posts_json_array(self):
        route = respx.post(URL).respond(202)
        _drain().emit_batch([_canon(_env()), _canon(_env())])
        assert route.called
        body = _body(route.calls.last.request)
        assert len(body) == 2
        assert body[0]["event"] == "asset.materialize"

    @respx.mock
    def test_bearer_auth_header(self):
        route = respx.post(URL).respond(202)
        _drain(token="tok123").emit_batch([_canon(_env())])
        assert route.calls.last.request.headers["authorization"] == "Bearer tok123"

    @respx.mock
    def test_api_key_header(self):
        route = respx.post(URL).respond(202)
        _drain(api_key="key9").emit_batch([_canon(_env())])
        assert route.calls.last.request.headers["x-api-key"] == "key9"

    @respx.mock
    def test_custom_headers(self):
        route = respx.post(URL).respond(202)
        _drain(headers={"x-team": "phlo"}).emit_batch([_canon(_env())])
        assert route.calls.last.request.headers["x-team"] == "phlo"

    @respx.mock
    def test_gzip_above_threshold(self):
        route = respx.post(URL).respond(202)
        _drain(gzip_threshold_bytes=1).emit_batch([_canon(_env())])
        req = route.calls.last.request
        assert req.headers["content-encoding"] == "gzip"
        assert json.loads(gzip.decompress(req.content))[0]["event"] == "asset.materialize"

    @respx.mock
    def test_no_gzip_below_threshold(self):
        route = respx.post(URL).respond(202)
        _drain(gzip_threshold_bytes=10**9).emit_batch([_canon(_env())])
        assert "content-encoding" not in route.calls.last.request.headers


class TestRetries:
    @respx.mock
    def test_retries_on_5xx(self):
        route = respx.post(URL).respond(500)
        drain = _drain(max_attempts=3)
        with pytest.raises(DrainFailure):
            drain.emit_batch([_canon(_env())])
        assert route.call_count == 3

    @respx.mock
    def test_retries_on_429(self):
        route = respx.post(URL).respond(429)
        drain = _drain(max_attempts=2)
        with pytest.raises(DrainFailure):
            drain.emit_batch([_canon(_env())])
        assert route.call_count == 2

    @respx.mock
    def test_no_retry_on_4xx(self):
        route = respx.post(URL).respond(400)
        drain = _drain(max_attempts=5)
        with pytest.raises(DrainFailure):
            drain.emit_batch([_canon(_env())])
        assert route.call_count == 1

    @respx.mock
    def test_retries_on_transport_error(self):
        route = respx.post(URL).mock(
            side_effect=[httpx.ConnectError("refused"), httpx.ConnectError("refused")]
        )
        drain = _drain(max_attempts=2)
        with pytest.raises(DrainFailure):
            drain.emit_batch([_canon(_env())])
        assert route.call_count == 2

    @respx.mock
    def test_success_after_retry(self):
        route = respx.post(URL).mock(side_effect=[httpx.Response(500), httpx.Response(202)])
        drain = _drain(max_attempts=3)
        drain.emit_batch([_canon(_env())])  # no raise
        assert route.call_count == 2


class TestPayloadSplitting:
    @respx.mock
    def test_splits_on_413(self):
        route = respx.post(URL).mock(
            side_effect=[
                httpx.Response(413),
                httpx.Response(202),
                httpx.Response(202),
            ]
        )
        drain = _drain(max_attempts=1)
        drain.emit_batch([_canon(_env()) for _ in range(4)])
        assert route.call_count == 3
        sizes = [len(_body(c.request)) for c in route.calls]
        assert sizes == [4, 2, 2]


class TestKeepalive:
    def test_client_reused(self):
        drain = _drain()
        assert drain._client is drain._client
        drain.close()
