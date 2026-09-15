"""observe-query client tests against a stub transport."""

import httpx
from observe_query import ObserverClient, call_tool


def _handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/v2/runs/r1":
        return httpx.Response(200, json={"run_id": "r1", "status": "success"})
    if request.url.path == "/v2/runs/r1/failures":
        return httpx.Response(200, json={"failures": [], "evidence": []})
    if request.url.path == "/v2/insights":
        return httpx.Response(200, json={"items": [{"rule": "run-failure"}]})
    return httpx.Response(404, json={"detail": "nope"})


def _client() -> ObserverClient:
    return ObserverClient(
        "http://test",
        client=httpx.Client(base_url="http://test", transport=httpx.MockTransport(_handler)),
    )


def test_get_run() -> None:
    assert _client().get_run("r1")["status"] == "success"


def test_run_handle_chains() -> None:
    run = _client().run("r1")
    assert run.get()["run_id"] == "r1"
    assert run.failures()["failures"] == []


def test_insights_unwraps_items() -> None:
    assert _client().insights()[0]["rule"] == "run-failure"


def test_call_tool_dispatch() -> None:
    assert call_tool(_client(), "get_run", run_id="r1")["status"] == "success"


def test_call_tool_unknown() -> None:
    import pytest

    with pytest.raises(KeyError):
        call_tool(_client(), "nope")


def test_404_raises() -> None:
    import pytest
    from observe_query.client import ObserverError

    with pytest.raises(ObserverError):
        _client().get_run("missing")
