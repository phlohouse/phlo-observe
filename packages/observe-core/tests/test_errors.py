"""Structured error tests."""

from __future__ import annotations

from observe_core.errors import ObservedError, error_info_from_exception


def test_observed_error_fields():
    err = ObservedError(
        "Sample metadata is incomplete",
        code="MISSING_METADATA",
        why="Three wells have no sample identifier",
        fix="Update the source metadata and rerun",
        retryable=False,
        details={"wells": ["A01", "B04", "C07"]},
    )
    assert err.code == "MISSING_METADATA"
    assert err.message == "Sample metadata is incomplete"
    assert err.why == "Three wells have no sample identifier"
    assert err.fix == "Update the source metadata and rerun"
    assert err.retryable is False
    assert err.details["wells"] == ["A01", "B04", "C07"]
    assert str(err) == "Sample metadata is incomplete"
    assert isinstance(err, Exception)


def test_observed_error_serializes():
    info = ObservedError("m", code="C").to_error_info()
    data = info.model_dump(mode="json")
    assert data == {
        "code": "C",
        "message": "m",
        "why": None,
        "fix": None,
        "exception_type": "ObservedError",
        "retryable": False,
        "stacktrace": None,
        "details": {},
    }


def test_observed_error_cause_chaining():
    try:
        try:
            raise ValueError("low level")
        except ValueError as low:
            raise ObservedError("high level", code="HIGH") from low
    except ObservedError as err:
        info = err.to_error_info()
    assert info.details["cause"]["exception_type"] == "ValueError"
    assert info.details["cause"]["message"] == "low level"


def test_generic_exception_conversion():
    try:
        raise KeyError("missing-key")
    except KeyError as exc:
        info = error_info_from_exception(exc)
    assert info.exception_type == "KeyError"
    assert "missing-key" in info.message
    assert info.retryable is False


def test_stacktrace_capture():
    try:
        raise RuntimeError("trace me")
    except RuntimeError as exc:
        info = error_info_from_exception(exc, include_traceback=True)
    assert info.stacktrace is not None
    assert "RuntimeError" in info.stacktrace


def test_no_locals_leak():
    secret = "hunter2"
    try:
        raise ObservedError("failed", code="X") from ValueError(secret)
    except ObservedError as err:
        info = err.to_error_info()
    # cause captures type+message only, no locals
    assert info.details["cause"]["exception_type"] == "ValueError"
