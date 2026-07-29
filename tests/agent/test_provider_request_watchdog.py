"""Tests for the provider request deadline error contract."""

from agent.provider_request_watchdog import (
    PROVIDER_REQUEST_STALLED,
    ProviderRequestStalledError,
)


def make_error() -> ProviderRequestStalledError:
    return ProviderRequestStalledError(
        provider="openrouter",
        model="anthropic/claude-sonnet-4",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="req_123",
        retry_count=2,
        bytes_received=4096,
    )


def test_provider_request_stalled_error_has_safe_structured_metadata():
    error = make_error()

    assert isinstance(error, TimeoutError)
    assert error.error_code == PROVIDER_REQUEST_STALLED == "provider_request_stalled"
    assert error.retryable is True
    assert error.provider == "openrouter"
    assert error.model == "anthropic/claude-sonnet-4"
    assert error.timeout_seconds == 30.0
    assert error.elapsed_seconds == 30.125
    assert error.api_request_id == "req_123"
    assert error.retry_count == 2
    assert error.bytes_received == 4096

    diagnostic = str(error)
    assert len(diagnostic) <= 500
    assert "request body" not in diagnostic.lower()
    assert "response payload" not in diagnostic.lower()
    assert not hasattr(error, "request_body")
    assert not hasattr(error, "response_payload")


def test_provider_request_stalled_error_optional_metadata_defaults():
    # Callers that have no request id yet (pre-flight stalls) must still be
    # able to raise the contract with only the required deadline metadata.
    error = ProviderRequestStalledError(
        provider="openrouter",
        model="anthropic/claude-sonnet-4",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
    )

    assert error.api_request_id == ""
    assert error.retry_count == 0
    assert error.bytes_received == 0


def test_provider_request_stalled_error_diagnostic_bounds_untrusted_metadata():
    # Provider/model/request-id strings come from upstream responses; a long
    # value must not make the diagnostic unbounded.
    error = ProviderRequestStalledError(
        provider="p" * 5000,
        model="m" * 5000,
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="r" * 5000,
    )

    assert len(str(error)) <= 500


def test_provider_request_stalled_error_escapes_control_characters():
    error = ProviderRequestStalledError(
        provider="provider\nforged",
        model="model\rforged",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="request\tforged",
    )

    diagnostic = str(error)
    assert "\n" not in diagnostic
    assert "\r" not in diagnostic
    assert "\t" not in diagnostic
    assert r"provider\nforged" in diagnostic
    assert r"model\rforged" in diagnostic
    assert r"request\tforged" in diagnostic
