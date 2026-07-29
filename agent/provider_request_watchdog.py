"""Provider request deadline contracts and lifecycle monitoring."""

from __future__ import annotations


PROVIDER_REQUEST_STALLED = "provider_request_stalled"


def _diagnostic_label(value: str) -> str:
    """Bound and escape an untrusted label before placing it in log text."""
    return ascii(value)[1:-1][:100]


class ProviderRequestStalledError(TimeoutError):
    """A provider request exceeded its absolute, attempt-local deadline."""

    error_code = PROVIDER_REQUEST_STALLED
    retryable = True

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        timeout_seconds: float,
        elapsed_seconds: float,
        api_request_id: str = "",
        retry_count: int = 0,
        bytes_received: int = 0,
    ) -> None:
        self.provider = provider
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        self.api_request_id = api_request_id
        self.retry_count = retry_count
        self.bytes_received = bytes_received
        super().__init__(
            "Provider request stalled after "
            f"{elapsed_seconds:.3f}s (deadline {timeout_seconds:.3f}s; "
            f"provider={_diagnostic_label(provider)}; "
            f"model={_diagnostic_label(model)}; "
            f"api_request_id={_diagnostic_label(api_request_id)}; "
            f"retry_count={retry_count}; "
            f"bytes_received={bytes_received})"
        )
