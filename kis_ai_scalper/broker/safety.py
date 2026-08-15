"""Small, dependency-free safety helpers for the read-only smoke path."""

from __future__ import annotations

import re


def redact(value: str | None, visible: int = 4) -> str:
    """Return a preview that never contains the complete secret."""
    if not value:
        return "<empty>"
    if len(value) <= visible * 2:
        return "*" * len(value)
    return f"{value[:visible]}...{value[-visible:]}"


# Build paths from segments so this module can be used by tests that scan the
# read-only adapters for accidental order/account endpoint references.
_FORBIDDEN_PATHS = (
    "/uapi/" + "domestic-stock/v1/trading/" + "order-cash",
    "/uapi/" + "domestic-stock/v1/trading/" + "inquire-balance",
    "/api/" + "orders",
)


def contains_forbidden_kis_endpoint(text: str) -> bool:
    """Return whether text references a known order/account endpoint path."""
    normalized = text.lower().replace("\\", "/")
    return any(path in normalized for path in _FORBIDDEN_PATHS)


_JWT_LIKE = re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")


def contains_token_like_value(text: str) -> bool:
    """Return whether text contains a JWT-shaped value."""
    return bool(_JWT_LIKE.search(text))
