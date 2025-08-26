from __future__ import annotations

from typing import Callable, Optional

import requests


class AuthStrategy:
    """Strategy interface for applying authentication to a requests session.

    Implementations modify the provided `requests.Session` so that subsequent
    requests made through the session are authenticated per the strategy.
    """

    def apply(self, session: requests.Session) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def refresh(
        self, session: requests.Session
    ) -> None:  # pragma: no cover - optional for refreshable auth
        """Optional hook for refreshing credentials on-demand."""
        return None


class NoAuth(AuthStrategy):
    """No-op authentication strategy."""

    def apply(self, session: requests.Session) -> None:
        return None


class BearerAuth(AuthStrategy):
    """Bearer token authentication strategy.

    Parameters
    - token: static token value to use for Authorization header
    - token_getter: callable invoked to retrieve a token value when applying
    """

    def __init__(
        self,
        token: Optional[str] = None,
        token_getter: Optional[Callable[[], str]] = None,
    ) -> None:
        if token is None and token_getter is None:
            raise ValueError(
                "Either token or token_getter must be provided for BearerAuth"
            )
        self._token = token
        self._token_getter = token_getter

    def _resolve_token(self) -> str:
        if self._token_getter is not None:
            return self._token_getter()
        assert self._token is not None
        return self._token

    def apply(self, session: requests.Session) -> None:
        token = self._resolve_token()
        session.headers.update({"Authorization": f"Bearer {token}"})


__all__ = [
    "AuthStrategy",
    "NoAuth",
    "BearerAuth",
]
