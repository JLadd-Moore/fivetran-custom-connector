from __future__ import annotations

from typing import Callable, Optional, Dict, Any

import requests
import threading
import time


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


class OAuth2RefreshTokenAuth(AuthStrategy):
    """OAuth2 refresh-token grant bearer authentication strategy.

    Exchanges a refresh token for access tokens, caches them with expiry, and
    auto-refreshes and retries once on HTTP 401 responses.

    Parameters
    - token_url: OAuth2 token endpoint URL
    - client_id: OAuth2 client identifier
    - client_secret: OAuth2 client secret
    - refresh_token: OAuth2 refresh token
    - extra_headers: Optional static headers to set on the session
    - token_field: JSON field name for the access token (default: "access_token")
    - expires_in_field: JSON field name for token TTL seconds (default: "expires_in")
    - request_timeout: Timeout in seconds for the token HTTP request (default: 30.0)
    - clock_skew_safety_margin: Seconds to refresh before actual expiry (default: 60)
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
        token_field: str = "access_token",
        expires_in_field: str = "expires_in",
        request_timeout: Optional[float] = 30.0,
        clock_skew_safety_margin: int = 60,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._extra_headers = dict(extra_headers) if extra_headers else {}
        self._token_field = token_field
        self._expires_in_field = expires_in_field
        self._request_timeout = request_timeout
        self._clock_skew_safety_margin = max(0, clock_skew_safety_margin)

        self._access_token: Optional[str] = None
        self._token_expires_at_epoch: Optional[float] = None
        self._lock = threading.Lock()

    def _token_is_valid(self) -> bool:
        if self._access_token is None:
            return False
        if self._token_expires_at_epoch is None:
            return True
        return time.time() < self._token_expires_at_epoch

    def _build_refresh_payload(self) -> Dict[str, str]:
        payload = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "refresh_token": self._refresh_token,
            "grant_type": "refresh_token",
        }
        return "&".join([f"{key}={val}" for key, val in payload.items()])

    def _fetch_new_token(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = self._build_refresh_payload()
        response = requests.post(
            self._token_url,
            data=payload,
            headers=headers,
        )
        print(
            f"""
Submitting payload: {payload}
Headers: {headers}
Timeout: {self._request_timeout}
Token URL: {self._token_url}
        """
        )
        response.raise_for_status()
        token_response: Dict[str, Any] = response.json()
        access_token = token_response.get(self._token_field)
        if not access_token:
            raise ValueError(
                f"OAuth2 token response missing '{self._token_field}' field"
            )

        expires_at: Optional[float] = None
        if self._expires_in_field in token_response:
            try:
                ttl_seconds = int(token_response[self._expires_in_field])
                refresh_ttl = max(0, ttl_seconds - self._clock_skew_safety_margin)
                expires_at = time.time() + refresh_ttl
            except Exception:
                expires_at = None

        self._access_token = access_token
        self._token_expires_at_epoch = expires_at

    def refresh(self, session: requests.Session) -> None:
        with self._lock:
            self._fetch_new_token()
            session.headers.update({"Authorization": f"Bearer {self._access_token}"})

    def apply(self, session: requests.Session) -> None:
        if not self._token_is_valid():
            self.refresh(session)

        session.headers.update({"Authorization": f"Bearer {self._access_token}"})
        if self._extra_headers:
            session.headers.update(self._extra_headers)

        wrapper_flag = "_oauth2_refresh_token_auth_wrapped"
        if getattr(session, wrapper_flag, False):
            return None

        original_request = session.request
        self_ref = self

        def request_with_refresh(method: str, url: str, **kwargs):  # type: ignore[override]
            response = original_request(method, url, **kwargs)
            if response is not None and getattr(response, "status_code", None) == 401:
                self_ref.refresh(session)
                return original_request(method, url, **kwargs)
            return response

        session.request = request_with_refresh  # type: ignore[assignment]
        setattr(session, wrapper_flag, True)


__all__ = [
    "AuthStrategy",
    "NoAuth",
    "BearerAuth",
    "OAuth2RefreshTokenAuth",
]
