"""Small, in-process context store for bounded analytical follow-ups.

Only resolved analytical references are retained. Questions, SQL text, answers,
and credentials are deliberately not stored in session state.
"""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ConversationContext:
    """The minimum context needed to resolve a follow-up such as "they"."""

    metric: str | None = None
    periods: tuple[str, ...] = ()
    zones: tuple[str, ...] = ()
    borough: str | None = None
    boroughs: tuple[str, ...] = ()
    payment_type: str | None = None


class SessionStore:
    """Thread-safe, expiring local sessions with unguessable server-issued IDs."""

    def __init__(self, *, ttl_seconds: int = 1800, max_sessions: int = 128,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if ttl_seconds < 1 or max_sessions < 1:
            raise ValueError("Session limits must be positive")
        self.ttl_seconds = ttl_seconds
        self.max_sessions = max_sessions
        self.clock = clock
        self._lock = threading.Lock()
        self._sessions: dict[str, tuple[float, ConversationContext]] = {}

    def _purge(self, now: float) -> None:
        expired = [key for key, (expires, _) in self._sessions.items()
                   if expires <= now]
        for key in expired:
            del self._sessions[key]

    def resolve(self, proposed_id: str | None) -> tuple[str, ConversationContext]:
        """Reuse a live server-issued ID; otherwise start an isolated session."""
        with self._lock:
            now = self.clock()
            self._purge(now)
            if proposed_id in self._sessions:
                _, context = self._sessions[proposed_id]
                self._sessions[proposed_id] = (now + self.ttl_seconds, context)
                return proposed_id, context
            if len(self._sessions) >= self.max_sessions:
                oldest = min(self._sessions, key=lambda key: self._sessions[key][0])
                del self._sessions[oldest]
            session_id = secrets.token_urlsafe(24)
            self._sessions[session_id] = (now + self.ttl_seconds, ConversationContext())
            return session_id, ConversationContext()

    def update(self, session_id: str, context: ConversationContext) -> None:
        """Keep only a validated analytical reference for the next request."""
        if not isinstance(context, ConversationContext):
            raise TypeError("context must be ConversationContext")
        with self._lock:
            now = self.clock()
            self._purge(now)
            if session_id in self._sessions:
                self._sessions[session_id] = (now + self.ttl_seconds, context)


__all__ = ["ConversationContext", "SessionStore"]
