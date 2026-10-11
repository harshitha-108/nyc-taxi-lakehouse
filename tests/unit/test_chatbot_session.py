"""Session-scoped context never crosses unknown, expired, or reset IDs."""

from chatbot.session import ConversationContext, SessionStore


def test_context_is_scoped_to_server_issued_id() -> None:
    now = [100.0]
    store = SessionStore(ttl_seconds=60, clock=lambda: now[0])
    first, initial = store.resolve(None)
    assert initial == ConversationContext()
    store.update(first, ConversationContext(metric="trip_count", zones=("Midtown Center",)))
    same, context = store.resolve(first)
    assert same == first
    assert context.zones == ("Midtown Center",)
    other, other_context = store.resolve("client-chosen-or-reset-id")
    assert other != first
    assert other_context == ConversationContext()


def test_expiry_and_capacity_forget_old_context() -> None:
    now = [100.0]
    store = SessionStore(ttl_seconds=5, max_sessions=1, clock=lambda: now[0])
    first, _ = store.resolve(None)
    store.update(first, ConversationContext(periods=("2024-06",)))
    replacement, _ = store.resolve(None)
    assert replacement != first
    fresh, context = store.resolve(first)
    assert fresh != first
    assert context == ConversationContext()
    now[0] = 106.0
    after_expiry, context = store.resolve(fresh)
    assert after_expiry != fresh
    assert context == ConversationContext()
