"""
Testes da janela de settle em session_poll (SPEC-poll-settle-window, CAP-1).

Dois eventos publicados a poucas dezenas de ms de distância na mesma room
devem chegar na MESMA resposta de `session_poll` de quem está esperando, sem
exigir uma segunda chamada — desde que SESSION_POLL_SETTLE_MS (default
~150ms) esteja ativo. Com SESSION_POLL_SETTLE_MS=0 o comportamento volta a
ser idêntico ao anterior a esta spec: só o primeiro evento na primeira
chamada, o segundo exige um poll_messages extra.

Roda contra um Redis local real (ver tests/conftest.py).
"""
import asyncio

import pytest

from app import config
from app.redis_store import SessionStore

from conftest import _make_room

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")


def _real(messages: list[dict]) -> list[dict]:
    """Filtra eventos de sistema — deixa só mensagens reais (têm intent)."""
    return [m for m in messages if m.get("intent") is not None]


async def test_settle_window_batches_fast_follower_into_same_poll(store: SessionStore):
    """CAP-1: com SESSION_POLL_SETTLE_MS no default, evento A seguido de
    evento B ~30ms depois chegam juntos num único poll_messages."""
    room_id, [sender_id, receiver_id] = await _make_room(store, n_participants=2)

    async def _publish_both():
        await store.send_message(room_id=room_id, participant_id=sender_id, text="evento A")
        await asyncio.sleep(0.03)
        await store.send_message(room_id=room_id, participant_id=receiver_id, text="evento B")

    publisher = asyncio.create_task(_publish_both())
    result = await store.poll_messages(room_id=room_id, participant_id=receiver_id, timeout_seconds=2)
    await publisher

    texts = [m["content"]["text"] for m in _real(result["messages"])]
    assert texts == ["evento A", "evento B"]


async def test_settle_window_disabled_requires_second_poll(store: SessionStore, monkeypatch):
    """Com SESSION_POLL_SETTLE_MS=0, o poll acorda no primeiro evento e NÃO
    espera o segundo — comportamento idêntico ao anterior a esta spec."""
    monkeypatch.setattr(config, "SESSION_POLL_SETTLE_MS", 0)
    room_id, [sender_id, receiver_id] = await _make_room(store, n_participants=2)

    async def _publish_both():
        await store.send_message(room_id=room_id, participant_id=sender_id, text="evento A")
        await asyncio.sleep(0.03)
        await store.send_message(room_id=room_id, participant_id=receiver_id, text="evento B")

    publisher = asyncio.create_task(_publish_both())
    first = await store.poll_messages(room_id=room_id, participant_id=receiver_id, timeout_seconds=2)
    await publisher

    first_texts = [m["content"]["text"] for m in _real(first["messages"])]
    assert first_texts == ["evento A"]

    second = await store.poll_messages(room_id=room_id, participant_id=receiver_id, timeout_seconds=1)
    second_texts = [m["content"]["text"] for m in _real(second["messages"])]
    assert second_texts == ["evento B"]
