"""
Testes de session_peek (SPEC-session-peek, CAP-1).

session_peek lê o que há de novo desde cursor:{pid} SEM avançar esse cursor
(ao contrário de session_poll, que sempre regrava cursor:{pid} ao final) —
chamar session_peek repetidas vezes é idempotente, e um session_poll chamado
depois ainda vê a mesma mensagem que o peek já tinha mostrado.

Roda contra um Redis local real (ver tests/conftest.py).
"""
import pytest

from app.redis_store import SessionStore

from conftest import _make_room

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")


def _real(messages: list[dict]) -> list[dict]:
    """Filtra eventos de sistema — deixa só mensagens reais (têm intent)."""
    return [m for m in messages if m.get("intent") is not None]


async def test_peek_is_idempotent_and_does_not_advance_cursor(store: SessionStore):
    room_id, [sender_id, receiver_id] = await _make_room(store, n_participants=2)

    await store.send_message(room_id=room_id, participant_id=sender_id, text="evento A")

    first_peek = await store.peek_messages(room_id=room_id, participant_id=receiver_id)
    second_peek = await store.peek_messages(room_id=room_id, participant_id=receiver_id)

    first_texts = [m["content"]["text"] for m in _real(first_peek["messages"])]
    second_texts = [m["content"]["text"] for m in _real(second_peek["messages"])]
    assert first_texts == ["evento A"]
    assert second_texts == ["evento A"]
    assert first_peek["room_status"] == "open"
    assert second_peek["room_status"] == "open"

    # Um poll de verdade, chamado depois de dois peeks, AINDA vê a mesma
    # mensagem — prova que nenhum dos dois peeks avançou cursor:{pid}.
    poll_result = await store.poll_messages(room_id=room_id, participant_id=receiver_id, timeout_seconds=1)
    poll_texts = [m["content"]["text"] for m in _real(poll_result["messages"])]
    assert poll_texts == ["evento A"]

    # Depois do poll real (que avança o cursor), um novo peek já não traz
    # mais aquele item.
    third_peek = await store.peek_messages(room_id=room_id, participant_id=receiver_id)
    assert _real(third_peek["messages"]) == []


async def test_peek_sees_new_message_after_cursor_advances(store: SessionStore):
    room_id, [sender_id, receiver_id] = await _make_room(store, n_participants=2)

    await store.send_message(room_id=room_id, participant_id=sender_id, text="evento A")
    await store.poll_messages(room_id=room_id, participant_id=receiver_id, timeout_seconds=1)

    # Nada de novo desde o cursor avançado pelo poll.
    empty_peek = await store.peek_messages(room_id=room_id, participant_id=receiver_id)
    assert _real(empty_peek["messages"]) == []

    await store.send_message(room_id=room_id, participant_id=sender_id, text="evento B")

    peek_after_new_message = await store.peek_messages(room_id=room_id, participant_id=receiver_id)
    texts = [m["content"]["text"] for m in _real(peek_after_new_message["messages"])]
    assert texts == ["evento B"]


async def test_peek_pending_participant_returns_empty_without_error(store: SessionStore):
    """Mesmo tratamento de poll_messages: um participante ainda não aprovado
    (join com join_code, sem session_approve) vê room_status='pending' e
    nenhuma mensagem — sem erro."""
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=False)
    room_id = room["room_id"]
    invite = await store.create_invite(room_id=room_id, participant_id=room["participant_id"])
    joined = await store.join_room(room_id=room_id, display_name="pending-guy", join_code=invite["join_code"])

    result = await store.peek_messages(room_id=room_id, participant_id=joined["participant_id"])
    assert result == {"messages": [], "room_status": "pending"}
