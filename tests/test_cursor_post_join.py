"""
CAP-4 (story 10): cursor inicial de quem entra = o próprio evento de
entrada — corrige o "0" antigo (app/redis_store.py, poll_messages) que
devolveria TODO o histórico retido na stream. Vale para os DOIS caminhos
de join: _join_room_pending (via session_approve) e _join_room_direct
(open_join=true, compatibilidade) — achado da revisão: o segundo também
vazava o histórico até ganhar o mesmo tratamento aqui.
"""
import pytest

from app.redis_store import _room_key, participant_hash


async def test_fifty_messages_before_approval_yield_zero_to_the_approved(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    creator_id = room["participant_id"]

    invite = await store.create_invite(room_id=room["room_id"], participant_id=creator_id)
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])
    pending_id = pending["participant_id"]

    for i in range(50):
        await store.send_message(room_id=room["room_id"], participant_id=creator_id, text=f"mensagem {i}")

    # enquanto pending, o poll dele não vê nada disso (nem sabe que existe)
    polled_while_pending = await store.poll_messages(room_id=room["room_id"], participant_id=pending_id, timeout_seconds=1)
    assert polled_while_pending == {"messages": [], "room_status": "pending"}

    await store.approve_participant(
        room_id=room["room_id"],
        participant_id=creator_id,
        target_hash=participant_hash(pending_id),
    )

    polled_after_approval = await store.poll_messages(room_id=room["room_id"], participant_id=pending_id, timeout_seconds=1)
    assert polled_after_approval["messages"] == []
    assert polled_after_approval["room_status"] == "open"


async def test_cursor_is_not_the_zero_sentinel_after_approval(store):
    """O cursor gravado no approve é um stream id de verdade (contém "-"),
    não a sentinela "0" que devolveria tudo desde o início."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])

    await store.approve_participant(
        room_id=room["room_id"],
        participant_id=room["participant_id"],
        target_hash=participant_hash(pending["participant_id"]),
    )

    cursor = await store._redis.get(_room_key(room["room_id"], f"cursor:{pending['participant_id']}"))
    assert cursor is not None
    assert cursor != "0"
    assert "-" in cursor  # formato <ms>-<seq> de um stream id real


async def test_messages_sent_after_approval_are_delivered_normally(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])
    pending_id = pending["participant_id"]

    await store.approve_participant(
        room_id=room["room_id"], participant_id=room["participant_id"], target_hash=participant_hash(pending_id)
    )
    await store.send_message(room_id=room["room_id"], participant_id=room["participant_id"], text="oi, bem-vindo")

    polled = await store.poll_messages(room_id=room["room_id"], participant_id=pending_id, timeout_seconds=1)
    texts = [m["content"]["text"] for m in polled["messages"] if m["content"].get("text")]
    assert "oi, bem-vindo" in texts


async def test_fifty_messages_before_direct_join_yield_zero_to_the_joiner(store):
    """Achado da revisão: _join_room_direct (open_join=true) não gravava
    cursor nenhum — o recém-chegado caía no sentinela "0" e via TODO o
    histórico retido, exatamente o vazamento que o CAP-4 existe pra fechar."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    creator_id = room["participant_id"]

    for i in range(50):
        await store.send_message(room_id=room["room_id"], participant_id=creator_id, text=f"mensagem {i}")

    joined = await store.join_room(room_id=room["room_id"], display_name="visitante")
    joiner_id = joined["participant_id"]

    cursor = await store._redis.get(_room_key(room["room_id"], f"cursor:{joiner_id}"))
    assert cursor is not None
    assert cursor != "0"
    assert "-" in cursor  # formato <ms>-<seq> de um stream id real

    polled = await store.poll_messages(room_id=room["room_id"], participant_id=joiner_id, timeout_seconds=1)
    assert polled["messages"] == []
    assert polled["room_status"] == "open"


async def test_messages_sent_after_direct_join_are_delivered_normally(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="visitante")
    joiner_id = joined["participant_id"]

    await store.send_message(room_id=room["room_id"], participant_id=room["participant_id"], text="oi, bem-vindo")

    polled = await store.poll_messages(room_id=room["room_id"], participant_id=joiner_id, timeout_seconds=1)
    texts = [m["content"]["text"] for m in polled["messages"] if m["content"].get("text")]
    assert "oi, bem-vindo" in texts
