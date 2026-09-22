"""
CAP-4 (story 10): convite de uso único, pending, aprovação e kick.

Testa principalmente a camada SessionStore direto (mesmo padrão de
test_autoloop_handshake.py/test_protocol_1to1.py) — mais rápido e mais
direto pra cobrir a matriz de erros (INVITE_REQUIRED/USED/EXPIRED,
FORBIDDEN, PARTICIPANT_NOT_FOUND) sem precisar montar um token JWT real
pra cada cenário. Rooms aqui usam o DEFAULT (open_join=False) — diferente
de `_make_room` (conftest.py), que passa open_join=True de propósito pra
não quebrar a suíte de autoloop/protocolo 1:1 que não testa convite.
"""
from mcp.server.mcpserver.exceptions import ToolError
import pytest

from app import config
from app.redis_store import (
    ForbiddenError,
    InviteExpiredError,
    InviteRequiredError,
    InviteUsedError,
    ParticipantNotFoundError,
    participant_hash,
)


async def test_join_without_code_requires_invite_by_default(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    with pytest.raises(InviteRequiredError, match="INVITE_REQUIRED"):
        await store.join_room(room_id=room["room_id"], display_name="visitante")


async def test_join_without_code_enters_directly_when_open_join(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="visitante")
    assert "participants" in joined  # caminho direto de sempre, não pending
    assert joined["participant_id"] != room["participant_id"]


async def test_join_with_valid_code_is_pending_and_notifies_creator(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])

    joined = await store.join_room(
        room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"]
    )
    assert joined == {"participant_id": joined["participant_id"], "room_status": "pending"}
    assert joined["participant_id"] != room["participant_id"]

    # a criadora vê o evento "pediu para entrar" no próprio poll
    polled = await store.poll_messages(
        room_id=room["room_id"], participant_id=room["participant_id"], timeout_seconds=1
    )
    assert any("pediu para entrar" in m["content"].get("text", "") for m in polled["messages"])


async def test_join_code_reused_is_invite_used(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    await store.join_room(room_id=room["room_id"], display_name="v1", join_code=invite["join_code"])

    with pytest.raises(InviteUsedError, match="INVITE_USED"):
        await store.join_room(room_id=room["room_id"], display_name="v2", join_code=invite["join_code"])


async def test_join_code_expired_is_invite_expired(store, monkeypatch):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    # TTL negativo: o convite já nasce expirado — evita precisar de sleep
    # real de 10 minutos pra provar o caminho de expiração.
    monkeypatch.setattr(config, "INVITE_TTL_SECONDS", -5)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])

    with pytest.raises(InviteExpiredError, match="INVITE_EXPIRED"):
        await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])


async def test_join_code_unknown_is_also_invite_expired(store):
    """Código que nunca existiu recebe a MESMA resposta de expirado — não
    vira oráculo pra descobrir se um código já existiu ou não."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    with pytest.raises(InviteExpiredError, match="INVITE_EXPIRED"):
        await store.join_room(room_id=room["room_id"], display_name="visitante", join_code="codigo-que-nunca-existiu")


async def test_invite_only_by_creator(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    other = await store.join_room(room_id=room["room_id"], display_name="outro")

    with pytest.raises(ForbiddenError, match="FORBIDDEN"):
        await store.create_invite(room_id=room["room_id"], participant_id=other["participant_id"])


async def test_approve_only_by_creator(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    other = await store.join_room(room_id=room["room_id"], display_name="outro")
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="v", join_code=invite["join_code"])

    with pytest.raises(ForbiddenError, match="FORBIDDEN"):
        await store.approve_participant(
            room_id=room["room_id"],
            participant_id=other["participant_id"],
            target_hash=participant_hash(pending["participant_id"]),
        )


async def test_kick_only_by_creator(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    other = await store.join_room(room_id=room["room_id"], display_name="outro")
    victim = await store.join_room(room_id=room["room_id"], display_name="alvo")

    with pytest.raises(ForbiddenError, match="FORBIDDEN"):
        await store.kick_participant(
            room_id=room["room_id"],
            participant_id=other["participant_id"],
            target_hash=participant_hash(victim["participant_id"]),
        )


async def test_pending_invisible_to_non_creator_in_status(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    other = await store.join_room(room_id=room["room_id"], display_name="outro")
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    await store.join_room(room_id=room["room_id"], display_name="v", join_code=invite["join_code"])

    creator_status = await store.status(room_id=room["room_id"], participant_id=room["participant_id"])
    assert len(creator_status["pending"]) == 1

    other_status = await store.status(room_id=room["room_id"], participant_id=other["participant_id"])
    assert "pending" not in other_status


async def test_approve_moves_pending_to_participant_and_notifies_room(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])

    result = await store.approve_participant(
        room_id=room["room_id"],
        participant_id=room["participant_id"],
        target_hash=participant_hash(pending["participant_id"]),
    )
    assert result == {"approved": True}

    status = await store.status(room_id=room["room_id"], participant_id=room["participant_id"])
    assert status["participant_count"] == 2
    assert status["pending"] == []

    # aprovado agora funciona normalmente em qualquer outra tool (deixou de
    # ser PARTICIPANT_NOT_FOUND)
    polled = await store.poll_messages(
        room_id=room["room_id"], participant_id=pending["participant_id"], timeout_seconds=1
    )
    assert polled["room_status"] == "open"


async def test_kicked_participant_gets_participant_not_found_afterwards(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    victim = await store.join_room(room_id=room["room_id"], display_name="alvo")

    result = await store.kick_participant(
        room_id=room["room_id"],
        participant_id=room["participant_id"],
        target_hash=participant_hash(victim["participant_id"]),
    )
    assert result == {"kicked": True}

    with pytest.raises(ParticipantNotFoundError, match="PARTICIPANT_NOT_FOUND"):
        await store.poll_messages(
            room_id=room["room_id"], participant_id=victim["participant_id"], timeout_seconds=1
        )


async def test_kick_also_removes_a_pending_participant(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])

    result = await store.kick_participant(
        room_id=room["room_id"],
        participant_id=room["participant_id"],
        target_hash=participant_hash(pending["participant_id"]),
    )
    assert result == {"kicked": True}

    status = await store.status(room_id=room["room_id"], participant_id=room["participant_id"])
    assert status["pending"] == []

    # não é mais nem participante nem pending: poll dele também falha agora
    with pytest.raises(ParticipantNotFoundError, match="PARTICIPANT_NOT_FOUND"):
        await store.poll_messages(
            room_id=room["room_id"], participant_id=pending["participant_id"], timeout_seconds=1
        )


async def test_kick_unknown_hash_is_participant_not_found(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    with pytest.raises(ParticipantNotFoundError, match="PARTICIPANT_NOT_FOUND"):
        await store.kick_participant(
            room_id=room["room_id"], participant_id=room["participant_id"], target_hash="00" * 6
        )


async def test_participant_hash_never_equals_raw_participant_id(store):
    """Sanity check da própria fórmula: o hash não é o id, nem um prefixo dele."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    h = participant_hash(room["participant_id"])
    assert h != room["participant_id"]
    assert h not in room["participant_id"]
    assert len(h) == 12


async def test_approve_unknown_hash_is_participant_not_found(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    with pytest.raises(ParticipantNotFoundError, match="PARTICIPANT_NOT_FOUND"):
        await store.approve_participant(
            room_id=room["room_id"], participant_id=room["participant_id"], target_hash="00" * 6
        )


async def test_approve_hash_of_kicked_pending_is_participant_not_found(store):
    """Índice reverso (SPEC-participant-hash-index): o hash de um pendente
    que já foi kickado continua batendo no índice (kick_participant apaga a
    entrada), então tentar aprová-lo depois precisa dar PARTICIPANT_NOT_FOUND
    — nunca um erro interno por match_pid apontar pra alguém que já não está
    mais em pending."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
    pending = await store.join_room(room_id=room["room_id"], display_name="visitante", join_code=invite["join_code"])
    target_hash = participant_hash(pending["participant_id"])

    await store.kick_participant(
        room_id=room["room_id"], participant_id=room["participant_id"], target_hash=target_hash
    )

    with pytest.raises(ParticipantNotFoundError, match="PARTICIPANT_NOT_FOUND"):
        await store.approve_participant(
            room_id=room["room_id"], participant_id=room["participant_id"], target_hash=target_hash
        )


async def _spy_hgetall_calls(store):
    """Espiona (sem alterar comportamento — delega pro Redis real) as chaves
    lidas via HGETALL, pra provar que kick/approve não varrem mais
    :participants/:pending inteiros. Retorna (calls_list, restore_fn)."""
    calls: list[str] = []
    original_hgetall = store._redis.hgetall

    async def spy(key, *args, **kwargs):
        calls.append(key)
        return await original_hgetall(key, *args, **kwargs)

    store._redis.hgetall = spy

    def restore():
        store._redis.hgetall = original_hgetall

    return calls, restore


def _touches_participants_or_pending(keys: list[str]) -> bool:
    return any(k.endswith(":participants") or k.endswith(":pending") for k in keys)


async def test_kick_at_max_participants_does_not_scan_participants_or_pending(store):
    """CAP-1 (SPEC-participant-hash-index): com a room cheia (MAX_PARTICIPANTS),
    session_kick de um participante ativo resolve o target_hash via HGET no
    índice reverso — não itera hgetall(:participants)/hgetall(:pending)."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = [room]
    for i in range(1, config.MAX_PARTICIPANTS):
        joined.append(await store.join_room(room_id=room["room_id"], display_name=f"p{i}"))
    victim = joined[-1]

    calls, restore = await _spy_hgetall_calls(store)
    try:
        result = await store.kick_participant(
            room_id=room["room_id"],
            participant_id=room["participant_id"],
            target_hash=participant_hash(victim["participant_id"]),
        )
    finally:
        restore()

    assert result == {"kicked": True}
    assert not _touches_participants_or_pending(calls)


async def test_approve_at_max_pending_does_not_scan_pending(store):
    """Mesmo CAP-1, para session_approve: resolve via HGET no índice, não
    hgetall(:pending) inteiro, mesmo com vários pendentes na room."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    pendings = []
    for i in range(config.MAX_PARTICIPANTS - 1):
        invite = await store.create_invite(room_id=room["room_id"], participant_id=room["participant_id"])
        pendings.append(
            await store.join_room(room_id=room["room_id"], display_name=f"v{i}", join_code=invite["join_code"])
        )
    target = pendings[-1]

    calls, restore = await _spy_hgetall_calls(store)
    try:
        result = await store.approve_participant(
            room_id=room["room_id"],
            participant_id=room["participant_id"],
            target_hash=participant_hash(target["participant_id"]),
        )
    finally:
        restore()

    assert result == {"approved": True}
    assert not _touches_participants_or_pending(calls)


async def test_tool_layer_session_invite_forbidden_for_non_creator(store, monkeypatch):
    """Acesso pela camada de tool (require_scope + FORBIDDEN do store),
    não só direto no SessionStore — prova que app.main.session_invite
    propaga o mesmo erro de domínio."""
    import app.main as app_main

    monkeypatch.setattr(app_main, "_store", store)
    monkeypatch.setattr(config, "AUTH_ENABLED", False)

    room = await app_main.session_share(display_name="criadora", ttl_seconds=120, policy={"open_join": True})
    other = await app_main.session_join(room_id=room["room_id"], display_name="outro")

    with pytest.raises(ToolError, match="FORBIDDEN"):
        await app_main.session_invite(room_id=room["room_id"], participant_id=other["participant_id"])
