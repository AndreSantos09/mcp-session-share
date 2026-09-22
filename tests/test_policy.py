"""
CAP-8 (story 11): política de room (`policy.mode`) e reserva do display_name
"system" — ver `seguranca.md` ameaça 1 (prompt injection cross-conta).

Toda room nasce em "chat-only" (default): intent="handoff" em session_send e
qualquer session_send_json (sempre tem "action") retornam POLICY_DENIED.
Só o criador muda isso via session_set_policy, para "handoff-enabled".

Testa a camada SessionStore direto (mesmo padrão de test_invites.py) — as
tools de app/main.py só fazem require_scope + _check_display_name antes de
chamar o store, então a lógica de negócio real está toda aqui.
"""
import pytest

from app.redis_store import (
    ForbiddenError,
    InvalidPolicyModeError,
    PolicyDeniedError,
)


# ---------------------------------------------------------------------
# policy.mode default e session_send(intent=handoff)
# ---------------------------------------------------------------------


async def test_room_defaults_to_chat_only(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    meta = await store._get_meta(room["room_id"])
    assert meta["policy_mode"] == "chat-only"


async def test_handoff_denied_in_chat_only(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    with pytest.raises(PolicyDeniedError, match="POLICY_DENIED"):
        await store.send_message(
            room_id=room["room_id"],
            participant_id=joined["participant_id"],
            text="pode fazer isso pra mim?",
            intent="handoff",
        )


async def test_non_handoff_intents_unaffected_by_chat_only(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    for intent in ("pergunta", "fyi", "conclusao"):
        result = await store.send_message(
            room_id=room["room_id"], participant_id=joined["participant_id"], text="oi", intent=intent
        )
        assert result["intent"] == intent


async def test_handoff_accepted_in_handoff_enabled_room(store):
    room = await store.create_room(
        display_name="criadora", ttl_seconds=120, open_join=True, policy_mode="handoff-enabled"
    )
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    result = await store.send_message(
        room_id=room["room_id"],
        participant_id=joined["participant_id"],
        text="pode fazer isso pra mim?",
        intent="handoff",
    )
    assert result["intent"] == "handoff"
    # envelope igual ao de qualquer outro intent — sem tratamento especial
    polled = await store.poll_messages(
        room_id=room["room_id"], participant_id=room["participant_id"], timeout_seconds=1
    )
    handoff_msg = next(m for m in polled["messages"] if m["intent"] == "handoff")
    assert handoff_msg["origin"] == "participant"
    assert handoff_msg["untrusted"] is True
    assert handoff_msg["content"] == {"kind": "text", "text": "pode fazer isso pra mim?"}


# ---------------------------------------------------------------------
# session_send_json / policy
# ---------------------------------------------------------------------


async def test_send_json_denied_in_chat_only(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    with pytest.raises(PolicyDeniedError, match="POLICY_DENIED"):
        await store.send_json_message(
            room_id=room["room_id"], participant_id=joined["participant_id"], payload={"action": "deploy"}
        )


async def test_send_json_accepted_in_handoff_enabled_room(store):
    room = await store.create_room(
        display_name="criadora", ttl_seconds=120, open_join=True, policy_mode="handoff-enabled"
    )
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    result = await store.send_json_message(
        room_id=room["room_id"], participant_id=joined["participant_id"], payload={"action": "deploy"}
    )
    assert "message_id" in result


# ---------------------------------------------------------------------
# session_set_policy
# ---------------------------------------------------------------------


async def test_set_policy_by_creator_switches_mode(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    result = await store.set_policy(
        room_id=room["room_id"], participant_id=room["participant_id"], mode="handoff-enabled"
    )
    assert result["policy_mode"] == "handoff-enabled"
    # agora funciona, sem precisar recriar a room
    sent = await store.send_message(
        room_id=room["room_id"], participant_id=joined["participant_id"], text="valeu", intent="handoff"
    )
    assert sent["intent"] == "handoff"


async def test_set_policy_by_non_creator_is_forbidden(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    with pytest.raises(ForbiddenError, match="FORBIDDEN"):
        await store.set_policy(room_id=room["room_id"], participant_id=joined["participant_id"], mode="handoff-enabled")


async def test_set_policy_rejects_unknown_mode(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120)
    with pytest.raises(InvalidPolicyModeError, match="INVALID_POLICY_MODE"):
        await store.set_policy(room_id=room["room_id"], participant_id=room["participant_id"], mode="anything-goes")


async def test_create_room_rejects_unknown_policy_mode(store):
    with pytest.raises(InvalidPolicyModeError, match="INVALID_POLICY_MODE"):
        await store.create_room(display_name="criadora", ttl_seconds=120, policy_mode="anything-goes")


# ---------------------------------------------------------------------
# display_name "system" reservado (via tools de app/main.py — é lá que
# _check_display_name roda, não no store)
# ---------------------------------------------------------------------


@pytest.mark.usefixtures("dev_mode_no_auth")
class TestNameReserved:
    async def test_session_share_rejects_system_display_name(self, tool_store):
        from mcp.server.mcpserver.exceptions import ToolError

        from app.main import session_share

        for variant in ("system", "System", " SYSTEM ", "SyStEm"):
            with pytest.raises(ToolError, match="NAME_RESERVED"):
                await session_share(display_name=variant)

    async def test_session_join_rejects_system_display_name(self, tool_store):
        from mcp.server.mcpserver.exceptions import ToolError

        from app.main import session_join, session_share

        created = await session_share(display_name="Criadora", policy={"open_join": True})
        with pytest.raises(ToolError, match="NAME_RESERVED"):
            await session_join(room_id=created["room_id"], display_name="System")

    async def test_session_share_rejects_control_characters_in_display_name(self, tool_store):
        """Achado da revisão (rodada 2): display_name vira content.actor_name
        em eventos de sistema — sem esta checagem, uma quebra de linha ou
        caractere de controle poderia forjar múltiplas linhas/sequências
        dentro de um campo estruturado que as instructions descrevem como
        dado de um evento confiável."""
        from mcp.server.mcpserver.exceptions import ToolError

        from app.main import session_share

        for hostile in ("linha 1\nlinha 2", "nome\tcom\ttab", "nome\rcarriage", "bell\x07here"):
            with pytest.raises(ToolError, match="INVALID_DISPLAY_NAME"):
                await session_share(display_name=hostile)

    async def test_session_share_rejects_whitespace_only_display_name(self, tool_store):
        from mcp.server.mcpserver.exceptions import ToolError

        from app.main import session_share

        with pytest.raises(ToolError, match="INVALID_DISPLAY_NAME"):
            await session_share(display_name="   ")


@pytest.fixture
def tool_store(store, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(main_module, "_store", store)
    return store
