"""
SPEC-invite-at-creation: session_share(policy={"invite_on_create": true})
devolve join_code (e invite_expires_at) no MESMO retorno de session_share —
sem precisar de uma chamada session_invite separada logo em seguida, pro
caso comum de "já sei quem vai entrar, é 1 pessoa combinada por fora".

Testa pela camada de tool (app.main.session_share), mesmo padrão de
test_tool_layer_session_invite_forbidden_for_non_creator em
tests/test_invites.py — prova que a validação e a composição
create_room+create_invite acontecem de fato no dispatch da tool, não só no
SessionStore.
"""
from mcp.server.mcpserver.exceptions import ToolError
import pytest

from app import config


async def test_session_share_invite_on_create_returns_working_join_code(store, monkeypatch):
    import app.main as app_main

    monkeypatch.setattr(app_main, "_store", store)
    monkeypatch.setattr(config, "AUTH_ENABLED", False)

    room = await app_main.session_share(
        display_name="criadora", ttl_seconds=120, policy={"invite_on_create": True}
    )

    assert "join_code" in room
    assert "invite_expires_at" in room
    # expires_at da ROOM (session_share já devolvia) não pode ser confundido
    # com invite_expires_at (TTL de 10 min do convite) — nomes diferentes.
    assert "expires_at" in room
    assert room["invite_expires_at"] != room["expires_at"]

    # o join_code funciona de verdade num session_join subsequente — entra
    # como pending, igual ao fluxo com session_invite separado (mesmo uso
    # único / TTL / validação).
    joined = await app_main.session_join(
        room_id=room["room_id"], display_name="visitante", join_code=room["join_code"]
    )
    assert joined["room_status"] == "pending"

    # uso único: o mesmo join_code não funciona de novo.
    with pytest.raises(ToolError, match="INVITE_USED"):
        await app_main.session_join(
            room_id=room["room_id"], display_name="outro visitante", join_code=room["join_code"]
        )


async def test_session_share_invite_on_create_with_open_join_fails_without_creating_room(store, monkeypatch):
    import app.main as app_main

    monkeypatch.setattr(app_main, "_store", store)
    monkeypatch.setattr(config, "AUTH_ENABLED", False)

    active_before = await store.count_active_rooms()

    with pytest.raises(ToolError, match="INVALID_POLICY"):
        await app_main.session_share(
            display_name="criadora",
            ttl_seconds=120,
            policy={"invite_on_create": True, "open_join": True},
        )

    # falhou ANTES de qualquer side-effect observável: nenhuma room nova foi
    # criada por trás do erro (sem room órfã).
    active_after = await store.count_active_rooms()
    assert active_after == active_before


async def test_session_share_default_has_no_invite_fields(store, monkeypatch):
    """Comportamento padrão (invite_on_create ausente) continua intocado —
    default false é opt-in, não muda o retorno de sempre."""
    import app.main as app_main

    monkeypatch.setattr(app_main, "_store", store)
    monkeypatch.setattr(config, "AUTH_ENABLED", False)

    room = await app_main.session_share(display_name="criadora", ttl_seconds=120)
    assert "join_code" not in room
    assert "invite_expires_at" not in room
