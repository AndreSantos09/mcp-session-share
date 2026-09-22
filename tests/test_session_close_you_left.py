"""
SPEC-session-close-you-left (CAP-1): `session_close`/`close_room` numa room
que continua aberta (outros participantes ainda dentro) deixa de devolver só
`{"room_status": "open"}` — ambíguo com "o close não teve efeito nenhum" — e
passa a incluir `you_left: true` e `remaining_participants: N` (o `remaining`
já calculado em `close_room` via `hlen` pós-`hdel`, sem round-trip extra).

O branch onde o chamador é o último participante (room fecha de vez)
continua devolvendo só `{"room_status": "closed"}`, sem os campos novos.

Testa a camada SessionStore direto (mesmo padrão de test_policy.py) e também
a tool `session_close` de app/main.py (mesmo padrão de
test_listener_template.py — tool chamada direto, `store` trocado via
monkeypatch em `app.main._store`, `dev_mode_no_auth` porque a chamada não
passa `ctx`).
"""
import pytest

import app.main as main_module
from app.main import session_close, session_share

from conftest import _make_room

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")


# ---------------------------------------------------------------------
# camada SessionStore (store.close_room)
# ---------------------------------------------------------------------


async def test_close_room_reports_you_left_when_room_stays_open(store):
    room_id, [p0, p1] = await _make_room(store, 2)

    result = await store.close_room(room_id=room_id, participant_id=p0)

    assert result == {"room_status": "open", "you_left": True, "remaining_participants": 1}


async def test_close_room_stays_closed_only_shape_when_last_participant(store):
    room_id, [p0] = await _make_room(store, 1)

    result = await store.close_room(room_id=room_id, participant_id=p0)

    assert result == {"room_status": "closed"}


# ---------------------------------------------------------------------
# tool session_close (app/main.py)
# ---------------------------------------------------------------------


@pytest.fixture
def tool_store(store, monkeypatch):
    monkeypatch.setattr(main_module, "_store", store)
    return store


async def test_session_close_reports_you_left_when_room_stays_open(tool_store):
    created = await session_share(display_name="p0", ttl_seconds=120, policy={"open_join": True})
    room_id = created["room_id"]
    from app.main import session_join

    joined = await session_join(room_id=room_id, display_name="p1")

    result = await session_close(room_id=room_id, participant_id=created["participant_id"])

    assert result == {"room_status": "open", "you_left": True, "remaining_participants": 1}
    assert joined["participant_id"]  # só pra deixar claro que o outro participante existe


async def test_session_close_stays_closed_only_shape_when_last_participant(tool_store):
    created = await session_share(display_name="p0", ttl_seconds=120)

    result = await session_close(room_id=created["room_id"], participant_id=created["participant_id"])

    assert result == {"room_status": "closed"}
