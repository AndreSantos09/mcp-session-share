"""
Fixtures e helpers compartilhados entre os módulos de teste: autoloop
(Story 1 — handshake — e Story 2 — turnos/watchdog) e protocolo 1:1
(test_protocol_1to1.py — ack, in_reply_to, turno, intenção).

Roda contra um Redis local real (REDIS_URL, mesma variável que app/config.py
já lê) — sem mock de Redis, por design (ver README "Rodando os testes"):

    docker run --rm -d -p 6379:6379 redis:7-alpine
    REDIS_URL=redis://localhost:6379/0 python3 -m pytest tests/ -v

Cada teste roda numa room própria (session_share cria um room_id novo,
aleatório) — não precisa de flush entre testes, mas um `db` isolado
(REDIS_URL apontando pra um db Redis dedicado a testes) evita ruído se
alguém rodar a suíte contra um Redis compartilhado.
"""
import os

# CAP-4 (story 10): app/config.py exige PARTICIPANT_HASH_KEY com
# AUTH_ENABLED=true (default) — precisa estar no ambiente ANTES do primeiro
# `import app.config` de qualquer módulo de teste (a checagem roda na
# importação, não dá pra corrigir depois com monkeypatch). Valor de teste
# fixo, nunca usado fora daqui.
os.environ.setdefault("PARTICIPANT_HASH_KEY", "test-participant-hash-key-not-secret")

import pytest

from app import config
from app.redis_store import SessionStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
async def store():
    s = SessionStore(redis_url=REDIS_URL)
    yield s
    await s.close()


@pytest.fixture
def dev_mode_no_auth(monkeypatch):
    """CAP-3 (story 8): `require_scope` falha fechado (`UNAUTHENTICATED`)
    com `AUTH_ENABLED=true` e `ctx=None` — e os módulos de teste que
    chamam as tools direto (test_autoloop_handshake.py,
    test_autoloop_turn.py, test_protocol_1to1.py) nunca passam `ctx`
    (não vão pelo dispatch real do SDK). Sem este fixture, todo tool call
    dessas suítes passaria a levar UNAUTHENTICATED — não é o que elas
    testam (autoloop/protocolo 1:1), então rodam em modo dev
    (`AUTH_ENABLED=false`, `require_scope` não checa nada), igual a antes
    da story 8. Não autouse: opt-in explícito via
    `pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")` no topo do
    módulo — tests/test_scopes.py não usa isto, testa com auth ligada de
    verdade (é o próprio objeto do arquivo)."""
    monkeypatch.setattr(config, "AUTH_ENABLED", False)


async def _make_room(store: SessionStore, n_participants: int = 1):
    """Cria uma room com n_participants (o 1º via create_room, os demais via
    join_room) — `open_join=True` (CAP-4, story 10): estes testes cobrem
    autoloop/protocolo 1:1, não convite; o modo de compatibilidade evita que
    todo teste pré-existente precise passar por session_invite/join_code.

    `policy_mode="handoff-enabled"` (CAP-8, story 11): estes testes
    pré-existentes cobrem intent=handoff/session_send_json em si (não a
    política de room, que tem sua própria suíte em test_policy.py) — sem
    isso, toda chamada equivalente aqui levaria POLICY_DENIED por padrão."""
    room = await store.create_room(
        display_name="p0", ttl_seconds=120, open_join=True, policy_mode="handoff-enabled"
    )
    room_id = room["room_id"]
    participant_ids = [room["participant_id"]]
    for i in range(1, n_participants):
        joined = await store.join_room(room_id=room_id, display_name=f"p{i}")
        participant_ids.append(joined["participant_id"])
    return room_id, participant_ids
