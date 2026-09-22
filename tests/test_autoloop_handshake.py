"""
Testes do handshake de consentimento do autoloop (Story 1):
autoloop_propose / autoloop_accept / autoloop_decline / autoloop_status.
Inclui também a parada manual a qualquer momento (Story 4): autoloop_stop.

Roda contra um Redis local real (REDIS_URL, mesma variável que app/config.py
já lê) — sem mock de Redis, por design (ver README "Rodando os testes"):

    docker run --rm -d -p 6379:6379 redis:7-alpine
    REDIS_URL=redis://localhost:6379/0 python3 -m pytest tests/ -v

Cada teste roda numa room própria (session_share cria um room_id novo,
aleatório) — não precisa de flush entre testes, mas um `db` isolado
(REDIS_URL apontando pra um db Redis dedicado a testes) evita ruído se
alguém rodar a suíte contra um Redis compartilhado.
"""
import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from app import main as app_main
from app.redis_store import (
    AlreadyAcceptedError,
    AutoloopAlreadyActiveError,
    AutoloopNotActiveError,
    NoPendingProposalError,
    ParticipantNotFoundError,
    RoomNotFoundError,
)

from conftest import _make_room  # fixture `store` também vem de conftest.py

# CAP-3 (story 8): chama as tools direto, sem ctx — precisa de AUTH_ENABLED=false
# (ver conftest.py, fixture dev_mode_no_auth) pra require_scope não recusar tudo.
pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")

# ---------------------------------------------------------------------------
# I/O & Edge-Case Matrix
# ---------------------------------------------------------------------------


async def test_propose_sets_proposed_status_with_one_participant(store):
    room_id, [p0] = await _make_room(store, 1)

    result = await store.propose_autoloop(
        room_id=room_id, participant_id=p0, goal="investigar X", max_turns=10, max_seconds=300
    )
    assert result["status"] == "proposed"
    assert result["loop_participants"] == [p0]
    assert result["max_turns"] == 10
    assert result["max_seconds"] == 300

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "proposed"
    assert status["loop_participants"] == [p0]


async def test_accept_by_second_participant_activates_loop(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    result = await store.accept_autoloop(room_id=room_id, participant_id=p1)
    assert result["status"] == "active"
    assert sorted(result["loop_participants"]) == sorted([p0, p1])

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "active"
    assert status["started_at"] is not None


async def test_accept_by_third_participant_stays_active(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)

    result = await store.accept_autoloop(room_id=room_id, participant_id=p2)
    assert result["status"] == "active"
    assert sorted(result["loop_participants"]) == sorted([p0, p1, p2])


async def test_decline_not_added_to_loop_participants(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    result = await store.decline_autoloop(room_id=room_id, participant_id=p1)
    assert result == {"status": "declined"}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["loop_participants"] == [p0]
    # proposta continua "proposed" pra quem não recusou
    assert status["status"] == "proposed"


async def test_decline_emits_declined_system_event_on_stream(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    # drena o que já está na stream (propose) antes de decline, pra isolar o evento
    await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)

    await store.decline_autoloop(room_id=room_id, participant_id=p1)

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)
    events = [m["content"] for m in poll_result["messages"] if m["content"].get("kind") == "system"]
    declined = next(c for c in events if c.get("event") == "autoloop_declined")
    assert declined["actor_name"] == "p1"
    assert declined["text"] == "Convite de modo autônomo recusado"


async def test_double_propose_raises_already_active(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    with pytest.raises(AutoloopAlreadyActiveError):
        await store.propose_autoloop(
            room_id=room_id, participant_id=p0, goal="g2", max_turns=10, max_seconds=300
        )


async def test_accept_with_no_proposal_raises_no_pending_proposal(store):
    room_id, [p0] = await _make_room(store, 1)

    with pytest.raises(NoPendingProposalError):
        await store.accept_autoloop(room_id=room_id, participant_id=p0)


async def test_decline_with_no_proposal_raises_no_pending_proposal(store):
    room_id, [p0] = await _make_room(store, 1)

    with pytest.raises(NoPendingProposalError):
        await store.decline_autoloop(room_id=room_id, participant_id=p0)


async def test_bad_room_id_raises_room_not_found(store):
    with pytest.raises(RoomNotFoundError):
        await store.propose_autoloop(
            room_id="not-a-real-room-00", participant_id="whatever", goal="g", max_turns=1, max_seconds=1
        )


async def test_bad_participant_id_raises_participant_not_found(store):
    room_id, _ = await _make_room(store, 1)
    with pytest.raises(ParticipantNotFoundError):
        await store.propose_autoloop(
            room_id=room_id, participant_id="not-a-real-participant", goal="g", max_turns=1, max_seconds=1
        )


async def test_concurrent_propose_only_one_winner(store):
    # Duas propostas simultâneas na mesma room (nenhum autoloop prévio):
    # exercita de fato o ramo WatchError do guard WATCH/MULTI/EXEC (não só a
    # checagem sequencial que test_double_propose_raises_already_active já
    # cobre) — exatamente a race que motivou a mudança de storage/atomicidade
    # no Spec Change Log (Review Triage Log #2).
    room_id, [p0, p1] = await _make_room(store, 2)

    results = await asyncio.gather(
        store.propose_autoloop(room_id=room_id, participant_id=p0, goal="a", max_turns=5, max_seconds=5),
        store.propose_autoloop(room_id=room_id, participant_id=p1, goal="b", max_turns=5, max_seconds=5),
        return_exceptions=True,
    )
    successes = [r for r in results if not isinstance(r, BaseException)]
    failures = [r for r in results if isinstance(r, BaseException)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], AutoloopAlreadyActiveError)

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "proposed"
    assert len(status["loop_participants"]) == 1


async def test_bystander_unaffected_by_active_loop(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    # p2 nunca chama accept/decline — segue bystander

    send_result = await store.send_message(room_id=room_id, participant_id=p2, text="oi")
    assert "message_id" in send_result

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p2, timeout_seconds=1)
    assert poll_result["room_status"] == "open"
    events = [m["content"] for m in poll_result["messages"] if m["content"].get("kind") == "system"]
    # bystander enxerga os eventos de sistema do autoloop, mesma stream de sempre
    proposed = next(c for c in events if c.get("event") == "autoloop_proposed")
    assert proposed["actor_name"] == "p0" and proposed["goal"] == "g"
    accepted = next(c for c in events if c.get("event") == "autoloop_accepted")
    assert accepted["actor_name"] == "p1"


# ---------------------------------------------------------------------------
# Coverage adicional (Round 2 — clamp, tool-level goal, decline-after-accept,
# re-accept idempotente) per Spec Change Log
# ---------------------------------------------------------------------------


async def test_propose_clamps_max_turns_and_max_seconds_at_bounds(store, monkeypatch):
    from app import config

    monkeypatch.setattr(config, "AUTOLOOP_HARD_CAP_TURNS", 50)
    monkeypatch.setattr(config, "AUTOLOOP_HARD_CAP_SECONDS", 600)

    room_id, [p0] = await _make_room(store, 1)

    # abaixo do piso (1) -> floor de 1
    result = await store.propose_autoloop(
        room_id=room_id, participant_id=p0, goal="g", max_turns=-5, max_seconds=0
    )
    assert result["max_turns"] == 1
    assert result["max_seconds"] == 1

    # força um novo ciclo pra testar o teto (propose de novo exige ended;
    # como Story 1 não alcança "ended", usamos uma 2ª room pra testar o teto)
    room_id2, [q0] = await _make_room(store, 1)
    result2 = await store.propose_autoloop(
        room_id=room_id2, participant_id=q0, goal="g", max_turns=9999, max_seconds=99999
    )
    assert result2["max_turns"] == 50
    assert result2["max_seconds"] == 600

    status = await store.autoloop_status(room_id=room_id2, participant_id=q0)
    assert status["max_turns"] == 50
    assert status["max_seconds"] == 600


async def test_hard_cap_env_var_floor_against_misconfiguration(monkeypatch):
    monkeypatch.setenv("SESSION_AUTOLOOP_HARD_CAP_TURNS", "0")
    monkeypatch.setenv("SESSION_AUTOLOOP_HARD_CAP_SECONDS", "-10")
    import importlib

    from app import config as config_module

    reloaded = importlib.reload(config_module)
    try:
        assert reloaded.AUTOLOOP_HARD_CAP_TURNS == 1
        assert reloaded.AUTOLOOP_HARD_CAP_SECONDS == 1
    finally:
        # reverte o módulo global pro resto da suíte não herdar esse reload
        monkeypatch.delenv("SESSION_AUTOLOOP_HARD_CAP_TURNS", raising=False)
        monkeypatch.delenv("SESSION_AUTOLOOP_HARD_CAP_SECONDS", raising=False)
        importlib.reload(config_module)



def _use_test_store(monkeypatch, store):
    # app.main usa um SessionStore singleton em nível de módulo, criado uma
    # vez no import (conectado ao REDIS_URL "de produção" do processo). Pra
    # cada teste rodar isolado no seu próprio event loop (pytest-asyncio é
    # function-scoped por padrão) sem reaproveitar uma conexão asyncio presa
    # a um loop já fechado, trocamos temporariamente esse singleton pelo
    # `store` da fixture (mesmo Redis de teste, client novo por teste).
    monkeypatch.setattr(app_main, "_store", store)


async def test_autoloop_propose_tool_rejects_empty_goal(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room = await store.create_room(display_name="p0", ttl_seconds=120)
    with pytest.raises(ToolError):
        await app_main.autoloop_propose(
            room_id=room["room_id"], participant_id=room["participant_id"], goal=""
        )


async def test_autoloop_propose_tool_rejects_overlong_goal(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room = await store.create_room(display_name="p0", ttl_seconds=120)
    too_long = "x" * (app_main.MAX_TEXT_LEN + 1)
    with pytest.raises(ToolError):
        await app_main.autoloop_propose(
            room_id=room["room_id"], participant_id=room["participant_id"], goal=too_long
        )


async def test_autoloop_propose_tool_accepts_valid_goal_through_the_mcp_wrapper(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room = await store.create_room(display_name="p0", ttl_seconds=120)
    result = await app_main.autoloop_propose(
        room_id=room["room_id"], participant_id=room["participant_id"], goal="objetivo válido"
    )
    assert result["status"] == "proposed"


async def test_decline_after_accept_raises_already_accepted_and_leaves_state_unchanged(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)

    with pytest.raises(AlreadyAcceptedError):
        await store.decline_autoloop(room_id=room_id, participant_id=p1)

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert sorted(status["loop_participants"]) == sorted([p0, p1])


async def test_reaccept_by_existing_member_is_idempotent_no_duplicate_event(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)

    # drena a stream até aqui (poll de p0, que ainda não chamou poll)
    await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)

    result = await store.accept_autoloop(room_id=room_id, participant_id=p1)
    assert result["status"] == "active"
    assert sorted(result["loop_participants"]) == sorted([p0, p1])

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)
    accept_events = [
        m for m in poll_result["messages"]
        if m["content"].get("text") and "aceitou o modo autônomo" in m["content"]["text"]
    ]
    assert accept_events == []


async def test_proposer_accepting_own_proposal_is_a_safe_noop(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    # p0 é o proponente — propose_autoloop já o incluiu em loop_participants
    # (SADD do próprio propose_autoloop); chamar accept de novo não deve
    # duplicar o membro, mudar o status, nem reemitir "aceitou".
    result = await store.accept_autoloop(room_id=room_id, participant_id=p0)
    assert result == {"status": "proposed", "loop_participants": [p0]}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "proposed"
    assert status["loop_participants"] == [p0]


# ---------------------------------------------------------------------------
# Acceptance Criteria adicionais: TTL refresh, concorrência, close_room
# ---------------------------------------------------------------------------


async def test_autoloop_calls_refresh_room_ttl_like_session_send(store):
    room_id, [p0, p1] = await _make_room(store, 2)

    # baixa o TTL deliberadamente pra simular "quase expirando"
    from app.redis_store import _room_key

    await store._redis.expire(_room_key(room_id, "meta"), 5)

    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    status_after_propose = await store.status(room_id=room_id, participant_id=p0)
    assert status_after_propose["ttl_remaining_seconds"] > 5

    await store._redis.expire(_room_key(room_id, "meta"), 5)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    status_after_accept = await store.status(room_id=room_id, participant_id=p0)
    assert status_after_accept["ttl_remaining_seconds"] > 5


async def test_concurrent_accept_no_lost_update(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    # drena a stream (evento de propose) antes da corrida, pra contar só o
    # que os dois accepts concorrentes emitem.
    await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)

    await asyncio.gather(
        store.accept_autoloop(room_id=room_id, participant_id=p1),
        store.accept_autoloop(room_id=room_id, participant_id=p2),
    )

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert sorted(status["loop_participants"]) == sorted([p0, p1, p2])
    assert status["status"] == "active"

    # Com o guard WATCH/MULTI/EXEC, só um dos dois accepts concorrentes pode
    # ser "o 2º membro que ativa o loop" — o evento "modo autônomo ativo" não
    # pode disparar duas vezes (era exatamente o bug do read-then-write não
    # atômico antes desse fix).
    poll_result = await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)
    active_events = [
        m for m in poll_result["messages"]
        if m["content"].get("text") and "modo autônomo ativo" in m["content"]["text"]
    ]
    assert len(active_events) == 1


async def test_concurrent_accept_and_decline_same_participant_is_consistent(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    results = await asyncio.gather(
        store.accept_autoloop(room_id=room_id, participant_id=p1),
        store.decline_autoloop(room_id=room_id, participant_id=p1),
        return_exceptions=True,
    )
    accept_result, decline_result = results

    # accept_autoloop nunca falha nesse cenário — decline não bloqueia um
    # accept concorrente/posterior do mesmo participante (Story 1 não tem
    # "sair depois de aceitar", mas também não impede "aceitar depois de
    # recusar"). accept sempre estabelece a adesão de forma determinística.
    assert not isinstance(accept_result, BaseException)

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert p1 in status["loop_participants"]

    # decline só tem dois desfechos consistentes: ou perdeu a corrida (o
    # accept concorrente já tinha comitado quando o WATCH releu o Set) e
    # levanta ALREADY_ACCEPTED sem publicar "recusou"; ou comitou primeiro e
    # devolve "declined" normalmente — nunca um estado no meio do caminho
    # (ex.: "declined" retornado E o SADD do accept perdido).
    if isinstance(decline_result, BaseException):
        assert isinstance(decline_result, AlreadyAcceptedError)
    else:
        assert decline_result == {"status": "declined"}


async def test_close_room_deletes_autoloop_key_on_last_participant_leave(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    from app.redis_store import _autoloop_done_key, _autoloop_extra_keys, _autoloop_key, _autoloop_participants_key

    # propose_autoloop escreve o hash e SADD o proponente no Set de
    # participantes — as duas chaves existem antes do close. O Set `done`
    # não é populado nesta story (reservado pra Story 3), então ele nunca
    # chega a existir de verdade — checado à parte, sem inflar o teto de
    # "existe" das outras duas chaves.
    assert await store._redis.exists(_autoloop_key(room_id)) == 1
    assert await store._redis.exists(_autoloop_participants_key(room_id)) == 1
    assert await store._redis.exists(_autoloop_done_key(room_id)) == 0

    result = await store.close_room(room_id=room_id, participant_id=p0)
    assert result["room_status"] == "closed"

    for key in _autoloop_extra_keys(room_id):
        assert await store._redis.exists(key) == 0


# ---------------------------------------------------------------------------
# Story 4 — autoloop_stop (parada manual a qualquer momento)
# ---------------------------------------------------------------------------


async def test_stop_pending_proposal_ends_it(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    result = await store.stop_autoloop(room_id=room_id, participant_id=p0)
    assert result == {"status": "ended", "ended_reason": "stopped"}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_stop_active_loop_ends_it(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)

    result = await store.stop_autoloop(room_id=room_id, participant_id=p0)
    assert result == {"status": "ended", "ended_reason": "stopped"}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_bystander_can_stop_active_loop_with_third_participant(store):
    # Acceptance Criteria: room com 3 participantes (proposer + 1 accepter
    # em loop_participants, loop já active); o 3º (nunca aceitou o convite)
    # chama autoloop_stop e consegue, sem precisar estar em loop_participants.
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    # p2 nunca chama accept/decline — segue bystander

    status_before = await store.autoloop_status(room_id=room_id, participant_id=p2)
    assert status_before["status"] == "active"
    assert p2 not in status_before["loop_participants"]

    result = await store.stop_autoloop(room_id=room_id, participant_id=p2)
    assert result == {"status": "ended", "ended_reason": "stopped"}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_bystander_can_stop_merely_proposed_loop_not_yet_active(store):
    # Bystander stop não exige que o loop já esteja "active" — funciona
    # igual num convite ainda "proposed" (só o proponente dentro,
    # loop_participants com 1 membro).
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    # p1 nunca chama accept/decline — segue bystander

    status_before = await store.autoloop_status(room_id=room_id, participant_id=p1)
    assert status_before["status"] == "proposed"
    assert p1 not in status_before["loop_participants"]

    result = await store.stop_autoloop(room_id=room_id, participant_id=p1)
    assert result == {"status": "ended", "ended_reason": "stopped"}

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_stop_with_no_proposal_ever_raises_autoloop_not_active(store):
    room_id, [p0] = await _make_room(store, 1)
    with pytest.raises(AutoloopNotActiveError):
        await store.stop_autoloop(room_id=room_id, participant_id=p0)


async def test_stop_already_ended_loop_raises_autoloop_not_active_and_state_unchanged(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.stop_autoloop(room_id=room_id, participant_id=p0)

    with pytest.raises(AutoloopNotActiveError):
        await store.stop_autoloop(room_id=room_id, participant_id=p0)

    # não idempotente: o segundo stop não muda nada no estado já ended
    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_stop_bad_room_id_raises_room_not_found(store):
    with pytest.raises(RoomNotFoundError):
        await store.stop_autoloop(room_id="not-a-real-room-00", participant_id="whatever")


async def test_stop_bad_participant_id_raises_participant_not_found(store):
    room_id, _ = await _make_room(store, 1)
    with pytest.raises(ParticipantNotFoundError):
        await store.stop_autoloop(room_id=room_id, participant_id="not-a-real-participant")


async def test_stop_emits_system_event_on_stream(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    # drena o que já está na stream (propose) antes do stop, pra isolar o evento
    await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)

    await store.stop_autoloop(room_id=room_id, participant_id=p0)

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)
    events = [m["content"] for m in poll_result["messages"] if m["content"].get("kind") == "system"]
    stopped = next(c for c in events if c.get("event") == "autoloop_stopped")
    assert stopped["actor_name"] == "p0"
    assert stopped["text"] == "Modo autônomo parado"


async def test_stop_refreshes_room_ttl_like_propose_accept_decline(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    from app.redis_store import _room_key

    await store._redis.expire(_room_key(room_id, "meta"), 5)

    await store.stop_autoloop(room_id=room_id, participant_id=p0)
    status_after_stop = await store.status(room_id=room_id, participant_id=p0)
    assert status_after_stop["ttl_remaining_seconds"] > 5


async def test_concurrent_accept_and_stop_is_consistent_no_unhandled_exception(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    results = await asyncio.gather(
        store.accept_autoloop(room_id=room_id, participant_id=p1),
        store.stop_autoloop(room_id=room_id, participant_id=p0),
        return_exceptions=True,
    )
    accept_result, stop_result = results

    # stop_autoloop nunca falha nesse cenário: mesmo que o accept concorrente
    # já tenha virado o loop "active" debaixo dele, o WATCH releu e o stop
    # ainda assim encerrou — não é o single-shot-fail de propose_autoloop.
    assert not isinstance(stop_result, BaseException)
    assert stop_result == {"status": "ended", "ended_reason": "stopped"}

    # accept, se ganhou a corrida e comitou antes do stop, sucede
    # normalmente; se perdeu (stop já tinha comitado "ended" quando o WATCH
    # do accept releu o autoloop_key), accept_autoloop levanta
    # NoPendingProposalError — os dois desfechos são consistentes, nunca uma
    # exceção não tratada nem um estado no meio do caminho.
    if isinstance(accept_result, BaseException):
        assert isinstance(accept_result, NoPendingProposalError)
    else:
        assert accept_result["status"] in ("proposed", "active")

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_concurrent_stop_stop_exactly_one_winner_no_unhandled_exception(store):
    # Duas chamadas de autoloop_stop concorrentes no mesmo loop: só uma pode
    # ser "quem parou de verdade" — a outra perde a corrida (WATCH releu
    # status já "ended") e levanta AUTOLOOP_NOT_ACTIVE, nunca uma exceção
    # não tratada nem as duas "vencendo" ao mesmo tempo.
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)

    results = await asyncio.gather(
        store.stop_autoloop(room_id=room_id, participant_id=p0),
        store.stop_autoloop(room_id=room_id, participant_id=p1),
        return_exceptions=True,
    )
    successes = [r for r in results if not isinstance(r, BaseException)]
    failures = [r for r in results if isinstance(r, BaseException)]

    assert len(successes) == 1
    assert successes[0] == {"status": "ended", "ended_reason": "stopped"}
    assert len(failures) == 1
    assert isinstance(failures[0], AutoloopNotActiveError)

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "stopped"


async def test_propose_after_stop_starts_a_clean_new_cycle(store):
    # Round-trip: propose -> stop -> propose de novo deve iniciar um ciclo
    # limpo (turn_count/ended_reason resetados, loop_participants só com o
    # novo proponente), mesmo comportamento já garantido pra "ended" em
    # geral por propose_autoloop (sobrescreve qualquer ciclo anterior).
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g1", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    await store.stop_autoloop(room_id=room_id, participant_id=p0)

    result = await store.propose_autoloop(
        room_id=room_id, participant_id=p1, goal="g2", max_turns=5, max_seconds=60
    )
    assert result["status"] == "proposed"
    assert result["loop_participants"] == [p1]

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "proposed"
    assert status["goal"] == "g2"
    assert status["loop_participants"] == [p1]
    assert status["turn_count"] == 0
    assert status["ended_reason"] is None


async def test_autoloop_stop_tool_works_through_the_mcp_wrapper(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room = await store.create_room(display_name="p0", ttl_seconds=120)
    await app_main.autoloop_propose(
        room_id=room["room_id"], participant_id=room["participant_id"], goal="g"
    )

    result = await app_main.autoloop_stop(
        room_id=room["room_id"], participant_id=room["participant_id"]
    )
    assert result == {"status": "ended", "ended_reason": "stopped"}
