"""
Testes da troca de turnos estruturados, watchdog e garantia de não-execução
do autoloop (Story 2): autoloop_turn, e o dispatch aditivo de
type=autoloop_turn em poll_messages/export_transcript. Também cobre a
assimetria de consenso/impasse (Story 3, CAP-4): blocked encerra o loop
unilateralmente (ended_reason=impasse), done só encerra (ended_reason=
consensus) quando done_by cobrir todos os loop_participants.

Roda contra um Redis local real (ver tests/conftest.py e README "Rodando os
testes").
"""
import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from app import config, main as app_main
from app.redis_store import (
    AutoloopLimitExceededError,
    AutoloopNotActiveError,
    InvalidTurnStatusError,
    NotLoopParticipantError,
    SessionStore,
    _autoloop_key,
    _now,
)

from conftest import _make_room

# CAP-3 (story 8): chama as tools direto, sem ctx — precisa de AUTH_ENABLED=false
# (ver conftest.py, fixture dev_mode_no_auth) pra require_scope não recusar tudo.
pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")


async def _active_loop(store: SessionStore, max_turns: int = 10, max_seconds: int = 300):
    """Cria uma room com 2 participantes e um loop já status=active, turn_count=0."""
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.propose_autoloop(
        room_id=room_id, participant_id=p0, goal="g", max_turns=max_turns, max_seconds=max_seconds
    )
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    return room_id, p0, p1


# ---------------------------------------------------------------------------
# I/O & Edge-Case Matrix
# ---------------------------------------------------------------------------


async def test_happy_path_increments_turn_count_and_stays_active(store):
    room_id, p0, p1 = await _active_loop(store, max_turns=10, max_seconds=300)

    result = await store.autoloop_turn(
        room_id=room_id, participant_id=p0, payload={"k": "v"}, turn_status="proposing"
    )
    assert result["turn_count"] == 1
    assert result["status"] == "active"
    assert "message_id" in result

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["turn_count"] == 1


async def test_watchdog_by_turn_count_ends_loop(store):
    room_id, p0, p1 = await _active_loop(store, max_turns=1, max_seconds=300)
    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")

    with pytest.raises(AutoloopLimitExceededError):
        await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="proposing")

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "watchdog_turns"
    assert status["turn_count"] == 1  # não incrementou no turno que estourou


async def test_watchdog_by_elapsed_time_ends_loop_regardless_of_turn_count(store):
    room_id, p0, p1 = await _active_loop(store, max_turns=1000, max_seconds=5)
    # simula started_at bem no passado, sem depender de sleep real no teste
    await store._redis.hset(_autoloop_key(room_id), "started_at", _now() - 10)

    with pytest.raises(AutoloopLimitExceededError):
        await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "watchdog_time"
    assert status["turn_count"] == 0


async def test_watchdog_turn_count_wins_tie_break_over_elapsed_time(store):
    # Quando turn_count/max_turns E started_at/max_seconds estouram na mesma
    # chamada, ended_reason="watchdog_turns" vence (ver Design Notes da story
    # e o comentário de tie-break em autoloop_turn).
    room_id, p0, p1 = await _active_loop(store, max_turns=1, max_seconds=5)
    # 1º turno: dentro do limite (turn_count 0 -> 1, max_turns=1)
    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")
    # simula started_at bem no passado, estourando também max_seconds — o 2º
    # turno agora excede turn_count E elapsed time na mesma chamada
    await store._redis.hset(_autoloop_key(room_id), "started_at", _now() - 10)

    with pytest.raises(AutoloopLimitExceededError):
        await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="proposing")

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "watchdog_turns"
    assert status["turn_count"] == 1  # não incrementou no turno que estourou


async def test_non_participant_caller_raises_not_loop_participant(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    # p2 nunca chamou autoloop_accept — continua bystander da room

    with pytest.raises(NotLoopParticipantError):
        await store.autoloop_turn(room_id=room_id, participant_id=p2, payload={}, turn_status="proposing")


async def test_loop_not_active_only_one_participant_raises_autoloop_not_active(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)

    with pytest.raises(AutoloopNotActiveError):
        await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")


async def test_loop_not_active_never_proposed_raises_autoloop_not_active(store):
    room_id, [p0] = await _make_room(store, 1)

    with pytest.raises(AutoloopNotActiveError):
        await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")


async def test_invalid_turn_status_raises(store):
    room_id, p0, p1 = await _active_loop(store)

    with pytest.raises(InvalidTurnStatusError):
        await store.autoloop_turn(
            room_id=room_id, participant_id=p0, payload={}, turn_status="something_else"
        )


async def test_turn_after_watchdog_end_raises_not_active_not_limit_exceeded_again(store):
    room_id, p0, p1 = await _active_loop(store, max_turns=1, max_seconds=300)
    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")
    with pytest.raises(AutoloopLimitExceededError):
        await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="proposing")

    # 2ª chamada depois do loop já ter sido encerrado pelo watchdog:
    # AUTOLOOP_NOT_ACTIVE, não AUTOLOOP_LIMIT_EXCEEDED de novo.
    with pytest.raises(AutoloopNotActiveError):
        await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")


async def test_non_execution_guarantee_payload_roundtrips_inert_via_poll(store):
    room_id, p0, p1 = await _active_loop(store)
    dangerous_payload = {"action": "delete_all_files"}

    await store.autoloop_turn(
        room_id=room_id, participant_id=p0, payload=dangerous_payload, turn_status="proposing"
    )

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p1, timeout_seconds=1)
    turn_messages = [m for m in poll_result["messages"] if m["content"].get("turn_status") is not None]
    assert len(turn_messages) == 1
    # o payload volta byte-a-byte idêntico, nunca inspecionado/executado
    assert turn_messages[0]["content"]["payload"] == dangerous_payload
    assert turn_messages[0]["content"]["turn_status"] == "proposing"


async def test_dispatch_in_poll_and_export(store):
    room_id, p0, p1 = await _active_loop(store)
    payload = {"k": "v"}

    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload=payload, turn_status="agreeing")

    poll_result = await store.poll_messages(room_id=room_id, participant_id=p1, timeout_seconds=1)
    turn_messages = [m for m in poll_result["messages"] if m["content"].get("turn_status") is not None]
    assert len(turn_messages) == 1
    msg = turn_messages[0]
    assert msg["content"]["payload"] == payload
    assert msg["content"]["turn_status"] == "agreeing"

    export_result = await store.export_transcript(room_id=room_id, participant_id=p0)
    transcript = export_result["transcript_markdown"]
    assert "[autoloop:agreeing]" in transcript
    assert '"k": "v"' in transcript


async def test_concurrent_turns_at_limit_exactly_one_succeeds(store):
    # Exercita de fato o ramo WatchError do guard WATCH/MULTI/EXEC do
    # watchdog-check-then-increment (não só a checagem sequencial que
    # test_watchdog_by_turn_count_ends_loop já cobre) — mesma race que
    # test_concurrent_propose_only_one_winner cobre pra propose_autoloop.
    room_id, p0, p1 = await _active_loop(store, max_turns=1, max_seconds=300)

    results = await asyncio.gather(
        store.autoloop_turn(room_id=room_id, participant_id=p0, payload={"who": "p0"}, turn_status="proposing"),
        store.autoloop_turn(room_id=room_id, participant_id=p1, payload={"who": "p1"}, turn_status="proposing"),
        return_exceptions=True,
    )
    successes = [r for r in results if not isinstance(r, BaseException)]
    failures = [r for r in results if isinstance(r, BaseException)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], AutoloopLimitExceededError)
    assert successes[0]["turn_count"] == 1
    assert successes[0]["status"] == "active"

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "watchdog_turns"
    # turn_count nunca passou de max_turns, mesmo sob a corrida
    assert status["turn_count"] == 1


# ---------------------------------------------------------------------------
# Coverage adicional (Boundaries "Never" de Story 2, ainda válida: proposing/
# agreeing seguem sem efeito de encerramento nenhum — blocked/done agora tem
# efeito de consenso/impasse, ver Story 3 abaixo)
# ---------------------------------------------------------------------------


async def test_proposing_and_agreeing_turn_status_recorded_without_ending_loop(store):
    room_id, p0, p1 = await _active_loop(store)

    result_proposing = await store.autoloop_turn(
        room_id=room_id, participant_id=p0, payload={}, turn_status="proposing"
    )
    assert result_proposing["status"] == "active"
    assert "ended_reason" not in result_proposing
    assert "done_by" not in result_proposing

    result_agreeing = await store.autoloop_turn(
        room_id=room_id, participant_id=p1, payload={}, turn_status="agreeing"
    )
    assert result_agreeing["status"] == "active"

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "active"
    assert status["turn_count"] == 2
    assert status["ended_reason"] is None


# ---------------------------------------------------------------------------
# Story 3 (CAP-4): consenso e impasse — a assimetria entre blocked
# (unilateral, encerra na hora) e done (bilateral, precisa de todos os
# loop_participants)
# ---------------------------------------------------------------------------


async def test_single_blocked_ends_loop_immediately_with_impasse(store):
    room_id, p0, p1 = await _active_loop(store)

    result = await store.autoloop_turn(
        room_id=room_id, participant_id=p0, payload={}, turn_status="blocked"
    )
    assert result["status"] == "ended"
    assert result["ended_reason"] == "impasse"
    assert result["turn_count"] == 1
    assert "done_by" not in result

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "impasse"

    # o turno que encerrou o loop ainda é dispatchado normalmente via
    # poll_messages (type=autoloop_turn), e o message_id devolvido por
    # autoloop_turn é de fato o id desse turno — não do anúncio type=system
    # que o mesmo commit também gravou logo em seguida.
    poll_result = await store.poll_messages(room_id=room_id, participant_id=p1, timeout_seconds=1)
    turn_messages = [m for m in poll_result["messages"] if m["content"].get("turn_status") is not None]
    assert len(turn_messages) == 1
    assert turn_messages[0]["id"] == result["message_id"]
    assert turn_messages[0]["content"]["turn_status"] == "blocked"


async def test_single_done_does_not_end_loop_and_records_in_done_by(store):
    room_id, p0, p1 = await _active_loop(store)

    result = await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    assert result["status"] == "active"
    assert "ended_reason" not in result
    assert result["done_by"] == [p0]

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "active"
    assert status["done_by"] == [p0]
    assert status["ended_reason"] is None


async def test_all_loop_participants_done_ends_loop_with_consensus(store):
    room_id, p0, p1 = await _active_loop(store)

    first = await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    assert first["status"] == "active"
    assert "ended_reason" not in first

    second = await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="done")
    assert second["status"] == "ended"
    assert second["ended_reason"] == "consensus"
    assert sorted(second["done_by"]) == sorted([p0, p1])

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "consensus"
    assert sorted(status["done_by"]) == sorted([p0, p1])

    # mesma verificação de dispatch/message_id de
    # test_single_blocked_ends_loop_immediately_with_impasse, pro caminho de
    # consenso: o 2º turno (o que fecha o consenso) ainda aparece no poll
    # como o próprio turno, com o message_id certo. p0 já tinha um turno
    # próprio antes (o 1º done) então o poll de p0 (nunca chamado até aqui)
    # traz os dois turnos — o último é o que fechou o consenso.
    poll_result = await store.poll_messages(room_id=room_id, participant_id=p0, timeout_seconds=1)
    turn_messages = [m for m in poll_result["messages"] if m["content"].get("turn_status") is not None]
    assert len(turn_messages) == 2
    assert turn_messages[-1]["id"] == second["message_id"]
    assert turn_messages[-1]["content"]["turn_status"] == "done"


async def test_departed_done_participant_does_not_permanently_block_consensus(store):
    # Regressão: close_room(Story 1) só fazia srem em
    # _autoloop_participants_key, não em _autoloop_done_key — um
    # participante que já tinha declarado done e depois sai da room deixava
    # done_by com um id a mais que loop_participants nunca teria de volta,
    # tornando consenso permanentemente inatingível pro resto do ciclo.
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    await store.accept_autoloop(room_id=room_id, participant_id=p2)

    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    # p0 já declarou done e agora sai da room — done_by não pode reter p0
    # depois que ele deixa de ser loop_participant.
    await store.close_room(room_id=room_id, participant_id=p0)

    status_after_leave = await store.autoloop_status(room_id=room_id, participant_id=p1)
    assert status_after_leave["done_by"] == []
    assert sorted(status_after_leave["loop_participants"]) == sorted([p1, p2])

    # p1 e p2 (os únicos loop_participants restantes) agora conseguem
    # atingir consenso normalmente — done_by não fica travado com o id de p0.
    first = await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="done")
    assert first["status"] == "active"
    second = await store.autoloop_turn(room_id=room_id, participant_id=p2, payload={}, turn_status="done")
    assert second["status"] == "ended"
    assert second["ended_reason"] == "consensus"
    assert sorted(second["done_by"]) == sorted([p1, p2])


async def test_repeated_done_from_same_participant_is_idempotent_and_stays_active(store):
    room_id, p0, p1 = await _active_loop(store)

    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    result = await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")

    assert result["status"] == "active"
    assert "ended_reason" not in result
    assert result["done_by"] == [p0]

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "active"
    assert status["done_by"] == [p0]
    # p0 declarou done 2x, turn_count reflete os 2 turnos de qualquer forma
    assert status["turn_count"] == 2


async def test_turn_after_impasse_end_raises_autoloop_not_active(store):
    room_id, p0, p1 = await _active_loop(store)
    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="blocked")

    with pytest.raises(AutoloopNotActiveError):
        await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="proposing")


async def test_turn_after_consensus_end_raises_autoloop_not_active(store):
    room_id, p0, p1 = await _active_loop(store)
    await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    await store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="done")

    with pytest.raises(AutoloopNotActiveError):
        await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="proposing")


async def test_blocked_wins_immediately_over_pending_partial_done_by(store):
    # 3 loop_participants, um já declarou done (done_by parcial, longe de
    # cobrir todos) — um blocked de um participante DIFERENTE encerra na
    # hora mesmo assim, sem esperar o resto (a assimetria em si).
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=10, max_seconds=300)
    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    await store.accept_autoloop(room_id=room_id, participant_id=p2)

    done_result = await store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done")
    assert done_result["status"] == "active"
    assert done_result["done_by"] == [p0]

    blocked_result = await store.autoloop_turn(
        room_id=room_id, participant_id=p1, payload={}, turn_status="blocked"
    )
    assert blocked_result["status"] == "ended"
    assert blocked_result["ended_reason"] == "impasse"

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "impasse"
    # done_by fica com o registro parcial de antes do impasse — não é apagado
    assert status["done_by"] == [p0]


async def test_concurrent_done_from_both_participants_no_lost_update(store):
    # Exercita o ramo WatchError do guard WATCH/MULTI/EXEC em torno de
    # done_set_key (não só a checagem sequencial que os testes acima já
    # cobrem) — mesma race que test_concurrent_turns_at_limit_exactly_one_succeeds
    # cobre pra turn_count, mas pra consenso: dois "done" concorrentes de
    # loop_participants diferentes não podem ambos lerem o mesmo done_by e
    # ambos concluírem "consenso incompleto".
    room_id, p0, p1 = await _active_loop(store)

    results = await asyncio.gather(
        store.autoloop_turn(room_id=room_id, participant_id=p0, payload={}, turn_status="done"),
        store.autoloop_turn(room_id=room_id, participant_id=p1, payload={}, turn_status="done"),
    )

    ended_results = [r for r in results if r["status"] == "ended"]
    active_results = [r for r in results if r["status"] == "active"]
    assert len(ended_results) == 1
    assert len(active_results) == 1
    assert ended_results[0]["ended_reason"] == "consensus"
    assert sorted(ended_results[0]["done_by"]) == sorted([p0, p1])

    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["status"] == "ended"
    assert status["ended_reason"] == "consensus"
    assert sorted(status["done_by"]) == sorted([p0, p1])


# ---------------------------------------------------------------------------
# Tool-level (app/main.py): validação de payload por tamanho, sem exigir
# "action" (diferente de session_send_json)
# ---------------------------------------------------------------------------


def _use_test_store(monkeypatch, store):
    monkeypatch.setattr(app_main, "_store", store)


async def test_autoloop_turn_tool_accepts_payload_without_action_field(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room_id, p0, p1 = await _active_loop(store)

    result = await app_main.autoloop_turn(
        room_id=room_id, participant_id=p0, payload={"no_action_here": True}, turn_status="proposing"
    )
    assert result["status"] == "active"
    assert result["turn_count"] == 1


async def test_autoloop_turn_tool_rejects_oversized_payload(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    monkeypatch.setattr(config, "MAX_JSON_PAYLOAD_LEN", 10)
    room_id, p0, p1 = await _active_loop(store)

    with pytest.raises(ToolError):
        await app_main.autoloop_turn(
            room_id=room_id, participant_id=p0, payload={"x": "y" * 100}, turn_status="proposing"
        )

    # rejeitado antes de qualquer side effect: turn_count segue em 0
    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["turn_count"] == 0


async def test_autoloop_turn_tool_rejects_non_dict_payload(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room_id, p0, p1 = await _active_loop(store)

    with pytest.raises(ToolError, match="INVALID_PAYLOAD"):
        await app_main.autoloop_turn(
            room_id=room_id, participant_id=p0, payload=["not", "a", "dict"], turn_status="proposing"
        )

    with pytest.raises(ToolError, match="INVALID_PAYLOAD"):
        await app_main.autoloop_turn(
            room_id=room_id, participant_id=p0, payload="not a dict either", turn_status="proposing"
        )

    # rejeitado antes de qualquer side effect: turn_count segue em 0
    status = await store.autoloop_status(room_id=room_id, participant_id=p0)
    assert status["turn_count"] == 0
