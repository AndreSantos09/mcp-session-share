"""
Testes do protocolo 1:1 (SPEC-session-share-mcp, CAP-1 a CAP-4):

  CAP-2  in_reply_to em session_send/session_send_json/file_send
  CAP-4  intent (pergunta default | fyi | handoff | conclusao)
  CAP-1  message_status com estado pendente/entregue/tratado por participante
         + message_ack (ack explícito, idempotente, nunca da própria mensagem)
  CAP-3  session_status.turn derivado em room de exatamente 2 participantes

Mais retrocompatibilidade: send sem os campos novos continua igual, e a
suíte antiga (autoloop) segue verde sem alteração.

Roda contra um Redis local real (ver tests/conftest.py e README "Rodando os
testes").
"""
import asyncio
import base64

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from app import main as app_main
from app.redis_store import (
    CannotAckOwnMessageError,
    InvalidAckTargetError,
    InvalidIntentError,
    InvalidMessageIdError,
    InvalidReplyTargetError,
    SessionStore,
    _ack_field,
    _acks_key,
    _room_key,
)

from conftest import _make_room

# CAP-3 (story 8): chama as tools direto, sem ctx — precisa de AUTH_ENABLED=false
# (ver conftest.py, fixture dev_mode_no_auth) pra require_scope não recusar tudo.
pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")

_B64 = base64.b64encode(b"conteudo").decode()


async def _poll(store: SessionStore, room_id: str, pid: str) -> list[dict]:
    """Drena o que houver pra `pid` (timeout mínimo) e devolve só as mensagens."""
    result = await store.poll_messages(room_id=room_id, participant_id=pid, timeout_seconds=1)
    return result["messages"]


def _real(messages: list[dict]) -> list[dict]:
    """Filtra eventos de sistema/ack — deixa só mensagens reais (têm intent).

    CAP-8 (story 11): toda entrada do envelope tem a CHAVE "intent" (pode ser
    None) — não dá mais pra distinguir por presença da chave, como antes;
    mensagem real é a que tem intent com valor (texto/json/arquivo)."""
    return [m for m in messages if m.get("intent") is not None]


# ---------------------------------------------------------------------------
# CAP-2 — in_reply_to
# ---------------------------------------------------------------------------


async def test_reply_to_existing_message_is_recorded_and_propagated_in_poll_and_export(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    original = await store.send_message(room_id=room_id, participant_id=p0, text="qual a porta?")
    reply = await store.send_message(
        room_id=room_id, participant_id=p1, text="6379", in_reply_to=original["message_id"]
    )
    assert reply["in_reply_to"] == original["message_id"]
    assert reply["intent"] == "pergunta"

    messages = _real(await _poll(store, room_id, p0))
    by_id = {m["id"]: m for m in messages}
    assert by_id[original["message_id"]]["in_reply_to"] is None  # tópico novo
    assert by_id[reply["message_id"]]["in_reply_to"] == original["message_id"]

    transcript = (await store.export_transcript(room_id=room_id, participant_id=p0))["transcript_markdown"]
    assert f"id={original['message_id']}" in transcript
    assert f"in_reply_to={original['message_id']}" in transcript


async def test_reply_to_works_for_json_and_file_sends_too(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    original = await store.send_message(room_id=room_id, participant_id=p0, text="manda o config")

    as_json = await store.send_json_message(
        room_id=room_id, participant_id=p1, payload={"action": "config"}, in_reply_to=original["message_id"]
    )
    as_file = await store.send_file(
        room_id=room_id,
        participant_id=p1,
        filename="config.yaml",
        content_base64=_B64,
        in_reply_to=original["message_id"],
    )
    assert as_json["in_reply_to"] == original["message_id"]
    assert as_file["in_reply_to"] == original["message_id"]

    by_id = {m["id"]: m for m in _real(await _poll(store, room_id, p0))}
    assert by_id[as_json["message_id"]]["in_reply_to"] == original["message_id"]
    assert by_id[as_file["message_id"]]["in_reply_to"] == original["message_id"]
    # arquivo agora carrega created_at (era None antes do protocolo 1:1)
    assert by_id[as_file["message_id"]]["created_at"] == pytest.approx(as_file["created_at"])


async def test_thread_is_reconstructible_with_interleaved_topics(store):
    # CAP-2 success: dois tópicos entrelaçados, a cadeia de um deles é
    # reconstruível sem ambiguidade seguindo in_reply_to de trás pra frente.
    room_id, [p0, p1] = await _make_room(store, 2)
    a1 = await store.send_message(room_id=room_id, participant_id=p0, text="A1")
    b1 = await store.send_message(room_id=room_id, participant_id=p0, text="B1")
    a2 = await store.send_message(room_id=room_id, participant_id=p1, text="A2", in_reply_to=a1["message_id"])
    b2 = await store.send_message(room_id=room_id, participant_id=p1, text="B2", in_reply_to=b1["message_id"])
    a3 = await store.send_message(room_id=room_id, participant_id=p0, text="A3", in_reply_to=a2["message_id"])

    # p1 nunca pollou: recebe tudo
    by_id = {m["id"]: m for m in _real(await _poll(store, room_id, p1))}
    chain = []
    cursor = a3["message_id"]
    while cursor is not None:
        chain.append(by_id[cursor]["content"]["text"])
        cursor = by_id[cursor]["in_reply_to"]
    assert chain == ["A3", "A2", "A1"]
    assert by_id[b2["message_id"]]["in_reply_to"] == b1["message_id"]


async def test_reply_to_malformed_id_raises_invalid_message_id(store):
    room_id, [p0] = await _make_room(store, 1)
    with pytest.raises(InvalidMessageIdError):
        await store.send_message(room_id=room_id, participant_id=p0, text="x", in_reply_to="nao-e-id")


async def test_reply_to_future_id_raises_invalid_reply_target(store):
    room_id, [p0] = await _make_room(store, 1)
    last = await store.send_message(room_id=room_id, participant_id=p0, text="x")
    ms, seq = last["message_id"].split("-")
    future_id = f"{int(ms) + 60_000}-0"
    with pytest.raises(InvalidReplyTargetError):
        await store.send_message(room_id=room_id, participant_id=p0, text="y", in_reply_to=future_id)


async def test_reply_to_nonexistent_past_id_raises_invalid_reply_target(store):
    room_id, [p0] = await _make_room(store, 1)
    await store.send_message(room_id=room_id, participant_id=p0, text="x")
    # id bem-formado, no passado, mas que nunca foi gerado nessa stream
    with pytest.raises(InvalidReplyTargetError):
        await store.send_message(room_id=room_id, participant_id=p0, text="y", in_reply_to="1-1")


async def test_reply_to_system_event_raises_invalid_reply_target(store):
    room_id, [p0] = await _make_room(store, 1)
    # a única entrada até aqui é o type=system "Room criada por ..."
    entries = await store._redis.xrange(_room_key(room_id, "stream"), min="-", max="+")
    system_id = entries[0][0]
    with pytest.raises(InvalidReplyTargetError):
        await store.send_message(room_id=room_id, participant_id=p0, text="y", in_reply_to=system_id)


async def test_invalid_reply_target_has_no_side_effect(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    with pytest.raises(InvalidReplyTargetError):
        await store.send_message(room_id=room_id, participant_id=p0, text="y", in_reply_to="1-1")
    assert _real(await _poll(store, room_id, p1)) == []


# ---------------------------------------------------------------------------
# CAP-4 — intent
# ---------------------------------------------------------------------------


async def test_intent_defaults_to_pergunta_and_is_always_explicit_in_stream_and_poll(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    sent = await store.send_message(room_id=room_id, participant_id=p0, text="oi")
    assert sent["intent"] == "pergunta"
    assert sent["in_reply_to"] is None

    entries = await store._redis.xrange(_room_key(room_id, "stream"), min=sent["message_id"], max=sent["message_id"])
    assert entries[0][1]["intent"] == "pergunta"  # gravado explicitamente, não implícito
    assert "in_reply_to" not in entries[0][1]  # ausente na stream quando não informado

    [msg] = _real(await _poll(store, room_id, p1))
    assert msg["intent"] == "pergunta"
    assert msg["in_reply_to"] is None


@pytest.mark.parametrize("intent", ["pergunta", "fyi", "handoff", "conclusao"])
async def test_each_intent_roundtrips_through_all_three_sends(store, intent):
    room_id, [p0, p1] = await _make_room(store, 2)
    t = await store.send_message(room_id=room_id, participant_id=p0, text="t", intent=intent)
    j = await store.send_json_message(room_id=room_id, participant_id=p0, payload={"action": "a"}, intent=intent)
    f = await store.send_file(room_id=room_id, participant_id=p0, filename="f", content_base64=_B64, intent=intent)
    assert t["intent"] == j["intent"] == f["intent"] == intent

    intents = {m["id"]: m["intent"] for m in _real(await _poll(store, room_id, p1))}
    assert intents == {t["message_id"]: intent, j["message_id"]: intent, f["message_id"]: intent}

    transcript = (await store.export_transcript(room_id=room_id, participant_id=p0))["transcript_markdown"]
    assert transcript.count(f"intent={intent}") == 3


@pytest.mark.parametrize("bad", ["question", "FYI", "", "handoff "])
async def test_invalid_intent_raises_and_writes_nothing(store, bad):
    room_id, [p0, p1] = await _make_room(store, 2)
    with pytest.raises(InvalidIntentError):
        await store.send_message(room_id=room_id, participant_id=p0, text="x", intent=bad)
    with pytest.raises(InvalidIntentError):
        await store.send_json_message(room_id=room_id, participant_id=p0, payload={"action": "a"}, intent=bad)
    with pytest.raises(InvalidIntentError):
        await store.send_file(room_id=room_id, participant_id=p0, filename="f", content_base64=_B64, intent=bad)
    assert _real(await _poll(store, room_id, p1)) == []


async def test_legacy_stream_entry_without_intent_reads_as_pergunta(store):
    # Evento gravado por uma versão anterior do servidor (sem campo intent):
    # poll/export/message_status defaultam pra "pergunta" em vez de quebrar.
    room_id, [p0, p1] = await _make_room(store, 2)
    legacy_id = await store._redis.xadd(
        _room_key(room_id, "stream"),
        {"type": "message", "sender_id": p0, "sender_name": "p0", "text": "legado", "created_at": 1.0},
    )
    [msg] = _real(await _poll(store, room_id, p1))
    assert msg["id"] == legacy_id
    assert msg["intent"] == "pergunta"
    assert msg["in_reply_to"] is None

    status = await store.message_status(room_id=room_id, participant_id=p0, message_id=legacy_id)
    assert status["intent"] == "pergunta"
    assert status["state_by"] == {"p1": "entregue"}


# ---------------------------------------------------------------------------
# CAP-1 — message_status (pendente / entregue / tratado) + message_ack
# ---------------------------------------------------------------------------


async def test_pergunta_is_pendente_then_entregue_then_tratado_by_reply(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    q = await store.send_message(room_id=room_id, participant_id=p0, text="?", intent="pergunta")

    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert s["intent"] == "pergunta"
    assert s["delivered_to"] == {"p1": False}
    assert s["state_by"] == {"p1": "pendente"}

    await _poll(store, room_id, p1)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert s["delivered_to"] == {"p1": True}
    assert s["state_by"] == {"p1": "entregue"}  # entregue não é tratado pra pergunta

    # uma mensagem SEM in_reply_to de p1 não trata nada (nada inferido do texto)
    await store.send_message(room_id=room_id, participant_id=p1, text="a resposta é 42")
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert s["state_by"] == {"p1": "entregue"}

    await store.send_message(room_id=room_id, participant_id=p1, text="42", in_reply_to=q["message_id"])
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert s["state_by"] == {"p1": "tratado"}
    raw = await store._redis.hget(_acks_key(room_id), _ack_field(q["message_id"], p1))
    assert '"via": "reply"' in raw


async def test_handoff_is_not_tratado_by_reply_only_by_explicit_ack(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="faz o deploy", intent="handoff")
    await _poll(store, room_id, p1)
    await store.send_message(room_id=room_id, participant_id=p1, text="ok, começando", in_reply_to=h["message_id"])

    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=h["message_id"])
    assert s["state_by"] == {"p1": "entregue"}  # responder não é ter executado

    ack = await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    assert ack["state"] == "tratado"
    assert ack["already_acked"] is False
    assert ack["ack_event_id"] is not None

    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=h["message_id"])
    assert s["state_by"] == {"p1": "tratado"}
    raw = await store._redis.hget(_acks_key(room_id), _ack_field(h["message_id"], p1))
    assert '"via": "explicit"' in raw


@pytest.mark.parametrize("intent", ["fyi", "conclusao"])
async def test_fyi_and_conclusao_are_tratado_as_soon_as_entregue(store, intent):
    room_id, [p0, p1] = await _make_room(store, 2)
    m = await store.send_message(room_id=room_id, participant_id=p0, text="aviso", intent=intent)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=m["message_id"])
    assert s["state_by"] == {"p1": "pendente"}

    await _poll(store, room_id, p1)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=m["message_id"])
    assert s["delivered_to"] == {"p1": True}
    assert s["state_by"] == {"p1": "tratado"}


async def test_reply_to_own_pergunta_is_followup_not_tratado(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    q = await store.send_message(room_id=room_id, participant_id=p0, text="?")
    await store.send_message(room_id=room_id, participant_id=p0, text="complementando", in_reply_to=q["message_id"])
    await _poll(store, room_id, p1)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert s["state_by"] == {"p1": "entregue"}
    assert await store._redis.hlen(_acks_key(room_id)) == 0


async def test_reply_to_fyi_does_not_create_ack_record(store):
    # fyi já é tratado ao entregar — uma resposta a ela não grava ack
    # automático (o auto-ack só existe pra `pergunta`).
    room_id, [p0, p1] = await _make_room(store, 2)
    f = await store.send_message(room_id=room_id, participant_id=p0, text="fyi", intent="fyi")
    await store.send_message(room_id=room_id, participant_id=p1, text="valeu", in_reply_to=f["message_id"])
    assert await store._redis.hlen(_acks_key(room_id)) == 0


async def test_state_never_regresses_and_ack_wins_over_cursor(store):
    # tratado > entregue > pendente: um ack explícito de quem ainda não
    # pollou até a mensagem já vale como tratado (quem tratou, viu).
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="h", intent="handoff")
    await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=h["message_id"])
    assert s["delivered_to"] == {"p1": False}
    assert s["state_by"] == {"p1": "tratado"}
    # pollar depois não "volta" o estado
    await _poll(store, room_id, p1)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=h["message_id"])
    assert s["state_by"] == {"p1": "tratado"}


async def test_message_status_keeps_delivered_to_shape_for_backcompat(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    m = await store.send_message(room_id=room_id, participant_id=p0, text="x")
    await _poll(store, room_id, p1)
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id=m["message_id"])
    assert s["message_id"] == m["message_id"]
    assert s["delivered_to"] == {"p1": True, "p2": False}
    assert s["state_by"] == {"p1": "entregue", "p2": "pendente"}
    assert "p0" not in s["delivered_to"]  # nunca inclui o próprio remetente


async def test_message_status_for_id_not_in_stream_reports_null_intent_and_only_explicit_acks(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    # CAP-4 (story 10): p1 entrou (join direto, open_join) com cursor já
    # inicializado no próprio evento de entrada (stream id real, tipo
    # "<epoch_ms>-0") — não mais no sentinela "0". Um id BEM no futuro
    # (maior que qualquer cursor possível) é o que ainda representa "não
    # entregue" aqui; "1-1" (bem no passado) já contaria como entregue,
    # de propósito: é assim que o cursor pós-join evita vazar histórico.
    s = await store.message_status(room_id=room_id, participant_id=p0, message_id="99999999999999-0")
    assert s["intent"] is None
    assert s["delivered_to"] == {"p1": False}
    assert s["state_by"] == {"p1": "pendente"}


async def test_ack_is_idempotent_and_emits_exactly_one_ack_event(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="h", intent="handoff")
    await _poll(store, room_id, p0)  # limpa a stream pro p0 antes do ack

    first = await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    second = await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    assert first["already_acked"] is False
    assert second["already_acked"] is True
    assert second["ack_event_id"] is None
    assert second["state"] == "tratado"

    messages = await _poll(store, room_id, p0)
    acks = [m for m in messages if m["content"]["kind"] == "ack"]
    assert len(acks) == 1
    assert acks[0]["id"] == first["ack_event_id"]
    assert acks[0]["content"]["ack_of"] == h["message_id"]
    assert acks[0]["sender_name"] == "p1"
    assert acks[0]["origin"] == "participant" and acks[0]["untrusted"] is True
    assert acks[0]["intent"] is None  # ack não é mensagem real

    transcript = (await store.export_transcript(room_id=room_id, participant_id=p0))["transcript_markdown"]
    assert f"[ack]: tratou a mensagem {h['message_id']}" in transcript


async def test_concurrent_acks_from_same_participant_emit_single_event(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="h", intent="handoff")
    results = await asyncio.gather(
        *[store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"]) for _ in range(5)]
    )
    assert sum(1 for r in results if not r["already_acked"]) == 1
    entries = await store._redis.xrange(_room_key(room_id, "stream"), min="-", max="+")
    assert sum(1 for _, f in entries if f.get("type") == "ack") == 1


async def test_ack_after_auto_ack_via_reply_is_already_acked_without_new_event(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    q = await store.send_message(room_id=room_id, participant_id=p0, text="?")
    await store.send_message(room_id=room_id, participant_id=p1, text="r", in_reply_to=q["message_id"])
    ack = await store.ack_message(room_id=room_id, participant_id=p1, message_id=q["message_id"])
    assert ack["already_acked"] is True
    assert ack["ack_event_id"] is None


async def test_cannot_ack_own_message(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    m = await store.send_message(room_id=room_id, participant_id=p0, text="x", intent="handoff")
    with pytest.raises(CannotAckOwnMessageError):
        await store.ack_message(room_id=room_id, participant_id=p0, message_id=m["message_id"])
    assert await store._redis.hlen(_acks_key(room_id)) == 0


async def test_ack_invalid_targets(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    with pytest.raises(InvalidMessageIdError):
        await store.ack_message(room_id=room_id, participant_id=p1, message_id="abc")
    with pytest.raises(InvalidAckTargetError):
        await store.ack_message(room_id=room_id, participant_id=p1, message_id="1-1")
    entries = await store._redis.xrange(_room_key(room_id, "stream"), min="-", max="+")
    system_id = entries[0][0]
    with pytest.raises(InvalidAckTargetError):
        await store.ack_message(room_id=room_id, participant_id=p1, message_id=system_id)


async def test_ack_works_for_json_and_file_messages_and_in_3_party_rooms(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    j = await store.send_json_message(room_id=room_id, participant_id=p0, payload={"action": "a"}, intent="handoff")
    f = await store.send_file(room_id=room_id, participant_id=p0, filename="f", content_base64=_B64, intent="handoff")
    await store.ack_message(room_id=room_id, participant_id=p1, message_id=j["message_id"])
    await store.ack_message(room_id=room_id, participant_id=p2, message_id=f["message_id"])

    sj = await store.message_status(room_id=room_id, participant_id=p0, message_id=j["message_id"])
    sf = await store.message_status(room_id=room_id, participant_id=p0, message_id=f["message_id"])
    assert sj["state_by"] == {"p1": "tratado", "p2": "pendente"}
    assert sf["state_by"] == {"p1": "pendente", "p2": "tratado"}


async def test_acks_key_follows_room_ttl_and_is_deleted_with_room(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="h", intent="handoff")
    await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    ttl = await store._redis.ttl(_acks_key(room_id))
    assert 0 < ttl <= 120  # room criada com ttl_seconds=120 em _make_room

    await store.close_room(room_id=room_id, participant_id=p0)
    await store.close_room(room_id=room_id, participant_id=p1)
    assert await store._redis.exists(_acks_key(room_id)) == 0


# ---------------------------------------------------------------------------
# CAP-3 — turno derivado (session_status.turn)
# ---------------------------------------------------------------------------


async def _turn(store: SessionStore, room_id: str, pid: str) -> dict:
    return (await store.status(room_id=room_id, participant_id=pid))["turn"]


async def test_turn_without_messages_belongs_to_whoever_joined_first(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    turn = await _turn(store, room_id, p0)
    assert turn == {
        "applies": True,
        "next_to_act": "p0",
        "next_to_act_is_you": True,
        "reason": "no_messages_yet",
        "last_message_id": None,
        "last_intent": None,
    }
    assert (await _turn(store, room_id, p1))["next_to_act_is_you"] is False


@pytest.mark.parametrize(
    "intent,expected_next",
    [("pergunta", "p1"), ("handoff", "p1"), ("fyi", "p0"), ("conclusao", "p0")],
)
async def test_turn_follows_intent_of_last_real_message(store, intent, expected_next):
    room_id, [p0, p1] = await _make_room(store, 2)
    m = await store.send_message(room_id=room_id, participant_id=p0, text="m", intent=intent)
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is True
    assert turn["reason"] == "derived_from_last_message"
    assert turn["next_to_act"] == expected_next
    assert turn["next_to_act_is_you"] is (expected_next == "p0")
    assert turn["last_message_id"] == m["message_id"]
    assert turn["last_intent"] == intent


async def test_turn_flips_deterministically_message_by_message(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.send_message(room_id=room_id, participant_id=p0, text="?")
    assert (await _turn(store, room_id, p0))["next_to_act"] == "p1"
    await store.send_message(room_id=room_id, participant_id=p1, text="!", intent="fyi")
    assert (await _turn(store, room_id, p0))["next_to_act"] == "p1"  # fyi não passa
    await store.send_message(room_id=room_id, participant_id=p1, text="e você?")
    assert (await _turn(store, room_id, p0))["next_to_act"] == "p0"
    await store.send_file(room_id=room_id, participant_id=p0, filename="f", content_base64=_B64, intent="handoff")
    assert (await _turn(store, room_id, p0))["next_to_act"] == "p1"  # arquivo também conta
    await store.send_json_message(room_id=room_id, participant_id=p1, payload={"action": "done"}, intent="conclusao")
    assert (await _turn(store, room_id, p0))["next_to_act"] == "p1"


async def test_turn_ignores_system_and_ack_events(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    h = await store.send_message(room_id=room_id, participant_id=p0, text="h", intent="handoff")
    # ack de p1 (type=ack) e um evento de sistema depois da última mensagem real
    await store.ack_message(room_id=room_id, participant_id=p1, message_id=h["message_id"])
    await store._redis.xadd(_room_key(room_id, "stream"), {"type": "system", "text": "ruído"})
    turn = await _turn(store, room_id, p0)
    assert turn["last_message_id"] == h["message_id"]
    assert turn["next_to_act"] == "p1"


async def test_turn_does_not_apply_outside_1to1(store):
    room_id, [p0] = await _make_room(store, 1)
    assert (await _turn(store, room_id, p0)) == {
        "applies": False,
        "next_to_act": None,
        "next_to_act_is_you": None,
        "reason": "room_not_1to1",
        "last_message_id": None,
        "last_intent": None,
    }
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    await store.send_message(room_id=room_id, participant_id=p0, text="?")
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is False
    assert turn["reason"] == "room_not_1to1"
    assert turn["next_to_act"] is None


async def test_turn_yields_to_active_autoloop_but_not_to_a_mere_proposal(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store.send_message(room_id=room_id, participant_id=p0, text="?")
    await store.propose_autoloop(room_id=room_id, participant_id=p0, goal="g", max_turns=5, max_seconds=60)
    assert (await _turn(store, room_id, p0))["applies"] is True  # proposed ainda não manda

    await store.accept_autoloop(room_id=room_id, participant_id=p1)
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is False
    assert turn["reason"] == "autoloop_active"
    assert turn["next_to_act"] is None

    await store.stop_autoloop(room_id=room_id, participant_id=p1)
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is True
    assert turn["next_to_act"] == "p1"


async def test_turn_when_last_sender_left_the_room(store):
    room_id, [p0, p1, p2] = await _make_room(store, 3)
    m = await store.send_message(room_id=room_id, participant_id=p2, text="?", intent="handoff")
    await store.close_room(room_id=room_id, participant_id=p2)
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is False
    assert turn["reason"] == "last_sender_left"
    assert turn["last_message_id"] == m["message_id"]
    assert turn["last_intent"] == "handoff"


async def test_turn_resolves_legacy_file_event_without_sender_id_by_name(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    await store._redis.xadd(
        _room_key(room_id, "stream"),
        {"type": "file", "file_id": "x", "filename": "f", "size_bytes": 1, "sender_name": "p1"},
    )
    turn = await _turn(store, room_id, p0)
    assert turn["applies"] is True
    assert turn["last_intent"] == "pergunta"
    assert turn["next_to_act"] == "p0"


async def test_session_status_keeps_existing_fields(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    status = await store.status(room_id=room_id, participant_id=p0)
    # p0 é o criador (CAP-4, story 10): ganha a chave extra "pending" (lista
    # vazia aqui — ninguém pendente nesta room open_join). Ver
    # test_invites.py pra "pending" invisível a quem não é criador.
    assert set(status) == {
        "status", "participant_count", "max_participants", "ttl_remaining_seconds", "participants", "turn", "pending",
    }
    assert status["pending"] == []
    assert status["participant_count"] == 2


# ---------------------------------------------------------------------------
# Retrocompatibilidade — send sem os campos novos
# ---------------------------------------------------------------------------


async def test_send_without_new_fields_behaves_as_before_plus_additive_fields(store):
    room_id, [p0, p1] = await _make_room(store, 2)
    t = await store.send_message(room_id=room_id, participant_id=p0, text="t")
    j = await store.send_json_message(room_id=room_id, participant_id=p0, payload={"action": "a", "k": 1})
    f = await store.send_file(room_id=room_id, participant_id=p0, filename="f.txt", content_base64=_B64)
    # os campos que já existiam continuam lá com os mesmos tipos
    assert set(t) >= {"message_id", "created_at"}
    assert set(j) >= {"message_id", "created_at"}
    assert set(f) >= {"file_id", "message_id", "size_bytes", "created_at"}
    assert f["size_bytes"] == len(b"conteudo")

    messages = _real(await _poll(store, room_id, p1))
    assert [m["id"] for m in messages] == [t["message_id"], j["message_id"], f["message_id"]]
    assert messages[0]["content"] == {"kind": "text", "text": "t"}
    assert messages[1]["content"] == {"kind": "json", "payload": {"action": "a", "k": 1}}
    assert messages[2]["content"]["kind"] == "file"
    assert messages[2]["content"]["filename"] == "f.txt"
    assert messages[2]["content"]["file_id"] == f["file_id"]
    assert all(m["intent"] == "pergunta" and m["in_reply_to"] is None for m in messages)

    received = await store.receive_file(room_id=room_id, participant_id=p1, file_id=f["file_id"])
    assert received["content_base64"] == _B64


# ---------------------------------------------------------------------------
# Tool-level (app/main.py): os parâmetros novos são opcionais e chegam ao store
# ---------------------------------------------------------------------------


def _use_test_store(monkeypatch, store):
    monkeypatch.setattr(app_main, "_store", store)


async def test_tools_accept_and_forward_protocol_fields(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room_id, [p0, p1] = await _make_room(store, 2)

    q = await app_main.session_send(room_id=room_id, participant_id=p0, text="?")
    assert q["intent"] == "pergunta"

    r = await app_main.session_send_json(
        room_id=room_id, participant_id=p1, payload={"action": "r"}, in_reply_to=q["message_id"], intent="fyi"
    )
    assert r["in_reply_to"] == q["message_id"] and r["intent"] == "fyi"

    f = await app_main.file_send(
        room_id=room_id, participant_id=p1, filename="f", content_base64=_B64, intent="handoff"
    )
    assert f["intent"] == "handoff"

    status = await app_main.message_status(room_id=room_id, participant_id=p0, message_id=q["message_id"])
    assert status["state_by"] == {"p1": "tratado"}  # o send_json com in_reply_to tratou a pergunta

    ack = await app_main.message_ack(room_id=room_id, participant_id=p0, message_id=f["message_id"])
    assert ack["state"] == "tratado"

    turn = (await app_main.session_status(room_id=room_id, participant_id=p0))["turn"]
    assert turn["next_to_act"] == "p0"  # último real: handoff de p1 -> vez de p0


async def test_tools_surface_domain_errors_as_tool_errors(store, monkeypatch):
    _use_test_store(monkeypatch, store)
    room_id, [p0, p1] = await _make_room(store, 2)
    with pytest.raises(ToolError, match="INVALID_INTENT"):
        await app_main.session_send(room_id=room_id, participant_id=p0, text="x", intent="urgent")
    with pytest.raises(ToolError, match="INVALID_REPLY_TARGET"):
        await app_main.session_send(room_id=room_id, participant_id=p0, text="x", in_reply_to="1-1")
    m = await app_main.session_send(room_id=room_id, participant_id=p0, text="x")
    with pytest.raises(ToolError, match="CANNOT_ACK_OWN_MESSAGE"):
        await app_main.message_ack(room_id=room_id, participant_id=p0, message_id=m["message_id"])
    with pytest.raises(ToolError, match="INVALID_ACK_TARGET"):
        await app_main.message_ack(room_id=room_id, participant_id=p1, message_id="1-1")
