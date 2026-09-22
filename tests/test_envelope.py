"""
CAP-8 (story 11): shape único de todo item de `session_poll` — ver
`seguranca.md` ameaça 1 (prompt injection cross-conta) e
`app/redis_store.py::_envelope`.

Todo item vem como `{id, origin, sender_name, intent, in_reply_to,
created_at, untrusted, content}`. `untrusted` é sempre `origin ==
"participant"` — nunca decidido por tipo de conteúdo. O servidor nunca
concatena conteúdo de participante em texto próprio: cada tipo de evento
isola seu conteúdo em `content` (`content.kind` + campos específicos).
"""
import pytest

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")

ENVELOPE_TOP_LEVEL_FIELDS = {
    "id",
    "origin",
    "sender_name",
    "intent",
    "in_reply_to",
    "created_at",
    "untrusted",
    "content",
}


async def _poll_all(store, room_id: str, participant_id: str) -> list[dict]:
    result = await store.poll_messages(room_id=room_id, participant_id=participant_id, timeout_seconds=1)
    return result["messages"]


async def test_system_event_on_join_has_trusted_origin(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    system_events = [m for m in messages if m["origin"] == "system"]
    assert system_events, "esperava pelo menos 1 evento de sistema (entrada do vizinho)"
    for event in system_events:
        assert set(event) == ENVELOPE_TOP_LEVEL_FIELDS
        assert event["untrusted"] is False
        assert event["sender_name"] == "system"
        assert event["content"]["kind"] == "system"
        assert isinstance(event["content"]["text"], str)
    # o evento de "vizinho" entrando isola o nome dele em actor_name, nunca
    # concatenado no "text" do servidor (achado da revisão)
    joined_event = next(e for e in system_events if e["content"]["event"] == "joined")
    assert joined_event["content"]["actor_name"] == "vizinho"
    assert joined_event["content"]["text"] == "Um participante entrou na room"


async def test_text_message_has_untrusted_participant_origin(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    await store.send_message(room_id=room["room_id"], participant_id=joined["participant_id"], text="oi, tudo bem?")
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    text_msgs = [m for m in messages if m["content"]["kind"] == "text" and m["origin"] == "participant"]
    assert len(text_msgs) == 1
    msg = text_msgs[0]
    assert set(msg) == ENVELOPE_TOP_LEVEL_FIELDS
    assert msg["untrusted"] is True
    assert msg["sender_name"] == "vizinho"
    assert msg["content"] == {"kind": "text", "text": "oi, tudo bem?"}


async def test_file_message_content_has_no_synthetic_text(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    content_b64 = "aGVsbG8="  # "hello"
    await store.send_file(
        room_id=room["room_id"],
        participant_id=joined["participant_id"],
        filename="relatorio.txt",
        content_base64=content_b64,
    )
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    file_msgs = [m for m in messages if m["content"]["kind"] == "file"]
    assert len(file_msgs) == 1
    msg = file_msgs[0]
    assert set(msg) == ENVELOPE_TOP_LEVEL_FIELDS
    assert msg["untrusted"] is True
    assert msg["content"]["filename"] == "relatorio.txt"
    assert int(msg["content"]["size_bytes"]) == len(b"hello")
    assert isinstance(msg["content"]["file_id"], str) and msg["content"]["file_id"]
    # nenhum campo de texto sintético tipo "[file] relatorio.txt" em lugar nenhum
    assert "text" not in msg["content"]


async def test_json_action_message_isolates_payload(store):
    room = await store.create_room(
        display_name="criadora", ttl_seconds=120, open_join=True, policy_mode="handoff-enabled"
    )
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    payload = {"action": "deploy", "target": "prod"}
    await store.send_json_message(room_id=room["room_id"], participant_id=joined["participant_id"], payload=payload)
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    json_msgs = [m for m in messages if m["content"]["kind"] == "json"]
    assert len(json_msgs) == 1
    msg = json_msgs[0]
    assert set(msg) == ENVELOPE_TOP_LEVEL_FIELDS
    assert msg["untrusted"] is True
    assert msg["content"]["payload"] == payload


async def test_ack_event_isolates_ack_of_no_synthetic_text_prefix(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    sent = await store.send_message(room_id=room["room_id"], participant_id=room["participant_id"], text="pergunta")
    await store.ack_message(
        room_id=room["room_id"], participant_id=joined["participant_id"], message_id=sent["message_id"]
    )
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    ack_msgs = [m for m in messages if m["content"]["kind"] == "ack"]
    assert len(ack_msgs) == 1
    msg = ack_msgs[0]
    assert set(msg) == ENVELOPE_TOP_LEVEL_FIELDS
    assert msg["untrusted"] is True
    assert msg["content"] == {"kind": "ack", "ack_of": sent["message_id"]}
    # o velho prefixo sintético "[ack]" não existe mais em lugar nenhum do envelope
    assert "[ack]" not in str(msg)


async def test_autoloop_turn_event_isolates_payload_and_turn_status(store):
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    joined = await store.join_room(room_id=room["room_id"], display_name="vizinho")
    await store.propose_autoloop(
        room_id=room["room_id"], participant_id=room["participant_id"], goal="testar envelope", max_turns=5, max_seconds=60
    )
    await store.accept_autoloop(room_id=room["room_id"], participant_id=joined["participant_id"])
    payload = {"step": 1}
    await store.autoloop_turn(
        room_id=room["room_id"], participant_id=room["participant_id"], payload=payload, turn_status="proposing"
    )
    messages = await _poll_all(store, room["room_id"], joined["participant_id"])
    turn_msgs = [m for m in messages if m["content"]["kind"] == "autoloop_turn"]
    assert len(turn_msgs) == 1
    msg = turn_msgs[0]
    assert set(msg) == ENVELOPE_TOP_LEVEL_FIELDS
    assert msg["untrusted"] is True
    assert msg["content"]["payload"] == payload
    assert msg["content"]["turn_status"] == "proposing"


async def test_untrusted_is_always_derived_from_origin_never_content_kind(store):
    """Garantia estrutural: `untrusted` nunca é decidido por tipo de
    conteúdo — é sempre `origin == "participant"`. Prova isso conferindo
    que TODO item de origin="system" (independente do content.kind) é
    untrusted=False e todo item de origin="participant" é untrusted=True,
    numa room com uma mistura dos dois."""
    room = await store.create_room(display_name="criadora", ttl_seconds=120, open_join=True)
    await store.send_message(room_id=room["room_id"], participant_id=room["participant_id"], text="oi")
    messages = await _poll_all(store, room["room_id"], room["participant_id"])
    system_events = [m for m in messages if m["origin"] == "system"]
    participant_events = [m for m in messages if m["origin"] == "participant"]
    assert system_events and participant_events
    assert all(m["untrusted"] is False for m in system_events)
    assert all(m["untrusted"] is True for m in participant_events)
