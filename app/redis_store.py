"""
Store de rooms/mensagens em Redis.

Layout de chaves (prefixo mcpshare):
  {prefix}:room:{room_id}:meta          hash   {created_at, ttl_seconds, status}
  {prefix}:room:{room_id}:participants  hash   {participant_id -> json({display_name, joined_at, last_polled_at?})}
  {prefix}:room:{room_id}:hash_index    hash   {participant_hash(participant_id) -> participant_id} — índice
                                                reverso pra resolver o target_hash de session_kick/
                                                session_approve com um HGET em vez de varrer
                                                participants+pending (SPEC-participant-hash-index). Escrito
                                                no MESMO pipeline que grava a entrada correspondente em
                                                :participants/:pending (create_room, join_room direto e
                                                pending); apagado (HDEL) quando o participante some de lá
                                                (kick_participant, close_room) ou junto da room inteira.
  {prefix}:room:{room_id}:stream        stream XADD por mensagem (sender_id, sender_name, text, created_at)
                                                e por arquivo (type=file: file_id, filename, size_bytes).
                                                Toda mensagem real (type=message|action|file) carrega
                                                também `intent` (sempre gravado, default "pergunta") e,
                                                opcionalmente, `in_reply_to` (id da mensagem respondida)
                                                — SPEC-session-share-mcp CAP-2/CAP-4.
                                                type=ack: {sender_id, sender_name, ack_of, created_at} —
                                                anúncio de que sender tratou a mensagem ack_of (CAP-1).
  {prefix}:room:{room_id}:acks          hash   {"<message_id>:<participant_id>" -> json({at, via})}
                                                — quem já tratou qual mensagem (CAP-1). via="explicit"
                                                (message_ack) ou "reply" (resposta com in_reply_to a uma
                                                `pergunta`). Só avança: nunca se remove um campo daqui.
  {prefix}:room:{room_id}:cursor:{pid}  string last stream-id entregue a esse participante
  {prefix}:room:{room_id}:file:{fid}    string conteúdo base64 de um arquivo enviado (file_send)
  {prefix}:room:{room_id}:file:{fid}:meta hash {sender_id, sender_name, filename, size_bytes, created_at}
  {prefix}:room:{room_id}:autoloop      hash   {status, goal, proposer_id, max_turns, max_seconds,
                                                 started_at, turn_count, ended_reason} — no máximo um
                                                 ciclo por vez; uma nova autoloop_propose sobrescreve
                                                 (turn_count/done_by resetam, histórico fica só na stream)
  {prefix}:room:{room_id}:autoloop:participants Set  quem optou (autoloop_accept) por entrar no loop
  {prefix}:room:{room_id}:autoloop:done Set  subconjunto de loop_participants que já declarou "done"
                                              (consenso: loop só encerra quando cobrir todos os
                                              loop_participants — ver autoloop_turn/Story 3)
  {prefix}:join_rl:{room_id}            string contador de tentativas de join (rate limit)

TTL é "sliding": toda operação de escrita (join/send/poll) reaplica o TTL
configurado da room em todas as chaves relacionadas — a room fica viva
enquanto houver atividade, e expira sozinha (Redis EXPIRE) após
ttl_seconds de silêncio.
"""
import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import redis.asyncio as redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError
from mcp.server.mcpserver.exceptions import ToolError

from app import config, metrics
from app.wordlist import WORDS

# SCAN em vez de KEYS (nunca bloqueia o Redis): count_active_rooms varre em
# lotes deste tamanho — valor pequeno o bastante para não segurar o event
# loop numa instância com muitas rooms, grande o bastante para poucos round-
# trips no caso comum.
_SCAN_COUNT_HINT = 200


class SessionError(ToolError):
    """
    Erro de domínio esperado (room/participant inválido, rate limit etc).

    Herda de ToolError de propósito: o SDK devolve a mensagem para o client
    (is_error=True, sem traceback assustador no log) em vez de mascarar como
    um "crash" genérico — são exatamente os casos em que quem chamou pode
    corrigir o parâmetro e tentar de novo (room_id errado, participant_id
    expirado etc), então a mensagem precisa chegar íntegra no outro lado.
    """


class RoomNotFoundError(SessionError):
    pass


class ParticipantNotFoundError(SessionError):
    pass


class RateLimitedError(SessionError):
    pass


class RoomFullError(SessionError):
    pass


class FileNotFoundInRoomError(SessionError):
    pass


class FileTooLargeError(SessionError):
    pass


class InvalidFileError(SessionError):
    pass


class InvalidMessageIdError(SessionError):
    pass


class AutoloopAlreadyActiveError(SessionError):
    pass


class NoPendingProposalError(SessionError):
    pass


class AlreadyAcceptedError(SessionError):
    pass


class InvalidTurnStatusError(SessionError):
    pass


class NotLoopParticipantError(SessionError):
    pass


class AutoloopNotActiveError(SessionError):
    pass


class InviteRequiredError(SessionError):
    pass


class InviteExpiredError(SessionError):
    pass


class InviteUsedError(SessionError):
    pass


class ForbiddenError(SessionError):
    pass


class PolicyDeniedError(SessionError):
    pass


class InvalidPolicyModeError(SessionError):
    pass


class InvalidPolicyError(SessionError):
    """
    Combinação inválida de campos dentro de `policy` num único session_share
    (SPEC-invite-at-creation) — hoje só policy.invite_on_create=true junto de
    policy.open_join=true (join_code não faz sentido numa room que já aceita
    entrada direta sem convite). Levantado ANTES de create_room, então nunca
    há room órfã por trás desse erro.
    """


class AutoloopLimitExceededError(SessionError):
    pass


class InvalidIntentError(SessionError):
    pass


class InvalidReplyTargetError(SessionError):
    pass


class InvalidAckTargetError(SessionError):
    pass


class CannotAckOwnMessageError(SessionError):
    pass


class StorageUnavailableError(SessionError):
    """
    Redis inacessível mesmo depois do retry/backoff configurado no cliente
    (SPEC-redis-resilience CAP-1) — o blip transitório não se resolveu
    sozinho, então quem chamou a tool recebe um erro de domínio acionável
    em vez de um traceback cru da lib `redis`.
    """


# Protocolo 1:1 (SPEC-session-share-mcp, CAP-1 a CAP-4).
#
# Enum fechado de intenção da mensagem (CAP-4), validado no store como
# TURN_STATUSES. "pergunta" é o default quando o remetente não informa nada
# — preserva o comportamento atual (toda mensagem espera resposta). O valor
# efetivo é SEMPRE gravado na stream, mesmo quando defaultado, para o
# consumidor (poll/export/message_status) nunca precisar conhecer o default.
INTENTS = ("pergunta", "fyi", "handoff", "conclusao")
DEFAULT_INTENT = "pergunta"
# Intenções que passam o turno pra quem recebe (CAP-3) e que exigem sinal
# estrutural (reply ou ack explícito) pra virar "tratado" (CAP-1). As outras
# duas (fyi/conclusao) não passam o turno e contam como tratadas assim que
# entregues.
TURN_PASSING_INTENTS = ("pergunta", "handoff")
# Tipos de evento da stream que são "mensagem real" de um participante — os
# únicos que podem ser alvo de in_reply_to/message_ack e os únicos que
# contam pra derivar o turno (system/ack/autoloop_* são ignorados).
REAL_MESSAGE_TYPES = ("message", "action", "file")
# Estados de message_status por destinatário (CAP-1). "pendente" é o estado
# implícito antes de "entregue" — a spec só nomeia entregue/tratado, mas o
# consumidor precisa de um valor explícito pra "ainda não passou pelo cursor".
ACK_STATE_PENDING = "pendente"
ACK_STATE_DELIVERED = "entregue"
ACK_STATE_HANDLED = "tratado"


# Enum fechado de turn_status (CAP-2) — validado no store (domain error),
# não duplicado em app/main.py (mesma tier que AUTOLOOP_ALREADY_ACTIVE etc).
# blocked encerra o loop unilateralmente (ended_reason="impasse"); done só
# encerra quando TODOS os loop_participants tiverem declarado (ended_reason=
# "consensus") — ver autoloop_turn (CAP-4 / Story 3).
TURN_STATUSES = ("proposing", "agreeing", "blocked", "done")


def _now() -> float:
    return time.time()


def _format_ts(value: Any) -> str:
    """
    Formata um created_at (epoch seconds, string ou float) como texto legível
    em UTC. Entradas 'system' não carregam created_at (ver create_room/
    join_room/close_room) — nesse caso vira '—', igual ao SPEC pede.
    """
    if value is None:
        return "—"
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError):
        return "—"


def _room_key(room_id: str, suffix: str) -> str:
    return f"{config.KEY_PREFIX}:room:{room_id}:{suffix}"


def _invites_key(room_id: str) -> str:
    return _room_key(room_id, "invites")


def _pending_key(room_id: str) -> str:
    return _room_key(room_id, "pending")


def _hash_index_key(room_id: str) -> str:
    # Índice reverso participant_hash(pid) -> pid (SPEC-participant-hash-index):
    # resolve target_hash de kick_participant/approve_participant com um HGET
    # em vez de varrer :participants/:pending calculando o hash de cada um.
    return _room_key(room_id, "hash_index")


def participant_hash(participant_id: str) -> str:
    """CAP-4 (story 10): HMAC-SHA256(PARTICIPANT_HASH_KEY, participant_id)[:12]
    — mesma fórmula do participantHash.ts do auth-service (CAP-0), MESMA
    chave (PARTICIPANT_HASH_KEY == AUTH_HASH_KEY). session_approve/
    session_kick recebem isto como entrada pra referir um terceiro sem
    nunca expor o participant_id real dele; session_status.pending[] mostra
    isto pro criador saber o que passar."""
    return hmac.new(config.PARTICIPANT_HASH_KEY.encode(), participant_id.encode(), hashlib.sha256).hexdigest()[:12]


POLICY_MODES = ("chat-only", "handoff-enabled")
DEFAULT_POLICY_MODE = "chat-only"


def _room_policy_mode(meta: dict) -> str:
    return meta.get("policy_mode", DEFAULT_POLICY_MODE)


def _autoloop_key(room_id: str) -> str:
    return _room_key(room_id, "autoloop")


def _autoloop_participants_key(room_id: str) -> str:
    # Set nativo (SADD/SMEMBERS/SCARD), não lista JSON num campo de hash —
    # evita a race de read-modify-write que um campo JSON teria sob
    # autoloop_accept concorrente (mesmo motivo pelo qual o hash
    # `participants` da room já dá um campo próprio pra cada participante).
    return _room_key(room_id, "autoloop:participants")


def _autoloop_done_key(room_id: str) -> str:
    # Set nativo (SADD/SMEMBERS), mesmo motivo de _autoloop_participants_key:
    # evita a race de read-modify-write que um campo JSON teria sob
    # autoloop_turn(turn_status="done") concorrente de participantes
    # diferentes (ver autoloop_turn, checagem de consenso).
    return _room_key(room_id, "autoloop:done")


def _acks_key(room_id: str) -> str:
    # Um hash flat por room, um campo por par (mensagem, quem tratou) — HSETNX
    # dá idempotência e atomicidade sem read-modify-write (mesmo motivo dos
    # Sets do autoloop). Não é um hash por mensagem porque isso multiplicaria
    # chaves a expirar/deletar por room sem ganho.
    return _room_key(room_id, "acks")


def _ack_field(message_id: str, participant_id: str) -> str:
    # ":" nunca aparece nem em stream id ("<ms>-<seq>") nem em participant_id
    # (token_urlsafe: [A-Za-z0-9_-]) — separador seguro.
    return f"{message_id}:{participant_id}"


def _invalid_intent_message(intent: Any) -> str:
    return f"INVALID_INTENT: '{intent}' não é uma intenção válida (esperado um de {INTENTS})"


def _invalid_reply_target_message(in_reply_to: str) -> str:
    return (
        f"INVALID_REPLY_TARGET: in_reply_to '{in_reply_to}' não aponta para uma mensagem real "
        "(texto/JSON/arquivo) já existente nessa room — não há referência futura, e eventos de "
        "sistema/ack/autoloop não podem ser respondidos"
    )


def _protocol_view(fields: dict) -> dict[str, Any]:
    """
    Campos do protocolo 1:1 expostos em poll_messages pra toda mensagem real.
    Sempre presentes (nunca omitidos): `intent` cai pro default pra eventos
    gravados antes do protocolo existir; `in_reply_to` é None quando a
    mensagem abre um tópico novo (não foi resposta a nada).
    """
    return {
        "intent": fields.get("intent") or DEFAULT_INTENT,
        "in_reply_to": fields.get("in_reply_to") or None,
    }


def _envelope(
    entry_id: str,
    *,
    origin: str,
    sender_name: Optional[str],
    created_at: Optional[float],
    content: dict[str, Any],
    intent: Optional[str] = None,
    in_reply_to: Optional[str] = None,
) -> dict[str, Any]:
    """
    CAP-8 (story 11): shape único de todo item de `session_poll` —
    `seguranca.md` ameaça 1 (prompt injection cross-conta). `untrusted` é
    SEMPRE `origin == "participant"`, nunca decidido caso a caso: o
    servidor não tenta "confiar" em conteúdo de participante conforme o
    tipo (isso seria o próprio servidor decidindo o que é seguro executar,
    voltando pro problema). `content` isola o conteúdo do participante em
    campos estruturados — o servidor NUNCA concatena esse conteúdo num
    texto sintético próprio (o "[action] ...", "[ack] ..." de antes viravam
    exatamente esse tipo de concatenação; agora é só dado em `content`).
    """
    return {
        "id": entry_id,
        "origin": origin,
        "sender_name": sender_name,
        "intent": intent,
        "in_reply_to": in_reply_to,
        "created_at": created_at,
        "untrusted": origin == "participant",
        "content": content,
    }


def _protocol_suffix(entry_id: str, fields: dict) -> str:
    """
    Anotação de fim de linha no transcript (export) — `id=` é o que permite
    reconstruir a thread de um tópico seguindo os in_reply_to (CAP-2), já que
    o transcript antes não mostrava ids de mensagem nenhum.
    """
    parts = [f"id={entry_id}", f"intent={fields.get('intent') or DEFAULT_INTENT}"]
    if fields.get("in_reply_to"):
        parts.append(f"in_reply_to={fields['in_reply_to']}")
    return " _(" + ", ".join(parts) + ")_"


def _autoloop_extra_keys(room_id: str) -> list[str]:
    """As três chaves de autoloop, pra sempre andarem juntas em EXPIRE/DELETE."""
    return [
        _autoloop_key(room_id),
        _autoloop_participants_key(room_id),
        _autoloop_done_key(room_id),
    ]


def _autoloop_already_active_message() -> str:
    return (
        "AUTOLOOP_ALREADY_ACTIVE: já existe uma proposta ou loop autônomo "
        "em andamento nessa room"
    )


def _no_pending_proposal_message() -> str:
    return (
        "NO_PENDING_PROPOSAL: não existe proposta de modo autônomo pendente ou "
        "ativa nessa room"
    )


def _autoloop_not_active_message(*, for_turn: bool = False) -> str:
    # Mesmo AUTOLOOP_NOT_ACTIVE, texto diferente por chamador: autoloop_turn
    # exige status=="active" estrito (uma proposta ainda "proposed" não
    # serve), enquanto autoloop_stop aceita "proposed" OU "active" (dá pra
    # parar uma proposta que nem virou loop ainda) — a condição real
    # verificada por cada um é diferente, então o texto também é.
    if for_turn:
        return "AUTOLOOP_NOT_ACTIVE: não existe loop autônomo ativo (status=active) nessa room"
    return (
        "AUTOLOOP_NOT_ACTIVE: não existe proposta ou loop autônomo em "
        "andamento nessa room para parar"
    )


def _not_loop_participant_message() -> str:
    return (
        "NOT_LOOP_PARTICIPANT: você não está em loop_participants dessa room — "
        "chame autoloop_accept primeiro"
    )


def _invalid_turn_status_message(turn_status: Any) -> str:
    return (
        f"INVALID_TURN_STATUS: '{turn_status}' não é um turn_status válido "
        f"(esperado um de {TURN_STATUSES})"
    )


def _autoloop_limit_exceeded_message(
    ended_reason: str,
    *,
    turn_count: int,
    max_turns: int,
    elapsed_seconds: float,
    max_seconds: int,
) -> str:
    if ended_reason == "watchdog_turns":
        detail = f"turn_count {turn_count + 1} excederia max_turns {max_turns}"
    else:
        detail = f"{elapsed_seconds:.1f}s decorridos excederiam max_seconds {max_seconds}"
    return (
        f"AUTOLOOP_LIMIT_EXCEEDED: loop autônomo encerrado pelo watchdog "
        f"({ended_reason}: {detail}) — turno não foi aceito"
    )


# Tentativas de retry no WATCH/MULTI/EXEC de accept_autoloop/decline_autoloop
# quando um accept/decline concorrente na mesma room invalida o watch em
# _autoloop_participants_key — bem acima do que qualquer contenção realista
# (poucos participantes por room) deveria precisar.
_AUTOLOOP_WATCH_RETRIES = 20


def _generate_room_id() -> str:
    words = [secrets.choice(WORDS) for _ in range(4)]
    suffix = f"{secrets.randbelow(100):02d}"
    return "-".join(words) + "-" + suffix


def _generate_participant_id() -> str:
    return secrets.token_urlsafe(24)


def _generate_file_id() -> str:
    return secrets.token_urlsafe(9)


def _parse_stream_id(stream_id: str) -> tuple[int, int]:
    """
    Parseia um stream id do Redis ("<ms>-<seq>") em tupla (ms, seq).

    Necessário porque stream ids não são inteiros simples nem comparáveis
    lexicograficamente como string: "9-0" > "10-0" em string compare, mas
    9 < 10 numericamente. `cursor:{pid}` usa "0" (sem "-seq") como sentinela
    de "nunca fez poll" — tratado aqui como (0, 0), sempre menor que
    qualquer id real de mensagem.
    """
    parts = stream_id.split("-", 1)
    try:
        ms = int(parts[0])
        seq = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        raise InvalidMessageIdError(
            f"INVALID_MESSAGE_ID: '{stream_id}' não é um stream id válido (formato esperado <ms>-<seq>)"
        )
    return (ms, seq)


def _stream_id_gte(a: str, b: str) -> bool:
    """True se o stream id `a` já alcançou/passou `b` (a >= b), comparando por tupla (ms, seq)."""
    return _parse_stream_id(a) >= _parse_stream_id(b)


@dataclass
class Participant:
    participant_id: str
    display_name: str
    joined_at: float


class SessionStore:
    def __init__(self, redis_url: str = config.REDIS_URL):
        # SPEC-redis-resilience CAP-1: um blip transitório de conexão (ex:
        # durante o XREAD BLOCK longo de poll_messages, até
        # MAX_POLL_TIMEOUT_SECONDS) não pode subir como exceção crua da lib
        # `redis` — o cliente reconecta sozinho, com backoff exponencial
        # curto e um teto de tentativas. Teto escolhido (retries=3,
        # base=0.5s, cap=3s): backoff de ~1s/2s/3s entre tentativas, no
        # máximo ~6s de espera adicional antes de desistir — rápido o
        # bastante pra não mascarar uma falha permanente como "quase
        # funcionando", e bem abaixo do timeout que qualquer client MCP
        # razoável já toleraria numa chamada de tool. Falhas que sobrevivem
        # a esse retry viram StorageUnavailableError na borda dos métodos
        # (ver poll_messages) em vez de um traceback da lib `redis`.
        self._redis = redis.from_url(
            redis_url,
            decode_responses=True,
            retry=Retry(ExponentialBackoff(base=0.5, cap=3.0), retries=3),
            retry_on_error=[RedisConnectionError, RedisTimeoutError],
        )

    async def close(self) -> None:
        await self._redis.aclose()

    async def count_active_rooms(self) -> int:
        """CAP-1 (spec-observability-metrics): número de rooms ativas agora,
        pra alimentar o Gauge `rooms_active` do endpoint /metrics.

        Rooms expiram via TTL do Redis, sem callback de expiração — por
        isso NÃO é um contador incremental (dessincronizaria assim que a
        primeira room expirasse). Em vez disso, conta na hora via
        `scan_iter` (nunca `KEYS`, que bloqueia o Redis inteiro) sobre o
        padrão `{KEY_PREFIX}:room:*:meta` — uma chave por room ativa.
        """
        pattern = f"{config.KEY_PREFIX}:room:*:meta"
        count = 0
        async for _ in self._redis.scan_iter(match=pattern, count=_SCAN_COUNT_HINT):
            count += 1
        return count

    async def _refresh_ttl(self, room_id: str, ttl_seconds: int, extra_keys: list[str] | None = None) -> None:
        # `acks`/`hash_index` entram na lista base (não em extra_keys) de
        # propósito: são chaves por room, como stream/participants, e EXPIRE
        # numa chave que ainda não existe é no-op — mais simples do que
        # lembrar de passar extra_keys em todo caminho de escrita que pode
        # ter gerado um ack ou um novo participante/pendente.
        keys = [
            _room_key(room_id, "meta"),
            _room_key(room_id, "participants"),
            _room_key(room_id, "stream"),
            _acks_key(room_id),
            _hash_index_key(room_id),
        ] + (extra_keys or [])
        pipe = self._redis.pipeline()
        for key in keys:
            pipe.expire(key, ttl_seconds)
        await pipe.execute()

    async def _get_meta(self, room_id: str) -> Optional[dict]:
        meta = await self._redis.hgetall(_room_key(room_id, "meta"))
        return meta or None

    async def _require_participant(self, room_id: str, participant_id: str) -> dict:
        raw = await self._redis.hget(_room_key(room_id, "participants"), participant_id)
        if raw is None:
            meta = await self._get_meta(room_id)
            if meta is None:
                raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
            raise ParticipantNotFoundError(
                "PARTICIPANT_NOT_FOUND: participant_id inválido para essa room "
                "(saiu da room, ou nunca fez join)"
            )
        return json.loads(raw)

    # ------------------------------------------------------------------
    # Protocolo 1:1 — helpers (CAP-1/CAP-2/CAP-4)
    # ------------------------------------------------------------------
    async def _read_real_message(self, room_id: str, message_id: str) -> Optional[dict]:
        """
        Lê o evento `message_id` da stream e devolve seus campos se ele
        existir E for uma mensagem real (REAL_MESSAGE_TYPES). None caso
        contrário (nunca existiu, id futuro, já saiu da retenção maxlen, ou
        é system/ack/autoloop_*). Formato inválido levanta INVALID_MESSAGE_ID
        via _parse_stream_id — o chamador não precisa validar antes.
        """
        _parse_stream_id(message_id)
        entries = await self._redis.xrange(_room_key(room_id, "stream"), min=message_id, max=message_id)
        if not entries:
            return None
        _entry_id, fields = entries[0]
        if fields.get("type") not in REAL_MESSAGE_TYPES:
            return None
        return fields

    async def _resolve_protocol_fields(
        self,
        room_id: str,
        participant_id: str,
        *,
        in_reply_to: Optional[str],
        intent: Optional[str],
    ) -> tuple[dict[str, Any], Optional[str]]:
        """
        Valida intent/in_reply_to de um send e devolve:
          - os campos extras a gravar no evento da stream (`intent` sempre;
            `in_reply_to` só quando informado — stream field não aceita None);
          - o message_id que este envio trata automaticamente (CAP-1), ou
            None. Só acontece quando a mensagem referenciada é uma `pergunta`
            de OUTRO participante: responder a própria pergunta é follow-up,
            não tratamento; `handoff` só é tratado por message_ack explícito;
            fyi/conclusao já contam como tratadas ao serem entregues.
        """
        effective_intent = intent if intent is not None else DEFAULT_INTENT
        if effective_intent not in INTENTS:
            raise InvalidIntentError(_invalid_intent_message(intent))

        fields: dict[str, Any] = {"intent": effective_intent}
        auto_ack_target: Optional[str] = None
        if in_reply_to is not None:
            # Existência na stream já cobre a regra "sem referência futura":
            # um id maior que o último gerado nunca existe. INVALID_MESSAGE_ID
            # (formato) sobe como está — mesmo erro de message_status.
            target = await self._read_real_message(room_id, in_reply_to)
            if target is None:
                raise InvalidReplyTargetError(_invalid_reply_target_message(in_reply_to))
            fields["in_reply_to"] = in_reply_to
            target_intent = target.get("intent") or DEFAULT_INTENT
            if target_intent == "pergunta" and target.get("sender_id") != participant_id:
                auto_ack_target = in_reply_to
        return fields, auto_ack_target

    # ------------------------------------------------------------------
    # session_share — cria a room
    # ------------------------------------------------------------------
    async def create_room(
        self,
        display_name: str,
        ttl_seconds: int,
        open_join: bool = False,
        policy_mode: str = DEFAULT_POLICY_MODE,
    ) -> dict[str, Any]:
        if policy_mode not in POLICY_MODES:
            raise InvalidPolicyModeError(f"INVALID_POLICY_MODE: '{policy_mode}' não é um de {POLICY_MODES}")
        ttl_seconds = min(max(ttl_seconds, 60), config.MAX_TTL_SECONDS)

        for _ in range(10):
            room_id = _generate_room_id()
            created = await self._redis.hsetnx(_room_key(room_id, "meta"), "created_at", str(_now()))
            if created:
                # TTL curto imediato: se o processo cair antes do pipeline
                # abaixo, a chave não fica órfã (sem expiração) pra sempre.
                await self._redis.expire(_room_key(room_id, "meta"), ttl_seconds)
                break
        else:
            raise SessionError("INTERNAL: falha ao gerar room_id único após 10 tentativas")

        participant_id = _generate_participant_id()
        now = _now()

        meta_key = _room_key(room_id, "meta")
        participants_key = _room_key(room_id, "participants")
        stream_key = _room_key(room_id, "stream")
        hash_index_key = _hash_index_key(room_id)

        pipe = self._redis.pipeline()
        pipe.hset(
            meta_key,
            mapping={
                "ttl_seconds": ttl_seconds,
                "status": "open",
                # CAP-4 (story 10): quem criou é o único com poder de
                # session_invite/session_approve/session_kick.
                "created_by": participant_id,
                "policy_open_join": "true" if open_join else "false",
                # CAP-8 (story 11): chat-only (default) recusa handoff/
                # session_send_json — ver _room_policy_mode/send_message/
                # send_json_message.
                "policy_mode": policy_mode,
            },
        )
        pipe.hset(
            participants_key,
            participant_id,
            json.dumps({"display_name": display_name, "joined_at": now}),
        )
        # Índice reverso (SPEC-participant-hash-index): grava junto, no mesmo
        # pipeline, pra nunca divergir de :participants.
        pipe.hset(hash_index_key, participant_hash(participant_id), participant_id)
        # garante que a stream existe (permite EXPIRE nela mesmo sem mensagens ainda)
        # maxlen aproximado: evita crescimento ilimitado de uma room abusada/travada.
        pipe.xadd(
            stream_key,
            {
                "type": "system",
                "event": "room_created",
                "actor_name": display_name,
                "text": "Room criada",
            },
            maxlen=1000,
            approximate=True,
        )
        for key in (meta_key, participants_key, hash_index_key, stream_key):
            pipe.expire(key, ttl_seconds)
        await pipe.execute()

        expires_at = now + ttl_seconds
        return {
            "room_id": room_id,
            "participant_id": participant_id,
            "expires_at": expires_at,
            "ttl_seconds": ttl_seconds,
        }

    # ------------------------------------------------------------------
    # session_join — entra numa room existente
    # ------------------------------------------------------------------
    async def join_room(self, room_id: str, display_name: str, join_code: Optional[str] = None) -> dict[str, Any]:
        # Teto efetivo do rate limit escala com MAX_PARTICIPANTS: um burst
        # legítimo de formação de grupo (N participantes entrando em sequência
        # rápida) não pode ser barrado pelo mesmo contador pensado para ~2
        # pessoas entrando ao longo do tempo. A janela (60s) não muda.
        effective_rate_limit = max(config.JOIN_RATE_LIMIT_ATTEMPTS, config.MAX_PARTICIPANTS * 3)
        rl_key = f"{config.KEY_PREFIX}:join_rl:{room_id}"
        attempts = await self._redis.incr(rl_key)
        if attempts == 1:
            await self._redis.expire(rl_key, config.JOIN_RATE_LIMIT_WINDOW_SECONDS)
        if attempts > effective_rate_limit:
            raise RateLimitedError(
                "RATE_LIMITED: muitas tentativas de entrar nessa room em pouco tempo; aguarde e tente de novo"
            )

        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")

        # CAP-4 (story 10): policy.open_join (default false, gravado em
        # create_room) — join_code ausente só entra direto (como antes desta
        # story) quando a room foi criada com open_join=true. É o modo de
        # compatibilidade citado na arquitetura.
        open_join = meta.get("policy_open_join", "false") == "true"
        if not join_code:
            if not open_join:
                raise InviteRequiredError(
                    "INVITE_REQUIRED: esta room exige convite — peça um join_code ao criador (session_invite)"
                )
            return await self._join_room_direct(room_id, display_name, meta)

        return await self._join_room_pending(room_id, display_name, join_code, meta)

    async def _join_room_direct(self, room_id: str, display_name: str, meta: dict) -> dict[str, Any]:
        """Caminho de compatibilidade (policy.open_join=true): entra direto
        como participante, sem convite — comportamento de antes da story 10."""
        participants_key = _room_key(room_id, "participants")

        current_count = await self._redis.hlen(participants_key)
        if current_count >= config.MAX_PARTICIPANTS:
            raise RoomFullError(
                f"ROOM_FULL: room já tem {current_count}/{config.MAX_PARTICIPANTS} participantes"
            )

        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        participant_id = _generate_participant_id()
        now = _now()

        stream_key = _room_key(room_id, "stream")

        pipe = self._redis.pipeline()
        pipe.hset(
            participants_key,
            participant_id,
            json.dumps({"display_name": display_name, "joined_at": now}),
        )
        # Índice reverso (SPEC-participant-hash-index): mesmo pipeline da
        # escrita em :participants, nunca um passo separado.
        pipe.hset(_hash_index_key(room_id), participant_hash(participant_id), participant_id)
        pipe.xadd(
            stream_key,
            {
                "type": "system",
                "event": "joined",
                "actor_name": display_name,
                "text": "Um participante entrou na room",
            },
            maxlen=1000,
            approximate=True,
        )
        results = await pipe.execute()
        join_event_id = results[-1]  # retorno do XADD, último comando do pipe

        # Achado da revisão da story 10: sem isto, este caminho (open_join,
        # compatibilidade) deixava o cursor no sentinela "0" — exatamente o
        # vazamento retroativo que o CAP-4 inteiro existe pra fechar. Mesmo
        # tratamento de approve_participant: cursor = o próprio evento de
        # entrada, não histórico anterior.
        cursor_key = _room_key(room_id, f"cursor:{participant_id}")
        await self._redis.set(cursor_key, join_event_id, ex=ttl_seconds)

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=[cursor_key])

        raw_participants = await self._redis.hgetall(participants_key)
        participants = [
            {"display_name": json.loads(v)["display_name"], "joined_at": json.loads(v)["joined_at"]}
            for v in raw_participants.values()
        ]

        return {
            "participant_id": participant_id,
            "expires_at": now + ttl_seconds,
            "participants": participants,
        }

    async def _join_room_pending(
        self, room_id: str, display_name: str, join_code: str, meta: dict
    ) -> dict[str, Any]:
        """Caminho com convite: valida o join_code (uso único, TTL de
        INVITE_TTL_SECONDS) e registra o chamador em `:pending` — não em
        `:participants` — até um session_approve do criador."""
        invites_key = _invites_key(room_id)
        pending_key = _pending_key(room_id)
        code_hash = hashlib.sha256(join_code.encode()).hexdigest()
        now = _now()

        async with self._redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(invites_key)
                raw_invite = await pipe.hget(invites_key, code_hash)
                self._check_invite(raw_invite, now)
                invite = json.loads(raw_invite)
                invite["used_at"] = now
                participant_id = _generate_participant_id()

                pipe.multi()
                pipe.hset(invites_key, code_hash, json.dumps(invite))
                pipe.hset(
                    pending_key,
                    participant_id,
                    json.dumps({"display_name": display_name, "requested_at": now}),
                )
                # Índice reverso (SPEC-participant-hash-index): também entra
                # aqui — session_kick precisa achar um pendente pelo hash,
                # não só um participante ativo.
                pipe.hset(_hash_index_key(room_id), participant_hash(participant_id), participant_id)
                pipe.xadd(
                    _room_key(room_id, "stream"),
                    {
                        "type": "system",
                        "event": "join_requested",
                        "actor_name": display_name,
                        "text": "Um participante pediu para entrar (aguardando aprovação)",
                    },
                    maxlen=1000,
                    approximate=True,
                )
                await pipe.execute()
            except redis.WatchError:
                # Corrida perdida contra outro join com o MESMO código: quem
                # venceu já gravou used_at. Reconsulta fora da transação (o
                # WATCH já foi invalidado) só pra devolver o erro certo
                # (INVITE_USED), não um erro de contenção genérico.
                raw_invite = await self._redis.hget(invites_key, code_hash)
                self._check_invite(raw_invite, now)
                # Não deveria chegar aqui (se passou na checagem, não havia
                # corrida de verdade) — mas por segurança, trata como código
                # inválido em vez de seguir num estado inconsistente.
                raise InviteExpiredError("INVITE_EXPIRED: join_code inválido ou expirado")

        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=[invites_key, pending_key])

        return {"participant_id": participant_id, "room_status": "pending"}

    @staticmethod
    def _check_invite(raw_invite: Optional[str], now: float) -> None:
        """Levanta o erro certo pra um join_code — nunca revela se um
        código simplesmente nunca existiu vs. já expirou (mesma resposta:
        INVITE_EXPIRED), pra não virar oráculo de enumeração de códigos."""
        if raw_invite is None:
            raise InviteExpiredError("INVITE_EXPIRED: join_code inválido ou expirado")
        invite = json.loads(raw_invite)
        if invite.get("used_at") is not None:
            raise InviteUsedError("INVITE_USED: este join_code já foi usado")
        if now > invite["expires_at"]:
            raise InviteExpiredError("INVITE_EXPIRED: join_code expirado (TTL de 10 min)")

    # ------------------------------------------------------------------
    # session_invite — gera um join_code de uso único (só o criador)
    # ------------------------------------------------------------------
    async def create_invite(self, room_id: str, participant_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        if meta.get("created_by") != participant_id:
            raise ForbiddenError("FORBIDDEN: só o criador da room pode gerar convites")

        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        join_code = secrets.token_urlsafe(12)
        code_hash = hashlib.sha256(join_code.encode()).hexdigest()
        now = _now()
        expires_at = now + config.INVITE_TTL_SECONDS

        invites_key = _invites_key(room_id)
        await self._redis.hset(
            invites_key,
            code_hash,
            json.dumps({"created_by": participant_id, "expires_at": expires_at, "used_at": None}),
        )
        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=[invites_key])

        return {"join_code": join_code, "expires_at": expires_at}

    # ------------------------------------------------------------------
    # session_approve — promove um pending a participante (só o criador)
    # ------------------------------------------------------------------
    async def approve_participant(self, room_id: str, participant_id: str, target_hash: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        if meta.get("created_by") != participant_id:
            raise ForbiddenError("FORBIDDEN: só o criador da room pode aprovar participantes pendentes")

        pending_key = _pending_key(room_id)
        # Índice reverso (SPEC-participant-hash-index): HGET direto em vez de
        # varrer hgetall(pending) calculando o hash de cada um.
        match_pid = await self._redis.hget(_hash_index_key(room_id), target_hash)
        raw_pending_entry = await self._redis.hget(pending_key, match_pid) if match_pid is not None else None
        if raw_pending_entry is None:
            # match_pid None (hash nunca indexado) OU o índice aponta pra um
            # pid que já não está mais em pending (race, já aprovado/kickado,
            # dado velho) — nos dois casos é "não encontrado", não erro interno.
            raise ParticipantNotFoundError(
                "PARTICIPANT_NOT_FOUND: nenhum participante pendente com esse participant_hash"
            )

        pending_data = json.loads(raw_pending_entry)
        now = _now()
        participants_key = _room_key(room_id, "participants")

        pipe = self._redis.pipeline()
        pipe.hdel(pending_key, match_pid)
        pipe.hset(
            participants_key,
            match_pid,
            json.dumps({"display_name": pending_data["display_name"], "joined_at": now}),
        )
        pipe.xadd(
            _room_key(room_id, "stream"),
            {
                "type": "system",
                "event": "approved",
                "actor_name": pending_data["display_name"],
                "text": "Um participante aprovado entrou na room",
            },
            maxlen=1000,
            approximate=True,
        )
        results = await pipe.execute()
        join_event_id = results[-1]  # retorno do XADD, último comando do pipe

        # Cursor inicial = o próprio evento de join dele — corrige o "0" que
        # devolveria todo o histórico retido na stream (app/redis_store.py,
        # poll_messages). Primeiro poll do aprovado não vê nada anterior.
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        cursor_key = _room_key(room_id, f"cursor:{match_pid}")
        await self._redis.set(cursor_key, join_event_id, ex=ttl_seconds)

        return {"approved": True}

    # ------------------------------------------------------------------
    # session_kick — remove um participante ou pendente (só o criador)
    # ------------------------------------------------------------------
    async def kick_participant(self, room_id: str, participant_id: str, target_hash: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        if meta.get("created_by") != participant_id:
            raise ForbiddenError("FORBIDDEN: só o criador da room pode expulsar participantes")

        participants_key = _room_key(room_id, "participants")
        pending_key = _pending_key(room_id)
        hash_index_key = _hash_index_key(room_id)

        # Índice reverso (SPEC-participant-hash-index): HGET direto em vez de
        # varrer hgetall(participants)+hgetall(pending) calculando o hash de
        # cada um.
        match_pid = await self._redis.hget(hash_index_key, target_hash)
        display_name = None
        if match_pid is not None:
            raw = await self._redis.hget(participants_key, match_pid)
            if raw is not None:
                display_name = json.loads(raw)["display_name"]
            else:
                raw = await self._redis.hget(pending_key, match_pid)
                if raw is not None:
                    display_name = json.loads(raw)["display_name"]
                else:
                    # Índice aponta pra um pid que já não está mais em
                    # participants/pending (race, dado velho) — não encontrado,
                    # não erro interno.
                    match_pid = None
        if match_pid is None:
            raise ParticipantNotFoundError(
                "PARTICIPANT_NOT_FOUND: nenhum participante (ativo ou pendente) com esse participant_hash"
            )

        cursor_key = _room_key(room_id, f"cursor:{match_pid}")
        pipe = self._redis.pipeline()
        pipe.hdel(participants_key, match_pid)
        pipe.hdel(pending_key, match_pid)
        pipe.hdel(hash_index_key, target_hash)
        # Mesmo motivo de close_room: quem sai (ou é expulso) não pode
        # continuar contando como loop_participant/done do autoloop.
        pipe.srem(_autoloop_participants_key(room_id), match_pid)
        pipe.srem(_autoloop_done_key(room_id), match_pid)
        pipe.delete(cursor_key)
        pipe.xadd(
            _room_key(room_id, "stream"),
            {
                "type": "system",
                "event": "kicked",
                "actor_name": display_name,
                "text": "Um participante foi removido da room",
            },
            maxlen=1000,
            approximate=True,
        )
        await pipe.execute()

        return {"kicked": True}

    # ------------------------------------------------------------------
    # session_set_policy — muda policy.mode (só o criador)
    # ------------------------------------------------------------------
    async def set_policy(self, room_id: str, participant_id: str, mode: str) -> dict[str, Any]:
        if mode not in POLICY_MODES:
            raise InvalidPolicyModeError(f"INVALID_POLICY_MODE: '{mode}' não é um de {POLICY_MODES}")
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        if meta.get("created_by") != participant_id:
            raise ForbiddenError("FORBIDDEN: só o criador da room pode mudar a política")

        await self._redis.hset(_room_key(room_id, "meta"), "policy_mode", mode)
        return {"policy_mode": mode}

    # ------------------------------------------------------------------
    # autoloop_propose
    # ------------------------------------------------------------------
    async def propose_autoloop(
        self, room_id: str, participant_id: str, goal: str, max_turns: int, max_seconds: int
    ) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        # Clampa como create_room já clampa ttl_seconds contra MAX_TTL_SECONDS —
        # um proponente não pode pedir um loop sem teto real.
        max_turns = min(max(max_turns, 1), config.AUTOLOOP_HARD_CAP_TURNS)
        max_seconds = min(max(max_seconds, 1), config.AUTOLOOP_HARD_CAP_SECONDS)

        autoloop_key = _autoloop_key(room_id)
        participants_set_key = _autoloop_participants_key(room_id)
        done_set_key = _autoloop_done_key(room_id)
        stream_key = _room_key(room_id, "stream")

        # WATCH/MULTI/EXEC: guarda o check-then-act ("já existe proposta
        # pendente/ativa?") contra duas propose concorrentes passando o guard
        # ao mesmo tempo. Perder a corrida (WatchError) vira o mesmo erro que
        # o perdedor veria numa checagem sequencial — sem retry automático,
        # é a escolha segura sob contenção (ver Code Map da story).
        async with self._redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(autoloop_key)
                current_status = await pipe.hget(autoloop_key, "status")
                if current_status in ("proposed", "active"):
                    await pipe.unwatch()
                    raise AutoloopAlreadyActiveError(_autoloop_already_active_message())
                pipe.multi()
                # Sobrescreve qualquer ciclo anterior (ended, ou nunca existiu)
                # — turn_count/done_by sempre resetam a zero num ciclo novo.
                pipe.delete(autoloop_key, participants_set_key, done_set_key)
                pipe.hset(
                    autoloop_key,
                    mapping={
                        "status": "proposed",
                        "goal": goal,
                        "proposer_id": participant_id,
                        "max_turns": max_turns,
                        "max_seconds": max_seconds,
                        "started_at": "",
                        "turn_count": 0,
                        "ended_reason": "",
                    },
                )
                pipe.sadd(participants_set_key, participant_id)
                pipe.xadd(
                    stream_key,
                    {
                        "type": "system",
                        "event": "autoloop_proposed",
                        "actor_name": participant["display_name"],
                        "goal": goal,
                        "text": "Modo autônomo proposto",
                    },
                    maxlen=1000,
                    approximate=True,
                )
                # Sem EXPIRE aqui pras 2 chaves recém-escritas: o
                # _refresh_ttl logo abaixo já cobre as 3 chaves de autoloop
                # (via _autoloop_extra_keys) — expirar aqui também seria
                # redundante, não errado, mas duplicado.
                await pipe.execute()
            except redis.WatchError:
                raise AutoloopAlreadyActiveError(_autoloop_already_active_message())

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))

        return {
            "status": "proposed",
            "loop_participants": [participant_id],
            "max_turns": max_turns,
            "max_seconds": max_seconds,
        }

    # ------------------------------------------------------------------
    # autoloop_accept
    # ------------------------------------------------------------------
    async def accept_autoloop(self, room_id: str, participant_id: str) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        autoloop_key = _autoloop_key(room_id)
        participants_set_key = _autoloop_participants_key(room_id)
        stream_key = _room_key(room_id, "stream")

        # WATCH(autoloop_key, participants_set_key)/MULTI/EXEC: o sismember
        # (idempotência) e a decisão "virei o 2º membro? disparo o evento
        # 'ativo'?" precisam ser atômicos junto do SADD — sem isso, dois
        # accept_autoloop concorrentes (ou um accept e um decline do mesmo
        # participant_id) fazem um read-then-write não atômico no mesmo Set
        # e podem disparar "ativo"/"aceitou" em duplicidade, ou deixar
        # accept+decline concorrentes do mesmo participante em estado
        # contraditório. Perder o watch (outro accept/decline mexeu no Set
        # entre nossa leitura e nosso EXEC) só significa que o estado mudou
        # sob nós — tentamos de novo com dado fresco, diferente de
        # propose_autoloop (onde perder a corrida é sempre erro definitivo).
        new_status: str | None = None
        for _ in range(_AUTOLOOP_WATCH_RETRIES):
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(autoloop_key, participants_set_key)
                current_status = await pipe.hget(autoloop_key, "status")
                if current_status not in ("proposed", "active"):
                    await pipe.unwatch()
                    raise NoPendingProposalError(_no_pending_proposal_message())

                already_member = await pipe.sismember(participants_set_key, participant_id)
                if already_member:
                    # Idempotente: chamada repetida (ou o próprio proponente
                    # aceitando a própria proposta, já auto-incluído por
                    # propose_autoloop) não reemite "aceitou o modo autônomo".
                    await pipe.unwatch()
                    await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
                    loop_participants = sorted(await self._redis.smembers(participants_set_key))
                    return {"status": current_status, "loop_participants": loop_participants}

                # Vira "active" no instante em que o 2º loop_participant
                # distinto entra — proponente + 1, sem esperar o resto da
                # room (CAP-1). member_count_before+1 é seguro (não precisa
                # reler depois do SADD): o WATCH garante que, se mais
                # alguém mexer no Set antes do nosso EXEC, a transação
                # inteira falha e tentamos de novo.
                member_count_before = await pipe.scard(participants_set_key)
                will_activate = current_status == "proposed" and (member_count_before + 1) >= 2

                pipe.multi()
                pipe.sadd(participants_set_key, participant_id)
                pipe.xadd(
                    stream_key,
                    {
                        "type": "system",
                        "event": "autoloop_accepted",
                        "actor_name": participant["display_name"],
                        "text": "Modo autônomo aceito",
                    },
                    maxlen=1000,
                    approximate=True,
                )
                if will_activate:
                    pipe.hset(autoloop_key, mapping={"status": "active", "started_at": _now()})
                    pipe.xadd(
                        stream_key,
                        {
                            "type": "system",
                            "text": "modo autônomo ativo (2 ou mais participantes já aceitaram)",
                        },
                        maxlen=1000,
                        approximate=True,
                    )
                try:
                    await pipe.execute()
                except redis.WatchError:
                    continue
                new_status = "active" if will_activate else current_status
                break
        else:
            raise SessionError(
                "INTERNAL: autoloop_accept não conseguiu commitar após tentativas concorrentes "
                "repetidas — tente de novo"
            )

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
        loop_participants = sorted(await self._redis.smembers(participants_set_key))
        return {"status": new_status, "loop_participants": loop_participants}

    # ------------------------------------------------------------------
    # autoloop_decline
    # ------------------------------------------------------------------
    async def decline_autoloop(self, room_id: str, participant_id: str) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        autoloop_key = _autoloop_key(room_id)
        participants_set_key = _autoloop_participants_key(room_id)
        stream_key = _room_key(room_id, "stream")

        # Mesmo padrão WATCH/MULTI/EXEC de accept_autoloop, pelo mesmo
        # motivo: sem isso, um accept_autoloop concorrente do MESMO
        # participant_id pode fazer SADD bem entre nossa checagem de
        # sismember e o XADD do evento "recusou" — WATCH em
        # participants_set_key garante que, se isso acontecer, nosso EXEC
        # falha e a releitura seguinte já enxerga o SADD do accept
        # (vira ALREADY_ACCEPTED em vez de publicar um "recusou" enganoso).
        for _ in range(_AUTOLOOP_WATCH_RETRIES):
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(autoloop_key, participants_set_key)
                current_status = await pipe.hget(autoloop_key, "status")
                if current_status not in ("proposed", "active"):
                    await pipe.unwatch()
                    raise NoPendingProposalError(_no_pending_proposal_message())

                already_member = await pipe.sismember(participants_set_key, participant_id)
                if already_member:
                    # Quem já está em loop_participants não pode "recusar"
                    # sem que isso vire uma mentira sobre o próprio estado
                    # de consentimento — Story 1 não tem um "sair depois de
                    # aceitar" (ver Code Map); isso é território de
                    # autoloop_stop (Story 4), não deste tool.
                    await pipe.unwatch()
                    raise AlreadyAcceptedError(
                        "ALREADY_ACCEPTED: você já está em loop_participants dessa room — "
                        "autoloop_decline só se aplica a quem ainda não aceitou"
                    )

                pipe.multi()
                pipe.xadd(
                    stream_key,
                    {
                        "type": "system",
                        "event": "autoloop_declined",
                        "actor_name": participant["display_name"],
                        "text": "Convite de modo autônomo recusado",
                    },
                    maxlen=1000,
                    approximate=True,
                )
                try:
                    await pipe.execute()
                except redis.WatchError:
                    continue
                break
        else:
            raise SessionError(
                "INTERNAL: autoloop_decline não conseguiu commitar após tentativas concorrentes "
                "repetidas — tente de novo"
            )

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
        return {"status": "declined"}

    # ------------------------------------------------------------------
    # autoloop_stop
    # ------------------------------------------------------------------
    async def stop_autoloop(self, room_id: str, participant_id: str) -> dict[str, Any]:
        # _require_participant apenas — nunca checa loop_participants (CAP-5):
        # qualquer participante da room, incluindo um bystander que nunca
        # chamou autoloop_accept, pode parar o loop a qualquer momento, sem
        # depender de acordo de mais ninguém.
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        autoloop_key = _autoloop_key(room_id)
        stream_key = _room_key(room_id, "stream")

        # Mesmo padrão WATCH/MULTI/EXEC bounded-retry de accept_autoloop/
        # decline_autoloop, não o single-shot-fail de propose_autoloop: um
        # accept concorrente que vira "proposed" -> "active" debaixo de nós
        # só significa reler e ainda assim parar (o loop continua
        # proposed/active, muda só o status exato que vemos) — diferente de
        # propose_autoloop, aqui perder o watch nunca é motivo pra desistir.
        for _ in range(_AUTOLOOP_WATCH_RETRIES):
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(autoloop_key)
                current_status = await pipe.hget(autoloop_key, "status")
                if current_status not in ("proposed", "active"):
                    await pipe.unwatch()
                    raise AutoloopNotActiveError(_autoloop_not_active_message())

                pipe.multi()
                pipe.hset(autoloop_key, mapping={"status": "ended", "ended_reason": "stopped"})
                pipe.xadd(
                    stream_key,
                    {
                        "type": "system",
                        "event": "autoloop_stopped",
                        "actor_name": participant["display_name"],
                        "text": "Modo autônomo parado",
                    },
                    maxlen=1000,
                    approximate=True,
                )
                try:
                    await pipe.execute()
                except redis.WatchError:
                    continue
                break
        else:
            raise SessionError(
                "INTERNAL: autoloop_stop não conseguiu commitar após tentativas concorrentes "
                "repetidas — tente de novo"
            )

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
        return {"status": "ended", "ended_reason": "stopped"}

    # ------------------------------------------------------------------
    # autoloop_status
    # ------------------------------------------------------------------
    async def autoloop_status(self, room_id: str, participant_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")

        autoloop_key = _autoloop_key(room_id)
        participants_set_key = _autoloop_participants_key(room_id)
        done_set_key = _autoloop_done_key(room_id)

        autoloop = await self._redis.hgetall(autoloop_key)
        if not autoloop:
            return {
                "status": None,
                "goal": None,
                "loop_participants": [],
                "done_by": [],
                "turn_count": 0,
                "max_turns": None,
                "max_seconds": None,
                "started_at": None,
                "ended_reason": None,
            }

        loop_participants = sorted(await self._redis.smembers(participants_set_key))
        done_by = sorted(await self._redis.smembers(done_set_key))

        # Não reaplica TTL sliding: consulta de status é leitura, mesma
        # decisão já tomada em session_status/session_export.
        return {
            "status": autoloop.get("status"),
            "goal": autoloop.get("goal"),
            "loop_participants": loop_participants,
            "done_by": done_by,
            "turn_count": int(autoloop.get("turn_count") or 0),
            "max_turns": int(autoloop["max_turns"]) if autoloop.get("max_turns") else None,
            "max_seconds": int(autoloop["max_seconds"]) if autoloop.get("max_seconds") else None,
            "started_at": float(autoloop["started_at"]) if autoloop.get("started_at") else None,
            "ended_reason": autoloop.get("ended_reason") or None,
        }

    # ------------------------------------------------------------------
    # autoloop_turn
    # ------------------------------------------------------------------
    async def autoloop_turn(
        self, room_id: str, participant_id: str, payload: dict, turn_status: str
    ) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        autoloop_key = _autoloop_key(room_id)
        participants_set_key = _autoloop_participants_key(room_id)
        done_set_key = _autoloop_done_key(room_id)
        stream_key = _room_key(room_id, "stream")
        payload_json = json.dumps(payload)

        # Mesmo padrão WATCH/MULTI/EXEC + retry limitado de accept_autoloop
        # (_AUTOLOOP_WATCH_RETRIES): o trio "ler turn_count/checar watchdog/
        # incrementar-ou-encerrar" precisa ser atômico, senão dois turnos
        # concorrentes de participantes diferentes podem ler o mesmo
        # turn_count e ambos incrementarem, empurrando turn_count além de
        # max_turns via lost-update race. Perder o WATCH (outro turno mexeu
        # em autoloop_key/participants_set_key/done_set_key entre nossa
        # leitura e nosso EXEC) só significa que o estado mudou sob nós —
        # relemos e reavaliamos toda a ordem de checagem (status/participante/
        # turn_status/watchdog/consenso) com dado fresco, igual
        # accept_autoloop. done_set_key entra no WATCH pelo mesmo motivo do
        # turn_count: dois turn_status="done" concorrentes de participantes
        # diferentes não podem ambos ler o mesmo done_by e ambos concluírem
        # "consenso incompleto" via lost-update race (CAP-4).
        for _ in range(_AUTOLOOP_WATCH_RETRIES):
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(autoloop_key, participants_set_key, done_set_key)

                current_status = await pipe.hget(autoloop_key, "status")
                if current_status != "active":
                    await pipe.unwatch()
                    raise AutoloopNotActiveError(_autoloop_not_active_message(for_turn=True))

                is_loop_participant = await pipe.sismember(participants_set_key, participant_id)
                if not is_loop_participant:
                    await pipe.unwatch()
                    raise NotLoopParticipantError(_not_loop_participant_message())

                if turn_status not in TURN_STATUSES:
                    await pipe.unwatch()
                    raise InvalidTurnStatusError(_invalid_turn_status_message(turn_status))

                turn_count_raw, started_at_raw, max_turns_raw, max_seconds_raw = await pipe.hmget(
                    autoloop_key, ["turn_count", "started_at", "max_turns", "max_seconds"]
                )
                turn_count = int(turn_count_raw or 0)
                started_at = float(started_at_raw) if started_at_raw else None
                max_turns = int(max_turns_raw)
                max_seconds = int(max_seconds_raw)
                now = _now()

                # Tie-break (checado nessa ordem de propósito, ver Design
                # Notes da story): quando os dois limites disparam na mesma
                # chamada, turn_count é o mais determinístico dos dois —
                # ended_reason="watchdog_turns" vence.
                turns_exceeded = (turn_count + 1) > max_turns
                time_exceeded = started_at is not None and (now - started_at) > max_seconds

                # CAP-4: consenso/impasse só é avaliado quando o turno é de
                # fato aceito (watchdog não disparou nesta chamada) — um
                # turno que estoura o watchdog nunca chega a ser "aceito", e
                # blocked/done não tem chance de encerrar por consenso/
                # impasse na mesma chamada em que o watchdog já encerrou.
                new_done_by: set[str] | None = None
                consensus_reached = False
                if not (turns_exceeded or time_exceeded) and turn_status == "done":
                    loop_participants_now = set(await pipe.smembers(participants_set_key))
                    existing_done = set(await pipe.smembers(done_set_key))
                    new_done_by = existing_done | {participant_id}
                    # new_done_by é sempre subconjunto de loop_participants_now
                    # (só quem está em loop_participants chega até aqui,
                    # checado acima) — consenso é atingido quando cobre todos.
                    consensus_reached = new_done_by == loop_participants_now

                pipe.multi()
                if turns_exceeded or time_exceeded:
                    ended_reason = "watchdog_turns" if turns_exceeded else "watchdog_time"
                    pipe.hset(autoloop_key, mapping={"status": "ended", "ended_reason": ended_reason})
                    pipe.xadd(
                        stream_key,
                        {
                            "type": "system",
                            "text": f"modo autônomo encerrado por watchdog ({ended_reason})",
                        },
                        maxlen=1000,
                        approximate=True,
                    )
                    try:
                        await pipe.execute()
                    except redis.WatchError:
                        continue
                    await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
                    elapsed_seconds = (now - started_at) if started_at is not None else 0.0
                    raise AutoloopLimitExceededError(
                        _autoloop_limit_exceeded_message(
                            ended_reason,
                            turn_count=turn_count,
                            max_turns=max_turns,
                            elapsed_seconds=elapsed_seconds,
                            max_seconds=max_seconds,
                        )
                    )

                pipe.hincrby(autoloop_key, "turn_count", 1)
                pipe.xadd(
                    stream_key,
                    {
                        "type": "autoloop_turn",
                        "sender_id": participant_id,
                        "sender_name": participant["display_name"],
                        "payload_json": payload_json,
                        "turn_status": turn_status,
                        "created_at": now,
                    },
                    maxlen=1000,
                    approximate=True,
                )

                # CAP-4: a assimetria em si. blocked é declaração unilateral —
                # um único loop_participant já encerra (ended_reason=
                # "impasse"), sem esperar acordo de mais ninguém. done é
                # confirmação bilateral — só adiciona o chamador a done_by;
                # o loop só encerra (ended_reason="consensus") quando done_by
                # cobrir TODOS os loop_participants correntes. Exigir acordo
                # pra declarar "travado" anularia o próprio propósito de
                # detectar impasse (ver SPEC.md CAP-4).
                ended_now = False
                ended_reason_result: str | None = None
                if turn_status == "blocked":
                    ended_now = True
                    ended_reason_result = "impasse"
                    pipe.hset(autoloop_key, mapping={"status": "ended", "ended_reason": "impasse"})
                    pipe.xadd(
                        stream_key,
                        {
                            "type": "system",
                            "event": "autoloop_impasse",
                            "actor_name": participant["display_name"],
                            "text": "Modo autônomo encerrado por impasse (turn_status=blocked)",
                        },
                        maxlen=1000,
                        approximate=True,
                    )
                elif turn_status == "done":
                    pipe.sadd(done_set_key, participant_id)
                    if consensus_reached:
                        ended_now = True
                        ended_reason_result = "consensus"
                        pipe.hset(autoloop_key, mapping={"status": "ended", "ended_reason": "consensus"})
                        pipe.xadd(
                            stream_key,
                            {
                                "type": "system",
                                "text": (
                                    "modo autônomo encerrado por consenso (done: "
                                    f"{', '.join(sorted(new_done_by))})"
                                ),
                            },
                            maxlen=1000,
                            approximate=True,
                        )

                try:
                    results = await pipe.execute()
                except redis.WatchError:
                    continue
                # results[1] é fixo — hincrby(turn_count) sempre é o 1º comando
                # queued nesta MULTI e xadd(type=autoloop_turn) sempre o 2º
                # (índice 1), antes de qualquer comando condicional que
                # blocked/done ainda apendam depois (hset/xadd de ended). Se a
                # ordem dos dois primeiros comandos mudar, este índice precisa
                # mudar junto — não há proteção de teste automática pra isso
                # além de test_dispatch_in_poll_and_export/os testes de
                # blocked/done que comparam message_id contra o "id" que
                # poll_messages devolve pro mesmo turno.
                message_id = results[1]
                new_turn_count = turn_count + 1
                done_by_result = sorted(new_done_by) if new_done_by is not None else None
                break
        else:
            raise SessionError(
                "INTERNAL: autoloop_turn não conseguiu commitar após tentativas concorrentes "
                "repetidas — tente de novo"
            )

        await self._refresh_ttl(room_id, ttl_seconds, extra_keys=_autoloop_extra_keys(room_id))
        response: dict[str, Any] = {
            "message_id": message_id,
            "turn_count": new_turn_count,
            "status": "ended" if ended_now else "active",
        }
        if ended_reason_result is not None:
            response["ended_reason"] = ended_reason_result
        if done_by_result is not None:
            response["done_by"] = done_by_result
        return response

    # ------------------------------------------------------------------
    # session_send
    # ------------------------------------------------------------------
    async def send_message(
        self,
        room_id: str,
        participant_id: str,
        text: str,
        in_reply_to: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        protocol_fields, auto_ack_target = await self._resolve_protocol_fields(
            room_id, participant_id, in_reply_to=in_reply_to, intent=intent
        )
        # CAP-8 (story 11): handoff descreve a intenção de quem manda, não
        # uma ordem — em chat-only (default) a room recusa de propósito,
        # pra não virar canal de "peça e a outra sessão executa".
        if protocol_fields["intent"] == "handoff" and _room_policy_mode(meta) != "handoff-enabled":
            metrics.policy_denied_total.inc()
            raise PolicyDeniedError(
                "POLICY_DENIED: intent=handoff exige policy.mode=handoff-enabled nessa room "
                "(peça ao criador pra chamar session_set_policy)"
            )

        stream_key = _room_key(room_id, "stream")
        now = _now()
        pipe = self._redis.pipeline()
        pipe.xadd(
            stream_key,
            {
                "type": "message",
                "sender_id": participant_id,
                "sender_name": participant["display_name"],
                "text": text,
                "created_at": now,
                **protocol_fields,
            },
            maxlen=1000,
            approximate=True,
        )
        self._queue_auto_ack(pipe, room_id, participant_id, auto_ack_target, now)
        message_id, *_ = await pipe.execute()
        metrics.messages_total.labels(kind="message").inc()
        await self._refresh_ttl(room_id, ttl_seconds)
        return {
            "message_id": message_id,
            "created_at": now,
            "intent": protocol_fields["intent"],
            "in_reply_to": in_reply_to,
        }

    def _queue_auto_ack(
        self,
        pipe: Any,
        room_id: str,
        participant_id: str,
        auto_ack_target: Optional[str],
        now: float,
    ) -> None:
        """
        Enfileira no pipeline o registro de "tratado" (CAP-1) que uma
        resposta com in_reply_to a uma `pergunta` de outro participante
        produz. HSETNX: se o mesmo participante já tinha tratado (ack
        explícito antes, ou outra resposta), o registro original fica —
        estado só avança. Não emite evento type=ack: a própria resposta já é
        o sinal estrutural visível no poll do remetente original.
        """
        if auto_ack_target is None:
            return
        pipe.hsetnx(
            _acks_key(room_id),
            _ack_field(auto_ack_target, participant_id),
            json.dumps({"at": now, "via": "reply"}),
        )

    # ------------------------------------------------------------------
    # session_send_json
    # ------------------------------------------------------------------
    async def send_json_message(
        self,
        room_id: str,
        participant_id: str,
        payload: dict,
        in_reply_to: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        protocol_fields, auto_ack_target = await self._resolve_protocol_fields(
            room_id, participant_id, in_reply_to=in_reply_to, intent=intent
        )
        # CAP-8 (story 11): payload sempre tem "action" (_check_json_payload,
        # app/main.py, já garante isso antes de chegar aqui) — em chat-only
        # (default), session_send_json inteiro fica bloqueado: é o formato
        # mais "parecido com comando" que existe no protocolo.
        if _room_policy_mode(meta) != "handoff-enabled":
            metrics.policy_denied_total.inc()
            raise PolicyDeniedError(
                "POLICY_DENIED: session_send_json exige policy.mode=handoff-enabled nessa room "
                "(peça ao criador pra chamar session_set_policy)"
            )

        stream_key = _room_key(room_id, "stream")
        now = _now()
        pipe = self._redis.pipeline()
        pipe.xadd(
            stream_key,
            {
                "type": "action",
                "sender_id": participant_id,
                "sender_name": participant["display_name"],
                "payload_json": json.dumps(payload),
                "created_at": now,
                **protocol_fields,
            },
            maxlen=1000,
            approximate=True,
        )
        self._queue_auto_ack(pipe, room_id, participant_id, auto_ack_target, now)
        message_id, *_ = await pipe.execute()
        metrics.messages_total.labels(kind="json").inc()
        await self._refresh_ttl(room_id, ttl_seconds)
        return {
            "message_id": message_id,
            "created_at": now,
            "intent": protocol_fields["intent"],
            "in_reply_to": in_reply_to,
        }

    # ------------------------------------------------------------------
    # file_send
    # ------------------------------------------------------------------
    async def send_file(
        self,
        room_id: str,
        participant_id: str,
        filename: str,
        content_base64: str,
        in_reply_to: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
        protocol_fields, auto_ack_target = await self._resolve_protocol_fields(
            room_id, participant_id, in_reply_to=in_reply_to, intent=intent
        )

        try:
            raw = base64.b64decode(content_base64, validate=True)
        except (binascii.Error, ValueError) as e:
            raise InvalidFileError(f"INVALID_BASE64: content_base64 não é base64 válido ({e})")

        size_bytes = len(raw)
        if size_bytes == 0:
            raise InvalidFileError("INVALID_FILE: arquivo vazio")
        if size_bytes > config.MAX_FILE_SIZE_BYTES:
            raise FileTooLargeError(
                f"FILE_TOO_LARGE: {size_bytes} bytes excede o limite de "
                f"{config.MAX_FILE_SIZE_BYTES} bytes"
            )

        file_id = _generate_file_id()
        now = _now()
        file_key = _room_key(room_id, f"file:{file_id}")
        file_meta_key = _room_key(room_id, f"file:{file_id}:meta")
        stream_key = _room_key(room_id, "stream")

        pipe = self._redis.pipeline()
        pipe.set(file_key, content_base64, ex=ttl_seconds)
        pipe.hset(
            file_meta_key,
            mapping={
                "sender_id": participant_id,
                "sender_name": participant["display_name"],
                "filename": filename,
                "size_bytes": size_bytes,
                "created_at": now,
            },
        )
        pipe.expire(file_meta_key, ttl_seconds)
        pipe.xadd(
            stream_key,
            {
                "type": "file",
                "file_id": file_id,
                "filename": filename,
                "size_bytes": size_bytes,
                # sender_id/created_at são aditivos (antes só sender_name):
                # o turno derivado (CAP-3) precisa saber QUEM mandou o
                # arquivo por id, não por display_name (que pode repetir).
                "sender_id": participant_id,
                "sender_name": participant["display_name"],
                "created_at": now,
                **protocol_fields,
            },
            maxlen=1000,
            approximate=True,
        )
        # results[3] é fixo: set/hset/expire/xadd são sempre os 4 primeiros
        # comandos deste pipeline, e o hsetnx do ack automático (quando
        # houver) vem DEPOIS do xadd — por isso não dá mais pra usar "o
        # último resultado" como antes. Precisamos do id do xadd (não só do
        # file_id, que é um token opaco pra file_receive) porque
        # message_status compara contra stream ids reais.
        self._queue_auto_ack(pipe, room_id, participant_id, auto_ack_target, now)
        results = await pipe.execute()
        message_id = results[3]
        metrics.messages_total.labels(kind="file").inc()
        await self._refresh_ttl(room_id, ttl_seconds)

        return {
            "file_id": file_id,
            "message_id": message_id,
            "size_bytes": size_bytes,
            "created_at": now,
            "intent": protocol_fields["intent"],
            "in_reply_to": in_reply_to,
        }

    # ------------------------------------------------------------------
    # file_receive
    # ------------------------------------------------------------------
    async def receive_file(self, room_id: str, participant_id: str, file_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)

        file_key = _room_key(room_id, f"file:{file_id}")
        file_meta_key = _room_key(room_id, f"file:{file_id}:meta")

        content_base64, file_meta = await asyncio.gather(
            self._redis.get(file_key), self._redis.hgetall(file_meta_key)
        )
        if content_base64 is None or not file_meta:
            raise FileNotFoundInRoomError(
                f"FILE_NOT_FOUND: file_id '{file_id}' não existe nessa room ou expirou"
            )

        return {
            "filename": file_meta["filename"],
            "sender_name": file_meta["sender_name"],
            "size_bytes": int(file_meta["size_bytes"]),
            "created_at": float(file_meta["created_at"]),
            "content_base64": content_base64,
        }

    # ------------------------------------------------------------------
    # session_poll / session_peek — parsing compartilhado (CAP-8, SPEC-
    # session-peek): o mesmo envelope estruturado, montado num único lugar,
    # para os dois formatos de leitura nunca divergirem.
    # ------------------------------------------------------------------
    @staticmethod
    def _format_stream_entries(entries: list[tuple[str, dict]]) -> list[dict[str, Any]]:
        """
        Formata entradas cruas da stream Redis (pares entry_id, fields, na
        ordem em que o XREAD devolveu) no mesmo envelope estruturado
        (`_envelope`) usado por `session_poll` e `session_peek` — extraído de
        `_poll_messages_body` (SPEC-session-peek) para as duas tools
        reaproveitarem exatamente a mesma lógica de parsing por tipo de
        evento, em vez de duplicá-la.
        """
        messages: list[dict[str, Any]] = []
        for entry_id, fields in entries:
            event_type = fields.get("type")
            if event_type == "message":
                messages.append(
                    _envelope(
                        entry_id,
                        origin="participant",
                        sender_name=fields.get("sender_name"),
                        created_at=float(fields.get("created_at", 0)),
                        content={"kind": "text", "text": fields.get("text")},
                        **_protocol_view(fields),
                    )
                )
            elif event_type == "system":
                # Único origin="system" (untrusted=false) — mas o "text"
                # é gerado pelo servidor, um TEMPLATE FIXO sem o nome de
                # ninguém (nem "goal") interpolado (achado da revisão da
                # story 11, fechado em 2 rodadas): tanto os eventos de
                # ciclo de vida de participante (join/leave/kick/etc)
                # quanto os de autoloop (propose/accept/decline/stop/
                # impasse) isolam o dado do participante em
                # content.actor_name/content.goal — nunca dentro de
                # content.text. Sem isso, um display_name ou goal hostil
                # viraria texto de um evento que as instructions chamam
                # de "sempre confiável". Watchdog/consenso ficam de fora
                # de propósito: não interpolam dado de participante
                # (ended_reason é do servidor; done_by é uma lista de
                # participant_id, token opaco gerado pelo servidor, não
                # texto livre escolhido por ninguém).
                system_content: dict[str, Any] = {
                    "kind": "system",
                    "event": fields.get("event"),
                    "actor_name": fields.get("actor_name"),
                    "text": fields.get("text"),
                }
                if fields.get("goal") is not None:
                    system_content["goal"] = fields.get("goal")
                messages.append(
                    _envelope(
                        entry_id,
                        origin="system",
                        sender_name="system",
                        created_at=None,
                        content=system_content,
                    )
                )
            elif event_type == "file":
                messages.append(
                    _envelope(
                        entry_id,
                        origin="participant",
                        sender_name=fields.get("sender_name"),
                        # eventos de arquivo antigos (pré-protocolo 1:1)
                        # não têm created_at — continua None pra eles.
                        created_at=(float(fields["created_at"]) if fields.get("created_at") else None),
                        content={
                            "kind": "file",
                            "filename": fields.get("filename"),
                            "size_bytes": fields.get("size_bytes"),
                            "file_id": fields.get("file_id"),
                        },
                        **_protocol_view(fields),
                    )
                )
            elif event_type == "action":
                # payload é DADO vindo de outra sessão, nunca interpretado
                # ou executado aqui — só relayado dentro de content, isolado
                # (CAP-8: o servidor não concatena isso num texto próprio).
                payload = json.loads(fields.get("payload_json", "{}"))
                messages.append(
                    _envelope(
                        entry_id,
                        origin="participant",
                        sender_name=fields.get("sender_name"),
                        created_at=float(fields.get("created_at", 0)),
                        content={"kind": "json", "payload": payload},
                        **_protocol_view(fields),
                    )
                )
            elif event_type == "ack":
                # CAP-1: anúncio de ack explícito (message_ack). É o que
                # permite ao remetente de um `handoff` enxergar, no
                # próprio poll, que o outro lado tratou — sem precisar
                # ficar consultando message_status.
                messages.append(
                    _envelope(
                        entry_id,
                        origin="participant",
                        sender_name=fields.get("sender_name"),
                        created_at=float(fields.get("created_at", 0)),
                        content={"kind": "ack", "ack_of": fields.get("ack_of")},
                    )
                )
            elif event_type == "autoloop_turn":
                # Mesma garantia de não-execução de "action": payload é
                # DADO vindo de outra sessão, nunca interpretado ou
                # executado aqui — só relayado dentro de content, junto do
                # campo estrutural fechado turn_status.
                payload = json.loads(fields.get("payload_json", "{}"))
                messages.append(
                    _envelope(
                        entry_id,
                        origin="participant",
                        sender_name=fields.get("sender_name"),
                        created_at=float(fields.get("created_at", 0)),
                        content={
                            "kind": "autoloop_turn",
                            "payload": payload,
                            "turn_status": fields.get("turn_status"),
                        },
                    )
                )
        return messages

    # ------------------------------------------------------------------
    # session_poll — long-poll
    # ------------------------------------------------------------------
    async def _poll_messages_impl(
        self, room_id: str, participant_id: str, timeout_seconds: int
    ) -> dict[str, Any]:
        """CAP-1 (spec-observability-metrics): mede em `poll_latency_seconds`
        o tempo total gasto aqui dentro — da entrada até QUALQUER retorno
        (inclusive o caminho `room_status="pending"` mais cedo), incluindo o
        bloqueio do XREAD abaixo. `try/finally` porque a lógica real
        (`_poll_messages_body`) tem mais de um ponto de retorno."""
        start = time.perf_counter()
        try:
            return await self._poll_messages_body(
                room_id=room_id, participant_id=participant_id, timeout_seconds=timeout_seconds
            )
        finally:
            metrics.poll_latency_seconds.observe(time.perf_counter() - start)

    async def _poll_messages_body(
        self, room_id: str, participant_id: str, timeout_seconds: int
    ) -> dict[str, Any]:
        # CAP-4 (story 10): pending não está em :participants (só entra lá
        # via session_approve) — _require_participant abaixo recusaria com
        # PARTICIPANT_NOT_FOUND, mas o pending precisa poder pollar pra
        # saber que ainda está esperando aprovação, sem ver nenhuma
        # mensagem da room.
        if await self._redis.hexists(_pending_key(room_id), participant_id):
            return {"messages": [], "room_status": "pending"}

        participant = await self._require_participant(room_id, participant_id)

        timeout_seconds = min(max(timeout_seconds, 1), config.MAX_POLL_TIMEOUT_SECONDS)
        cursor_key = _room_key(room_id, f"cursor:{participant_id}")
        cursor = await self._redis.get(cursor_key) or "0"

        # Grava no INÍCIO do poll (antes do xread bloquear) — "comecei a
        # escutar agora", não "recebi algo agora". Não é I/O novo: já
        # escrevemos no hash de participants; só um campo a mais aqui, e o
        # cursor/TTL abaixo cobre o resto da room como já fazia.
        participant["last_polled_at"] = _now()
        await self._redis.hset(
            _room_key(room_id, "participants"),
            participant_id,
            json.dumps(participant),
        )

        stream_key = _room_key(room_id, "stream")
        result = await self._redis.xread({stream_key: cursor}, block=timeout_seconds * 1000, count=200)

        messages: list[dict[str, Any]] = []
        new_cursor = cursor
        if result:
            _, entries = result[0]
            entries = list(entries)
            # SPEC-poll-settle-window (CAP-1): a primeira leitura já trouxe
            # >=1 entrada nova — tenta uma segunda leitura curta e NÃO-
            # bloqueante (ou quase) a partir do que já foi lido, pra pegar
            # fast-followers (eventos publicados a poucas dezenas de ms de
            # distância) na MESMA resposta, sem round-trip extra de
            # session_poll. Só roda aqui dentro do `if result:` — um poll
            # que não recebeu nada no timeout normal NUNCA espera essa
            # janela adicional (isso violaria a constraint de latência do
            # caso "nada chegou"). count cap total continua 200 (constraint
            # do SPEC: settle só preenche o lote mais cedo, não aumenta o
            # teto).
            settle_ms = config.SESSION_POLL_SETTLE_MS
            remaining = 200 - len(entries)
            if settle_ms > 0 and remaining > 0:
                last_entry_id = entries[-1][0]
                settle_result = await self._redis.xread(
                    {stream_key: last_entry_id}, block=settle_ms, count=remaining
                )
                if settle_result:
                    _, extra_entries = settle_result[0]
                    entries.extend(extra_entries)
            if entries:
                new_cursor = entries[-1][0]
            messages = self._format_stream_entries(entries)

        meta = await self._get_meta(room_id)
        room_status = "open" if meta else "expired"

        if meta:
            ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))
            pipe = self._redis.pipeline()
            pipe.set(cursor_key, new_cursor)
            pipe.expire(cursor_key, ttl_seconds)
            await pipe.execute()
            if messages:
                await self._refresh_ttl(room_id, ttl_seconds, extra_keys=[cursor_key])

        return {"messages": messages, "room_status": room_status}

    async def poll_messages(
        self, room_id: str, participant_id: str, timeout_seconds: int
    ) -> dict[str, Any]:
        """
        SPEC-redis-resilience CAP-1: este é o `XREAD BLOCK` que pode ficar
        parado até MAX_POLL_TIMEOUT_SECONDS (120s) — o caso mais crítico pra
        um blip transitório de conexão. O cliente Redis (retry/backoff
        configurado em __init__) já reconecta sozinho nesse caso; isto aqui
        cobre só o residual — Redis ainda inacessível depois do retry se
        esgotar — trocando a exceção crua da lib `redis` por um erro de
        domínio que quem chamou a tool consegue interpretar.
        """
        try:
            return await self._poll_messages_impl(room_id, participant_id, timeout_seconds)
        except RedisError as exc:
            raise StorageUnavailableError(
                "STORAGE_UNAVAILABLE: Redis inacessível — tente novamente"
            ) from exc

    # ------------------------------------------------------------------
    # session_peek — leitura de novidade sem consumir o cursor (SPEC-
    # session-peek)
    # ------------------------------------------------------------------
    async def peek_messages(self, room_id: str, participant_id: str) -> dict[str, Any]:
        """
        CAP-1 (SPEC-session-peek): devolve o que há de novo desde
        `cursor:{pid}` — SEM avançar esse cursor, sem tocar
        `last_polled_at`/`is_listening`, sem reaplicar TTL (`_refresh_ttl`).
        Puramente read-only sobre estado que `poll_messages` já mantém: um
        listener em background com o MESMO participant_id (logo o MESMO
        cursor) da sessão principal pode ter um XREAD BLOCK pendente nesse
        exato cursor — `session_peek` nunca compete por essa mensagem porque
        nunca escreve nada.

        XREAD aqui é SEM `block` (não-bloqueante: volta na hora, vazio se não
        houver nada novo) e com o mesmo `count=200` de `poll_messages` —
        mesmo teto de itens por resposta, sem paginação própria (Constraints
        do SPEC). Reaproveita `_format_stream_entries` — o mesmo envelope
        estruturado (origin/content/intent/in_reply_to) de `session_poll`,
        pra nunca divergir entre os dois formatos de leitura.
        """
        if await self._redis.hexists(_pending_key(room_id), participant_id):
            return {"messages": [], "room_status": "pending"}

        await self._require_participant(room_id, participant_id)

        cursor_key = _room_key(room_id, f"cursor:{participant_id}")
        cursor = await self._redis.get(cursor_key) or "0"

        stream_key = _room_key(room_id, "stream")
        result = await self._redis.xread({stream_key: cursor}, count=200)

        messages: list[dict[str, Any]] = []
        if result:
            _, entries = result[0]
            messages = self._format_stream_entries(list(entries))

        meta = await self._get_meta(room_id)
        room_status = "open" if meta else "expired"

        return {"messages": messages, "room_status": room_status}

    # ------------------------------------------------------------------
    # message_status — confirmação de entrega (CAP-1 read receipts)
    # ------------------------------------------------------------------
    async def message_status(self, room_id: str, participant_id: str, message_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        # valida o formato cedo (fail-fast com erro claro) — não valida se o
        # id realmente existiu na stream, por design (ver companion doc).
        _parse_stream_id(message_id)

        participants_key = _room_key(room_id, "participants")
        raw_participants, original = await asyncio.gather(
            self._redis.hgetall(participants_key),
            self._read_real_message(room_id, message_id),
        )
        # Se a mensagem não está (mais) na stream, intent fica None e o único
        # caminho pra "tratado" é o ack explícito — sem intent conhecida não
        # dá pra aplicar o atalho de fyi/conclusao nem o auto-tratado.
        intent = (original.get("intent") or DEFAULT_INTENT) if original is not None else None

        delivered_to: dict[str, bool] = {}
        state_by: dict[str, str] = {}
        for pid, raw in raw_participants.items():
            if pid == participant_id:
                continue  # nunca compara contra o cursor do próprio remetente
            display_name = json.loads(raw)["display_name"]
            cursor, ack_raw = await asyncio.gather(
                self._redis.get(_room_key(room_id, f"cursor:{pid}")),
                self._redis.hget(_acks_key(room_id), _ack_field(message_id, pid)),
            )
            delivered = _stream_id_gte(cursor or "0", message_id)
            delivered_to[display_name] = delivered
            # CAP-1, ordem "só avança": tratado > entregue > pendente. Um ack
            # (explícito ou via reply) vence mesmo que o cursor ainda não
            # tenha passado — quem tratou obviamente viu. fyi/conclusao não
            # exigem ação, então entregue já é tratado (companion doc).
            if ack_raw is not None:
                state_by[display_name] = ACK_STATE_HANDLED
            elif delivered and intent in ("fyi", "conclusao"):
                state_by[display_name] = ACK_STATE_HANDLED
            elif delivered:
                state_by[display_name] = ACK_STATE_DELIVERED
            else:
                state_by[display_name] = ACK_STATE_PENDING

        return {
            "message_id": message_id,
            "intent": intent,
            "delivered_to": delivered_to,
            "state_by": state_by,
        }

    # ------------------------------------------------------------------
    # message_ack — ack explícito de "tratado" (CAP-1)
    # ------------------------------------------------------------------
    async def ack_message(self, room_id: str, participant_id: str, message_id: str) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        if meta is None:
            raise RoomNotFoundError(f"ROOM_NOT_FOUND: room '{room_id}' não existe ou expirou")
        ttl_seconds = int(meta.get("ttl_seconds", config.DEFAULT_TTL_SECONDS))

        # Diferente de message_status (que aceita id inexistente por design),
        # aqui a mensagem PRECISA estar na stream: sem ela não dá pra saber
        # quem é o remetente (regra "não acka a própria mensagem") — e um ack
        # de algo que ninguém enviou seria estado fantasma.
        original = await self._read_real_message(room_id, message_id)
        if original is None:
            raise InvalidAckTargetError(
                f"INVALID_ACK_TARGET: message_id '{message_id}' não aponta para uma mensagem real "
                "(texto/JSON/arquivo) existente nessa room"
            )
        if original.get("sender_id") == participant_id:
            raise CannotAckOwnMessageError(
                "CANNOT_ACK_OWN_MESSAGE: message_ack marca a mensagem de OUTRO participante como "
                "tratada por você — a sua própria mensagem não tem o que ser tratada por você mesmo"
            )

        now = _now()
        # HSETNX é o que dá idempotência sem WATCH: dois message_ack
        # concorrentes do mesmo participante pra mesma mensagem — só um
        # grava (e só esse emite o evento type=ack). Um ack automático
        # anterior (via reply) também deixa este como "já tratado".
        created = await self._redis.hsetnx(
            _acks_key(room_id),
            _ack_field(message_id, participant_id),
            json.dumps({"at": now, "via": "explicit"}),
        )
        ack_event_id: Optional[str] = None
        if created:
            ack_event_id = await self._redis.xadd(
                _room_key(room_id, "stream"),
                {
                    "type": "ack",
                    "sender_id": participant_id,
                    "sender_name": participant["display_name"],
                    "ack_of": message_id,
                    "created_at": now,
                },
                maxlen=1000,
                approximate=True,
            )
        await self._refresh_ttl(room_id, ttl_seconds)
        return {
            "message_id": message_id,
            "state": ACK_STATE_HANDLED,
            "already_acked": not bool(created),
            "ack_event_id": ack_event_id,
        }

    # ------------------------------------------------------------------
    # session_close
    # ------------------------------------------------------------------
    async def close_room(self, room_id: str, participant_id: str) -> dict[str, Any]:
        participant = await self._require_participant(room_id, participant_id)
        participants_key = _room_key(room_id, "participants")
        hash_index_key = _hash_index_key(room_id)

        # Índice reverso (SPEC-participant-hash-index): sai de :participants
        # e do índice junto, no mesmo pipeline — nunca um passo separado que
        # possa deixar o índice apontando pra alguém que já saiu.
        pipe = self._redis.pipeline()
        pipe.hdel(participants_key, participant_id)
        pipe.hdel(hash_index_key, participant_hash(participant_id))
        await pipe.execute()
        remaining = await self._redis.hlen(participants_key)

        if remaining == 0:
            keys = [
                _room_key(room_id, "meta"),
                _room_key(room_id, "participants"),
                _room_key(room_id, "stream"),
                _acks_key(room_id),
                hash_index_key,
                *_autoloop_extra_keys(room_id),
            ]
            cursor_keys = [k async for k in self._redis.scan_iter(match=_room_key(room_id, "cursor:*"))]
            file_keys = [k async for k in self._redis.scan_iter(match=_room_key(room_id, "file:*"))]
            await self._redis.delete(*keys, *cursor_keys, *file_keys)
            return {"room_status": "closed"}

        # Quem sai da room não pode continuar contando como loop_participant
        # do autoloop — sem isso, autoloop_status/ativação de loop segue
        # enxergando um participante que já foi embora da room.
        await self._redis.srem(_autoloop_participants_key(room_id), participant_id)
        # Mesmo motivo, pro Set de done_by (Story 3/CAP-4): se quem saiu já
        # tinha declarado done antes de sair, deixá-lo em done_by sem estar
        # mais em loop_participants quebraria pra sempre a checagem de
        # consenso (new_done_by == loop_participants_now nunca mais bateria,
        # já que done_by teria um id a mais que loop_participants nunca vai
        # ter de volta) — ver autoloop_turn.
        await self._redis.srem(_autoloop_done_key(room_id), participant_id)

        await self._redis.xadd(
            _room_key(room_id, "stream"),
            {
                "type": "system",
                "event": "left",
                "actor_name": participant["display_name"],
                "text": "Um participante saiu da room",
            },
            maxlen=1000,
            approximate=True,
        )
        return {"room_status": "open", "you_left": True, "remaining_participants": remaining}

    # ------------------------------------------------------------------
    # session_status
    # ------------------------------------------------------------------
    async def status(self, room_id: str, participant_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)
        meta = await self._get_meta(room_id)
        participants_key = _room_key(room_id, "participants")
        raw_participants = await self._redis.hgetall(participants_key)
        ttl_remaining = await self._redis.ttl(_room_key(room_id, "meta"))
        # -1 (sem expiração) e -2 (chave ausente) são sentinelas do Redis, não
        # segundos reais — normaliza pra None em vez de vazar isso pro client.
        ttl_remaining = ttl_remaining if ttl_remaining is not None and ttl_remaining >= 0 else None

        now = _now()
        participants = [
            {
                "display_name": p["display_name"],
                "joined_at": p["joined_at"],
                "last_polled_at": p.get("last_polled_at"),
                "is_listening": (
                    p.get("last_polled_at") is not None
                    and (now - p["last_polled_at"]) < config.PRESENCE_THRESHOLD_SECONDS
                ),
                # CAP-4 (story 10): o hash (nunca o participant_id) — é o que
                # o criador passa pra session_kick pra remover um participante
                # ATIVO (pending[] abaixo já carrega o dele pra session_approve).
                "participant_hash": participant_hash(pid),
            }
            for pid, p in ((pid, json.loads(v)) for pid, v in raw_participants.items())
        ]
        turn = await self._derive_turn(room_id, participant_id, raw_participants)
        result: dict[str, Any] = {
            "status": meta.get("status", "open") if meta else "expired",
            "participant_count": len(participants),
            "max_participants": config.MAX_PARTICIPANTS,
            "ttl_remaining_seconds": ttl_remaining,
            "participants": participants,
            "turn": turn,
        }

        # CAP-4 (story 10): pending[] só aparece pro criador — pra qualquer
        # outro participante, a chave nem existe na resposta (não é lista
        # vazia: é invisível, como o Boundaries pede).
        if meta and meta.get("created_by") == participant_id:
            raw_pending = await self._redis.hgetall(_pending_key(room_id))
            result["pending"] = [
                {
                    "display_name": (data := json.loads(raw))["display_name"],
                    "requested_at": data["requested_at"],
                    "participant_hash": participant_hash(pid),
                }
                for pid, raw in raw_pending.items()
            ]

        return result

    async def _derive_turn(
        self, room_id: str, caller_id: str, raw_participants: dict[str, str]
    ) -> dict[str, Any]:
        """
        Turno derivado numa room 1:1 (CAP-3). Campo puramente calculado — não
        há estado novo persistido: é sempre função de (participantes atuais,
        estado do autoloop, última mensagem real da stream e sua intent).

        reason (enum fechado):
          room_not_1to1        participantes != 2 (broadcast puro, não se aplica)
          autoloop_active      loop autônomo status=active tem precedência
          no_messages_yet      nenhuma mensagem real ainda — vez de quem entrou
                               primeiro (o criador da room, normalmente)
          last_sender_left     a última mensagem real é de alguém que já saiu:
                               os 2 atuais são ambos "quem recebeu" — indefinido
          derived_from_last_message  regra do companion doc aplicada
        """
        def _unresolved(reason: str) -> dict[str, Any]:
            return {
                "applies": False,
                "next_to_act": None,
                "next_to_act_is_you": None,
                "reason": reason,
                "last_message_id": None,
                "last_intent": None,
            }

        if len(raw_participants) != 2:
            return _unresolved("room_not_1to1")
        autoloop_status = await self._redis.hget(_autoloop_key(room_id), "status")
        if autoloop_status == "active":
            return _unresolved("autoloop_active")

        participants = {pid: json.loads(raw) for pid, raw in raw_participants.items()}

        def _resolved(next_pid: str, reason: str, last_id: Optional[str], last_intent: Optional[str]):
            return {
                "applies": True,
                "next_to_act": participants[next_pid]["display_name"],
                "next_to_act_is_you": next_pid == caller_id,
                "reason": reason,
                "last_message_id": last_id,
                "last_intent": last_intent,
            }

        last = await self._last_real_message(room_id)
        if last is None:
            first_pid = min(participants, key=lambda pid: participants[pid]["joined_at"])
            return _resolved(first_pid, "no_messages_yet", None, None)

        last_id, fields = last
        sender_id = fields.get("sender_id")
        if sender_id is None:
            # evento de arquivo antigo (pré-protocolo 1:1) sem sender_id:
            # resolve por display_name, único caminho disponível.
            matches = [pid for pid, p in participants.items() if p["display_name"] == fields.get("sender_name")]
            sender_id = matches[0] if len(matches) == 1 else None
        if sender_id not in participants:
            result = _unresolved("last_sender_left")
            result["last_message_id"] = last_id
            result["last_intent"] = fields.get("intent") or DEFAULT_INTENT
            return result

        intent = fields.get("intent") or DEFAULT_INTENT
        other_pid = next(pid for pid in participants if pid != sender_id)
        # pergunta/handoff passam o turno pra quem recebeu; fyi/conclusao não
        # passam — quem mandou continua podendo agir (companion doc, CAP-3).
        next_pid = other_pid if intent in TURN_PASSING_INTENTS else sender_id
        return _resolved(next_pid, "derived_from_last_message", last_id, intent)

    async def _last_real_message(self, room_id: str) -> Optional[tuple[str, dict]]:
        """
        Última entrada da stream com type em REAL_MESSAGE_TYPES, varrendo de
        trás pra frente em lotes (XREVRANGE) — system/ack/autoloop_* no fim
        da stream não contam pra turno. A stream tem maxlen ~1000, então o
        pior caso é limitado.
        """
        stream_key = _room_key(room_id, "stream")
        upper = "+"
        while True:
            entries = await self._redis.xrevrange(stream_key, max=upper, min="-", count=100)
            if not entries:
                return None
            for entry_id, fields in entries:
                if fields.get("type") in REAL_MESSAGE_TYPES:
                    return entry_id, fields
            last_seen_id = entries[-1][0]
            if len(entries) < 100:
                return None
            # exclusivo: "(<id>" é a sintaxe do Redis pra range aberto
            upper = f"({last_seen_id}"

    # ------------------------------------------------------------------
    # session_export — transcript completo e ordenado da room, em markdown
    # ------------------------------------------------------------------
    async def export_transcript(self, room_id: str, participant_id: str) -> dict[str, Any]:
        await self._require_participant(room_id, participant_id)

        # Histórico completo desde o início — diferente de poll_messages,
        # não usa cursor por participante (XRANGE, não XREAD): export é
        # sempre do que sobrou na stream inteira, não incremental.
        stream_key = _room_key(room_id, "stream")
        entries = await self._redis.xrange(stream_key, min="-", max="+")

        participants_key = _room_key(room_id, "participants")
        raw_participants = await self._redis.hgetall(participants_key)
        participant_names = sorted(json.loads(v)["display_name"] for v in raw_participants.values())

        lines = [f"# Transcript — room {room_id}"]
        lines.append(
            "Participantes atuais: " + (", ".join(participant_names) if participant_names else "(nenhum)")
        )
        lines.append("")

        for entry_id, fields in entries:
            entry_type = fields.get("type")
            if entry_type == "system":
                lines.append(f"- *{_format_ts(fields.get('created_at'))}* — {fields.get('text')}")
            elif entry_type == "message":
                lines.append(
                    f"- **{fields.get('sender_name')}** ({_format_ts(fields.get('created_at'))}): "
                    f"{fields.get('text')}{_protocol_suffix(entry_id, fields)}"
                )
            elif entry_type == "file":
                lines.append(
                    f"- **{fields.get('sender_name')}** enviou arquivo: {fields.get('filename')} "
                    f"({fields.get('size_bytes')} bytes, file_id={fields.get('file_id')})"
                    f"{_protocol_suffix(entry_id, fields)}"
                )
            elif entry_type == "action":
                payload = json.loads(fields.get("payload_json", "{}"))
                lines.append(
                    f"- **{fields.get('sender_name')}** ({_format_ts(fields.get('created_at'))}) "
                    f"[action:{payload.get('action')}]: `{json.dumps(payload, ensure_ascii=False)}`"
                    f"{_protocol_suffix(entry_id, fields)}"
                )
            elif entry_type == "ack":
                lines.append(
                    f"- **{fields.get('sender_name')}** ({_format_ts(fields.get('created_at'))}) "
                    f"[ack]: tratou a mensagem {fields.get('ack_of')}"
                )
            elif entry_type == "autoloop_turn":
                payload = json.loads(fields.get("payload_json", "{}"))
                turn_status = fields.get("turn_status")
                lines.append(
                    f"- **{fields.get('sender_name')}** ({_format_ts(fields.get('created_at'))}) "
                    f"[autoloop:{turn_status}]: `{json.dumps(payload, ensure_ascii=False)}`"
                )
            # tipos desconhecidos (evolução futura da stream) são ignorados
            # silenciosamente no transcript, em vez de quebrar o export.

        transcript_markdown = "\n".join(lines) + "\n"
        # Não reaplica TTL sliding: export é leitura, mesma decisão já
        # implícita em session_status (que também não estende o TTL).
        return {"transcript_markdown": transcript_markdown, "entry_count": len(entries)}
