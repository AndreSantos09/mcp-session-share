"""
Métricas Prometheus básicas do session-share (spec-observability-metrics).

Cinco objetos `prometheus_client`, todos SEM label variável por room/
participante — constraint da spec: `/metrics` nunca pode expor texto de
mensagem, `display_name`, `room_id` ou `participant_id` (mesmo espírito de
`app/auth.py` nunca logar claim de JWT). O único label usado é `kind` em
`messages_total` (message|json|file), um valor fechado do próprio servidor.

`rooms_active` é um Gauge recalculado sob demanda (não um contador
incremental): rooms expiram via TTL do Redis, sem callback de expiração —
um contador que só soma joins/creates e nunca desconta dessincronizaria do
estado real assim que a primeira room expirasse. `count_active_rooms`
(SessionStore) faz um SCAN (nunca KEYS, que bloqueia o Redis) contando
`{KEY_PREFIX}:room:*:meta`; o handler de `/metrics` seta o Gauge com esse
valor a cada scrape, antes de gerar o texto de exposição.
"""
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

rooms_active = Gauge(
    "session_share_rooms_active",
    "Rooms atualmente ativas (chave de meta presente no Redis).",
    registry=REGISTRY,
)

messages_total = Counter(
    "session_share_messages_total",
    "Mensagens persistidas com sucesso na room, por tipo.",
    ["kind"],
    registry=REGISTRY,
)

poll_latency_seconds = Histogram(
    "session_share_poll_latency_seconds",
    "Tempo gasto dentro de session_poll (inclui o bloqueio do XREAD).",
    registry=REGISTRY,
)

scope_denied_total = Counter(
    "session_share_scope_denied_total",
    "Chamadas recusadas por falta de escopo (SCOPE_DENIED/UNAUTHENTICATED) em require_scope.",
    registry=REGISTRY,
)

policy_denied_total = Counter(
    "session_share_policy_denied_total",
    "Chamadas recusadas por política de room (POLICY_DENIED).",
    registry=REGISTRY,
)


async def render(store) -> bytes:
    """Texto de exposição Prometheus para o handler de `/metrics`.

    `store`: app.redis_store.SessionStore — usado só para recalcular
    rooms_active (via SCAN) antes de gerar a resposta; os outros
    contadores/histogram já vivem atualizados nos pontos de instrumentação
    (send_message/send_json_message/send_file, poll_messages, require_scope,
    PolicyDeniedError).
    """
    rooms_active.set(await store.count_active_rooms())
    return generate_latest(REGISTRY)


__all__ = [
    "CONTENT_TYPE_LATEST",
    "render",
    "rooms_active",
    "messages_total",
    "poll_latency_seconds",
    "scope_denied_total",
    "policy_denied_total",
]
