"""
spec-observability-metrics: `GET /metrics` (formato de exposição Prometheus)
com rooms_active, messages_total{kind}, poll_latency_seconds,
scope_denied_total e policy_denied_total.

Decisão de implementação (documentada, como o SPEC pede — mesma linha de
spec-health-endpoint, em paralelo): em vez de montar a rota Starlette real
via ASGI/HTTP (o que exigiria subir app.main.mcp inteiro, JWKS etc — já
coberto por tests/test_scopes.py para o dispatch de tools), testamos a
LÓGICA DE COLETA diretamente:
  - os pontos de instrumentação (SessionStore.send_message/
    send_json_message/send_file, poll_messages, require_scope) via
    chamada direta às funções reais, sem mock;
  - a renderização do texto de exposição via `app.metrics.render(store)`,
    que é exatamente o que o handler `/metrics` de app/main.py chama.
Isso cobre CAP-1/CAP-2 (os contadores aparecem e refletem a atividade real)
sem duplicar a cobertura de dispatch/autenticação que já existe em
test_scopes.py/test_auth_jwt.py.

Os Counters/Histogram do prometheus_client são globais no processo (mesmo
REGISTRY entre testes) — cada teste lê o valor ANTES e depois, e compara a
DELTA (não um valor absoluto), pra não depender de ordem de execução nem
de estado deixado por outro teste/módulo.
"""
from __future__ import annotations

import pytest

from app import config, metrics
from app.auth import require_scope
from app.redis_store import PolicyDeniedError
from mcp.server.mcpserver.exceptions import ToolError

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")


def _sample_value(metric, sample_name: str, **labels) -> float:
    """Lê o valor exposto de UMA sample (por nome completo + labels) via a
    API pública `collect()` — evita depender de atributos privados
    (`_value`, `_sum`, `_count`) do prometheus_client, que variam entre
    Counter/Histogram e não são API estável."""
    for family in metric.collect():
        for sample in family.samples:
            if sample.name == sample_name and sample.labels == labels:
                return sample.value
    raise AssertionError(f"sample {sample_name!r} labels={labels!r} não encontrada em {metric}")


def _counter_value(counter, name: str, **labels) -> float:
    return _sample_value(counter, name, **labels)


MESSAGES_TOTAL = "session_share_messages_total"
POLICY_DENIED_TOTAL = "session_share_policy_denied_total"
SCOPE_DENIED_TOTAL = "session_share_scope_denied_total"
POLL_LATENCY_SUM = "session_share_poll_latency_seconds_sum"
POLL_LATENCY_COUNT = "session_share_poll_latency_seconds_count"


# ---------------------------------------------------------------------
# CAP-1: rooms_active / messages_total
# ---------------------------------------------------------------------


async def test_render_contains_all_five_metric_families(store):
    """Smoke test: os 5 nomes da spec aparecem no texto de exposição, mesmo
    sem nenhuma atividade prévia (valores podem ser zero, mas a família
    precisa existir — client Prometheus real depende disso pra montar o
    painel mesmo antes da 1ª atividade)."""
    text = (await metrics.render(store)).decode()
    for family in (
        "session_share_rooms_active",
        "session_share_messages_total",
        "session_share_poll_latency_seconds",
        "session_share_scope_denied_total",
        "session_share_policy_denied_total",
    ):
        assert family in text


async def test_rooms_active_reflects_real_room_count(store):
    """CAP-1: rooms_active não é um contador incremental — é recalculado
    (SCAN no Redis) a cada render. Cria N rooms de teste e confirma que o
    Gauge bate com a contagem real, não com "quantas vezes create_room foi
    chamado historicamente" (que incluiria rooms de outros testes já
    expiradas por TTL curto)."""
    before = await store.count_active_rooms()
    room_a = await store.create_room(display_name="a", ttl_seconds=120)
    room_b = await store.create_room(display_name="b", ttl_seconds=120)
    after = await store.count_active_rooms()
    assert after == before + 2

    text = (await metrics.render(store)).decode()
    assert f"session_share_rooms_active {float(after)}" in text

    # fecha as duas: a contagem cai de novo (prova que não é um contador
    # que só soma — reflete o estado real do Redis).
    await store.close_room(room_id=room_a["room_id"], participant_id=room_a["participant_id"])
    await store.close_room(room_id=room_b["room_id"], participant_id=room_b["participant_id"])
    assert await store.count_active_rooms() == before


async def test_send_message_increments_messages_total_kind_message(store):
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=True)
    before = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="message")
    await store.send_message(room_id=room["room_id"], participant_id=room["participant_id"], text="oi")
    after = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="message")
    assert after == before + 1


async def test_send_json_message_increments_messages_total_kind_json(store):
    room = await store.create_room(
        display_name="p0", ttl_seconds=120, open_join=True, policy_mode="handoff-enabled"
    )
    before = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="json")
    await store.send_json_message(
        room_id=room["room_id"], participant_id=room["participant_id"], payload={"action": "deploy"}
    )
    after = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="json")
    assert after == before + 1


async def test_send_file_increments_messages_total_kind_file(store):
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=True)
    before = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="file")
    await store.send_file(
        room_id=room["room_id"],
        participant_id=room["participant_id"],
        filename="a.txt",
        content_base64="eA==",
    )
    after = _counter_value(metrics.messages_total, MESSAGES_TOTAL, kind="file")
    assert after == before + 1


async def test_poll_messages_observes_latency(store):
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=True)
    before_sum = _sample_value(metrics.poll_latency_seconds, POLL_LATENCY_SUM)
    before_count = _sample_value(metrics.poll_latency_seconds, POLL_LATENCY_COUNT)
    result = await store.poll_messages(
        room_id=room["room_id"], participant_id=room["participant_id"], timeout_seconds=1
    )
    assert result["room_status"] == "open"
    assert _sample_value(metrics.poll_latency_seconds, POLL_LATENCY_COUNT) == before_count + 1
    assert _sample_value(metrics.poll_latency_seconds, POLL_LATENCY_SUM) >= before_sum


# ---------------------------------------------------------------------
# CAP-2: policy_denied_total / scope_denied_total
# ---------------------------------------------------------------------


async def test_send_json_in_chat_only_room_increments_policy_denied_total(store):
    """chat-only é o default de create_room — session_send_json aqui SEMPRE
    leva POLICY_DENIED (todo payload tem "action")."""
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=True)
    before = _counter_value(metrics.policy_denied_total, POLICY_DENIED_TOTAL)
    with pytest.raises(PolicyDeniedError, match="POLICY_DENIED"):
        await store.send_json_message(
            room_id=room["room_id"], participant_id=room["participant_id"], payload={"action": "deploy"}
        )
    after = _counter_value(metrics.policy_denied_total, POLICY_DENIED_TOTAL)
    assert after == before + 1


async def test_require_scope_denied_increments_scope_denied_total(monkeypatch):
    """SCOPE_DENIED de verdade: token autenticado, mas sem o verbo exigido
    por session_send — mesmo padrão de tests/test_scopes.py
    (test_wrong_verb_scope_denied), só que chamando require_scope
    diretamente (sem montar a rota ASGI/JWT completa)."""
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.auth.provider import AccessToken

    monkeypatch.setattr(config, "AUTH_ENABLED", True)

    access_token = AccessToken(token="t", client_id="user-1", scopes=["session-share:read"])
    user = AuthenticatedUser(access_token)

    class _FakeRequest:
        scope = {"user": user}

    class _FakeRequestContext:
        request = _FakeRequest()

    class _FakeCtx:
        request_context = _FakeRequestContext()

    before = _counter_value(metrics.scope_denied_total, SCOPE_DENIED_TOTAL)
    with pytest.raises(ToolError, match="SCOPE_DENIED"):
        require_scope(_FakeCtx(), "session_send")
    after = _counter_value(metrics.scope_denied_total, SCOPE_DENIED_TOTAL)
    assert after == before + 1


async def test_require_scope_unauthenticated_increments_scope_denied_total(monkeypatch):
    """UNAUTHENTICATED (ctx sem usuário autenticado nenhum) também
    incrementa scope_denied_total — decisão documentada em app/auth.py
    (require_scope): as duas recusas contam pro mesmo contador."""
    monkeypatch.setattr(config, "AUTH_ENABLED", True)

    before = _counter_value(metrics.scope_denied_total, SCOPE_DENIED_TOTAL)
    with pytest.raises(ToolError, match="UNAUTHENTICATED"):
        require_scope(None, "session_send")
    after = _counter_value(metrics.scope_denied_total, SCOPE_DENIED_TOTAL)
    assert after == before + 1


async def test_render_reflects_policy_and_scope_denials(store, monkeypatch):
    """Fim a fim (SPEC "Success signal"): força um POLICY_DENIED e um
    SCOPE_DENIED, depois confirma que o texto gerado por render() já reflete
    os dois contadores atualizados."""
    room = await store.create_room(display_name="p0", ttl_seconds=120, open_join=True)
    with pytest.raises(PolicyDeniedError):
        await store.send_json_message(
            room_id=room["room_id"], participant_id=room["participant_id"], payload={"action": "x"}
        )

    monkeypatch.setattr(config, "AUTH_ENABLED", True)
    with pytest.raises(ToolError):
        require_scope(None, "session_send")

    text = (await metrics.render(store)).decode()
    policy_line = next(
        line for line in text.splitlines() if line.startswith("session_share_policy_denied_total ")
    )
    scope_line = next(
        line for line in text.splitlines() if line.startswith("session_share_scope_denied_total ")
    )
    assert float(policy_line.split()[-1]) >= 1
    assert float(scope_line.split()[-1]) >= 1


# ---------------------------------------------------------------------
# Constraint: nenhum dado de conteúdo de room em /metrics
# ---------------------------------------------------------------------


async def test_render_never_leaks_room_content(store):
    """Constraint do SPEC: /metrics não pode expor texto de mensagem,
    display_name, room_id ou participant_id. Usa valores bem distintivos
    (impossíveis de aparecer por acaso no texto de exposição) pra provar
    que nada deles vaza — só `kind` (message/json/file) é label, e é um
    valor fechado do próprio servidor, nunca dado de participante."""
    secret_name = "NOME-SECRETO-XYZ-987"
    secret_text = "TEXTO-SECRETO-DA-MENSAGEM-123"
    room = await store.create_room(display_name=secret_name, ttl_seconds=120, open_join=True)
    await store.send_message(
        room_id=room["room_id"], participant_id=room["participant_id"], text=secret_text
    )

    text = (await metrics.render(store)).decode()
    assert secret_name not in text
    assert secret_text not in text
    assert room["room_id"] not in text
    assert room["participant_id"] not in text
