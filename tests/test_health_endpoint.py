"""
spec-health-endpoint (achado 02 do Radar do session-share): endpoint GET
/health usado pelas readinessProbe/livenessProbe do k8s (ver
k8s/deployment.yaml) — CAP-1 (Redis) e CAP-2 (fail-closed de auth).

`mcp.custom_route` (SDK) registra `/health` como uma rota Starlette pura,
fora do dispatch de tools autenticado — montar a app ASGI de verdade exigiria
`httpx.AsyncClient(transport=ASGITransport(...))`, mas o httpx "real" (não o
`httpx2` vendorizado que o SDK usa) não é uma dependência declarada deste
repo (ver comentário em app/auth.py e requirements.txt) e este arquivo não
pode alterar requirements-dev.txt. Alternativa aceitável (documentada no
próprio SPEC): a lógica é extraída em `app.main._health_check` (pura,
testável direto) e `app.main.health_check` (a função decorada por
`@mcp.custom_route`, mas que `custom_route` devolve inalterada — dá pra
chamar direto, sem Request de verdade, provando a integração real da rota
sem montar a app ASGI).
"""
from __future__ import annotations

import json
import time

import httpx2 as httpx
import pytest

from app import config
from app.auth import JWTVerifier
from app.redis_store import SessionStore
from app import main


@pytest.fixture
async def raw_redis_client():
    """Cliente redis.asyncio cru (não SessionStore) — só para dar bootstrap
    num JWTVerifier de teste, mesmo padrão de tests/test_auth_jwt.py."""
    import redis.asyncio as redis_asyncio

    client = redis_asyncio.from_url(config.REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


def _make_verifier(redis_client, *, fail_closed_after_seconds: int = 300) -> JWTVerifier:
    """JWTVerifier de teste — nunca chama sync()/verify_token(), só precisa
    existir para `_health_check` ler `_clock()`/`_last_synced_at`. http_client
    aponta para um MockTransport que nunca é de fato usado."""
    return JWTVerifier(
        issuer="auth-service",
        audience="session-share",
        jwks_url="https://dashboard.local/jwks",
        revocations_url="https://dashboard.local/revocations",
        revocations_token="test-token",
        cache_seconds=60,
        fail_closed_after_seconds=fail_closed_after_seconds,
        redis_client=redis_client,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404))),
    )


# --- CAP-1: Redis --------------------------------------------------------


async def test_health_check_ok_when_redis_reachable():
    store = SessionStore(redis_url=config.REDIS_URL)
    try:
        result = await main._health_check(store, None)
    finally:
        await store.close()

    assert result == {"status": "ok", "redis": True, "auth_sync": None}


async def test_health_check_unhealthy_when_redis_unreachable():
    # Porta sem nenhum Redis escutando — connection refused rápido, mas o
    # timeout curto do _health_check (constraint do SPEC) garante que isto
    # não trava mesmo se o comportamento de rede variar.
    store = SessionStore(redis_url="redis://localhost:1/0")
    try:
        result = await main._health_check(store, None)
    finally:
        await store.close()

    assert result["status"] == "unhealthy"
    assert result["redis"] is False
    assert result["auth_sync"] is None


# --- CAP-2: fail-closed de auth -------------------------------------------


async def test_health_check_unhealthy_when_auth_fail_closed(raw_redis_client):
    """Mesma condição que JWTVerifier.verify_token usa (app/auth.py):
    agora - _last_synced_at > AUTH_FAIL_CLOSED_AFTER_SECONDS -> fail-closed."""
    store = SessionStore(redis_url=config.REDIS_URL)
    verifier = _make_verifier(raw_redis_client, fail_closed_after_seconds=300)
    verifier._last_synced_at = time.time() - 301  # > 300s sem sincronizar

    try:
        result = await main._health_check(store, verifier)
    finally:
        await store.close()
        await verifier.aclose()

    assert result["status"] == "unhealthy"
    assert result["redis"] is True
    assert result["auth_sync"] is False


async def test_health_check_ok_when_auth_recently_synced(raw_redis_client):
    store = SessionStore(redis_url=config.REDIS_URL)
    verifier = _make_verifier(raw_redis_client, fail_closed_after_seconds=300)
    verifier._last_synced_at = time.time()  # acabou de sincronizar

    try:
        result = await main._health_check(store, verifier)
    finally:
        await store.close()
        await verifier.aclose()

    assert result == {"status": "ok", "redis": True, "auth_sync": True}


async def test_health_check_auth_not_checked_when_verifier_is_none():
    """AUTH_ENABLED=false (dev local) — non-goal do SPEC: /health não checa
    estado de auth, só Redis."""
    store = SessionStore(redis_url=config.REDIS_URL)
    try:
        result = await main._health_check(store, None)
    finally:
        await store.close()

    assert result["auth_sync"] is None


# --- Integração: a rota /health de fato registrada ------------------------


def test_health_route_is_registered_without_auth():
    """`/health` precisa existir como rota custom do SDK (mcp.custom_route),
    GET, fora do dispatch de tools — é isto que garante que o kubelet (sem
    credencial) consegue chamar."""
    paths_and_methods = [
        (route.path, route.methods) for route in main.mcp._custom_starlette_routes
    ]
    assert any(
        path == "/health" and methods is not None and "GET" in methods
        for path, methods in paths_and_methods
    )


async def test_health_route_handler_returns_ok_response():
    """`mcp.custom_route` devolve a função decorada inalterada — chamar
    `main.health_check` direto exercita exatamente o código que a rota HTTP
    registrada roda, sem precisar montar a app ASGI inteira. Redis está
    acessível (container efêmero do CI) e `main._auth_verifier` (quando
    AUTH_ENABLED=true), se existir, acabou de ser construído nesta mesma
    importação — bem dentro de AUTH_FAIL_CLOSED_AFTER_SECONDS — então a
    resposta esperada é sempre 200 aqui."""
    response = await main.health_check(None)  # handler não usa `request`

    assert response.status_code == 200
    body = json.loads(bytes(response.body))
    assert body["status"] == "ok"
    assert body["redis"] is True
