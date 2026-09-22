"""
CAP-2 (story 4): valida `app.auth.JWTVerifier` direto — sem subir o
transporte streamable-http (isso já foi provado pelo spike, story 2,
`test_auth_spike.py`). Aqui o alvo é a lógica de validação em si: JWKS,
`iss`/`aud`/`exp`+leeway, denylist e fail-closed.

Par de chaves ES256 gerado no próprio teste (nenhuma chave hardcoded no
repo). O "dashboard" (JWKS + revocations) é mockado com `httpx.MockTransport`
(aqui `httpx2`, o mesmo vendorizado pelo SDK `mcp` — ver test_auth_spike.py).
Redis é real (mesma convenção do resto da suíte, `REDIS_URL`), só o lado
HTTP é mockado.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx2 as httpx
import jwt
import pytest
import redis.asyncio as redis_asyncio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm

from app import config
from app.auth import JWTVerifier

ISSUER = "auth-service"
AUDIENCE = "session-share"
KID = "kid-1"


def _generate_keypair():
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    jwk = json.loads(ECAlgorithm(ECAlgorithm.SHA256).to_jwk(private_key.public_key()))
    return private_pem, jwk


PRIVATE_PEM, JWK = _generate_keypair()
JWK["kid"] = KID

# Segundo par (simula rotação: um kid que só aparece depois de um refresh).
PRIVATE_PEM_2, JWK_2 = _generate_keypair()
JWK_2["kid"] = "kid-2"


def _make_token(*, kid: str = KID, key: bytes = PRIVATE_PEM, **claim_overrides) -> tuple[str, str]:
    now = int(time.time())
    jti = str(uuid.uuid4())
    payload = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "user-1",
        "iat": now,
        "exp": now + 900,
        "jti": jti,
        "scope": ["session-share:read", "session-share:send"],
    }
    payload.update(claim_overrides)
    token = jwt.encode(payload, key, algorithm="ES256", headers={"kid": kid})
    return token, payload["jti"]


class _MockDashboard:
    """Serve JWKS e revocations via httpx.MockTransport; `jwks_keys` e
    `revocations` são mutáveis entre chamadas (simula rotação/novas
    revogações aparecendo). `calls` conta requisições por path, para os
    testes provarem que um refresh de fato aconteceu."""

    def __init__(self, jwks_keys: list[dict] | None = None):
        self.jwks_keys = jwks_keys if jwks_keys is not None else [JWK]
        self.revocations: list[dict] = []
        self.calls: dict[str, int] = {"jwks": 0, "revocations": 0}
        self.fail = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.fail:
            raise httpx.ConnectError("dashboard indisponível (simulado)", request=request)
        if request.url.path.endswith("/jwks"):
            self.calls["jwks"] += 1
            return httpx.Response(200, json={"keys": self.jwks_keys})
        if request.url.path.endswith("/revocations"):
            self.calls["revocations"] += 1
            # Contrato real (arquitetura.md / story 7 do dashboard):
            # {"revoked": [...], "now": <epoch do servidor>} — não uma lista
            # pura (isso só é aceito como fallback de compat no verifier).
            return httpx.Response(200, json={"revoked": self.revocations, "now": time.time()})
        return httpx.Response(404)

    def http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


@pytest.fixture
async def redis_client():
    client = redis_asyncio.from_url(config.REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


def _make_verifier(dashboard: _MockDashboard, redis_client, *, clock=time.time, fail_closed_after_seconds=300):
    return JWTVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://dashboard.local/jwks",
        revocations_url="https://dashboard.local/revocations",
        revocations_token="internal-test-token",
        cache_seconds=60,
        fail_closed_after_seconds=fail_closed_after_seconds,
        redis_client=redis_client,
        http_client=dashboard.http_client(),
        clock=clock,
    )


async def test_valid_token_is_authorized(redis_client):
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    token, _jti = _make_token()
    access_token = await verifier.verify_token(token)

    assert access_token is not None
    assert access_token.subject == "user-1"
    assert access_token.scopes == ["session-share:read", "session-share:send"]


async def test_wrong_audience_rejected(redis_client):
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    token, _jti = _make_token(aud="other-service")
    assert await verifier.verify_token(token) is None


async def test_expired_beyond_leeway_rejected(redis_client):
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    now = int(time.time())
    token, _jti = _make_token(iat=now - 1000, exp=now - 31)  # 31s > leeway de 30s
    assert await verifier.verify_token(token) is None


async def test_expired_within_leeway_still_accepted(redis_client):
    """`exp` vencido há <= 30s ainda passa (leeway) — só "> 30s" é recusado,
    per seguranca.md."""
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    now = int(time.time())
    token, _jti = _make_token(iat=now - 1000, exp=now - 10)
    assert await verifier.verify_token(token) is not None


async def test_revoked_token_rejected_before_exp(redis_client):
    """jti na denylist é recusado mesmo com exp no futuro (token, em si,
    seria válido)."""
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    token, jti = _make_token()
    assert await verifier.verify_token(token) is not None  # ainda não revogado

    key = f"{config.KEY_PREFIX}:revoked:{jti}"
    await redis_client.set(key, "1", ex=60)
    try:
        assert await verifier.verify_token(token) is None
    finally:
        await redis_client.delete(key)


async def test_unknown_kid_triggers_refresh_and_is_rejected_if_still_unknown(redis_client):
    dashboard = _MockDashboard(jwks_keys=[JWK])  # kid-2 nunca aparece
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()
    assert dashboard.calls["jwks"] == 1

    token, _jti = _make_token(kid="kid-2", key=PRIVATE_PEM_2)
    assert await verifier.verify_token(token) is None
    # provou que um refresh de fato aconteceu (não só recusou de cara)
    assert dashboard.calls["jwks"] == 2


async def test_unknown_kid_triggers_refresh_and_is_accepted_after_rotation(redis_client):
    """Simula rotação: kid-2 só existe no JWKS depois do refresh forçado."""
    dashboard = _MockDashboard(jwks_keys=[JWK])
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    dashboard.jwks_keys = [JWK, JWK_2]  # dashboard "publica" a chave nova
    token, _jti = _make_token(kid="kid-2", key=PRIVATE_PEM_2)
    access_token = await verifier.verify_token(token)

    assert access_token is not None
    assert dashboard.calls["jwks"] == 2


async def test_unknown_kid_forced_refresh_is_rate_limited(redis_client):
    """Achado da revisão: kid desconhecido não pode disparar 1 chamada HTTP
    por requisição — duas em sequência (mesmo instante) causam só 1 refresh
    forçado extra; a 2ª cai no fail normal (kid segue desconhecido) sem
    bater no dashboard de novo."""
    dashboard = _MockDashboard(jwks_keys=[JWK])  # kid-2 nunca aparece
    clock_value = {"now": time.time()}
    verifier = _make_verifier(dashboard, redis_client, clock=lambda: clock_value["now"])
    await verifier.sync()
    assert dashboard.calls["jwks"] == 1

    token_a, _ = _make_token(kid="kid-2", key=PRIVATE_PEM_2)
    token_b, _ = _make_token(kid="kid-2", key=PRIVATE_PEM_2)

    assert await verifier.verify_token(token_a) is None
    assert dashboard.calls["jwks"] == 2  # 1º kid desconhecido -> refresh forçado

    assert await verifier.verify_token(token_b) is None
    assert dashboard.calls["jwks"] == 2  # 2º, mesmo instante -> throttled

    clock_value["now"] += config.AUTH_FORCED_REFRESH_MIN_SECONDS + 1
    assert await verifier.verify_token(token_b) is None
    assert dashboard.calls["jwks"] == 3  # throttle expirou -> refresh de novo


async def test_revocations_cursor_uses_server_now_not_local_clock(redis_client):
    """Achado da revisão: o cursor `since` do próximo sync precisa ser o
    `now` do SERVIDOR (corpo da resposta), não o relógio local do processo —
    um cursor local perderia revogações emitidas durante a própria
    requisição HTTP."""
    dashboard = _MockDashboard()
    local_clock = {"now": time.time() - 500}  # bem atrás do "servidor"
    verifier = _make_verifier(dashboard, redis_client, clock=lambda: local_clock["now"])

    await verifier.sync()

    assert verifier._revocations_since != local_clock["now"]
    assert verifier._revocations_since == pytest.approx(time.time(), abs=5)


async def test_dashboard_down_under_fail_closed_threshold_uses_cache(redis_client):
    dashboard = _MockDashboard()
    clock_value = {"now": time.time()}
    verifier = _make_verifier(dashboard, redis_client, clock=lambda: clock_value["now"], fail_closed_after_seconds=300)
    await verifier.sync()

    dashboard.fail = True
    clock_value["now"] += 200  # < 300s desde o último sync bem-sucedido

    token, _jti = _make_token()
    assert await verifier.verify_token(token) is not None  # cache local ainda vale


async def test_dashboard_down_over_fail_closed_threshold_rejects_everything(redis_client):
    dashboard = _MockDashboard()
    clock_value = {"now": time.time()}
    verifier = _make_verifier(dashboard, redis_client, clock=lambda: clock_value["now"], fail_closed_after_seconds=300)
    await verifier.sync()

    dashboard.fail = True
    clock_value["now"] += 301  # > 300s sem sincronizar

    token, _jti = _make_token()  # token em si seria válido offline
    assert await verifier.verify_token(token) is None


async def test_never_synced_fails_closed_after_startup_grace_period(redis_client):
    """Sem nenhum sync bem-sucedido (dashboard indisponível desde o boot),
    o relógio de fail-closed conta a partir da construção do verifier."""
    dashboard = _MockDashboard()
    dashboard.fail = True
    clock_value = {"now": time.time()}
    verifier = _make_verifier(dashboard, redis_client, clock=lambda: clock_value["now"], fail_closed_after_seconds=300)

    clock_value["now"] += 301
    token, _jti = _make_token()
    assert await verifier.verify_token(token) is None


async def test_no_raw_token_in_logs(redis_client, caplog):
    """Vários cenários de recusa — nenhuma mensagem de log deve conter o
    JWT bruto (só kid/jti/tipo de exceção)."""
    dashboard = _MockDashboard()
    verifier = _make_verifier(dashboard, redis_client)
    await verifier.sync()

    tokens = [
        _make_token(aud="other-service")[0],
        _make_token(kid="kid-2", key=PRIVATE_PEM_2)[0],
    ]
    now = int(time.time())
    tokens.append(_make_token(iat=now - 1000, exp=now - 100)[0])

    with caplog.at_level("WARNING"):
        for token in tokens:
            await verifier.verify_token(token)

    all_log_text = "\n".join(record.getMessage() for record in caplog.records)
    for token in tokens:
        assert token not in all_log_text


async def test_auth_enabled_false_disables_auth_entirely():
    """Subprocess isolado (evita reimportar app.main no processo do pytest,
    que já o importou uma vez com AUTH_ENABLED=true via outros módulos de
    teste): com AUTH_ENABLED=false, o MCPServer sobe sem auth/token_verifier
    e sem a tool de spike, com aviso no log."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from app import main;"
                "assert main._auth_kwargs == {}, main._auth_kwargs;"
                "print('tools=' + str(len(main.mcp._tool_manager._tools)));"
                "print('has_auth_probe=' + str('_auth_probe' in main.mcp._tool_manager._tools))"
            ),
        ],
        cwd=str(Path(__file__).resolve().parent.parent),
        env={**os.environ, "AUTH_ENABLED": "false"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "tools=23" in result.stdout  # 22 (story 11) + session_peek (SPEC-session-peek)
    assert "has_auth_probe=False" in result.stdout
    assert "AUTH_ENABLED=false" in result.stderr  # warning logado
