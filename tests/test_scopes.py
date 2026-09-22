"""
CAP-3 (story 8): allowlist por verbo no dispatch e filtro de `tools/list`.

Sobe o app.main.mcp DE PRODUÇÃO (a `_ScopedMCPServer` real, com as 18 tools
já decoradas com `require_scope`) via ASGITransport — não um mock — e
exercita com um cliente MCP de verdade (`ClientSession`), assinando JWTs
ES256 reais. Em vez de rodar o fetch de JWKS pela rede (dashboard não
existe em teste), semeia `app_main._auth_verifier._jwks_keys` direto — o
verifier em si (assinatura, iss/aud/exp/denylist) já é regression-tested em
test_auth_jwt.py; aqui o alvo é o DISPATCH (require_scope) e o filtro de
`tools/list`, não a validação do token.

Achado central da story: chamar uma tool direto (`call_tool`), IGNORANDO
`tools/list`, ainda leva `SCOPE_DENIED` — o filtro de listagem é só UX.
"""
from __future__ import annotations

import json
import time
import uuid
from contextlib import asynccontextmanager

import anyio
import httpx2 as httpx
import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver.exceptions import ToolError

from app import config
from app import main as app_main
from app.scopes import TOOL_SCOPES
from mcp.server.transport_security import TransportSecuritySettings

KID = "kid-story-8"


def _generate_keypair():
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    jwk = json.loads(ECAlgorithm(ECAlgorithm.SHA256).to_jwk(private_key.public_key()))
    jwk["kid"] = KID
    return private_pem, jwk


PRIVATE_PEM, JWK = _generate_keypair()


def _make_token(scopes: list[str]) -> str:
    now = int(time.time())
    payload = {
        "iss": config.AUTH_ISSUER,
        "aud": config.AUTH_AUDIENCE,
        "sub": "user-1",
        "iat": now,
        "exp": now + 900,
        "jti": str(uuid.uuid4()),
        "scope": scopes,
    }
    return pyjwt.encode(payload, PRIVATE_PEM, algorithm="ES256", headers={"kid": KID})


@pytest.fixture(autouse=True)
def _seed_verifier_jwks():
    """Injeta o JWK direto no verifier singleton do app (sem fetch de rede
    — o dashboard não existe em teste) e evita fail-closed.

    `_redis`/`_http` também são recriados a cada teste: são clientes
    assíncronos presos ao event loop em que nasceram (mesma classe de
    problema do session-share/other-service em outras stories) — como
    `_auth_verifier` é um singleton do módulo mas cada teste roda no seu
    próprio event loop (`asyncio_mode=auto`, function-scoped), e a lifespan
    do app dispara um `sync()` logo no startup, os clientes antigos
    (presos ao loop do teste anterior) quebrariam com "bound to a
    different event loop"."""
    import redis.asyncio as redis_asyncio
    from jwt import PyJWK

    assert config.AUTH_ENABLED, "este arquivo pressupõe AUTH_ENABLED=true (default)"
    verifier = app_main._auth_verifier
    verifier._jwks_keys = {KID: PyJWK(JWK, algorithm="ES256")}
    verifier._last_synced_at = time.time()
    verifier._redis = redis_asyncio.from_url(config.REDIS_URL, decode_responses=True)
    verifier._http = httpx.AsyncClient(timeout=3.0, follow_redirects=False)
    yield


@asynccontextmanager
async def _run_asgi_lifespan(asgi_app):
    startup_complete = anyio.Event()
    shutdown_complete = anyio.Event()
    shutdown_requested = anyio.Event()
    sent_startup = False

    async def receive():
        nonlocal sent_startup
        if not sent_startup:
            sent_startup = True
            return {"type": "lifespan.startup"}
        await shutdown_requested.wait()
        return {"type": "lifespan.shutdown"}

    async def send(message):
        if message["type"] == "lifespan.startup.complete":
            startup_complete.set()
        elif message["type"] == "lifespan.shutdown.complete":
            shutdown_complete.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(asgi_app, {"type": "lifespan"}, receive, send)
        await startup_complete.wait()
        try:
            yield
        finally:
            shutdown_requested.set()
            await shutdown_complete.wait()


def _build_app():
    return app_main.mcp.streamable_http_app(
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False)
    )


@asynccontextmanager
async def _client_session(app, token: str):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://localhost",
        headers={"Authorization": f"Bearer {token}"},
    ) as http_client:
        async with streamable_http_client("http://localhost/mcp", http_client=http_client) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session


async def test_tool_map_covers_every_registered_tool():
    """Aceite explícito da story: contagem do mapa == tools registradas."""
    registered = set(app_main.mcp._tool_manager._tools.keys())
    assert set(TOOL_SCOPES.keys()) == registered
    assert len(TOOL_SCOPES) == 23  # 22 (story 11) + session_peek (SPEC-session-peek)


async def test_registering_tool_without_scope_entry_raises_runtime_error():
    with pytest.raises(RuntimeError, match="TOOL_SCOPES"):
        @app_main._tool()
        async def _tool_sem_entrada_no_mapa(ctx=None) -> dict:  # pragma: no cover
            return {}


async def test_registering_tool_without_ctx_param_raises_runtime_error():
    """Achado da revisão (guard b): tool no mapa mas sem parâmetro ctx/Context
    — require_scope nunca teria o que checar, então nem sobe."""
    with pytest.raises(RuntimeError, match="ctx"):
        @app_main._tool()
        async def session_poll() -> dict:  # pragma: no cover — mesmo nome do mapa, sem ctx
            return {}


async def test_auth_enabled_direct_call_without_ctx_is_unauthenticated():
    """Achado da revisão (a): com AUTH_ENABLED=true, ctx=None não pode
    liberar a tool — precisa falhar fechado (UNAUTHENTICATED), nunca
    silenciosamente autorizar por faltar o parâmetro opcional."""
    assert config.AUTH_ENABLED
    with pytest.raises(ToolError, match="UNAUTHENTICATED"):
        await app_main.session_status(room_id="x", participant_id="y")


async def test_scope_denied_on_direct_call_ignoring_tools_list():
    """Achado central: token só com read chamando session_send DIRETO (sem
    passar por tools/list antes) recebe SCOPE_DENIED."""
    app = _build_app()
    token = _make_token(["session-share:read"])
    async with _run_asgi_lifespan(app):
        async with _client_session(app, token) as session:
            result = await session.call_tool("session_send", {"room_id": "x", "participant_id": "y", "text": "oi"})

    assert result.is_error is True
    assert "SCOPE_DENIED" in result.content[0].text
    assert "session_send" in result.content[0].text


async def test_tools_list_filtered_to_read_and_send_scopes():
    app = _build_app()
    token = _make_token(["session-share:read", "session-share:send"])
    async with _run_asgi_lifespan(app):
        async with _client_session(app, token) as session:
            tools = await session.list_tools()

    names = {t.name for t in tools.tools}
    expected = {name for name, verb in TOOL_SCOPES.items() if verb in ("session-share:read", "session-share:send")}
    assert names == expected
    assert len(names) == 8  # +session_peek (SPEC-session-peek, mesmo scope session-share:read)


async def test_token_without_scope_executes_nothing_and_tools_list_empty():
    app = _build_app()
    token = _make_token([])
    async with _run_asgi_lifespan(app):
        async with _client_session(app, token) as session:
            tools = await session.list_tools()
            result = await session.call_tool("session_poll", {"room_id": "x", "participant_id": "y"})

    assert tools.tools == []
    assert result.is_error is True
    assert "SCOPE_DENIED" in result.content[0].text


@pytest.mark.parametrize(
    "verb,tool_name,arguments",
    [
        ("session-share:read", "session_status", {"room_id": "x", "participant_id": "y"}),
        ("session-share:send", "message_ack", {"room_id": "x", "participant_id": "y", "message_id": "z"}),
        ("session-share:json", "autoloop_turn", {"room_id": "x", "participant_id": "y", "payload": {}, "turn_status": "proposing"}),
        ("session-share:file", "file_receive", {"room_id": "x", "participant_id": "y", "file_id": "z"}),
        ("session-share:join", "session_close", {"room_id": "x", "participant_id": "y"}),
        ("session-share:autoloop", "autoloop_stop", {"room_id": "x", "participant_id": "y"}),
        ("session-share:admin", "session_share", {"display_name": "worker-a"}),
    ],
)
async def test_matching_scope_reaches_tool_body(verb, tool_name, arguments):
    """Um por verbo (mínimo pedido pela story): token com o verbo certo
    passa do require_scope — a chamada pode falhar depois por motivo de
    domínio (room inexistente etc), mas NUNCA com SCOPE_DENIED/
    UNAUTHENTICATED."""
    app = _build_app()
    token = _make_token([verb])
    async with _run_asgi_lifespan(app):
        async with _client_session(app, token) as session:
            result = await session.call_tool(tool_name, arguments)

    if result.is_error:
        text = result.content[0].text
        assert "SCOPE_DENIED" not in text
        assert "UNAUTHENTICATED" not in text


@pytest.mark.parametrize(
    "verb,tool_name,arguments",
    [
        ("session-share:read", "session_send", {"room_id": "x", "participant_id": "y", "text": "oi"}),
        ("session-share:send", "session_poll", {"room_id": "x", "participant_id": "y"}),
        ("session-share:json", "file_send", {"room_id": "x", "participant_id": "y", "filename": "a.txt", "content_base64": "eA=="}),
        ("session-share:file", "session_send_json", {"room_id": "x", "participant_id": "y", "payload": {"action": "x"}}),
        ("session-share:join", "autoloop_propose", {"room_id": "x", "participant_id": "y", "goal": "g"}),
        ("session-share:autoloop", "session_join", {"room_id": "x", "display_name": "worker-a"}),
        ("session-share:admin", "autoloop_accept", {"room_id": "x", "participant_id": "y"}),
    ],
)
async def test_wrong_verb_scope_denied(verb, tool_name, arguments):
    """Complemento do teste acima: o MESMO verbo, numa tool de OUTRO verbo,
    é negado."""
    app = _build_app()
    token = _make_token([verb])
    async with _run_asgi_lifespan(app):
        async with _client_session(app, token) as session:
            result = await session.call_tool(tool_name, arguments)

    assert result.is_error is True
    assert "SCOPE_DENIED" in result.content[0].text
