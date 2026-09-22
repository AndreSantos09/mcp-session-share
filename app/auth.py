"""
CAP-2 (story 4): validação real de token para o session-share.

Substitui o `StaticTokenVerifier` do spike (story 2, CAP-3) por
`JWTVerifier`: JWT ES256 assinado pelo dashboard, chave pública de um JWKS
cacheado (`AUTH_JWKS_URL`), `iss`/`aud`/`exp` (com leeway) e denylist de
`jti` no Redis local (`mcpshare:revoked:<jti>`), sincronizada de
`AUTH_REVOCATIONS_URL`. Fail-closed: sem sincronizar (JWKS + revogações) há
mais de `AUTH_FAIL_CLOSED_AFTER_SECONDS`, todo token é recusado.

Formato do token (seguranca.md): `alg=ES256`, `kid` p/ rotação, `iss`,
`aud`=nome do servidor, `exp=iat+900`, `jti` uuid, `sub`=user_id, `scope[]`.

`require_scope` (CAP-3, story 8) é agora a primeira linha de cada uma das
18 tools em `app/main.py`, com o mapa tool->verbo de `app/scopes.py`.
Nenhum token aparece em log: só `kid`/`jti`/`sub` (nunca o JWT bruto) e só o
nome da exceção, nunca a mensagem completa da lib (pode ecoar claims).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx2 as httpx
import jwt
import redis.asyncio as redis
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError

from app import config, metrics
from app.scopes import TOOL_SCOPES

logger = logging.getLogger(__name__)

_REQUIRED_CLAIMS = ["exp", "iat", "jti", "sub"]
_HTTP_TIMEOUT_SECONDS = 3.0


class JWTVerifier:
    """`TokenVerifier` real do CAP-2. Instância de longa duração: mantém o
    JWKS cacheado e o cursor de revogações em memória; a denylist em si mora
    no Redis (`mcpshare:revoked:<jti>`, TTL até `exp`) — outros processos
    (ou um `other-service` futuro) veem a mesma denylist.

    `clock` é injetável (callable -> epoch float) para os testes avançarem o
    relógio sem `sleep` real (não usamos `freezegun`: é uma dependência a
    mais para um `time.time` isolado — injeção direta já cobre a matriz).
    """

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        revocations_url: str,
        revocations_token: str,
        cache_seconds: int,
        fail_closed_after_seconds: int,
        leeway_seconds: int = config.AUTH_JWT_LEEWAY_SECONDS,
        forced_refresh_min_seconds: int = config.AUTH_FORCED_REFRESH_MIN_SECONDS,
        redis_client: "redis.Redis | None" = None,
        http_client: httpx.AsyncClient | None = None,
        clock: Any = time.time,
    ) -> None:
        self._issuer = issuer
        self._audience = audience
        self._jwks_url = jwks_url
        self._revocations_url = revocations_url
        self._revocations_token = revocations_token
        self._cache_seconds = cache_seconds
        self._fail_closed_after_seconds = fail_closed_after_seconds
        self._leeway_seconds = leeway_seconds
        self._forced_refresh_min_seconds = forced_refresh_min_seconds
        self._redis = redis_client or redis.from_url(config.REDIS_URL, decode_responses=True)
        self._http = http_client or httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS, follow_redirects=False)
        self._clock = clock

        self._jwks_keys: dict[str, jwt.PyJWK] = {}
        self._revocations_since: float = 0.0
        # Grace period de startup: até fail_closed_after_seconds para o 1º
        # sync completar antes de recusar tudo por "nunca sincronizou".
        self._last_synced_at: float = clock()
        # -inf: um kid desconhecido antes de qualquer refresh forçado nunca
        # é throttled pelo cold start.
        self._last_forced_refresh_at: float = float("-inf")
        self._sync_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._http.aclose()
        await self._redis.aclose()

    async def run_sync_loop(self) -> None:
        """Loop de fundo (registrado via `lifespan=` do MCPServer): chama
        `sync()` a cada `AUTH_CACHE_SECONDS`, para sempre, até ser
        cancelado. `sync()` nunca propaga exceção (loga e mantém o cache
        anterior), então este loop não precisa de try/except próprio."""
        while True:
            await self.sync()
            await asyncio.sleep(self._cache_seconds)

    async def sync(self) -> None:
        """Um ciclo de sincronização: refaz o JWKS e busca revogações novas
        desde o último cursor, grava a denylist no Redis. Só avança
        `_last_synced_at` (o relógio do fail-closed) se AMBAS as chamadas
        remotas tiverem sucesso — uma falha parcial mantém o cache anterior
        e loga warning, sem travar o processo."""
        async with self._sync_lock:
            try:
                jwks_keys = await self._fetch_jwks()
                since = self._revocations_since
                revocations, server_now = await self._fetch_revocations(since=since)
            except Exception:
                logger.warning(
                    "auth: sync de JWKS/revocations falhou, mantendo cache local", exc_info=True
                )
                return

            self._jwks_keys = jwks_keys
            now = self._clock()
            for entry in revocations:
                jti = entry.get("jti")
                exp = entry.get("exp")
                if not jti or exp is None:
                    continue
                ttl_seconds = int(exp - now)
                if ttl_seconds > 0:
                    await self._redis.set(f"{config.KEY_PREFIX}:revoked:{jti}", "1", ex=ttl_seconds)
            # Cursor pro próximo `since=`: o relógio do SERVIDOR (`body["now"]`)
            # quando presente, não o local — um cursor local perderia
            # revogações emitidas entre o instante em que o dashboard montou
            # a resposta e o instante em que terminamos de processá-la aqui.
            self._revocations_since = server_now if server_now is not None else now
            self._last_synced_at = now

    async def _fetch_jwks(self) -> dict[str, jwt.PyJWK]:
        response = await self._http.get(self._jwks_url)
        response.raise_for_status()
        body = response.json()
        keys: dict[str, jwt.PyJWK] = {}
        for raw_key in body.get("keys", []):
            kid = raw_key.get("kid")
            if not kid:
                continue
            keys[kid] = jwt.PyJWK(raw_key, algorithm="ES256")
        return keys

    async def _fetch_revocations(self, *, since: float) -> tuple[list[dict[str, Any]], float | None]:
        """Devolve (revogações, `now` do servidor | None). Contrato real
        (arquitetura.md / story 7 do dashboard): `{"revoked": [{"jti","exp"}],
        "now": <epoch>}`. Uma lista pura (sem `now`) é aceita como fallback —
        só pros mocks de teste, não é o shape real do dashboard."""
        headers = {"X-Internal-Token": self._revocations_token} if self._revocations_token else {}
        response = await self._http.get(
            self._revocations_url, params={"since": since}, headers=headers
        )
        response.raise_for_status()
        body = response.json()
        if isinstance(body, list):
            return body, None
        return body.get("revoked", []), body.get("now")

    async def verify_token(self, token: str) -> AccessToken | None:
        now = self._clock()

        if now - self._last_synced_at > self._fail_closed_after_seconds:
            logger.error(
                "auth: fail-closed — sem sincronizar JWKS/revocations há mais de %ss",
                self._fail_closed_after_seconds,
            )
            return None

        try:
            header = jwt.get_unverified_header(token)
        except jwt.InvalidTokenError:
            logger.warning("auth: token com header inválido")
            return None

        if header.get("alg") != "ES256":
            logger.warning("auth: alg != ES256 recusado (alg=%s)", header.get("alg"))
            return None

        kid = header.get("kid")
        pyjwk = self._jwks_keys.get(kid) if kid else None
        if pyjwk is None:
            # Refresh forçado em kid desconhecido (seguranca.md) — mas
            # limitado a no máx. 1x a cada AUTH_FORCED_REFRESH_MIN_SECONDS:
            # sem isso, um cliente mandando kids aleatórios amplifica cada
            # requisição não autenticada numa chamada HTTP ao dashboard.
            # Marca o timestamp ANTES de chamar sync() (não depois) para que
            # uma 2ª requisição concorrente, ainda dentro do mesmo instante,
            # já veja o throttle armado em vez de disparar outro sync().
            if self._clock() - self._last_forced_refresh_at >= self._forced_refresh_min_seconds:
                self._last_forced_refresh_at = self._clock()
                await self.sync()
                pyjwk = self._jwks_keys.get(kid) if kid else None
            if pyjwk is None:
                logger.warning("auth: kid desconhecido mesmo após refresh (kid=%s)", kid)
                return None

        try:
            payload = jwt.decode(
                token,
                key=pyjwk.key,
                algorithms=["ES256"],
                audience=self._audience,
                issuer=self._issuer,
                options={"verify_exp": False, "require": _REQUIRED_CLAIMS},
            )
        except jwt.InvalidTokenError as exc:
            logger.warning("auth: token recusado (%s)", type(exc).__name__)
            return None

        exp = payload["exp"]
        if now > exp + self._leeway_seconds:
            logger.warning("auth: token expirado além do leeway (jti=%s)", payload.get("jti"))
            return None

        jti = payload["jti"]
        if await self._redis.exists(f"{config.KEY_PREFIX}:revoked:{jti}"):
            logger.warning("auth: token revogado (jti=%s)", jti)
            return None

        return AccessToken(
            token=token,
            client_id=payload.get("sub", ""),
            scopes=list(payload.get("scope", [])),
            expires_at=int(exp),
            subject=payload.get("sub"),
            claims=payload,
        )


def require_scope(ctx: Context | None, tool_name: str) -> AccessToken | None:
    """CAP-3 (story 8): primeira linha de cada tool. `tool_name` é a chave
    em `app.scopes.TOOL_SCOPES` (normalmente o nome da própria tool
    chamadora) — o verbo exigido vem de lá, nunca duplicado como string
    solta em cada tool (single source of truth do mapa tool->verbo).

    Recusa com `ToolError`:
      - `UNAUTHENTICATED` se não houver token autenticado na requisição;
      - `SCOPE_DENIED: <tool_name> requer <verbo>` se o verbo exigido não
        estiver em `AccessToken.scopes` (sem vazar claims — só o nome do
        verbo, que já é público em seguranca.md).

    CAP-2 (spec-observability-metrics): as duas recusas incrementam o MESMO
    contador `scope_denied_total` — decisão de implementação, não
    especificada 1:1 na spec (que só nomeia SCOPE_DENIED). UNAUTHENTICATED é
    também, na prática, "faltou o escopo necessário" (nenhum token == nenhum
    escopo); manter um único contador evita uma métrica nova não pedida
    pela spec só para separar os dois casos, que operacionalmente pedem a
    mesma ação (checar o token/claims do chamador).
    Devolve o `AccessToken` quando autorizado, ou `None` (sem checar nada)
    só com `AUTH_ENABLED=false` (dev local: sem `token_verifier` nesse
    modo, não há claims pra exigir).

    Achado da revisão: `ctx=None` com `AUTH_ENABLED=true` FALHA FECHADO
    (`UNAUTHENTICATED`), nunca libera — autorização não pode depender de um
    parâmetro opcional estar presente (uma tool futura que esquecesse
    `ctx: Context` passaria pelo guard de registro do `@_tool()` decorator
    — que agora TAMBÉM exige o parâmetro — mas rodaria sem checar nada se
    `require_scope` tratasse `ctx=None` como "sem auth pra checar"). A
    suíte pré-existente (que chama tools direto, sem `ctx`, sem passar
    pelo dispatch real do SDK) precisa de `AUTH_ENABLED=false` explícito
    pra continuar funcionando — ver `tests/conftest.py` (fixture
    `dev_mode_no_auth`)."""
    if not config.AUTH_ENABLED:
        return None

    verb = TOOL_SCOPES[tool_name]
    request = ctx.request_context.request if ctx is not None else None
    user = request.scope.get("user") if request is not None else None
    if not isinstance(user, AuthenticatedUser):
        metrics.scope_denied_total.inc()
        raise ToolError("UNAUTHENTICATED: nenhum token autenticado nesta requisição")
    access_token = user.access_token
    if verb not in access_token.scopes:
        metrics.scope_denied_total.inc()
        raise ToolError(f"SCOPE_DENIED: {tool_name} requer {verb}")
    return access_token
