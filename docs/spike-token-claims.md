# Spike CAP-3 — claims do token no handler de tool (story 2)

## Pergunta

Com `MCPServer(auth=AuthSettings(...), token_verifier=<verifier>)` do SDK
`mcp` 2.2.0, em transporte streamable-http, os scopes/claims do token chegam
num handler de tool via `Context` (`ctx.request_context.request.scope["user"]`,
`ctx.request_context.request.user`, etc.)?

## Resposta: sim, sem precisar de middleware Starlette próprio

Versão exata usada: `mcp==2.2.0` (`pip show mcp`), venv `.venv` deste repo.

Ao construir `MCPServer(..., auth=AuthSettings(...), token_verifier=<verifier>)`
e chamar `.streamable_http_app(...)`, o SDK monta sozinho (ver
`mcp/server/mcpserver/server.py:1180-1199`, e o mesmo caminho em
`streamable_http_app()` → `_lowlevel_server.streamable_http_app(auth=...,
token_verifier=...)`):

```python
middleware = [
    Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(token_verifier, ...)),
    Middleware(AuthContextMiddleware),
]
```

- `BearerAuthBackend.authenticate` lê o header `Authorization: Bearer <token>`,
  chama `token_verifier.verify_token(token)` e, se válido, devolve
  `AuthCredentials(scopes)` + `AuthenticatedUser(access_token)`. O Starlette
  `AuthenticationMiddleware` grava isso em `scope["user"]`/`scope["auth"]`.
- Quando `token_verifier` está configurado, **toda rota** (não só as tools)
  é envolvida por `RequireAuthMiddleware`: sem `AuthenticatedUser` em
  `scope["user"]`, a requisição recebe 401 **antes** de qualquer handler.

Dentro do handler de uma tool, `ctx.request_context.request` é o
`starlette.requests.Request` daquela chamada HTTP (populado em
`mcp/server/streamable_http.py:_message_metadata` → `ServerMessageMetadata.
request_context=request`, e propagado até `ServerRequestContext.request` em
`mcp/server/runner.py`). Logo:

```python
request = ctx.request_context.request
user = request.scope.get("user")          # AuthenticatedUser | None
access_token = user.access_token          # AccessToken: token, client_id, scopes, subject, claims, ...
```

funciona exatamente como a pergunta propôs — **caminho confirmado e usado em
`app/auth.require_scope`**. (O SDK também expõe um atalho equivalente,
`mcp.server.auth.middleware.auth_context.get_access_token()`, baseado numa
contextvar que o `AuthContextMiddleware` já popula — mesmo dado, sem precisar
de `ctx`; não usado aqui porque a arquitetura pediu para verificar
especificamente o caminho via `Context`/`request.scope`.)

**Fallback do CAP-3 (middleware Starlette próprio gravando em
`request.state`) não foi necessário** — o SDK já expõe o suficiente.

## Prova empírica

`tests/test_auth_spike.py` sobe o app em processo (`ASGITransport`, sem porta
real) e prova os 3 cenários do I/O matrix com um cliente MCP de verdade
(`ClientSession` + `streamable_http_client`):

| Cenário | Resultado observado |
|---|---|
| Token válido (`Authorization: Bearer spike-static-token`) | `_auth_probe` devolve `{"scopes": ["spike:probe"], "sub": "alice"}` — round-trip completo (`initialize` + `tools/call`) |
| Sem `Authorization` | `401 Unauthorized` no transporte, handler nunca roda |
| Bearer inválido/desconhecido | `401 Unauthorized`, mesmo comportamento |
| `tools/list` sem token (extra, fora da pergunta original) | também `401` — o SDK exige auth por rota inteira, não por tool; `tools/list` não vira uma lista "filtrada mas acessível" quando `token_verifier` está configurado. Se o CAP-3 real (stories 8/9) quiser `tools/list` sem token devolvendo uma lista vazia/pública em vez de 401, isso exige uma rota própria fora do `RequireAuthMiddleware` do SDK — decisão para essas stories, documentada aqui como achado do spike. |

Saída real (ver conclusão da story na room): `125 passed` (121 testes
pré-existentes + 4 deste spike), `REDIS_URL=redis://localhost:6379/0
python3 -m pytest tests/ -q`.

## Assinatura proposta de `require_scope` (para as stories 4/8/9)

Implementada como esboço em `app/auth.py` (não é enforcement de produção —
isso é story 4, com JWT/JWKS/denylist reais; aqui o verifier é estático):

```python
def require_scope(ctx: Context, verb: str) -> AccessToken:
    """Primeira linha de cada tool. ToolError se não autenticado ou sem `verb`
    em AccessToken.scopes. Devolve o AccessToken quando autorizado."""
```

Chamada como `require_scope(ctx, "session-share:send")` (mapa tool→verbo em
`seguranca.md`, per a arquitetura). `tools/list` continua sendo só UX —
`require_scope` é o enforcement autoritativo, no dispatch.

## Escopo desta story (não mudou)

Nenhuma das 18 tools existentes mudou de assinatura ou comportamento por
padrão (`AUTH_SPIKE` não setada = `"0"`, sem `auth=`/`token_verifier=` no
`MCPServer` de produção). `_auth_probe` e a autenticação só existem quando o
processo sobe com `AUTH_SPIKE=1` — nunca em produção. Nenhuma validação real
de JWT/JWKS/denylist foi implementada (story 4).
