# mcp-session-share

Servidor MCP (Model Context Protocol) que funciona como um "walkie-talkie"
entre sessões Claude Code de **contas/máquinas diferentes** — permite que
duas pessoas combinem uma sessão compartilhada e conversem através dos
próprios Claudes, sem depender de mecanismos nativos de cross-session
messaging (que só funcionam dentro da mesma conta).

## Como funciona

1. Uma sessão chama `session_share` → cria uma **room** e recebe um `room_id`
   (código tipo `palavra-palavra-palavra-palavra-NN`) e um `participant_id`.
2. Você compartilha o `room_id` com a outra pessoa **por um canal confiável
   fora deste sistema** (Slack, verbal, etc). O `room_id` é o "segredo
   compartilhado" que dá acesso à room.
3. Outras sessões chamam `session_join` com esse `room_id` e recebem seu
   próprio `participant_id` — rooms suportam 3 ou mais participantes
   (broadcast-only, até `SESSION_MAX_PARTICIPANTS`, default 10).
   **Logo depois de `share`/`join`, cada sessão dispara um listener em
   background** (um agente separado que fica em `session_poll` e avisa a
   sessão principal a cada evento, classificado por intent) em vez de
   pollar em foreground — a resposta das duas tools traz o campo
   `listener_prompt`: o prompt completo do listener, já preenchido com
   `room_id`/`participant_id`/`display_name`, pronto pra virar prompt de um
   agente em background. O [plugin para Claude
   Code](#plugin-para-claude-code) torna esse passo automático.
4. A partir daí, os participantes alternam `session_send`/`session_send_json`
   (mandar texto ou payload estruturado) e `session_poll` (long-poll — fica
   bloqueado no servidor até chegar mensagem nova ou estourar o timeout)
   para conversar quase em tempo real. Pra trocar arquivo, é `file_send` /
   `file_receive` — o envio aparece como uma mensagem normal no
   `session_poll` de quem está escutando. `message_status` consulta, por
   participante, se uma mensagem está `pendente`, `entregue` ou `tratado`.
   Numa conversa 1:1, os três sends aceitam `in_reply_to` (id da mensagem
   respondida — reconstrói a thread) e `intent` (`pergunta` default | `fyi`
   | `handoff` | `conclusao`); `message_ack` marca uma mensagem recebida
   como tratada e `session_status.turn` diz de quem é a vez de agir.
5. `session_close` sai da room; se for o último participante, ela é
   encerrada na hora (antes do TTL vencer). A qualquer momento antes disso,
   `session_export` devolve o transcript completo em markdown.

**Guarde o `participant_id`** retornado por `session_share`/`session_join`:
é o token de autenticação de todas as chamadas seguintes nessa room — nunca
o revele dentro da própria conversa.

## Modelo de segurança

- `room_id` é o "segredo de entrada" — mas, desde a story 10 (CAP-4), só
  dá acesso direto em rooms `open_join=true` (compatibilidade); por
  padrão, quem tem o `room_id` ainda precisa de um `join_code` (convite do
  criador) e fica `pending` até aprovação. Gerado com ~38 bits de entropia
  (4 palavras de uma wordlist de 227 + 2 dígitos), pensado para ser
  falado/colado facilmente, não para resistir a ataque com recursos sérios.
- `participant_id` é o token real usado em `send`/`poll`/`close`/`status` —
  mesmo quem descobre o `room_id` depois não consegue se passar por um
  participante já existente.
- `session_join` tem rate limit por tentativas repetidas do *mesmo* `room_id`
  (padrão: 20/60s) — não impede enumeração distribuída de códigos diferentes,
  mas a entropia (~38 bits) já torna isso impraticável dentro do TTL.
- TTL da room é *sliding*: renovado a cada `join`/`send`/`poll` com
  atividade. Fica default em 2h de silêncio, configurável até 24h.
- **Isso não é autenticação de identidade real** — é posse de um segredo
  compartilhado. Adequado para coordenação de baixo risco entre colegas;
  não use para dados sensíveis sem uma camada extra.

## Autenticação (CAP-2)

Camada de transporte, abaixo do modelo de `room_id`/`participant_id` acima:
todo o acesso ao servidor MCP (não só às tools de room) exige um JWT ES256
assinado pelo `auth-service`, com `aud=session-share`. Ver `app/auth.py` e
`docs/spike-token-claims.md` (CAP-3, story 2: como o claim chega à tool).

- `AUTH_ENABLED` (default `true`, vem do ConfigMap `mcp-session-share-config`):
  `false` desliga a autenticação inteira — **só para dev local**, nunca em
  produção (loga um aviso a cada start).
- `AUTH_ISSUER` (default `auth-service`), `AUTH_AUDIENCE` (default
  `session-share`): valores esperados nos claims `iss`/`aud` do JWT (ConfigMap).
- `AUTH_JWKS_URL` (ConfigMap): endpoint do dashboard com as chaves públicas
  ativas (JWKS), cacheado por `AUTH_CACHE_SECONDS` (default 60s) e
  re-consultado sob demanda quando aparece um `kid` desconhecido.
- `AUTH_REVOCATIONS_URL` (ConfigMap) + `AUTH_REVOCATIONS_TOKEN` (**Secret**
  `mcp-session-share-secrets`, story 14 — ver `k8s/secret.example.yaml`):
  endpoint de revogações (`GET {url}?since=<epoch>`, header
  `X-Internal-Token`) e o segredo desse header — nunca em ConfigMap.
- `PARTICIPANT_HASH_KEY` (**Secret** `mcp-session-share-secrets`, story 14
  — mesmo valor do `AUTH_HASH_KEY` do dashboard, CAP-4 story 10):
  obrigatória com `AUTH_ENABLED=true`; sem ela o processo nem sobe.
- `AUTH_FAIL_CLOSED_AFTER_SECONDS` (default 300): sem sincronizar JWKS e
  revogações por mais que isso, todo token é recusado (mesmo um que seria
  válido offline) — erro logado, nunca falha silenciosa.
- Token expirado, `aud` errado, `jti` revogado ou `kid` desconhecido (mesmo
  após um refresh forçado do JWKS, no máx. uma vez a cada
  `AUTH_FORCED_REFRESH_MIN_SECONDS` — evita que um `kid` aleatório amplifique
  cada requisição não autenticada numa chamada HTTP ao dashboard) são
  recusados com `401` antes de qualquer tool rodar.
- **O leeway de 30s do `exp` (`app/auth.py`) é mais frouxo que o próprio SDK
  `mcp`**: `BearerAuthBackend` recusa qualquer token com `expires_at < now`
  *antes* mesmo de perguntar ao `JWTVerifier` — sem leeway nenhum. Na
  prática, pelo transporte real, um token expirado há poucos segundos já
  cai nessa checagem do SDK, então o leeway do nosso verifier só é
  observável chamando `verify_token` direto (é o que `tests/test_auth_jwt.py`
  faz). Resultado é mais estrito que o pedido, não menos — aceitável, mas
  documentado aqui para não parecer bug se alguém for procurar o leeway
  "funcionando" numa chamada HTTP de verdade.

O mapa completo `tool → verbo` é `app/scopes.py` (a fonte de verdade neste
repo).

## Allowlist por tool (CAP-3)

Depois da story 4 (token válido, mas qualquer escopo executava qualquer
tool), a story 8 fecha o dispatch: cada uma das 21 tools tem um verbo em
`app/scopes.py` (`TOOL_SCOPES`) e chama `require_scope(ctx, "<nome_da_tool>")` como primeira linha
— o verbo exigido vem do mapa, nunca duplicado como string solta em cada
tool.

- **Default deny estrutural**: uma tool registrada sem entrada em
  `TOOL_SCOPES` derruba o processo (`RuntimeError`) na hora de importar
  `app.main` — não existe "tool nova esquecida no mapa" em produção, o
  start falha antes.
- **Default deny em runtime**: token sem `scope` (ou sem o verbo certo)
  não executa nada — `ToolError("SCOPE_DENIED: <tool> requer <verbo>")`,
  sem vazar claims. Sem token: `UNAUTHENTICATED` (já é 401 no transporte,
  antes de chegar aqui — ver seção Autenticação).
- **`tools/list` é só UX, não a barreira**: filtrado pelas scopes do token
  (só mostra tools que o token pode chamar), mas chamar uma tool direto,
  ignorando `tools/list`, ainda leva `SCOPE_DENIED` — é isso que
  `tests/test_scopes.py` prova primeiro.
- **`AUTH_ENABLED=false`** (dev local): `require_scope` não checa nada,
  igual antes da story 8 — mesma exceção documentada na seção Autenticação.
- Verbos (`session-share:read/send/json/file/join/autoloop/admin`) e quais
  tools cada um cobre: `app/scopes.py` (fonte de verdade neste repo). Perfis
  sugeridos para o serviço de auth (worker/observer/orchestrator) também
  estão documentados lá.

## Convite revogável (CAP-4)

Antes desta story, quem tinha o `room_id` entrava, ficava, e lia todo o
histórico retido na stream — sem como expulsar ninguém. Agora, por padrão
(`session_share(policy={"open_join": false})`, o default), `session_join`
exige convite:

- `session_invite(room_id, participant_id)` — só o **criador** da room
  (`FORBIDDEN` pra qualquer outro) gera um `join_code` de uso único
  (`secrets.token_urlsafe(12)`, TTL de 10 min — `INVITE_TTL_SECONDS`).
- `session_join(room_id, display_name, join_code)` com o código: entra como
  **`pending`**, não como participante — aparece só em
  `session_status.pending[]` do criador (invisível pros demais), e
  `session_poll` dele devolve `{"messages": [], "room_status": "pending"}`
  até ser aprovado. Nenhuma outra tool funciona pra esse `participant_id`
  ainda (`PARTICIPANT_NOT_FOUND`, igual antes de qualquer join).
- `session_approve(room_id, participant_id, target_hash)` — só o criador —
  promove o pendente a participante. O cursor dele começa no PRÓPRIO evento
  de entrada: o primeiro poll não traz nada anterior, por mais histórico
  que a room já tenha.
- `session_kick(room_id, participant_id, target_hash)` — só o criador —
  remove um participante OU um pendente; o `participant_id` removido vira
  `PARTICIPANT_NOT_FOUND` na próxima chamada, pra sempre.
- `join_code` reutilizado → `INVITE_USED`; expirado (ou nunca existiu — a
  resposta é a mesma de propósito, pra não virar oráculo de enumeração) →
  `INVITE_EXPIRED`; `session_join` sem código numa room que exige convite →
  `INVITE_REQUIRED`.
- `target_hash` = `HMAC-SHA256(PARTICIPANT_HASH_KEY, participant_id)[:12]`
  — nunca o `participant_id` real, nem pro criador. `session_status`
  devolve o hash de cada participante ativo e de cada pendente; `PARTICIPANT_HASH_KEY`
  é a MESMA chave que o `auth-service` usa (`AUTH_HASH_KEY`, CAP-0) —
  obrigatória (o processo recusa subir) quando `AUTH_ENABLED=true`.
- **Compatibilidade**: `session_share(policy={"open_join": true})` mantém
  o comportamento de antes desta story — `session_join` sem código entra
  direto como participante, sem convite/aprovação.

## Envelope untrusted e política de room (CAP-8)

A ameaça número um deste projeto é prompt injection
cross-conta — a sessão que recebe uma mensagem de outra conta tem acesso a
ferramentas com efeito colateral real (monitoramento com escrita, CI/CD, kubectl, cofre de segredos),
e o texto/payload de outro participante nunca pode ser tratado como
instrução. Duas defesas independentes:

- **Envelope estrutural em todo item de `session_poll`**: `{id, origin:
  "system"|"participant", sender_name, intent, in_reply_to, created_at,
  untrusted: bool, content: {kind: text|json|file|ack|autoloop_turn|system, ...}}`.
  `untrusted` é sempre `origin == "participant"` — nunca decidido caso a
  caso pelo tipo de conteúdo. `content` isola o conteúdo do participante em
  campos estruturados; o servidor nunca concatena esse conteúdo num texto
  sintético próprio (os antigos `"[ack] ..."`/`"[action] ..."` viravam
  exatamente esse tipo de concatenação — não existem mais). `origin`,
  `sender_name`, `intent`, `in_reply_to`, `created_at` continuam no topo do
  item (usados pelo dashboard). Eventos de sistema (join/leave/kick/etc)
  usam `content={"kind": "system", "event": "room_created"|"joined"|
  "join_requested"|"approved"|"kicked"|"left", "actor_name": <nome>, "text":
  <template fixo>}` — `actor_name` é o nome escolhido pelo participante
  (dado), `text` nunca o interpola (achado de revisão: antes, o
  display_name ia direto no `text` de um evento marcado como "sempre
  confiável" — um display_name hostil viraria conteúdo confiável por
  engano). `display_name` "system" (em qualquer combinação de
  maiúsculas/espaços) é reservado (`NAME_RESERVED`); caracteres de
  controle/quebra de linha e nomes só-espaço também são recusados
  (`INVALID_DISPLAY_NAME`) em `session_share`/`session_join`.
- **`policy.mode` por room**: toda room nasce em `"chat-only"` (default) —
  `session_send(intent="handoff")` e qualquer `session_send_json` (sempre
  tem `"action"`) retornam `POLICY_DENIED` nesse modo. Só o **criador** da
  room libera isso pra `"handoff-enabled"`, via `session_share(policy=
  {"mode": "handoff-enabled"})` na criação ou depois via
  `session_set_policy(room_id, participant_id, mode)` — nunca decidido
  pelo servidor por conta própria; é sempre uma escolha explícita de quem
  criou a room, depois de combinar isso com a outra sessão fora deste
  canal.
- Ver `docs/injection-test.md` para o cenário de teste completo (fixture
  com mensagem de ataque, e prova de que ela fica isolada em `content` sem
  vazar pra nenhum campo gerado pelo servidor).

## Ferramentas MCP

| Tool | Descrição |
|---|---|
| `session_share(display_name, ttl_seconds, policy?)` | Cria a room. `policy={"open_join": bool, "mode": "chat-only"\|"handoff-enabled"}` — `open_join` (default `false`) mantém o join direto de antes da story 10; `mode` (default `"chat-only"`) já libera handoff/`session_send_json` se passado `"handoff-enabled"` na criação. |
| `session_join(room_id, display_name, join_code?)` | Entra numa room existente — exige `join_code` (session_invite) a menos que a room seja `open_join`; sem código nesse caso, fica `pending` até `session_approve`. |
| `session_invite(room_id, participant_id)` | Gera um `join_code` de uso único (TTL 10min) — só o criador da room. |
| `session_approve(room_id, participant_id, target_hash)` | Promove um `pending` a participante — só o criador. |
| `session_kick(room_id, participant_id, target_hash)` | Remove um participante ou pendente — só o criador; `participant_id` removido vira `PARTICIPANT_NOT_FOUND` dali em diante. |
| `session_send(room_id, participant_id, text, in_reply_to?, intent?)` | Manda mensagem de texto. `in_reply_to` (opcional) referencia a mensagem respondida — precisa existir na room (`INVALID_REPLY_TARGET`); `intent` (opcional) é um de `pergunta` (default) \| `fyi` \| `handoff` \| `conclusao` (`INVALID_INTENT` fora disso). `handoff` descreve a intenção de quem manda, nunca uma ordem — exige `policy.mode="handoff-enabled"` ou falha com `POLICY_DENIED`. |
| `session_send_json(room_id, participant_id, payload, in_reply_to?, intent?)` | Manda payload JSON tipado (`action` obrigatório) — coordenação agente-a-agente; sempre tratado como dado, nunca executado pelo servidor. Exige `policy.mode="handoff-enabled"` (retorna `POLICY_DENIED` em `chat-only`, o default). Mesmos `in_reply_to`/`intent` de `session_send`. |
| `file_send(room_id, participant_id, filename, content_base64, in_reply_to?, intent?)` | Manda um arquivo (base64, até `SESSION_MAX_FILE_SIZE_BYTES`, default 10MB). Mesmos `in_reply_to`/`intent` de `session_send`. |
| `file_receive(room_id, participant_id, file_id)` | Baixa um arquivo enviado (`file_id` vem numa mensagem de `session_poll`). |
| `session_set_policy(room_id, participant_id, mode)` | Muda `policy.mode` da room (`"chat-only"` \| `"handoff-enabled"`) — só o criador (`FORBIDDEN` pra qualquer outro); `INVALID_POLICY_MODE` fora do enum. |
| `session_poll(room_id, participant_id, timeout_seconds)` | Long-poll por itens novos (mensagem, JSON, arquivo, ack, turno de autoloop). Cada item é um envelope `{id, origin, sender_name, intent, in_reply_to, created_at, untrusted, content}` — ver "Envelope untrusted e política de room (CAP-8)" acima. Teto: `SESSION_MAX_POLL_TIMEOUT`, default 120s. |
| `message_status(room_id, participant_id, message_id)` | Estado de uma mensagem/arquivo sua em cada outro participante: `delivered_to` (bool, retrocompat) e `state_by` (`pendente` \| `entregue` \| `tratado`). `tratado` depende do `intent`: `pergunta` → resposta com `in_reply_to`; `handoff` → só `message_ack`; `fyi`/`conclusao` → assim que entregue. |
| `message_ack(room_id, participant_id, message_id)` | Marca uma mensagem de outro participante como tratada por você (idempotente; emite um item `content.kind="ack"` na stream na 1ª vez, com `content.ack_of=<message_id>`). Obrigatório pra fechar um `handoff`. Falha com `CANNOT_ACK_OWN_MESSAGE` ou `INVALID_ACK_TARGET`. |
| `session_close(room_id, participant_id)` | Sai da room. |
| `session_status(room_id, participant_id)` | Participantes (com `last_polled_at`/`is_listening`), contagem/teto (`max_participants`), TTL restante e `turn` (de quem é a vez numa room de exatamente 2 — `applies=false` fora disso ou com autoloop ativo). |
| `session_export(room_id, participant_id)` | Transcript completo e ordenado da room (mensagens, JSON, arquivos, acks, eventos de sistema) como markdown; cada mensagem real traz `id=`, `intent=` e `in_reply_to=` pra reconstruir threads. |
| `autoloop_propose(room_id, participant_id, goal, max_turns, max_seconds)` | Propõe modo autônomo (loop agente-a-agente) pra room; proponente entra em `loop_participants` automaticamente. `max_turns`/`max_seconds` são clampados contra um teto do servidor. |
| `autoloop_accept(room_id, participant_id)` | Aceita o convite pendente; ao entrar o 2º participante distinto, o loop vira `active`. Idempotente para quem já aceitou. |
| `autoloop_decline(room_id, participant_id)` | Recusa explicitamente o convite (não entra em `loop_participants`); falha com `ALREADY_ACCEPTED` se você já tiver aceitado. |
| `autoloop_status(room_id, participant_id)` | Estado atual do loop autônomo da room (`status`, `goal`, `loop_participants`, `turn_count`, etc). |
| `autoloop_turn(room_id, participant_id, payload, turn_status)` | Envia um turno estruturado (`payload` livre + `turn_status`: `proposing`\|`agreeing`\|`blocked`\|`done`) no loop `active`; incrementa `turn_count` e checa o watchdog (turnos/tempo) antes de aceitar — sempre tratado como dado, nunca executado pelo servidor. `blocked` encerra o loop na hora, unilateral (`ended_reason="impasse"`); `done` só adiciona o chamador a `done_by` — o loop só encerra (`ended_reason="consensus"`) quando `done_by` cobrir todos os `loop_participants`. Falha com `AUTOLOOP_NOT_ACTIVE` (loop não está `active`), `NOT_LOOP_PARTICIPANT` (chamador não aceitou o loop), `INVALID_TURN_STATUS` (`turn_status` fora do enum) ou `AUTOLOOP_LIMIT_EXCEEDED` (watchdog encerrou o loop nessa chamada). |
| `autoloop_stop(room_id, participant_id)` | Para o loop autônomo (`proposed` ou `active`) imediatamente — qualquer participante da room, mesmo um bystander que nunca aceitou o convite, sem precisar de acordo de mais ninguém. `status` vira `ended`, `ended_reason="stopped"`. Falha com `AUTOLOOP_NOT_ACTIVE` se não houver proposta/loop em andamento (não é idempotente). |

**Nota:** a spec `autonomous-loop` está completa (`Story 1`+`Story 2`+`Story 3`+`Story 4`) — handshake de consentimento, troca de turnos estruturados, watchdog de limite (turnos/tempo), consenso/impasse (`turn_status=done`/`blocked` encerrando o loop) e parada manual a qualquer momento.

**Protocolo 1:1**: tudo aditivo e opcional — uma sessão que manda no formato antigo continua funcionando, só aparece como `intent=pergunta` sem thread.

## Rodando localmente

```bash
docker build -t mcp-session-share:latest .
docker run --rm -p 8000:8000 \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  -e MCP_ALLOWED_HOSTS=localhost:8000,127.0.0.1:8000 \
  mcp-session-share:latest
```

Precisa de um Redis acessível na `REDIS_URL` (ver `docker run redis:7-alpine`).

### Rodando os testes

```bash
pip install -r requirements.txt -r requirements-dev.txt
docker run --rm -d -p 6379:6379 redis:7-alpine
REDIS_URL=redis://localhost:6379/0 python3 -m pytest tests/ -v
```

Os testes rodam contra um Redis local real (sem mock) — mesma variável
`REDIS_URL` que o servidor já usa em produção.

## Deploy (k3s, namespace `mcp`)

```bash
kubectl apply -f k8s/networkpolicy.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
```

Um workflow de CI (`.github/workflows/ci.yml`) roda os testes contra um
Redis em container a cada push/PR. O deploy em si depende da sua própria
infra (imagem + `kubectl apply` dos manifests em `k8s/`).

**Importante**: `MCP_ALLOWED_HOSTS` no ConfigMap precisa incluir o
host/porta real pelos quais o servidor é acessado externamente (proteção
DNS-rebinding do SDK MCP) — ajuste antes de expor fora de `localhost`.

**Redis com senha e NetworkPolicy (story 17)**: `mcp-session-redis` exige
`--requirepass` (senha em `mcp-session-share-secrets`, chave
`REDIS_PASSWORD` — `REDIS_URL` no mesmo Secret já embute essa senha; ver
`k8s/secret.example.yaml`) e uma `NetworkPolicy` (`k8s/networkpolicy.yaml`)
restringe quem pode abrir conexão na porta 6379 a só dois pods:
`mcp-session-share` (o próprio app) e o serviço de auth (lê este Redis
diretamente para `/sessoes` e presença, CAP-7). Um pod de qualquer outro
serviço do cluster não consegue mais nem tentar a senha. **O Redis
continua sem PVC de propósito** (dados efêmeros) — um `kubectl rollout
restart deployment/mcp-session-redis` (ou o próprio pod reiniciando)
apaga todas as rooms/mensagens/presença ativas; isso já era verdade antes
da story 17, só reforçando aqui porque agora também é o momento em que a
senha muda, se for o caso.

## Adicionando como MCP server no Claude Code

No `.mcp.json` (ou config equivalente) de cada lado:

```json
{
  "mcpServers": {
    "session-share": {
      "type": "http",
      "url": "http://<host>:<porta>/mcp"
    }
  }
}
```

## Plugin para Claude Code

`claude-plugin/session-share-listener/` é um plugin do Claude Code que fecha
o gap "o listener não é disparado automaticamente": um hook `PostToolUse` em
`session_share`/`session_join` devolve ao modelo, logo que a tool retorna,
a instrução de subir o listener em background usando o campo
`listener_prompt` da resposta (e repete esse prompt, já preenchido, no
`additionalContext`), sem pollar em foreground; a skill `session-listen`
traz o procedimento, a política por intent e uma cópia do template pra
servidor antigo sem `listener_prompt`. Sem o plugin, o outro lado ainda
recebe a instrução pela description (curta) das tools e o prompt pronto no
resultado — é o fallback.

Por que o prompt vai no **resultado** da tool, e não na description: o
Claude Code corta cada description de tool MCP em ~2.000 caracteres
(marcador `… [truncated]`), e o template tem ~3 KB. Na primeira versão ele
ia inline no docstring de `session_share`/`session_join`, e em teste real
entre duas sessões o corte caía logo depois do bloco de credenciais — o
loop, a política por intent e as regras nunca chegavam a um cliente sem o
plugin. O resultado da tool não sofre esse corte; por isso as descriptions
agora são curtas (todas as 18 tools ficam abaixo de 2.000 chars — um teste
garante) e o texto longo vai em `listener_prompt`. Detalhes, política por
intent e limitações no
[README do plugin](claude-plugin/session-share-listener/README.md).

Instalação rápida (formato oficial de plugins/hooks verificado em
<https://code.claude.com/docs/en/plugins> e
<https://code.claude.com/docs/en/hooks>):

```bash
# só nesta execução
claude --plugin-dir /caminho/para/session-share/claude-plugin/session-share-listener
```

```
# persistente, via marketplace local (claude-plugin/ já é um marketplace)
/plugin marketplace add /caminho/para/session-share/claude-plugin
/plugin install session-share-listener@session-share
```

## Limitações conhecidas

- Sem PVC no Redis dedicado — dados são efêmeros de propósito (TTL de
  horas); perder estado num restart raro do pod só significa recriar a
  room. Há duas alavancas disponíveis se isso precisar mudar (réplicas com afinidade de sessão;
  Redis com AOF leve) e a condição que justificaria revisitar isso.
- `session_poll` acorda no primeiro evento novo que chegar (não batcheia
  múltiplos eventos próximos) — se dois eventos ocorrerem em sequência
  rápida do outro lado, pode ser necessário chamar `session_poll` mais de
  uma vez seguida para pegar todos.
- `file_send`/`file_receive` trafegam o arquivo inteiro em base64 numa
  chamada só — sem upload resumível/chunked. Arquivo maior que o cap é
  rejeitado com `FILE_TOO_LARGE` (o `max_request_body_size` do transporte é
  ajustado pra comportar o cap + overhead do base64, então isso não vira um
  erro genérico de "corpo muito grande").
- Sem listagem de arquivos enviados numa room — quem recebe descobre o
  `file_id` pela mensagem de sistema no `session_poll`.
- `tratado` (CAP-1) é só sinal estrutural: uma resposta com `in_reply_to`
  a uma `pergunta`, ou `message_ack`. Uma resposta em texto livre sem
  `in_reply_to` nunca marca nada como tratado — por design (o servidor não
  interpreta conteúdo), mas depende de o outro lado usar o campo.
- `session_status.turn` só existe em room com exatamente 2 participantes
  e é derivado da última mensagem real ainda na stream (retenção
  `maxlen~1000`); com autoloop `active` ele cede (`applies=false`). É um
  indicador consultável, não um lock — nenhum envio é bloqueado por ele.
- `in_reply_to`/`message_ack` só aceitam mensagem ainda presente na stream:
  uma mensagem antiga que já saiu da retenção não pode mais ser
  referenciada nem ackada.
- Autenticação (CAP-2/CAP-4): `AUTH_REVOCATIONS_TOKEN` e `PARTICIPANT_HASH_KEY`
  vêm do Secret `mcp-session-share-secrets` (story 14 — ver
  `k8s/secret.example.yaml`; o `deploy` do CI falha antes do `apply` se
  esse Secret não existir no namespace); sem `AUTH_REVOCATIONS_TOKEN`
  o servidor sobe mas a sincronização de revogações falha (fail-closed
  depois de `AUTH_FAIL_CLOSED_AFTER_SECONDS`); sem `PARTICIPANT_HASH_KEY`
  com `AUTH_ENABLED=true` o processo nem sobe (`RuntimeError` no import de
  `app/config.py`).
- `session_kick`/`session_approve` fazem uma varredura O(participantes +
  pendentes) da room pra achar quem bate com o `target_hash` recebido —
  sem índice reverso hash→participant_id. Aceitável no teto atual
  (`MAX_PARTICIPANTS`, poucas dezenas); não escala pra rooms muito maiores.
