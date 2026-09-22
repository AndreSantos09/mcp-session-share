# session-share-listener (plugin do Claude Code)

Plugin que fecha um gap do MCP `session-share`: **o listener não era
disparado automaticamente quando uma sessão criava ou entrava numa room.**

Desde a v0.2.0, a opção primária de listener não usa mais um sub-agente
Claude em loop — é um script comum (`scripts/session_poll_watch.py`, sem
LLM) observado pela tool `Monitor` do Claude Code. Motivo: um sub-agente
pagava um turno de LLM a cada ciclo de `session_poll`, mesmo com a room em
silêncio — em sessões longas isso acumulava contexto até compactar, e era
aí que apareciam alucinações no listener. O sub-agente continua existindo
como fallback (ambiente sem `python3`/pacote `mcp`).

## O que faz

- **Hook `PostToolUse`** em `mcp__session-share__session_share` e
  `mcp__session-share__session_join`: logo depois que a tool retorna, o script
  `hooks/remind-listener.py` lê `room_id`/`participant_id` do `tool_response`
  (e `display_name` do `tool_input`; pra `session_join`, que não devolve
  `room_id`, usa o do `tool_input`), resolve o caminho absoluto de
  `scripts/session_poll_watch.py` via `CLAUDE_PLUGIN_ROOT` (garantido no
  ambiente do próprio hook) e devolve ao modelo, no `additionalContext`, o
  comando pronto pra rodar em background + armar o `Monitor` nele. Junto vai
  o fallback: se `python3`/`mcp` não derem certo, usar o Agent tool com o
  campo `listener_prompt` da resposta (texto completo repetido no contexto —
  o modelo não precisa nem voltar ao resultado da tool). Se a tool falhou
  (`ROOM_NOT_FOUND`, `ROOM_FULL`...) ou os campos não vieram, o hook sai em
  silêncio com código 0. Nada é gravado em arquivo — o `participant_id` só
  volta no contexto do modelo, que já o tinha.
- **Script `session_poll_watch.py`** (`scripts/`, `pip install -r
  scripts/requirements.txt` uma vez): cliente MCP puro que faz o long-poll de
  `session_poll` direto, sem LLM. Fica mudo em poll vazio; imprime uma linha
  por evento real (mensagem, sistema, ack, erro) — é esse stdout que a tool
  `Monitor` do Claude Code transforma em notificação, só quando há algo de
  fato. Detalhe do design e do que ainda falta pra rodar 100% sem depender
  de nenhum dashboard: ver o design doc do "cenário 3" (histórico da
  conversa que motivou isto).
- **Skill `session-listen`** (`skills/session-listen/SKILL.md`): os dois
  procedimentos (script+`Monitor` como padrão, Agent tool como fallback), a
  política por intent do protocolo 1:1 e uma cópia do prompt-template do
  fallback — **o mesmo texto** que o servidor devolve em `listener_prompt`
  (`LISTENER_PROMPT_TEMPLATE` em `app/main.py`); um teste no repo
  (`tests/test_listener_template.py`) falha se os dois divergirem.

## Por que existe

O servidor já pedia pra sessão "ficar em loop de `session_poll`". Isso trava a
conversa do usuário enquanto o Claude polla e, na prática, o modelo às vezes
nem faz isso. Um servidor MCP não consegue acordar uma sessão sozinho, então
o único "push" viável é um
agente separado em background que escuta e avisa a sessão principal via
`SendMessage`. O hook torna esse passo **determinístico** no cliente: ele não
depende de o modelo lembrar de ler a description da tool.

Por que o prompt do listener vem no **resultado** da tool (`listener_prompt`)
e não na description: o Claude Code corta cada description de tool MCP em
~2.000 caracteres (`… [truncated]`), e o template tem ~3 KB. Na primeira
versão ele ia inline no docstring de `session_share`/`session_join`; em teste
real entre duas sessões, o corte caía logo depois do bloco de credenciais e o
loop, a política por intent e as regras nunca chegavam a um cliente sem o
plugin — o fallback falhava exatamente no caso pra que existia. O resultado
da tool não sofre esse corte, então as descriptions ficaram curtas (só a
ordem imperativa de disparar o listener) e o texto longo vai preenchido no
`listener_prompt`.

A política do listener (ver a skill): ele **só avisa**. `pergunta`/`handoff`
recebidos → aviso imediato com destaque (handoff exige `message_ack` quando o
trabalho terminar — decisão da sessão principal); `fyi`/`conclusao` → aviso
breve, agrupado; evento `ack` → uma linha sem destaque; `system` → uma linha.
Nunca responde na room nem chama `message_ack`. Resposta automática só com
critério explícito do usuário.

## Instalação

Formato verificado na doc oficial em 2026-09-12:
<https://code.claude.com/docs/en/plugins>,
<https://code.claude.com/docs/en/plugins-reference>,
<https://code.claude.com/docs/en/plugin-marketplaces>,
<https://code.claude.com/docs/en/hooks> (também servida como
`hooks.md`), <https://code.claude.com/docs/en/skills>. As URLs antigas em
`docs.claude.com/en/docs/claude-code/...` redirecionam pra essas.

### Opção A — carregar direto do repo (desenvolvimento / uma sessão)

```bash
claude --plugin-dir /caminho/para/session-share/claude-plugin/session-share-listener
```

Vale só pra essa execução do `claude`. A skill aparece como
`session-share-listener:session-listen` e o hook fica ativo.

### Opção B — marketplace local (instalação persistente)

O diretório `claude-plugin/` deste repo já é um marketplace
(`claude-plugin/.claude-plugin/marketplace.json`). Dentro do Claude Code:

```
/plugin marketplace add /caminho/para/session-share/claude-plugin
/plugin install session-share-listener@session-share
```

Também dá pra apontar o marketplace pro repo git (`/plugin marketplace add
<url-do-repo>`) se ele for publicado com o `.claude-plugin/marketplace.json`
na raiz — hoje ele está em `claude-plugin/`, então pelo git é preciso mover
ou usar o caminho local.

### Opção C — manual, sem plugin

1. Skill: copie `skills/session-listen/` pra `~/.claude/skills/session-listen/`.
2. Script sem LLM: copie `scripts/` (script + `requirements.txt`) pra dentro
   dessa mesma pasta (`~/.claude/skills/session-listen/scripts/`) e rode
   `pip install -r requirements.txt` uma vez. Sem `CLAUDE_PLUGIN_ROOT`
   (esta opção não usa plugin manager), a skill precisa do caminho fixo
   onde você copiou — ajuste as instruções da skill de acordo, ou informe
   esse caminho quando pedir pra subir o listener.
3. Hook: copie `hooks/remind-listener.py` pra algum lugar fixo (ex.
   `~/.claude/hooks/session-share-remind-listener.py`) e adicione ao
   `~/.claude/settings.json` (mesmo formato do `hooks/hooks.json`, sem o
   `${CLAUDE_PLUGIN_ROOT}`):

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "mcp__session-share__session_share|mcp__session-share__session_join",
        "hooks": [
          {
            "type": "command",
            "command": "python3 \"$HOME/.claude/hooks/session-share-remind-listener.py\"",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

Se o seu `settings.json` já tem uma chave `hooks`, mescle o array
`PostToolUse` em vez de substituir.

## Como atualizar (quem já instalou)

Não existe push automático — cada pessoa precisa rodar o comando de update
depois de puxar a versão nova do repo. Comandos confirmados na doc oficial
(plugins-reference, discover-plugins) em 2026-09-18:

- **Instalou via Opção B** (`/plugin marketplace add` + `/plugin install`):
  `git pull` no clone local do repo, depois `/plugin install
  session-share-listener@session-share` (reinstala já puxando a versão
  atual do marketplace) — ou, separado, `/plugin marketplace update
  session-share` seguido de `/plugin update session-share-listener@session-share`.
  **Marketplace local (caminho de disco, não URL git) não tem
  auto-refresh em segundo plano** — sem rodar o update manualmente, o
  Claude Code não nota que o diretório mudou.
- **Instalou via Opção A** (`claude --plugin-dir`, uma sessão): `git pull` e
  `/reload-plugins` na sessão aberta, ou simplesmente abrir uma sessão nova
  — pega a mudança sozinho, sem update explícito, porque aponta direto pro
  diretório.
- **Instalou via Opção C** (manual, sem plugin manager): não tem update
  automatizado nenhum — precisa recopiar `skills/session-listen/`,
  `hooks/remind-listener.py` e (novidade da v0.2.0) `scripts/` na mão.
- Não existe notificação proativa pra marketplace local — avise o time por
  fora (Slack etc.) quando publicar uma versão nova; o Claude Code não vai
  avisar sozinho.

## Testando o hook na mão

```bash
cd claude-plugin/session-share-listener
# caso feliz (session_share)
echo '{"hook_event_name":"PostToolUse","tool_name":"mcp__session-share__session_share","tool_input":{"display_name":"Andre"},"tool_response":{"room_id":"casa-rio-sol-mar-42","participant_id":"tok_x","expires_at":1.0,"ttl_seconds":7200,"listener_prompt":"Você é um listener dedicado da room casa-rio-sol-mar-42 ..."}}' | python3 hooks/remind-listener.py
# servidor antigo, sem listener_prompt -> lembrete aponta pro template da skill, com os ids
echo '{"hook_event_name":"PostToolUse","tool_name":"mcp__session-share__session_share","tool_input":{"display_name":"Andre"},"tool_response":{"room_id":"casa-rio-sol-mar-42","participant_id":"tok_x","expires_at":1.0,"ttl_seconds":7200}}' | python3 hooks/remind-listener.py
# erro da tool -> sem saída, exit 0
echo '{"hook_event_name":"PostToolUse","tool_name":"mcp__session-share__session_join","tool_input":{"room_id":"x"},"tool_response":{"isError":true,"content":[{"type":"text","text":"ROOM_NOT_FOUND"}]}}' | python3 hooks/remind-listener.py; echo "exit=$?"
```

## Como parar o listener

- **Modo sem LLM (padrão)**: `TaskStop` no processo do `Monitor` — encerra o
  script na hora, sem esperar o `session_poll` em andamento.
- **Fallback com sub-agente**: `SendMessage(to: "<nome-ou-id-do-agente
  listener>", message: "pare de escutar e encerre")`. Ele só vê a ordem no
  próximo retorno do `session_poll` (até 30-45s).

Nos dois casos, o listener **não** chama `session_close` por conta própria
— ele usa o mesmo `participant_id` da sessão principal. Pra sair da room de
fato, a sessão principal chama `session_close` (ou pede isso explicitamente
na ordem de parada).

## Limitações

- O hook só dispara **no cliente que tem o plugin instalado**. O outro lado
  da room, sem plugin, depende da description (curta) de
  `session_share`/`session_join` e do `listener_prompt` que vem no resultado
  da tool — funciona como fallback, mas não é determinístico.
- O matcher assume que o servidor foi registrado com o nome `session-share`
  no `.mcp.json` (`mcp__session-share__*`). Outro nome → ajuste o matcher.
- O hook só lembra; quem sobe o listener é o modelo. Se ele ignorar o
  `additionalContext`, nada acontece — mas isso é bem mais raro do que
  ignorar um parágrafo perdido na description da tool.
- Hook: depende de `python3` no PATH (stdlib apenas, sem pacotes). Script
  `session_poll_watch.py`: depende de `python3` **e** `pip install -r
  scripts/requirements.txt` (pacote `mcp`) — se isso não for viável no
  ambiente de quem vai rodar o listener, use o fallback com sub-agente.
- `Monitor` expira sozinho no máximo a cada 30 min — pra vigilância
  indefinida é preciso rearmar a cada expiração (1 chamada de tool, não um
  turno cheio). O sub-agente fallback consome uma chamada de tool a cada
  30-45s de silêncio enquanto a room estiver aberta. Os dois param sozinhos
  quando `room_status != "open"`.
- Se o `session-share` algum dia exigir token (`AUTH_ENABLED=true`), exporte
  `SESSION_SHARE_TOKEN` no ambiente de quem roda `session_poll_watch.py`
  antes de armar o `Monitor` — o script manda como Bearer automaticamente.
