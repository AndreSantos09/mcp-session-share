#!/usr/bin/env python3
"""
Hook PostToolUse do plugin session-share-listener.

Dispara depois de mcp__session-share__session_share / session_join (ver
hooks.json). Lê o JSON do hook no stdin, extrai room_id e participant_id do
tool_response (session_join não devolve room_id — cai pro tool_input) e
devolve, como additionalContext pro modelo, uma instrução curta e imperativa
pra subir o listener AGORA, em vez de pollar em foreground.

Cenário 3 (SPEC-listener-sem-llm): a primeira opção agora é um poller SEM
LLM (`scripts/session_poll_watch.py`, deste mesmo plugin) rodado em
background + a tool Monitor do Claude Code em cima dele — custo de token
~zero enquanto a room fica quieta, em vez de um sub-agente gastando um
turno a cada ciclo de `session_poll`. `CLAUDE_PLUGIN_ROOT` é garantido no
ambiente deste hook (o próprio `hooks.json` o usa pra achar este arquivo),
então o caminho absoluto do script já sai pronto pra copiar/colar — o
modelo não precisa adivinhar onde o plugin está instalado.

O fallback antigo (Agent tool com `listener_prompt`, sub-agente em loop de
`session_poll`) continua disponível pra quando `python3`/pacote `mcp` não
estiverem no ambiente de quem vai rodar o script — a skill
session-share-listener:session-listen documenta os dois caminhos.

Contrato (doc oficial de hooks, https://code.claude.com/docs/en/hooks):
  stdin  -> {"hook_event_name": "PostToolUse", "tool_name": ..., "tool_input": {...},
             "tool_response": <resultado da tool>, ...}
  stdout -> {"hookSpecificOutput": {"hookEventName": "PostToolUse",
             "additionalContext": "..."}}   (exit 0)
  exit 0 sem stdout = não faz nada (caso de erro da tool / campos ausentes).

Sem dependências fora da stdlib. NUNCA grava participant_id em arquivo/log:
ele só volta no contexto do modelo, que já o recebeu na resposta da tool.
"""
import json
import os
import sys

TOOL_SUFFIXES = ("__session_share", "__session_join")

CONTEXT_HEAD = "Room {room_id} conectada como '{display_name}' ({tool})."

# Caminho primário: script sem LLM + Monitor. CLAUDE_PLUGIN_ROOT é garantido
# aqui (mesma variável que hooks.json usa pra invocar este próprio arquivo).
CONTEXT_NO_LLM = (
    " Suba o listener SEM sub-agente (skill session-share-listener:session-listen, "
    "seção \"Como funciona (sem LLM)\"): rode em background (Bash run_in_background) "
    "'{script} --room-id {room_id} --participant-id {participant_id}' — uma vez por "
    "máquina, antes: 'pip install -r {requirements}' — e arme a tool Monitor nesse "
    "processo (cada linha de stdout já formatada vira notificação sozinha; nada "
    "impresso em poll vazio, nenhum turno de LLM gasto). Monitor expira em até 30min: "
    "rearme quando notificar; pra parar de vez, TaskStop no processo. NUNCA fique em "
    "loop de session_poll em foreground: isso trava a conversa do usuário."
)

# Fallback: python3/mcp indisponíveis no ambiente de quem for rodar o script, ou
# preferência explícita pelo sub-agente — mesmo texto de antes do cenário 3.
CONTEXT_AGENT_FALLBACK = (
    " Se `python3`/o pacote `mcp` não estiverem disponíveis aqui, use o fallback: "
    "Agent tool (subagent_type general-purpose, não fork, em background) usando "
    "como prompt, SEM EDITAR, o campo listener_prompt devolvido por {tool} — ele já "
    "vem preenchido com room_id, participant_id e display_name. O listener só avisa "
    "(pergunta/handoff com destaque; fyi/conclusao breve; ack e sistema em uma "
    "linha) — responder, dar message_ack e decidir continua sendo seu papel."
)

# servidor antigo, sem listener_prompt na resposta: o modelo preenche o template da skill
CONTEXT_FALLBACK = (
    " Esta resposta NÃO trouxe listener_prompt (servidor antigo): use o template da "
    "skill session-share-listener:session-listen preenchendo room_id={room_id}, "
    "participant_id={participant_id}, display_name={display_name}."
)

CONTEXT_TAIL = " Depois avise o usuário que o listener está rodando e como parar. Nunca revele o participant_id na room."

CONTEXT_PROMPT_HEADER = "\n\nlistener_prompt (fallback — use exatamente este texto como prompt do agente):\n"


def _as_dict(resp):
    """Normaliza tool_response: dict direto, JSON em string, envelope MCP
    ({content: [...], structuredContent: {...}}) ou lista de content blocks."""
    if isinstance(resp, str):
        try:
            return _as_dict(json.loads(resp))
        except ValueError:
            return {}
    if isinstance(resp, list):
        merged = {}
        for item in resp:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                merged.update(_as_dict(item["text"]))
            elif isinstance(item, dict):
                merged.update(_as_dict(item))
        return merged
    if isinstance(resp, dict):
        for key in ("structuredContent", "structured_content"):
            if isinstance(resp.get(key), dict):
                return resp[key]
        if isinstance(resp.get("content"), list):
            inner = _as_dict(resp["content"])
            if inner:
                return inner
        return resp
    return {}


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except ValueError:
        return 0
    if not isinstance(payload, dict):
        return 0

    tool_name = payload.get("tool_name") or ""
    if not isinstance(tool_name, str) or not tool_name.endswith(TOOL_SUFFIXES):
        return 0  # o matcher já filtra; isto é só defesa extra

    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        tool_input = {}

    resp = _as_dict(payload.get("tool_response"))
    if resp.get("isError") or resp.get("is_error"):
        return 0  # tool falhou (ROOM_NOT_FOUND, ROOM_FULL...) — sem listener pra disparar

    participant_id = resp.get("participant_id")
    room_id = resp.get("room_id") or tool_input.get("room_id")
    if not (isinstance(participant_id, str) and participant_id):
        return 0
    if not (isinstance(room_id, str) and room_id):
        return 0

    display_name = tool_input.get("display_name")
    if not (isinstance(display_name, str) and display_name):
        display_name = "Claude"  # default do servidor

    plugin_root = os.environ.get("CLAUDE_PLUGIN_ROOT")
    fields = {
        "room_id": room_id,
        "participant_id": participant_id,
        "display_name": display_name,
        "tool": tool_name.rsplit("__", 1)[-1],
        "script": f"{plugin_root}/scripts/session_poll_watch.py" if plugin_root else "",
        "requirements": f"{plugin_root}/scripts/requirements.txt" if plugin_root else "",
    }
    context = CONTEXT_HEAD.format(**fields)
    if plugin_root:
        context += CONTEXT_NO_LLM.format(**fields)
    context += CONTEXT_AGENT_FALLBACK.format(**fields)
    context += CONTEXT_TAIL

    listener_prompt = resp.get("listener_prompt")
    if isinstance(listener_prompt, str) and listener_prompt.strip():
        context += CONTEXT_PROMPT_HEADER + listener_prompt.strip()
    else:
        context += CONTEXT_FALLBACK.format(**fields)
    out = {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": context}}
    sys.stdout.write(json.dumps(out, ensure_ascii=False))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
