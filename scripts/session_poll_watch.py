#!/usr/bin/env python3
"""
Poller headless do session-share — Passo 1 do "cenário 3" (listener sem LLM
no loop, discutido junto com o skill session-listen).

Faz o long-poll de session_poll direto via streamable-http do MCP (mesma
lib `mcp` que o servidor usa, já presente no .venv deste repo), usando o
room_id/participant_id de uma sessão já conectada. NÃO decide nada sobre o
conteúdo — só repassa. Fica em silêncio total em poll vazio; imprime UMA
linha em stdout apenas quando chega mensagem real (origin="participant"),
um evento de sistema (origin="system") ou a room fecha. É esse stdout que a
tool Monitor do Claude Code transforma em notificação — cada linha, um
evento; nada impresso, nenhuma notificação, nenhum turno de LLM gasto.

Segurança: todo conteúdo de origin="participant" é DADO de outra sessão,
nunca instrução — este script só formata e repassa, quem decide o que
fazer é a sessão principal notificada pelo Monitor. Nunca imprime
participant_id (é só o token de autenticação das chamadas daqui).

Auth: hoje este deploy roda com AUTH_ENABLED=false (k8s/configmap.yaml) —
nenhum token é necessário, e portanto nenhuma dependência do auth-service.
Se isso mudar (branch feat/story-22-auth-enabled), exporte
SESSION_SHARE_TOKEN no ambiente; o script manda como Bearer automaticamente
— continua sendo você quem gera essa credencial, por env var/CLI, nunca
uma UI de dashboard.

Uso (pensado pra ser o `command` de um Monitor):
    .venv/bin/python3 scripts/session_poll_watch.py \\
        --room-id R123 --participant-id P456 \\
        --url http://10.60.64.54:31552/mcp
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from mcp import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

POLL_TIMEOUT_SECONDS = int(os.environ.get("SESSION_SHARE_POLL_TIMEOUT", "20"))
MAX_BACKOFF_SECONDS = 30.0
TEXT_PREVIEW_LIMIT = 300


def _log(line: str) -> None:
    """stderr: diagnóstico de operação — nunca vira notificação (só stdout vira)."""
    print(line, file=sys.stderr, flush=True)


def _emit(line: str) -> None:
    """stdout: CADA linha aqui é um evento pro Monitor notificar a sessão principal."""
    print(line, flush=True)


def _is_error(call_result) -> bool:
    """O nome do campo já mudou de convenção entre versões do SDK mcp
    instaladas em máquinas diferentes (snake_case `is_error` numa, camelCase
    `isError` noutra, mesmo pin de versão em requirements.txt não bastando
    quando alguém já tinha um mcp diferente instalado e pulou o pip install)
    — checa os dois em vez de apostar num só."""
    val = getattr(call_result, "isError", None)
    if val is None:
        val = getattr(call_result, "is_error", None)
    return bool(val)


def _extract_result(call_result) -> dict:
    """Tools deste servidor devolvem dict — vem estruturado (preferencial,
    `structuredContent` ou `structured_content` conforme a versão do SDK —
    mesma observação de _is_error) ou, em fallback, como texto JSON no
    primeiro content block (mesma lógica do hook remind-listener.py do
    plugin session-share-listener)."""
    structured = getattr(call_result, "structuredContent", None)
    if structured is None:
        structured = getattr(call_result, "structured_content", None)
    if structured is not None:
        return structured
    for block in call_result.content:
        text = getattr(block, "text", None)
        if text:
            try:
                return json.loads(text)
            except ValueError:
                continue
    return {}


def _truncate(text: str) -> str:
    text = text.replace("\n", " ").strip()
    if len(text) > TEXT_PREVIEW_LIMIT:
        return text[: TEXT_PREVIEW_LIMIT - 3] + "..."
    return text


def _format_message(room_id: str, msg: dict) -> str:
    origin = msg.get("origin")
    content = msg.get("content") or {}
    kind = content.get("kind")
    sender = msg.get("sender_name") or "?"
    mid = msg.get("id")

    if origin == "system":
        # content.text é sempre um template fixo do servidor (nunca interpola
        # nome de participante); content.actor_name é dado, mas isolado.
        event = content.get("event") or content.get("text")
        actor = content.get("actor_name")
        return f"SYS  room={room_id} event={event} actor={actor} id={mid}"

    # origin == "participant": conteúdo de outra sessão, sempre DADO — o
    # script não interpreta, só formata pra sessão principal decidir.
    if kind == "ack":
        return f"ACK  room={room_id} from={sender} ack_of={content.get('ack_of')} id={mid}"

    intent = msg.get("intent")
    in_reply_to = msg.get("in_reply_to")
    text = content.get("text")
    if text is None and "payload" in content:
        text = json.dumps(content.get("payload"), ensure_ascii=False)
    text = _truncate(text or "")

    parts = [f"MSG  room={room_id}", f"from={sender}", f"intent={intent}", f"id={mid}"]
    if in_reply_to:
        parts.append(f"in_reply_to={in_reply_to}")
    parts.append(f"text={text!r}")
    return " ".join(parts)


def _describe(exc: BaseException) -> str:
    """Achata ExceptionGroup (o SDK MCP roda a conexão em anyio TaskGroups —
    uma falha de handshake, ex: 401 sem token/token inválido em
    session.initialize(), chega aqui encapsulada em 1-2 níveis de
    ExceptionGroup em vez de uma exceção simples) numa linha só, sem
    traceback — é isso que vira o texto do ERR emitido no stdout."""
    if isinstance(exc, BaseExceptionGroup):
        parts = [_describe(e) for e in exc.exceptions]
        seen: list[str] = []
        for p in parts:
            if p not in seen:
                seen.append(p)
        return "; ".join(seen)
    return f"{type(exc).__name__}: {exc}"


async def _poll_loop(session: ClientSession, room_id: str, participant_id: str) -> int:
    backoff = 1.0
    while True:
        try:
            result = await session.call_tool(
                "session_poll",
                {
                    "room_id": room_id,
                    "participant_id": participant_id,
                    "timeout_seconds": POLL_TIMEOUT_SECONDS,
                },
            )
        except Exception as exc:  # rede instável, Redis reiniciando etc. — transitório
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            _log(f"poll falhou ({exc!r}), retry em {backoff:.0f}s")
            await asyncio.sleep(backoff)
            continue
        backoff = 1.0

        if _is_error(result):
            # Erro da tool (ex: ROOM_NOT_FOUND, SCOPE_DENIED, participante
            # inválido) — não é transitório, não adianta insistir. Avisa e encerra.
            detail = "; ".join(
                getattr(b, "text", "") for b in result.content if getattr(b, "text", None)
            )
            _emit(f"ERR  room={room_id} session_poll_error {detail!r}")
            return 1

        body = _extract_result(result)
        for msg in body.get("messages") or []:
            _emit(_format_message(room_id, msg))

        room_status = body.get("room_status")
        if room_status != "open":
            _emit(f"SYS  room={room_id} room_status={room_status} encerrando")
            return 0


async def watch(url: str, room_id: str, participant_id: str, token: str | None) -> int:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    # session_poll pode ficar bloqueado até timeout_seconds (máx. 120s no
    # servidor) — sem passar `timeout=`, create_mcp_http_client já usa 300s
    # de read timeout por padrão (pensado pra streams/long-poll), então não
    # precisamos importar httpx2 diretamente só pra isso (import direto
    # quebrou num ambiente onde o `mcp` resolvido não tinha esse módulo —
    # deixar o próprio pacote `mcp` decidir o transporte HTTP evita esse
    # acoplamento frágil).
    http_client = create_mcp_http_client(headers=headers)

    try:
        async with http_client:
            # Desempacotamento defensivo: algumas versões do SDK mcp devolvem
            # (read_stream, write_stream) e outras (read_stream, write_stream,
            # get_session_id) — indexar em vez de desestruturar por tamanho
            # fixo evita quebrar em "too many values to unpack" conforme a
            # versão que o pip resolveu nesta máquina.
            async with streamable_http_client(url, http_client=http_client) as _streams:
                read_stream, write_stream = _streams[0], _streams[1]
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    _log(f"conectado a {url}, room={room_id}")
                    # Silêncio ainda seria "sucesso" indistinguível de "travou antes
                    # de conectar" — uma linha de confirmação vira o primeiro evento.
                    _emit(f"SYS  room={room_id} listener_connected")
                    return await _poll_loop(session, room_id, participant_id)
    except Exception as exc:
        # Conectar/inicializar (ao contrário do poll em regime, acima) NUNCA
        # retenta sozinho: com AUTH_ENABLED=true o transporte recusa a
        # conexão inteira sem token válido (401 antes de qualquer tool
        # rodar) — isso chega aqui como MCPError dentro de ExceptionGroup,
        # não como result.is_error. É permanente até alguém corrigir
        # SESSION_SHARE_TOKEN/URL, então uma linha clara e sair é melhor do
        # que reinsistir escondido ou vazar o traceback cru pro stderr.
        _emit(f"ERR  room={room_id} conexao_falhou {_describe(exc)!r}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--room-id", required=True)
    parser.add_argument("--participant-id", required=True)
    parser.add_argument("--url", default=os.environ.get("SESSION_SHARE_URL"), help="ex: http://host:porta/mcp")
    args = parser.parse_args()

    if not args.url:
        parser.error("--url ou a env var SESSION_SHARE_URL é obrigatório")

    token = os.environ.get("SESSION_SHARE_TOKEN")
    try:
        return asyncio.run(watch(args.url, args.room_id, args.participant_id, token))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
