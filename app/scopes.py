"""
CAP-3 (story 8): mapa único tool -> verbo, fonte de verdade de
`seguranca.md` ("Mapa tool → verbo (session-share)"). Tool ausente daqui é
negada por padrão — o decorator `@_tool()` em `app/main.py` recusa subir
(RuntimeError) se uma tool for registrada sem entrada aqui, e o filtro de
`tools/list` também usa este mapa para decidir o que mostrar por token.

O envelope de `session_poll` (story 11) não entra aqui — fora do escopo
desta story.
"""

TOOL_SCOPES: dict[str, str] = {
    # session-share:read — só consulta, nunca escreve na room.
    "session_poll": "session-share:read",
    # session_peek (SPEC-session-peek): mesmo verbo de session_poll — é
    # leitura pura (nunca avança cursor, nunca escreve), então o mesmo scope
    # de quem já pode ler a room também pode espiar sem consumir.
    "session_peek": "session-share:read",
    "session_status": "session-share:read",
    "message_status": "session-share:read",
    "autoloop_status": "session-share:read",
    "session_export": "session-share:read",
    # session-share:send — manda mensagem/ack na room.
    "session_send": "session-share:send",
    "message_ack": "session-share:send",
    # session-share:json — payload estruturado (coordenação agente-a-agente).
    "session_send_json": "session-share:json",
    "autoloop_turn": "session-share:json",
    # session-share:file — troca de arquivo.
    "file_send": "session-share:file",
    "file_receive": "session-share:file",
    # session-share:join — entra/sai da room.
    "session_join": "session-share:join",
    "session_close": "session-share:join",
    # session-share:autoloop — modo autônomo entre sessões.
    "autoloop_propose": "session-share:autoloop",
    "autoloop_accept": "session-share:autoloop",
    "autoloop_decline": "session-share:autoloop",
    "autoloop_stop": "session-share:autoloop",
    # session-share:admin — cria a room, ou age como dono/criador dela.
    "session_share": "session-share:admin",
    "session_invite": "session-share:admin",
    "session_approve": "session-share:admin",
    "session_kick": "session-share:admin",
    "session_set_policy": "session-share:admin",
}
