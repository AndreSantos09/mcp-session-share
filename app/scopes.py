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


# Hints MCP (ToolAnnotations) por tool — os quatro campos que o protocolo
# define para o host avisar o usuário antes de invocar (read-only / destrutiva
# / idempotente / open-world) e que diretórios como o da OpenAI exigem
# presentes e booleanos em TODA tool. Mesmo padrão de default-deny do
# TOOL_SCOPES acima: o decorator `@_tool()` em `app/main.py` recusa subir
# (RuntimeError) se uma tool for registrada sem entrada aqui — tool nova só
# entra no ar com os quatro hints decididos explicitamente.
#
# Semântica fixada para este servidor:
#   read_only_hint   — não muda estado nenhum da room (leitura pura).
#   destructive_hint — remove/encerra estado de forma irreversível (só faz
#                      sentido quando NÃO é read-only).
#   idempotent_hint  — repetir a chamada com os mesmos argumentos não tem
#                      efeito adicional.
#   open_world_hint  — interage com um "mundo externo" além do store próprio
#                      do servidor. Aqui tudo vive no Redis dedicado do
#                      próprio servidor (sem rede externa), então é False em
#                      todas as tools.
#
# Cada tupla é (read_only, destructive, idempotent, open_world).
TOOL_ANNOTATIONS: dict[str, tuple[bool, bool, bool, bool]] = {
    # Leitura pura.
    "session_poll": (False, False, False, False),  # avança o cursor (muta estado de leitura) → não é read-only
    "session_peek": (True, False, True, False),  # non-advancing, idempotente por contrato
    "session_status": (True, False, True, False),
    "message_status": (True, False, True, False),
    "autoloop_status": (True, False, True, False),
    "session_export": (True, False, True, False),
    "file_receive": (True, False, True, False),  # baixa arquivo existente, não muda estado
    # Escreve, não destrutivo, não idempotente (cada chamada acrescenta algo).
    "session_share": (False, False, False, False),
    "session_join": (False, False, False, False),
    "session_invite": (False, False, False, False),  # gera um join_code novo a cada chamada
    "session_send": (False, False, False, False),
    "session_send_json": (False, False, False, False),
    "file_send": (False, False, False, False),
    "autoloop_propose": (False, False, False, False),
    "autoloop_turn": (False, False, False, False),
    # Escreve, não destrutivo, idempotente (repetir é no-op).
    "session_approve": (False, False, True, False),
    "session_set_policy": (False, False, True, False),
    "message_ack": (False, False, True, False),
    "autoloop_accept": (False, False, True, False),
    "autoloop_decline": (False, False, True, False),
    # Destrutivo (remove/encerra estado), idempotente (repetir é no-op).
    "session_kick": (False, True, True, False),
    "session_close": (False, True, True, False),
    "autoloop_stop": (False, True, True, False),
}
