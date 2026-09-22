"""Configuração via variáveis de ambiente."""
import os


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _int_env(name: str, default: str) -> int:
    """Lê `name` do ambiente (ou `default`) e converte para int.

    Levanta RuntimeError citando nome da variável e valor recebido em vez de
    deixar o ValueError cru do int() subir na importação do módulo — mesmo
    espírito do check manual de PARTICIPANT_HASH_KEY logo abaixo.
    """
    raw = os.environ.get(name, default)
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name}={raw!r} não é um inteiro válido") from None


REDIS_URL = os.environ.get("REDIS_URL", "redis://mcp-session-redis:6379/0")

HOST = os.environ.get("MCP_HOST", "0.0.0.0")
PORT = int(os.environ.get("MCP_PORT", "8000"))

# Host/Origin permitidos (proteção DNS-rebinding do SDK). Ex:
# "10.60.64.54:*,localhost:8000,127.0.0.1:8000"
ALLOWED_HOSTS = _split_csv(
    os.environ.get("MCP_ALLOWED_HOSTS", "localhost:8000,127.0.0.1:8000")
)
# Lista vazia = nenhuma origem via browser é aceita quando a proteção DNS-
# rebinding está ativa; clientes MCP diretos (sem header Origin, nosso caso
# normal) não são afetados por essa checagem.
ALLOWED_ORIGINS = _split_csv(os.environ.get("MCP_ALLOWED_ORIGINS", ""))
ENABLE_DNS_REBINDING_PROTECTION = (
    os.environ.get("MCP_ENABLE_DNS_REBINDING_PROTECTION", "true").lower() == "true"
)

# TTL da room (segundos). Sliding: renovado a cada send/join/poll.
DEFAULT_TTL_SECONDS = _int_env("SESSION_DEFAULT_TTL_SECONDS", str(2 * 60 * 60))
MAX_TTL_SECONDS = _int_env("SESSION_MAX_TTL_SECONDS", str(24 * 60 * 60))

# Long-poll: quanto tempo session_poll fica bloqueado esperando mensagem nova.
DEFAULT_POLL_TIMEOUT_SECONDS = _int_env("SESSION_DEFAULT_POLL_TIMEOUT", "20")
# max(..., 1): nunca deixa isso chegar a 0 — XREAD BLOCK 0 no Redis significa
# "bloqueia para sempre", não "sem timeout imediato".
# Teto elevado de 45s para 120s (SPEC-server-push CAP-1): reduz o número de
# round-trips de session_poll numa espera longa. Nenhum Ingress/proxy L7
# intermediário foi encontrado neste repo (k8s/deployment.yaml expõe o
# serviço via NodePort direto) para validar contra timeout de proxy/LB — se
# um dia existir um proxy/LB na frente com timeout de conexão HTTP menor que
# isso, valide empiricamente antes de confiar neste valor em produção.
MAX_POLL_TIMEOUT_SECONDS = max(_int_env("SESSION_MAX_POLL_TIMEOUT", "120"), 1)

# Janela de settle (SPEC-poll-settle-window, CAP-1): depois que o XREAD BLOCK
# inicial de poll_messages já retornou com >=1 entrada nova, uma segunda
# leitura curta e não-bloqueante (block=SESSION_POLL_SETTLE_MS) a partir do
# cursor já avançado tenta capturar fast-followers (eventos publicados a
# poucas dezenas de ms de distância) na MESMA resposta, evitando um round-
# trip extra de session_poll. 0 desativa (comportamento idêntico ao anterior
# a esta spec) — só entra em jogo quando o poll já teve retorno, nunca
# aumenta a latência de quem está esperando sem nenhuma mensagem nova ainda.
SESSION_POLL_SETTLE_MS = int(os.environ.get("SESSION_POLL_SETTLE_MS", "150"))

# Janela pra considerar um participante "escutando agora": last_polled_at
# grava no INÍCIO do poll, então isso precisa acomodar timeout_seconds em
# uso (senão marca is_listening=false enquanto a pessoa ainda está dentro
# de um poll longo em andamento).
PRESENCE_THRESHOLD_SECONDS = _int_env(
    "SESSION_PRESENCE_THRESHOLD_SECONDS", str(MAX_POLL_TIMEOUT_SECONDS + 10)
)

# Rate limit em session_join (tentativas por chave, por janela) — dificulta
# brute-force de room_id.
JOIN_RATE_LIMIT_ATTEMPTS = _int_env("SESSION_JOIN_RATE_LIMIT_ATTEMPTS", "20")
JOIN_RATE_LIMIT_WINDOW_SECONDS = _int_env("SESSION_JOIN_RATE_LIMIT_WINDOW", "60")

# Teto de participantes simultâneos por room. Cada participante ativo mantém
# um session_poll (long-poll) bloqueado no servidor; o pod é pequeno
# (128-256Mi/100-500m) e compartilhado por todas as rooms — sem esse teto,
# uma única room grande pode esgotar capacidade do serviço inteiro. Default
# generoso o bastante para coordenação em equipe (ex: dois ou três
# operadores + especialistas) e conservador frente à preocupação de
# conexões concorrentes.
MAX_PARTICIPANTS = _int_env("SESSION_MAX_PARTICIPANTS", "10")

# Cap de tamanho por arquivo (file_send), em bytes do arquivo original (não
# do base64 já inflado ~33%). Pod do Redis dedicado tem limite de 256Mi
# compartilhado entre todas as rooms ativas — default conservador.
MAX_FILE_SIZE_BYTES = _int_env("SESSION_MAX_FILE_SIZE_BYTES", str(10 * 1024 * 1024))

# Cap de tamanho do JSON serializado (session_send_json), em caracteres —
# espelha MAX_TEXT_LEN de session_send. Payload estruturado é para
# coordenação/controle, não transporte de arquivo (isso é papel do file_send).
MAX_JSON_PAYLOAD_LEN = _int_env("SESSION_MAX_JSON_PAYLOAD_LEN", "4000")

# Autoloop (modo autônomo entre sessões) — limites default e teto absoluto de
# turnos/tempo que uma proposta pode pedir. autoloop_propose clampa
# max_turns/max_seconds contra os hard caps, igual create_room já clampa
# ttl_seconds contra MAX_TTL_SECONDS.
AUTOLOOP_DEFAULT_MAX_TURNS = _int_env("SESSION_AUTOLOOP_MAX_TURNS", "20")
AUTOLOOP_DEFAULT_MAX_SECONDS = _int_env("SESSION_AUTOLOOP_MAX_SECONDS", "1800")
# max(..., 1): mesmo motivo do MAX_POLL_TIMEOUT_SECONDS acima — um hard cap
# misconfigurado (0/negativo) não pode colapsar todo clamp de autoloop_propose
# pra 0.
AUTOLOOP_HARD_CAP_TURNS = max(_int_env("SESSION_AUTOLOOP_HARD_CAP_TURNS", "200"), 1)
AUTOLOOP_HARD_CAP_SECONDS = max(_int_env("SESSION_AUTOLOOP_HARD_CAP_SECONDS", "14400"), 1)

KEY_PREFIX = "mcpshare"

# Autenticação de transporte (CAP-2, story 4) — ver app/auth.py. Default
# ligada: só desliga com AUTH_ENABLED=false, e isso é só para dev local (o
# processo loga um aviso a cada start nesse caso — nunca desliga por header
# ou parâmetro de tool).
AUTH_ENABLED = os.environ.get("AUTH_ENABLED", "true").lower() == "true"

# `iss`/`aud` esperados no JWT (ver seguranca.md "Formato do token"). `aud`
# é o nome exato deste servidor — outro MCP usando o mesmo serviço de auth
# como emissor tem um `aud` diferente e é recusado aqui.
AUTH_ISSUER = os.environ.get("AUTH_ISSUER", "auth-service")
AUTH_AUDIENCE = os.environ.get("AUTH_AUDIENCE", "session-share")

# JWKS do dashboard (chaves públicas ativas, por `kid`) e endpoint de
# revogações (`GET {url}?since=<epoch>` -> lista de {"jti", "exp"}), com o
# header de autenticação de serviço-a-serviço para o segundo. Sem default:
# vazio quebra logo (fail-closed já cobre a falta de sincronização, mas uma
# URL vazia é erro de configuração, não "dashboard fora do ar").
AUTH_JWKS_URL = os.environ.get("AUTH_JWKS_URL", "")
AUTH_REVOCATIONS_URL = os.environ.get("AUTH_REVOCATIONS_URL", "")
AUTH_REVOCATIONS_TOKEN = os.environ.get("AUTH_REVOCATIONS_TOKEN", "")

# Intervalo do loop de sincronização (JWKS + revogações) e leeway fixo de
# exp (30s, per seguranca.md — não configurável via env, é regra de formato).
AUTH_CACHE_SECONDS = _int_env("AUTH_CACHE_SECONDS", "60")
AUTH_JWT_LEEWAY_SECONDS = 30

# Fail-closed: sem sincronizar (JWKS + revogações, as duas) há mais que isso,
# todo token é recusado — mesmo um que seria válido offline.
AUTH_FAIL_CLOSED_AFTER_SECONDS = _int_env("AUTH_FAIL_CLOSED_AFTER_SECONDS", "300")

# Achado da revisão da story 4: um `kid` desconhecido dispara refresh
# forçado do JWKS (fora do intervalo normal de AUTH_CACHE_SECONDS) — sem um
# teto próprio, tokens com kid aleatório amplificam cada requisição não
# autenticada numa chamada HTTP ao dashboard. No máx. 1 refresh forçado a
# cada N segundos; o resto cai no fail-closed normal se o kid seguir
# desconhecido.
AUTH_FORCED_REFRESH_MIN_SECONDS = _int_env("AUTH_FORCED_REFRESH_MIN_SECONDS", "10")

# CAP-4 (story 10): convite de uso único. join_code = secrets.token_urlsafe(12);
# TTL fixo de 10 min por spec (não configurável via env — regra de formato,
# mesmo espírito de AUTH_JWT_LEEWAY_SECONDS).
INVITE_TTL_SECONDS = 600

# HMAC-SHA256(PARTICIPANT_HASH_KEY, participant_id)[:12] — usado nas tools
# que referem terceiros (session_approve/session_kick) e no participant_hash
# que session_status mostra em pending[] pro criador, pra nunca expor o
# participant_id real de ninguém. MESMA chave que o auth-service usa
# (AUTH_HASH_KEY, CAP-0) — sem isso, um hash calculado aqui não bate
# com o hash que o dashboard mostra pro operador pra identificar quem
# aprovar/expulsar. Via um secret manager em produção, nunca hardcoded.
PARTICIPANT_HASH_KEY = os.environ.get("PARTICIPANT_HASH_KEY", "")
if AUTH_ENABLED and not PARTICIPANT_HASH_KEY:
    raise RuntimeError(
        "PARTICIPANT_HASH_KEY é obrigatória com AUTH_ENABLED=true (CAP-4, story 10) — "
        "mesma chave usada pelo auth-service (AUTH_HASH_KEY, CAP-0)"
    )
