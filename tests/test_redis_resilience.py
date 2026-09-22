"""
Testes de SPEC-redis-resilience (CAP-1): retry/backoff no cliente Redis
(configurado em SessionStore.__init__) e StorageUnavailableError como erro
de domínio residual, quando o Redis continua inacessível mesmo depois do
retry se esgotar.

Roda contra o mesmo Redis real dos outros módulos de teste (ver
tests/conftest.py) para o caminho feliz. O teste do caminho residual NÃO
derruba esse Redis real — aponta uma SessionStore separada pra uma porta
que nunca teve Redis nenhum (ver limitação documentada no docstring do
teste abaixo).
"""
import pytest

from app.redis_store import SessionStore, StorageUnavailableError

from conftest import _make_room

pytestmark = pytest.mark.usefixtures("dev_mode_no_auth")

# Porta alta, sem nada escutando nela em nenhum ambiente de CI/local
# razoável — usada só para simular "Redis inacessível" (connection
# refused), nunca para tentar falar com um Redis de verdade.
_UNREACHABLE_REDIS_URL = "redis://127.0.0.1:65100/0"


async def test_poll_messages_normal_behavior_unchanged(store: SessionStore):
    """O retry/backoff configurado no __init__ não muda o caminho feliz:
    uma chamada comum de poll_messages contra o Redis real do teste
    continua funcionando exatamente como antes (SPEC-redis-resilience
    Non-goals: não muda MAX_POLL_TIMEOUT_SECONDS nem a semântica do XREAD
    BLOCK, só a resiliência da conexão por baixo)."""
    room_id, (pid,) = await _make_room(store, n_participants=1)
    # drena o system event de room_created emitido pelo create_room
    await store.poll_messages(room_id=room_id, participant_id=pid, timeout_seconds=1)

    result = await store.poll_messages(room_id=room_id, participant_id=pid, timeout_seconds=1)
    assert result["room_status"] == "open"
    assert result["messages"] == []


async def test_poll_messages_storage_unavailable_never_raw_redis_error():
    """CAP-1 — caminho residual: depois do retry configurado (3 tentativas,
    ExponentialBackoff base=0.5s/cap=3s) se esgotar, poll_messages precisa
    falhar com StorageUnavailableError (mensagem STORAGE_UNAVAILABLE) —
    nunca com uma exceção crua de redis.exceptions subindo até quem chamou
    a tool.

    Limitação assumida (documentada no relatório da implementação): isto
    simula "Redis nunca existiu nesta porta", não uma reconexão de fato a
    uma conexão que caiu no meio de um poll em andamento — cobre o caminho
    "esgotou o retry e virou erro de domínio", não o caminho "a conexão
    caiu e depois voltou, e o cliente reconectou sozinho" (esse exigiria
    derrubar/religar um container Redis de verdade no meio do teste, o que
    o teste acima e a suíte normal já não fazem por padrão).
    """
    broken_store = SessionStore(redis_url=_UNREACHABLE_REDIS_URL)
    try:
        with pytest.raises(StorageUnavailableError) as exc_info:
            await broken_store.poll_messages(
                room_id="does-not-exist", participant_id="does-not-exist", timeout_seconds=1
            )
        assert "STORAGE_UNAVAILABLE" in str(exc_info.value)
    finally:
        await broken_store.close()
