"""SPEC-config-env-validation: env vars inteiras de app/config.py falham com
RuntimeError legível (nome + valor), não com ValueError cru de traceback.

Cada caso importa `app.config` num subprocesso isolado, não in-process: o
módulo já foi importado uma vez por tests/conftest.py e é compartilhado por
toda a suíte (inclusive via monkeypatch em outros testes) — um
`importlib.reload` aqui vazaria o estado alterado (env inválida, exceção na
importação) para os demais módulos de teste que rodam na mesma sessão do
pytest. Subprocesso evita esse acoplamento sem precisar desfazer o reload no
fim do teste.
"""
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _import_config(extra_env: dict[str, str]) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "PARTICIPANT_HASH_KEY": "test-participant-hash-key-not-secret",
        "AUTH_ENABLED": "false",
        **extra_env,
    }
    return subprocess.run(
        [sys.executable, "-c", "import app.config"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_env_var_valida_continua_funcionando():
    """Uma env var inteira válida importa app.config normalmente, como hoje."""
    result = _import_config({"SESSION_MAX_TTL_SECONDS": "3600"})
    assert result.returncode == 0, result.stderr


def test_env_var_nao_numerica_levanta_runtime_error_com_nome_e_valor():
    """Um valor não-numérico (ex: '2h' em vez de segundos) produz um
    RuntimeError citando a variável e o valor recebidos, não um ValueError
    cru de traceback do int()."""
    result = _import_config({"SESSION_MAX_TTL_SECONDS": "2h"})
    assert result.returncode != 0
    assert "ValueError" not in result.stderr
    assert "RuntimeError" in result.stderr
    assert "SESSION_MAX_TTL_SECONDS" in result.stderr
    assert "2h" in result.stderr


def test_outra_env_var_nao_numerica_tambem_e_coberta():
    """Cobertura de mais uma das ~12 constantes do SPEC (não só a primeira),
    incluindo uma que já tem clamp (max(...)) ao redor — o clamp não deve
    esconder o erro de parsing."""
    result = _import_config({"SESSION_MAX_POLL_TIMEOUT": "abc"})
    assert result.returncode != 0
    assert "RuntimeError" in result.stderr
    assert "SESSION_MAX_POLL_TIMEOUT" in result.stderr
    assert "abc" in result.stderr
