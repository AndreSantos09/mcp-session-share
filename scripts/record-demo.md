# Regravar o GIF da demo (`docs/demo.gif`)

A demo (`scripts/demo.py`) simula o fluxo completo entre dois agentes (Alice e
Bob) chamando as tools do servidor diretamente — não precisa de dois clientes
MCP reais. Rode da **raiz do repositório**.

## Pré-requisitos

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
# Docker (para o Redis) precisa estar rodando.
```

## Opção 1 — asciinema + agg (recomendado, sem dependência de browser)

```bash
brew install asciinema agg          # macOS
asciinema rec --overwrite -c "bash scripts/demo_run.sh" /tmp/demo.cast
agg --theme monokai --font-size 20 /tmp/demo.cast docs/demo.gif
```

`scripts/demo_run.sh` sobe um Redis isolado (porta 6398), roda a demo e
derruba o Redis no fim.

## Opção 2 — vhs

```bash
brew install vhs
vhs scripts/demo.tape               # escreve docs/demo.gif
```

> Nota: o `vhs` depende de um Chromium headless (via go-rod). Em alguns
> ambientes esse download falha silenciosamente e o GIF não é gerado — nesse
> caso, use a Opção 1.

## Ajustar o ritmo

A variável `DEMO_PACE` (segundos entre passos, default `0.9`) controla a
velocidade da animação:

```bash
DEMO_PACE=1.2 bash scripts/demo_run.sh   # mais devagar
DEMO_PACE=0   bash scripts/demo_run.sh   # instantâneo (para testar)
```
