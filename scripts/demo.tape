# Gera docs/demo.gif a partir da demo do fluxo.
#
# Pré-requisitos: vhs, docker, e um venv em .venv com as deps instaladas.
#   brew install vhs
#   python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
#
# Rodar da RAIZ do repositório:
#   vhs scripts/demo.tape
#
# O Redis é subido/derrubado pelo próprio tape (porta 6398, isolada).

Output "docs/demo.gif"

Set Shell zsh
Set FontSize 18
Set Width 1100
Set Height 820
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 0ms

# Sobe o Redis (visível, para garantir que subiu antes da demo).
Hide
Type "docker rm -f ss-demo-redis >/dev/null 2>&1; docker run --rm -d --name ss-demo-redis -p 6398:6379 redis:7-alpine >/dev/null; sleep 2; clear" Enter
Sleep 4s
Show

Type "AUTH_ENABLED=false REDIS_URL=redis://localhost:6398/0 DEMO_PACE=0.9 .venv/bin/python scripts/demo.py"
Sleep 500ms
Enter
Sleep 24s

Hide
Type "docker rm -f ss-demo-redis >/dev/null 2>&1; clear" Enter
Sleep 1s
Show
