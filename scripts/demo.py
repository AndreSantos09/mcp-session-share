#!/usr/bin/env python3
"""
Demo cinematográfica do mcp-session-share.

Simula o fluxo completo entre dois agentes (Alice e Bob) chamando as tools do
servidor diretamente — sem precisar de dois clientes MCP reais. Serve para
gravar um GIF (ver scripts/demo.tape) e para entender o protocolo lendo um
arquivo só.

Uso:
    docker run --rm -d -p 6379:6379 redis:7-alpine
    AUTH_ENABLED=false REDIS_URL=redis://localhost:6379/0 python3 scripts/demo.py
"""

import asyncio
import os
import sys
import time

# Garante import de `app` rodando de qualquer lugar.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("AUTH_ENABLED", "false")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from app.redis_store import SessionStore  # noqa: E402

# --- estética ---------------------------------------------------------------
RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
RED = "\033[31m"

PACE = float(os.environ.get("DEMO_PACE", "0.9"))  # segundos entre passos


def pause(mult: float = 1.0) -> None:
    time.sleep(PACE * mult)


def step(n: int, title: str) -> None:
    print(f"\n{BOLD}{CYAN}[{n}] {title}{RESET}")
    pause()


def alice(msg: str) -> None:
    print(f"  {MAGENTA}Alice ▸{RESET} {msg}")
    pause(0.7)


def bob(msg: str) -> None:
    print(f"  {BLUE}Bob   ▸{RESET} {msg}")
    pause(0.7)


def server(msg: str) -> None:
    print(f"  {DIM}server · {msg}{RESET}")
    pause(0.5)


def ok(msg: str) -> None:
    print(f"  {GREEN}ok{RESET} {msg}")
    pause(0.6)


async def main() -> None:
    store = SessionStore()
    print(f"{BOLD}mcp-session-share — demo do fluxo{RESET}")
    print(f"{DIM}dois agentes, contas diferentes, um canal MCP{RESET}")
    pause()

    # 1. Alice cria a room
    step(1, "Alice cria uma room")
    alice("session_share(display_name='Alice')")
    room = await store.create_room(display_name="Alice", ttl_seconds=3600)
    room_id = room["room_id"]
    alice_id = room["participant_id"]
    server(f"room criada · room_id = {YELLOW}{room_id}{RESET}")
    ok(f"Alice guarda o participant_id em segredo")

    # 2. Alice gera um convite
    step(2, "Alice gera um convite de uso único")
    alice(f"session_invite(room_id='{room_id}')")
    invite = await store.create_invite(room_id, alice_id)
    join_code = invite["join_code"]
    server(f"join_code = {YELLOW}{join_code}{RESET} {DIM}(TTL 10 min, uso único){RESET}")
    ok("Alice manda o room_id + join_code pro Bob por um canal confiável")

    # 3. Bob entra (fica pendente)
    step(3, "Bob entra — mas fica pendente até aprovação")
    bob(f"session_join(room_id='{room_id}', join_code='...')")
    pending = await store.join_room(room_id, "Bob", join_code=join_code)
    bob_id = pending["participant_id"]
    server(f"Bob está {YELLOW}pending{RESET} · invisível pros outros")

    # 4. Alice aprova
    step(4, "Alice vê o pedido e aprova")
    status = await store.status(room_id, alice_id)
    bob_hash = status["pending"][0]["participant_hash"]
    alice(f"session_approve(target_hash='{bob_hash}')")
    await store.approve_participant(room_id, alice_id, bob_hash)
    server(f"{GREEN}Bob aprovado{RESET} · identidade referida por hash, nunca pelo id real")

    # 5. Conversa
    step(5, "Os dois agentes conversam em tempo real")
    alice("session_send('Bob, tá vendo o deploy quebrado em staging?')")
    m1 = await store.send_message(room_id, alice_id, "Bob, tá vendo o deploy quebrado em staging?")
    poll = await store.poll_messages(room_id, bob_id, timeout_seconds=2)
    item = poll["messages"][-1]
    server(
        f"Bob recebe envelope · origin={item['origin']} "
        f"{RED}untrusted={item['untrusted']}{RESET}"
    )
    bob("session_send('Tô. É o healthcheck. Mando o patch?', in_reply_to=...)")
    await store.send_message(
        room_id, bob_id, "Tô. É o healthcheck. Mando o patch?", in_reply_to=m1["message_id"]
    )
    await store.poll_messages(room_id, alice_id, timeout_seconds=2)
    ok("conteúdo de cada agente chega isolado em content{} — nunca como instrução")

    # 6. Encerrar
    step(6, "Alice exporta o transcript e sai")
    export = await store.export_transcript(room_id, alice_id)
    lines = export["transcript_markdown"].strip().splitlines()
    server(f"transcript · {len(lines)} linhas em markdown")
    await store.close_room(room_id, alice_id)
    await store.close_room(room_id, bob_id)
    server(f"último a sair encerra a room {DIM}(antes mesmo do TTL){RESET}")

    print(f"\n{BOLD}{GREEN}✓ fluxo completo.{RESET} {DIM}github.com/AndreSantos09/mcp-session-share{RESET}\n")
    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
