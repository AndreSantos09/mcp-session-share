"""
Lista de palavras curtas em português (ASCII, minúsculas, sem espaço) usada
para gerar room_id no estilo Diceware — fácil de falar/copiar, com entropia
suficiente para o modelo de ameaça (segredo compartilhado por fora, sessão
com TTL curto, join rate-limited).

227 palavras únicas -> ~7.83 bits/palavra. 4 palavras + sufixo numérico de
2 dígitos (0-99, ~6.64 bits) dão ~37.95 bits de entropia total por room_id.
"""

_RAW_WORDS = [
    "abelha", "abacate", "agua", "aranha", "arvore", "asa", "atalho", "aveia",
    "azul", "balde", "banana", "barco", "batata", "bode", "bola", "bolha",
    "bosque", "braco", "branco", "cabo", "cacau", "cactus", "cafe", "caixa",
    "caju", "cama", "camelo", "campo", "canela", "canoa", "capa", "carta",
    "carvao", "casa", "castelo", "cavalo", "cebola", "cedro", "cereja", "cesto",
    "chave", "chuva", "cinza", "cipo", "cobre", "coco", "coelho", "cogumelo",
    "cometa", "concha", "coral", "corvo", "cravo", "crianca", "cristal", "cubo",
    "curva", "delta", "disco", "dourado", "duna", "eco", "escada", "espelho",
    "espuma", "estrela", "faisca", "falcao", "farol", "feixe", "ferro", "figo",
    "flauta", "flecha", "flor", "floresta", "fogo", "folha", "fonte", "forte",
    "fumaca", "funil", "gaivota", "galho", "ganso", "garfo", "gatinho", "gaveta",
    "gelo", "girassol", "golfinho", "gota", "grama", "granito", "grilo", "grua",
    "guarda", "harpa", "ilha", "iman", "jade", "jaguar", "janela", "jardim",
    "jarra", "joaninha", "jornal", "lago", "lampada", "lanterna", "laranja",
    "lata", "leao", "leque", "libelula", "limao", "lince", "lontra",
    "lua", "luz", "maca", "madeira", "mala", "manga", "manta", "mapa",
    "maquina", "mar", "margarida", "martelo", "mesa", "milho", "moeda", "moinho",
    "montanha", "morango", "musgo", "neve", "nevoa", "ninho", "noz", "nuvem",
    "olho", "onda", "orvalho", "ouro", "ovelha", "paisagem", "palha", "pantera",
    "papel", "papagaio", "pardal", "passaro", "pedra", "peixe", "pena", "perola",
    "picareta", "pimenta", "pinguim", "pinha", "pinheiro", "pipoca", "planeta",
    "poco", "ponte", "porto", "praia", "prata", "prato", "prisma", "queijo",
    "quintal", "raiz", "raio", "rampa", "regua", "relogio", "riacho", "rio",
    "rocha", "roda", "roma", "sabao", "sal", "sapato", "sapo", "selva",
    "sino", "sol", "sombra", "tapete", "tartaruga", "tecla", "telha", "tempo",
    "terra", "tigre", "tijolo", "tinta", "toca", "tocha", "tomate", "topazio",
    "torre", "touro", "trave", "trigo", "trilha", "tromba", "trovao", "tucano",
    "tulipa", "tunel", "vale", "vaso", "vela", "veleiro", "vento", "verde",
    "vidro", "vinha", "violeta", "vulcao", "xarope", "zebra",
]

# Falha alto e cedo (import time) se um futuro edit introduzir um duplicado —
# sorted(set(...)) absorveria isso em silêncio e reduziria a entropia real
# do room_id sem avisar ninguém.
if len(_RAW_WORDS) != len(set(_RAW_WORDS)):
    _dupes = sorted({w for w in _RAW_WORDS if _RAW_WORDS.count(w) > 1})
    raise ValueError(f"app.wordlist: palavra(s) duplicada(s) em _RAW_WORDS: {_dupes}")

WORDS = sorted(_RAW_WORDS)
