"""
Filter konfirmasi TAMBAHAN pakai Claude API, jalan SETELAH sinyal
MACD + filter Tren/ADX/RSI/Volume sudah valid secara teknikal.

PENTING -- batasan yang harus dipahami:
Claude (atau LLM manapun) TIDAK memprediksi harga masa depan dan TIDAK
punya akses data pasar real-time di luar apa yang diberikan lewat prompt.
Modul ini HANYA meminta Claude menilai konsistensi angka indikator yang
SUDAH dihitung bot (MACD/tren/RSI/ADX) -- semacam sanity-check kedua, BUKAN
sumber sinyal baru dan BUKAN saran finansial.

Filter ini defaultnya MATI (config.USE_AI_CONFIRMATION = False). Kalau
dinyalakan, ini cuma lapisan TAMBAHAN di atas strategi yang sudah ada,
bukan pengganti.
"""

import json
import logging

import config

logger = logging.getLogger("trading_bot")

try:
    from anthropic import Anthropic
    _client = Anthropic(api_key=config.ANTHROPIC_API_KEY) if config.ANTHROPIC_API_KEY else None
except ImportError:
    _client = None

SYSTEM_PROMPT = """Kamu adalah pemeriksa konsistensi teknikal untuk sebuah bot trading otomatis.
Bot sudah menghitung sinyal entry berdasarkan MACD crossover, filter tren (EMA), RSI, dan ADX
secara matematis. Tugasmu HANYA menilai apakah angka-angka indikator yang diberikan KONSISTEN
dan MASUK AKAL untuk mendukung entry yang diajukan. Kamu TIDAK memprediksi arah harga di masa
depan, TIDAK punya data pasar tambahan di luar yang diberikan, dan TIDAK memberi saran finansial.

Balas HANYA dalam format JSON, tanpa teks lain, tanpa markdown code fence:
{"confirm": true atau false, "reason": "alasan singkat, maksimal 20 kata"}

Tolak (confirm: false) kalau:
- RSI sudah sangat ekstrem berlawanan arah dengan entry yang diajukan
- ADX menunjukkan tren sangat lemah (mendekati batas minimum yang sudah dilewati)
- Harga berada di sisi EMA tren yang berlawanan dengan arah entry yang diajukan
- Data yang diberikan tidak konsisten satu sama lain (misal MACD line/signal tidak sinkron dengan arah entry)

Setujui (confirm: true) kalau setup terlihat konsisten dengan sinyal yang sudah divalidasi bot."""


def confirm_signal(context: dict):
    """
    context: dict berisi symbol, arah_diajukan, harga, macd_line, macd_signal,
    ema_trend, rsi, adx (lihat TradingBot.confirm_with_ai di bot.py).
    Return: (allowed: bool, reason: str)
    """
    if not config.USE_AI_CONFIRMATION:
        return True, "Filter AI dimatikan"

    if _client is None:
        logger.warning(
            "Filter AI aktif tapi ANTHROPIC_API_KEY belum di-set (atau library 'anthropic' "
            "belum terinstall: pip install anthropic --break-system-packages)."
        )
        return config.AI_FAIL_OPEN, "AI tidak tersedia, fallback ke sinyal teknikal asli"

    user_prompt = (
        f"Pair: {context['symbol']}\n"
        f"Arah entry yang diajukan: {context['arah_diajukan']}\n"
        f"Harga saat ini: {context['harga']}\n"
        f"MACD line: {context.get('macd_line')}\n"
        f"MACD signal: {context.get('macd_signal')}\n"
        f"EMA tren: {context.get('ema_trend')}\n"
        f"RSI: {context.get('rsi')}\n"
        f"ADX: {context.get('adx')}\n\n"
        f"Apakah setup ini konsisten untuk entry {context['arah_diajukan']}?"
    )

    try:
        response = _client.messages.create(
            model=config.AI_MODEL,
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = "".join(block.text for block in response.content if hasattr(block, "text")).strip()
        text = text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        allowed = bool(data.get("confirm", False))
        reason = str(data.get("reason", ""))[:200]
        return allowed, reason
    except Exception as e:
        logger.error(f"Gagal memanggil filter AI: {e}")
        return config.AI_FAIL_OPEN, f"Error AI ({e}), fallback ke sinyal teknikal asli"
