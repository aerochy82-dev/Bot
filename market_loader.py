"""
market_loader.py
Ambil daftar SEMUA pair yang sedang TRADING di OKX secara otomatis,
dipakai kalau salah satu flag ALL_MARKET_*_OKX di config.py (bagian 3b)
diaktifkan.

Fork OKX-ONLY (21 Sept 2026): fungsi fetch_binance_spot_symbols() dan
fetch_binance_futures_symbols() (dan cabang Binance di resolve_all_markets())
sudah DIHAPUS -- lihat catatan FORK OKX-ONLY di bot.py.

Dipanggil SEKALI lewat resolve_all_markets(), di awal TradingBot.__init__()
(bot.py) -- SEBELUM validasi overlap symbol dan pembuatan PairState.
Hasil fetch-nya di-assign BALIK ke config.SYMBOLS_*_OKX (menimpa isi
module config di memory, bukan file config.py-nya), supaya SEMUA kode
lain di bot.py yang baca `config.SYMBOLS_...` apa adanya otomatis ikut
pakai daftar hasil fetch ini -- tidak perlu ubah kode di tempat lain
sama sekali.

OKX diambil lewat endpoint PUBLIK (tanpa API key/secret), jadi tetap bisa
jalan walau OKX_API_KEY/SECRET/PASSWORD belum diisi -- kredensial OKX
baru divalidasi belakangan di TradingBot.__init__() kalau ternyata hasil
fetch-nya tidak kosong.
"""

import logging

import requests

logger = logging.getLogger("trading_bot")

OKX_BASE_URL = "https://www.okx.com"
_HTTP_TIMEOUT = 10


def _rank_and_limit(symbol_volume_pairs, max_symbols):
    """symbol_volume_pairs: list [(symbol, quote_volume_24h), ...].
    Urutkan volume DESC (paling likuid duluan), potong ke max_symbols
    kalau diisi, balikin cuma daftar symbol-nya saja."""
    symbol_volume_pairs.sort(key=lambda pair: pair[1], reverse=True)
    if max_symbols:
        symbol_volume_pairs = symbol_volume_pairs[:max_symbols]
    return [sym for sym, _ in symbol_volume_pairs]


def _excluded(base_asset: str, exclude_keywords) -> bool:
    base_upper = base_asset.upper()
    return any(kw.upper() in base_upper for kw in exclude_keywords)


# -------------------------------------------------------------------- OKX --

def fetch_okx_symbols(inst_type: str, quote: str, exclude_keywords, max_symbols):
    """inst_type: 'SPOT' atau 'SWAP' (perpetual futures). Pakai REST
    publik OKX (tanpa auth), independen dari okx_client.py."""
    resp = requests.get(
        f"{OKX_BASE_URL}/api/v5/public/instruments",
        params={"instType": inst_type},
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") not in (None, "0"):
        raise RuntimeError(f"OKX instruments API error: {payload.get('msg', payload)}")
    data = payload.get("data", [])

    quote_field = "quoteCcy" if inst_type == "SPOT" else "settleCcy"

    def base_of(inst_id: str) -> str:
        return inst_id.split("-")[0]

    candidates = [
        d["instId"] for d in data
        if d.get("state") == "live"
        and d.get(quote_field) == quote
        and not _excluded(base_of(d["instId"]), exclude_keywords)
    ]
    if not candidates:
        return []

    volumes = {}
    try:
        tresp = requests.get(
            f"{OKX_BASE_URL}/api/v5/market/tickers",
            params={"instType": inst_type},
            timeout=_HTTP_TIMEOUT,
        )
        tresp.raise_for_status()
        for t in tresp.json().get("data", []):
            volumes[t["instId"]] = float(t.get("volCcy24h") or 0.0)
    except Exception as e:
        logger.warning(f"market_loader: gagal ambil volume 24h OKX {inst_type} ({e}), urutan tidak dibatasi volume.")

    ranked = [(sym, volumes.get(sym, 0.0)) for sym in candidates]
    return _rank_and_limit(ranked, max_symbols)


# --------------------------------------------------------------- WIRING --

def resolve_all_markets(config):
    """Cek flag ALL_MARKET_*_OKX di config -- timpa config.SYMBOLS_*_OKX
    (dan ALL_SYMBOLS_OKX) kalau flag-nya True. Tidak melakukan apa-apa
    (no-op) kalau SEMUA flag False -- SYMBOLS_* manual di config.py
    dipakai apa adanya seperti sebelumnya.

    Parameter `binance_client` dari versi asli (multi-exchange) sudah
    DIHAPUS -- caller di bot.py sekarang cukup panggil
    resolve_all_markets(config), tanpa client apa pun."""

    quote = getattr(config, "ALL_MARKET_QUOTE", "USDT")
    exclude_keywords = getattr(config, "ALL_MARKET_EXCLUDE_KEYWORDS", [])
    max_symbols = getattr(config, "ALL_MARKET_MAX_SYMBOLS", None)

    any_flag = any([
        getattr(config, "ALL_MARKET_SPOT_OKX", False),
        getattr(config, "ALL_MARKET_FUTURES_OKX", False),
    ])
    if not any_flag:
        return

    if getattr(config, "ALL_MARKET_SPOT_OKX", False):
        symbols = fetch_okx_symbols("SPOT", quote, exclude_keywords, max_symbols)
        logger.info(f"[ALL MARKET] OKX Spot: {len(symbols)} pair ({quote}) di-load otomatis.")
        config.SYMBOLS_SPOT_OKX = symbols

    if getattr(config, "ALL_MARKET_FUTURES_OKX", False):
        symbols = fetch_okx_symbols("SWAP", quote, exclude_keywords, max_symbols)
        logger.info(f"[ALL MARKET] OKX Futures (SWAP): {len(symbols)} pair ({quote}) di-load otomatis.")
        config.SYMBOLS_FUTURES_OKX = symbols

    # Sinkronkan ulang ALL_SYMBOLS_OKX -- dipakai di beberapa tempat lain
    # di config.py/bot.py sebagai gabungan spot+futures.
    config.ALL_SYMBOLS_OKX = config.SYMBOLS_SPOT_OKX + config.SYMBOLS_FUTURES_OKX
