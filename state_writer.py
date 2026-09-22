"""
Menulis status bot saat ini ke file JSON (state.json), supaya bisa dibaca
oleh dashboard_server.py secara real-time. Ditulis tiap ada update harga
atau sinyal baru, jadi selalu representasi kondisi bot terkini.
"""

import json
import os
import threading
from datetime import datetime, timezone

STATE_FILE = "state.json"
STATE_FILE_TMP = "state.json.tmp"
CANDLES_FILE = "candles.json"
CANDLES_FILE_TMP = "candles.json.tmp"
LIQUIDATIONS_FILE = "liquidations.json"
LIQUIDATIONS_FILE_TMP = "liquidations.json.tmp"
MARKERS_FILE = "markers.json"
MARKERS_FILE_TMP = "markers.json.tmp"
LIVE_PAIRS_FILE = "live_pairs.json"
LIVE_PAIRS_FILE_TMP = "live_pairs.json.tmp"
CONTROL_FILE = "control.json"
CONTROL_FILE_TMP = "control.json.tmp"
PNL_FILE = "pnl_history.json"
PNL_FILE_TMP = "pnl_history.json.tmp"

# bot.py menulis state/candles dari BANYAK thread berbeda (satu callback
# WebSocket per pair, portfolio_loop, control_loop) -- tanpa lock, dua
# thread bisa saja sama-sama menulis ke file tmp yang sama secara
# bersamaan (satu men-truncate sementara yang lain masih menulis), hasilnya
# file JSON yang ke-rename bisa korup/setengah-tertulis. Itu yang bikin
# dashboard_server.py sesekali gagal parse (ready: false) dan sebelumnya
# bikin grafik candle di dashboard hilang. Lock per-file di bawah ini
# menyerialkan write ke file yang sama supaya tidak pernah saling tumpang
# tindih.
_state_lock = threading.Lock()
_candles_lock = threading.Lock()
_markers_lock = threading.Lock()
_live_pairs_lock = threading.Lock()
_control_lock = threading.Lock()
_pnl_lock = threading.Lock()
_liquidations_lock = threading.Lock()


def write_state(state: dict):
    """Tulis state secara atomic (tulis ke file tmp lalu rename) supaya
    dashboard tidak pernah membaca file yang setengah tertulis."""
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        with _state_lock:
            with open(STATE_FILE_TMP, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(STATE_FILE_TMP, STATE_FILE)
    except Exception:
        # Kalau gagal tulis state, jangan sampai bot utama ikut crash.
        pass


def write_candles(candles: list):
    """Tulis riwayat candle (OHLC) untuk ditampilkan sebagai grafik di dashboard."""
    try:
        with _candles_lock:
            with open(CANDLES_FILE_TMP, "w") as f:
                json.dump(candles, f)
            os.replace(CANDLES_FILE_TMP, CANDLES_FILE)
    except Exception:
        pass


def write_liquidations(liquidations: dict):
    """Tulis buffer liquidasi historis per symbol (dict {symbol: [event, ...]})
    -- overlay "Liquidasi Historis" di chart dashboard. Sama pola dengan
    write_candles(), atomic write biar tidak korup kalau dibaca bersamaan
    dengan penulisan."""
    try:
        with _liquidations_lock:
            with open(LIQUIDATIONS_FILE_TMP, "w") as f:
                json.dump(liquidations, f)
            os.replace(LIQUIDATIONS_FILE_TMP, LIQUIDATIONS_FILE)
    except Exception:
        pass


def write_markers(markers: dict):
    """Tulis marker posisi buka/tutup per symbol (dict {symbol: [marker, ...]}),
    buat ditampilkan sebagai panah di grafik candlestick dashboard."""
    try:
        with _markers_lock:
            with open(MARKERS_FILE_TMP, "w") as f:
                json.dump(markers, f)
            os.replace(MARKERS_FILE_TMP, MARKERS_FILE)
    except Exception:
        pass


def read_live_pairs() -> list:
    """Baca daftar pair yang ditambah LEWAT DASHBOARD saat bot jalan
    (add_pair_live()) -- PERSISTEN lintas restart, TERPISAH dari
    config.SYMBOLS_*_BINANCE/OKX (yang statis, cuma berubah kalau file
    config.py sendiri diedit manual). Format tiap entri:
    {"symbol": ..., "market_type": "SPOT"/"FUTURES", "exchange": "BINANCE"/"OKX"}.

    Return list kosong kalau file belum ada/rusak (bot baru pertama kali
    jalan, atau belum pernah ada pair yang ditambah live)."""
    try:
        with open(LIVE_PAIRS_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def write_live_pairs(pairs: list):
    """Simpan daftar pair yang ditambah lewat dashboard -- dipanggil
    SETIAP KALI ada pair baru ditambah/dihapus lewat add_pair_live()/
    remove_pair_live(), supaya tetap ada walau bot di-restart."""
    try:
        with _live_pairs_lock:
            with open(LIVE_PAIRS_FILE_TMP, "w") as f:
                json.dump(pairs, f, indent=2)
            os.replace(LIVE_PAIRS_FILE_TMP, LIVE_PAIRS_FILE)
    except Exception:
        pass


def read_control() -> dict:
    """Baca sinyal kontrol (misal 'paused') yang ditulis dashboard, dibaca bot.
    Kalau file belum ada/rusak, default aman: tidak dijeda."""
    try:
        with open(CONTROL_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"paused": False}


def write_control(control: dict):
    """Tulis sinyal kontrol dari dashboard (misal toggle pause/resume)."""
    try:
        with _control_lock:
            with open(CONTROL_FILE_TMP, "w") as f:
                json.dump(control, f)
            os.replace(CONTROL_FILE_TMP, CONTROL_FILE)
    except Exception:
        pass


def read_pnl_history() -> dict:
    """Baca akumulasi realized PnL DAN total uptime dari sesi-sesi
    SEBELUMNYA, supaya bot tidak mulai dari 0 lagi tiap kali di-restart.
    Kalau file belum ada (bot baru pertama kali jalan) atau rusak, mulai
    dari nol seperti biasa."""
    try:
        with open(PNL_FILE) as f:
            data = json.load(f)
        # Validasi minimal supaya field yang hilang/rusak tidak bikin bot.py crash
        data.setdefault("realized_pnl_quote", 0.0)
        data.setdefault("realized_pnl_by_market", {})
        data.setdefault("total_uptime_seconds", 0.0)
        return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"realized_pnl_quote": 0.0, "realized_pnl_by_market": {}, "total_uptime_seconds": 0.0}


def write_pnl_history(realized_pnl_quote: float, realized_pnl_by_market: dict, total_uptime_seconds: float = None):
    """Simpan akumulasi realized PnL (DAN opsional total uptime) ke disk --
    dipanggil setiap kali posisi ditutup, supaya kalaupun bot crash tiba-tiba
    di antara dua restart, PnL yang sudah terealisasi tidak hilang.

    total_uptime_seconds: kalau None (default, dipakai jalur close_position()
    yang cuma update PnL), nilai uptime yang SUDAH ADA di file DIPERTAHANKAN
    apa adanya (read-modify-write) -- supaya panggilan dari sini tidak
    menghapus tracking uptime yang di-update lewat jalur terpisah
    (lihat TradingBot._persist_uptime())."""
    try:
        with _pnl_lock:
            existing = {}
            try:
                with open(PNL_FILE) as f:
                    existing = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                pass
            uptime_to_save = total_uptime_seconds if total_uptime_seconds is not None else existing.get("total_uptime_seconds", 0.0)
            with open(PNL_FILE_TMP, "w") as f:
                json.dump({
                    "realized_pnl_quote": realized_pnl_quote,
                    "realized_pnl_by_market": realized_pnl_by_market,
                    "total_uptime_seconds": uptime_to_save,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }, f, indent=2)
            os.replace(PNL_FILE_TMP, PNL_FILE)
    except Exception:
        pass
