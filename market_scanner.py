"""
market_scanner.py
Scanner otomatis -- tiap SCANNER_INTERVAL_MINUTES menit, ambil top N
pair terliquid dari OKX Futures (SWAP), jalankan semua filter strategi
yang aktif (SAMA persis dengan yang dipakai bot.py buat pair biasa),
urutkan berdasarkan kekuatan sinyal, lalu tambahkan pair terpilih ke bot
lewat add_pair_live() yang sudah ada.

Fork OKX-ONLY (21 Sept 2026): dulu scanner ini juga scan Binance Futures
bersamaan (candidates gabungan, slot terpisah per exchange) -- semua
jalur Binance-nya (fetch candle, client python-binance, slot terpisah)
sudah dihapus, scanner ini sekarang murni OKX. Formula skor/threshold
sinyal SAMA SEKALI TIDAK berubah.

Dijalankan sebagai THREAD TERPISAH di dalam proses bot.py yang sudah
jalan -- bukan proses baru.

Optimasi (vs versi serial lama):
  - Fetch candle kandidat PARALEL (ThreadPoolExecutor)
  - compute_indicators HANYA SEKALI per pair (hasil dipakai deteksi
    cross + generate_signal)
  - Skip scan kandidat baru kalau slot hasil-scan sudah penuh DAN
    rotate-out mati
  - Client REST dibuat sekali per siklus, dipakai bersama worker
  - Ringkasan timing di log tiap siklus
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Optional, Tuple

import pandas as pd

import config
import market_loader
import strategy
import telegram_notifier

if TYPE_CHECKING:
    from bot import TradingBot

logger = logging.getLogger(__name__)

_scanner_thread = None
_scanner_stop_event = threading.Event()

# Penghitung berapa kali BERTURUT-TURUT tiap pair (symbol) ketemu HOLD
# (sinyal tidak aktif) di siklus rotate-out -- dipakai supaya pair yang
# BARU SAJA masuk tidak langsung dibuang di siklus pertama begitu HOLD
# sekali muncul (permintaan 20 Sept 2026: "untuk pair kondisi hold tapi
# tidak open posisi lakukan resuffle", dengan syarat HOLD harus BERTAHAN
# dulu, bukan langsung di siklus pertama). Direset ke 0 (dihapus dari
# dict) begitu sinyalnya balik aktif, begitu pair itu masuk posisi, atau
# begitu pair itu sudah tidak ada lagi di bot.pairs. Key = symbol.
_rotate_out_hold_streak: dict = {}

# Berapa worker paralel untuk fetch+analisis (REST exchange). 6 cukup
# cepat tanpa mudah kena rate-limit OKX publik.
_SCAN_WORKERS = 6


def _to_okx_bar(interval: str) -> str:
    """Konversi interval gaya Binance ('1m','5m','1h') ke format bar OKX."""
    if interval.endswith(("h", "d", "w")):
        return interval[:-1] + interval[-1].upper()
    return interval


def _interval_minutes(interval: str, default: int = 5) -> int:
    try:
        return int("".join(filter(str.isdigit, interval))) or default
    except Exception:
        return default


def _fetch_candles_okx(okx_client_inst, symbol: str, bar: str) -> pd.DataFrame:
    """Download candle OKX REST (publik)."""
    try:
        bars = okx_client_inst.get_candles(
            inst_id=symbol,
            bar=bar,
            limit=int(config.SCANNER_CANDLE_LIMIT),
        )
        rows = []
        for b in reversed(bars):  # OKX: terbaru duluan → balik chronologis
            rows.append({
                "open_time": int(b[0]),
                "open": float(b[1]),
                "high": float(b[2]),
                "low": float(b[3]),
                "close": float(b[4]),
                "volume": float(b[5]),
                "close_time": int(b[0]) + 60000,
            })
        return pd.DataFrame(rows)
    except Exception as e:
        logger.warning(f"[Scanner] Gagal fetch candle OKX {symbol} (bar {bar}): {e}")
        return pd.DataFrame()


def _detect_ema_cross_on_ind(
    df_ind: pd.DataFrame,
    interval_minutes: int = 5,
) -> Tuple[str, float]:
    """Deteksi EMA cross pada df yang SUDAH ada indikatornya.
    Cek N candle terakhir (N = interval scanner / interval candle + buffer)
    supaya cross di antara siklus tidak terlewat."""
    try:
        if len(df_ind) < 2:
            return "HOLD", 0.0
        if "ema_fast_x" not in df_ind.columns or "ema_slow_x" not in df_ind.columns:
            return "HOLD", 0.0

        n_candles = max(1, (config.SCANNER_INTERVAL_MINUTES // max(interval_minutes, 1)) + 1)

        for i in range(1, min(n_candles + 1, len(df_ind))):
            curr = df_ind.iloc[-i]
            prev = df_ind.iloc[-i - 1]
            if pd.isna(curr.get("ema_fast_x")) or pd.isna(curr.get("ema_slow_x")):
                continue
            if pd.isna(prev.get("ema_fast_x")) or pd.isna(prev.get("ema_slow_x")):
                continue

            crossed_up = (
                prev["ema_fast_x"] <= prev["ema_slow_x"]
                and curr["ema_fast_x"] > curr["ema_slow_x"]
            )
            crossed_down = (
                prev["ema_fast_x"] >= prev["ema_slow_x"]
                and curr["ema_fast_x"] < curr["ema_slow_x"]
            )
            if not crossed_up and not crossed_down:
                continue

            if getattr(config, "CONFIRM_CANDLE", True):
                if crossed_up and float(curr["close"]) <= float(curr["open"]):
                    continue
                if crossed_down and float(curr["close"]) >= float(curr["open"]):
                    continue

            signal = "BUY" if crossed_up else "SELL"
            adx_val = float(curr.get("adx", 0) or 0)
            strength = min(adx_val, 100.0)
            # Bonus: spread EMA (semakin terpisah = lebih kuat) + recency
            close = float(curr.get("close") or 1) or 1.0
            spread_pct = abs(float(curr["ema_fast_x"]) - float(curr["ema_slow_x"])) / close * 100
            strength = strength * 0.6 + min(spread_pct * 20, 40) * 0.4
            recency_bonus = (n_candles - i + 1) / n_candles * 10
            strength = round(min(strength + recency_bonus, 100.0), 4)
            return signal, strength

        return "HOLD", 0.0
    except Exception:
        return "HOLD", 0.0


def _detect_ema_cross(
    df: pd.DataFrame,
    market_type: str = None,
    interval_minutes: int = None,
) -> Tuple[str, float]:
    """Wrapper kompatibilitas: hitung indikator lalu deteksi cross."""
    try:
        if interval_minutes is None:
            interval_minutes = _interval_minutes(getattr(config, "INTERVAL", "5m"), 5)
        df_ind = strategy.compute_indicators(df, market_type=market_type)
        return _detect_ema_cross_on_ind(df_ind, interval_minutes=interval_minutes)
    except Exception:
        return "HOLD", 0.0


def _analyze_candidate(
    symbol: str,
    market_type: str,
    exchange: str,
    futures_interval: str,
    okx_bar: str,
    interval_minutes: int,
    okx_rest,
) -> Optional[Tuple[float, str, str, str, str]]:
    """Fetch + analisis SATU kandidat. Dipanggil dari worker paralel.
    Return (strength, symbol, market_type, exchange, signal) atau None."""
    try:
        df = _fetch_candles_okx(okx_rest, symbol, okx_bar)

        if df is None or len(df) < 30:
            return None

        # Satu kali compute_indicators
        df_ind = strategy.compute_indicators(df, market_type=market_type)

        # Mode spread-trigger: EMA cross event di-lewati — langsung generate_signal
        use_spread_trigger = (
            bool(getattr(config, "USE_EMA_CROSS_SPREAD_FILTER", False))
            and bool(getattr(config, "USE_PRICE_SPREAD_FILTER", False))
            and bool(getattr(config, "USE_EMA_CROSS_STRATEGY", True))
        )
        if use_spread_trigger:
            full_signal = strategy.generate_signal(
                df, market_type=market_type, indicators_df=df_ind
            )
            if full_signal == "HOLD":
                return None
            # skor dari ADX + besarnya selisih spread
            curr = df_ind.iloc[-1]
            adx_val = float(curr.get("adx", 0) or 0)
            close = float(curr.get("close") or 1) or 1.0
            ef, es = curr.get("ema_fast_x"), curr.get("ema_slow_x")
            strength = min(adx_val, 100.0)
            if pd.notna(ef) and pd.notna(es) and close > 0:
                ecs = abs(float(ef) - float(es)) / close * 100
                ps = abs(close - float(es)) / close * 100
                strength = round(min(strength + abs(ecs - ps) * 10, 100.0), 4)
            signal = full_signal
        else:
            signal, strength = _detect_ema_cross_on_ind(
                df_ind, interval_minutes=interval_minutes
            )
            if signal == "HOLD":
                return None
            full_signal = strategy.generate_signal(
                df, market_type=market_type, indicators_df=df_ind
            )
            if full_signal == "HOLD":
                logger.debug(
                    f"[Scanner] {symbol}: cross {signal} ada tapi diblokir filter strategi."
                )
                return None

        logger.info(
            f"[Scanner] ✨ {symbol} ({exchange}): sinyal {full_signal} "
            f"lolos semua filter! Skor {strength:.1f}"
        )
        return (strength, symbol, market_type, exchange, full_signal)
    except Exception as e:
        logger.warning(f"[Scanner] Gagal scan {symbol}/{exchange}: {e}")
        return None


def _pair_signal_still_active(df: pd.DataFrame, market_type: str, interval_minutes: int) -> bool:
    """True kalau pair ini MASIH akan lolos syarat masuk kalau di-scan ulang
    SEKARANG -- dipakai buat rotate-out, supaya kriteria BERTAHAN persis
    sama dengan kriteria MASUK di _analyze_candidate() (bukan lagi cek
    cross klasik terpisah yang sudah tidak nyambung kalau strategi aktif
    sekarang mode "spread-trigger").

    BUG yang diperbaiki: sebelum ini, rotate-out selalu pakai
    _detect_ema_cross() (cross klasik, cuma true PERSIS di momen cross
    baru dalam beberapa candle terakhir) walau strategi yang benar-benar
    dipakai buat MASUK-kan pair itu (mode spread-trigger, kalau
    USE_EMA_CROSS_SPREAD_FILTER + USE_PRICE_SPREAD_FILTER aktif) sinyalnya
    BISA bertahan valid berkali-kali candle tanpa perlu momen cross baru
    sama sekali. Akibatnya pair yang BARU SAJA ditambahkan (lolos syarat
    spread-trigger) langsung gagal cek rotate-out (tidak ada cross klasik)
    dan dibuang lagi DALAM SIKLUS YANG SAMA -- pola "add lalu langsung
    dirotasi keluar" yang terlihat di log, cuma soal timing candle mana
    yang kebetulan masih ada cross klasik segar)."""
    try:
        if df is None or len(df) < 30:
            return True  # data belum cukup -- jangan buang, biarkan siklus berikut yang cek ulang
        df_ind = strategy.compute_indicators(df, market_type=market_type)
        use_spread_trigger = (
            bool(getattr(config, "USE_EMA_CROSS_SPREAD_FILTER", False))
            and bool(getattr(config, "USE_PRICE_SPREAD_FILTER", False))
            and bool(getattr(config, "USE_EMA_CROSS_STRATEGY", True))
        )
        if use_spread_trigger:
            # SAMA PERSIS dengan cabang spread-trigger di _analyze_candidate().
            full_signal = strategy.generate_signal(df, market_type=market_type, indicators_df=df_ind)
            return full_signal != "HOLD"
        # SAMA PERSIS dengan cabang klasik di _analyze_candidate(): butuh
        # cross klasik segar DAN tetap lolos semua filter strategi.
        cross_sig, _ = _detect_ema_cross_on_ind(df_ind, interval_minutes=interval_minutes)
        if cross_sig == "HOLD":
            return False
        full_signal = strategy.generate_signal(df, market_type=market_type, indicators_df=df_ind)
        return full_signal != "HOLD"
    except Exception:
        return True  # error teknis -- jangan buang pair cuma karena gagal cek, biar aman


def _permanent_symbols() -> set:
    return (
        set(getattr(config, "SYMBOLS_FUTURES_OKX", []) or [])
        | set(getattr(config, "SYMBOLS_SPOT_OKX", []) or [])
    )


def run_scanner_cycle(bot: "TradingBot"):
    """SATU siklus scan -- dipanggil tiap SCANNER_INTERVAL_MINUTES menit."""
    t0 = time.time()
    logger.info("[Scanner] Memulai siklus scan pasar...")
    quote = getattr(config, "ALL_MARKET_QUOTE", "USDT")
    exclude = getattr(config, "ALL_MARKET_EXCLUDE_KEYWORDS", [])
    n = config.SCANNER_TOP_N_SYMBOLS

    futures_interval = bot.futures_interval
    okx_bar = _to_okx_bar(futures_interval)
    interval_minutes = _interval_minutes(futures_interval, 5)

    # Batas slot hasil-scan -- lihat komentar SCANNER_MAX_ACTIVE_PAIRS_OKX
    # di config.py. Dihitung terpisah dari live_pairs yang SEDANG aktif.
    # (Riwayat: dulu ada perhitungan Binance+OKX terpisah di sini karena
    # scanner-nya multi-exchange -- fork OKX-only ini sudah menghapusnya.)
    all_permanent = _permanent_symbols()
    live_pairs_now = list(bot.pairs.items())
    live_added_okx = sum(
        1 for sym, p in live_pairs_now if sym not in all_permanent and p.exchange == "OKX"
    )
    max_active_okx = config.SCANNER_MAX_ACTIVE_PAIRS_OKX
    slots_available_okx = (
        10**9 if max_active_okx is None else max(0, int(max_active_okx) - live_added_okx)
    )
    # slots_available CUMA dipakai buat keputusan "skip total scan kalau
    # tidak ada slot kosong dan rotate-out mati" di bagian 3 di bawah.
    slots_available = slots_available_okx

    # ---- 1. Daftar kandidat (top volume) ----
    candidates = []
    try:
        okx_syms = market_loader.fetch_okx_symbols("SWAP", quote, exclude, max_symbols=n)
        candidates += [(s, "FUTURES", "OKX") for s in okx_syms]
    except Exception as e:
        logger.warning(f"[Scanner] Gagal ambil pair OKX: {e}")

    if not candidates:
        logger.warning("[Scanner] Tidak ada kandidat pair ditemukan, skip siklus ini.")
        return

    # ---- 2. Skip yang sudah aktif ----
    active_keys = {(sym, p.market_type, p.exchange) for sym, p in list(bot.pairs.items())}
    new_candidates = [c for c in candidates if c not in active_keys]

    # ---- 3. Scan kandidat (paralel) -- skip total kalau slot penuh & no rotate ----
    scored = []
    if slots_available <= 0 and not config.SCANNER_ROTATE_OUT:
        logger.info(
            f"[Scanner] Slot hasil-scan penuh (OKX {live_added_okx}/{max_active_okx}) "
            "dan rotate-out OFF -- skip scan kandidat baru."
        )
    elif not new_candidates:
        logger.info("[Scanner] Semua kandidat top-volume sudah aktif di bot.")
    else:
        okx_rest = None
        try:
            from okx_client import OKXClient
            okx_rest = OKXClient(
                api_key=getattr(config, "OKX_API_KEY", "") or "",
                api_secret=getattr(config, "OKX_API_SECRET", "") or "",
                passphrase=getattr(config, "OKX_PASSWORD", "") or "",
                demo_trading=bool(getattr(config, "OKX_DEMO_TRADING", False)),
            )
        except Exception as e:
            logger.warning(f"[Scanner] Gagal init client OKX: {e}")

        workers = min(_SCAN_WORKERS, max(1, len(new_candidates)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scan") as pool:
            futures = {
                pool.submit(
                    _analyze_candidate,
                    symbol,
                    market_type,
                    exchange,
                    futures_interval,
                    okx_bar,
                    interval_minutes,
                    okx_rest,
                ): (symbol, exchange)
                for symbol, market_type, exchange in new_candidates
            }
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    if result is not None:
                        scored.append(result)
                except Exception as e:
                    sym, ex = futures[fut]
                    logger.warning(f"[Scanner] Worker error {sym}/{ex}: {e}")

    # ---- 4. Rank & pilih ----
    scored.sort(key=lambda x: x[0], reverse=True)
    selected = (
        scored if max_active_okx is None
        else scored[: max(0, slots_available_okx)]
    )

    # ---- 5. Tambah pair terpilih ----
    added = []
    for score, symbol, market_type, exchange, signal in selected:
        ok, msg = bot.add_pair_live(symbol, market_type, exchange, score=score, source="scanner")
        if ok:
            added.append((symbol, exchange, signal, score))
            logger.info(
                f"[Scanner] ✅ {symbol} ({exchange}) ditambahkan -- "
                f"sinyal {signal}, skor {score:.2f}"
            )
        else:
            logger.warning(
                f"[Scanner] Gagal tambah {symbol} ({exchange}/{market_type}): {msg}"
            )
            bot.log_event(f"[Scanner] Gagal tambah {symbol}: {msg}", "error")

    # ---- 6. Rotate-out (pair live tanpa posisi, HOLD bertahan N siklus) ----
    # Diperkuat 20 Sept 2026 ("untuk pair kondisi hold tapi tidak open posisi
    # lakukan resuffle"): sebelumnya pair langsung dibuang di SIKLUS PERTAMA
    # begitu _pair_signal_still_active() bilang HOLD. Sekarang harus HOLD
    # bertahan SCANNER_ROTATE_OUT_HOLD_CYCLES siklus berturut-turut dulu
    # (lihat config.py) -- supaya pair yang baru masuk atau cuma sesaat HOLD
    # sebelum sinyal valid lagi tidak langsung dirotasi keluar prematur.
    removed = []
    if config.SCANNER_ROTATE_OUT:
        hold_cycles_needed = max(1, int(getattr(config, "SCANNER_ROTATE_OUT_HOLD_CYCLES", 1)))
        for sym, pair in list(bot.pairs.items()):
            if sym in all_permanent:
                continue
            if pair.in_position:
                # Baru mulai posisi -- reset streak, dihitung ulang dari nol
                # begitu posisi ini ditutup dan pair-nya flat lagi.
                _rotate_out_hold_streak.pop(sym, None)
                continue
            try:
                if pair.df is None or len(pair.df) < 30:
                    continue
                pair_interval = (
                    bot.spot_interval if pair.market_type == "SPOT" else bot.futures_interval
                )
                pair_mins = _interval_minutes(pair_interval, interval_minutes)
                # Lihat _pair_signal_still_active() -- pakai kriteria yang SAMA
                # dengan syarat MASUK (_analyze_candidate), bukan cross klasik
                # terpisah yang dulu bikin pair yang BARU ditambahkan langsung
                # dirotasi keluar lagi di siklus yang sama.
                if _pair_signal_still_active(pair.df, pair.market_type, pair_mins):
                    # Sinyal balik aktif -- reset streak, pair aman lagi.
                    _rotate_out_hold_streak.pop(sym, None)
                    continue
                streak = _rotate_out_hold_streak.get(sym, 0) + 1
                _rotate_out_hold_streak[sym] = streak
                if streak < hold_cycles_needed:
                    logger.debug(
                        f"[Scanner] {sym} HOLD siklus ke-{streak}/{hold_cycles_needed} -- "
                        "belum dirotasi, tunggu bertahan lebih lama dulu."
                    )
                    continue
                ok_rm, _ = bot.remove_pair_live(sym)
                if ok_rm:
                    removed.append(sym)
                    _rotate_out_hold_streak.pop(sym, None)
                    logger.info(
                        f"[Scanner] 🔄 {sym} dirotasi keluar (HOLD bertahan "
                        f"{streak} siklus berturut-turut tanpa posisi)."
                    )
            except Exception as e:
                logger.debug(f"[Scanner] Gagal cek rotate-out untuk {sym}: {e}")

        # Bersihkan entri streak untuk pair yang sudah tidak ada lagi di
        # bot.pairs (misal dihapus lewat jalur lain) supaya dict ini tidak
        # numpuk tanpa batas kalau bot jalan berbulan-bulan.
        current_syms = set(bot.pairs.keys())
        for stale_sym in list(_rotate_out_hold_streak.keys()):
            if stale_sym not in current_syms:
                _rotate_out_hold_streak.pop(stale_sym, None)

    # ---- 7. Notifikasi + ringkasan timing ----
    elapsed = time.time() - t0
    if added or (config.SCANNER_ROTATE_OUT and removed):
        lines = ["🔍 <b>Scanner Pasar</b>"]
        if added:
            lines.append(f"✅ Ditambahkan ({len(added)}):")
            for sym, ex, sig, sc in added:
                lines.append(f"  • {sym} ({ex}) -- {sig} (skor {sc:.0f})")
        if config.SCANNER_ROTATE_OUT and removed:
            lines.append(f"🔄 Dirotasi keluar ({len(removed)}): {', '.join(removed)}")
        telegram_notifier.send_telegram(config, "\n".join(lines))
    elif not scored:
        logger.info(
            f"[Scanner] Tidak ada cross EMA ditemukan dari "
            f"{len(new_candidates)} kandidat yang di-scan. ({elapsed:.1f}s)"
        )
    else:
        logger.info(
            f"[Scanner] {len(scored)} cross ditemukan tapi slot penuh "
            f"(OKX {live_added_okx}/{max_active_okx} slot hasil-scan terpakai). ({elapsed:.1f}s)"
        )

    if added or removed:
        logger.info(
            f"[Scanner] Siklus selesai dalam {elapsed:.1f}s -- "
            f"+{len(added)} / -{len(removed)} pair."
        )


def start_scanner(bot: "TradingBot"):
    """Mulai thread scanner. IDEMPOTENT."""
    global _scanner_thread, _scanner_stop_event
    if _scanner_thread and _scanner_thread.is_alive():
        return
    _scanner_stop_event.clear()

    def _loop():
        okx_label = (
            "TANPA BATAS" if config.SCANNER_MAX_ACTIVE_PAIRS_OKX is None
            else config.SCANNER_MAX_ACTIVE_PAIRS_OKX
        )
        logger.info(
            f"[Scanner] Thread aktif -- scan tiap {config.SCANNER_INTERVAL_MINUTES} menit, "
            f"top {config.SCANNER_TOP_N_SYMBOLS} pair, maks OKX {okx_label} aktif, "
            f"{_SCAN_WORKERS} worker paralel."
        )
        # Tunggu bot selesai load pair + candle awal
        _scanner_stop_event.wait(timeout=60)
        while not _scanner_stop_event.is_set():
            try:
                run_scanner_cycle(bot)
            except Exception as e:
                logger.error(f"[Scanner] Error tak terduga di siklus scan: {e}")
            _scanner_stop_event.wait(timeout=config.SCANNER_INTERVAL_MINUTES * 60)

    _scanner_thread = threading.Thread(target=_loop, name="MarketScanner", daemon=True)
    _scanner_thread.start()


def stop_scanner():
    """Hentikan thread scanner."""
    _scanner_stop_event.set()
    if _scanner_thread:
        _scanner_thread.join(timeout=5)
