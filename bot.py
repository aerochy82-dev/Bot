"""
Bot Trading Real-Time MULTI-PAIR untuk OKX -- SPOT & FUTURES (SWAP) BERSAMAAN.
Strategi: MACD Crossover + filter Tren/ADX/RSI/Volume | Mode: DRY_RUN (paper trading) by default.

Menjalankan strategi yang sama di banyak pair sekaligus (lihat
config.SYMBOLS_SPOT_OKX dan config.SYMBOLS_FUTURES_OKX), semuanya dipantau
lewat REST polling (okx_poll_loop) -- fork ini TIDAK PUNYA WebSocket sama sekali.

Jalankan:
    python bot.py

PERINGATAN:
Trading kripto berisiko tinggi. Bot ini adalah alat bantu, BUKAN saran
finansial. Uji dulu dengan DRY_RUN=True sebelum menggunakan dana sungguhan.
Anda bertanggung jawab penuh atas keputusan trading Anda sendiri.
"""

# Fork OKX-ONLY (21 Sept 2026) dari bot.py di folder "bot" (versi asli
# multi-exchange Binance+OKX). SEMUA kode Binance -- WebSocket
# (ThreadedWebsocketManager), REST client (binance.client.Client), dan
# dependency python-binance -- sudah DIHAPUS TOTAL di sini, bukan cuma
# dikonfigurasi kosong. run() juga sudah dirombak: dulu blocking lewat
# self.twm.join() (WebSocket Binance), sekarang heartbeat-nya thread
# okx_poll_loop() (REST polling OKX) yang dipantau dari main thread.

import glob
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from strategy_schema import STRATEGY_SETTINGS_SCHEMA, PARAM_STEPS as _SCHEMA_PARAM_STEPS
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
from flask import Flask, jsonify, request, send_from_directory

import config
import strategy
import risk_manager
import state_writer
import ai_filter
import okx_client
import market_loader
import market_scanner
import telegram_notifier
import discord_notifier
...


class WipeFileHandler(TimedRotatingFileHandler):
    """TimedRotatingFileHandler yang benar-benar WIPE TOTAL saat rotasi --
    bukan cuma membatasi jumlah arsip (perilaku default backupCount), tapi
    langsung menghapus file arsip begitu terbentuk, jadi tidak ada histori
    log lama yang tersisa sama sekali."""

    def doRollover(self):
        super().doRollover()
        for old_file in glob.glob(self.baseFilename + ".*"):
            try:
                os.remove(old_file)
            except OSError:
                pass


# ====== SETUP LOGGING ======
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        WipeFileHandler(
            config.LOG_FILE,
            when="H",
            interval=config.LOG_WIPE_HOURS,
            backupCount=0,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("trading_bot")


# ====== LOG TERSTRUKTUR (format logfmt) UNTUK EVENT TRADING PENTING ======
# TERPISAH TOTAL dari logger utama di atas -- file sendiri (METRICS_LOG_FILE),
# format sendiri (cuma "%(message)s", karena kita rakit string logfmt-nya
# sendiri lewat log_metric()), dan TIDAK ikut di-wipe otomatis kayak
# trading_bot.log (propagate=False mencegah baris ini ikut ke handler
# trading_bot.log juga -- supaya tidak dobel-tulis di dua tempat).
metric_logger = logging.getLogger("trading_bot.metrics")
metric_logger.setLevel(logging.INFO)
metric_logger.propagate = False
_metric_handler = logging.FileHandler(config.METRICS_LOG_FILE, encoding="utf-8")
_metric_handler.setFormatter(logging.Formatter("%(message)s"))
metric_logger.addHandler(_metric_handler)


def log_metric(event: str, **fields):
    """Tulis SATU baris log terstruktur format logfmt ke metrics.log:

        time=2026-09-11T10:00:00+00:00 event=position_closed symbol=BTCUSDT
        side=LONG reason=TAKE_PROFIT pnl_quote=12.5 pnl_pct=2.3

    Gampang di-parse tool/script apapun (regex sederhana per key=value),
    di-import ke spreadsheet, atau diolah lebih lanjut buat analisa
    performa trading -- beda dari trading_bot.log yang isinya kalimat
    manusia biasa (dua-duanya tetap jalan berdampingan, tidak saling
    gantikan).

    Value yang mengandung spasi otomatis dibungkus tanda kutip supaya
    tetap satu "kolom" saat di-parse. Value None ditulis sebagai string
    kosong (bukan literal "None") supaya lebih rapi dibaca tool parsing.
    """
    ts = datetime.now(timezone.utc).isoformat()
    parts = [f"time={ts}", f"event={event}"]
    for key, value in fields.items():
        value = "" if value is None else value
        value_str = str(value)
        if " " in value_str or "=" in value_str:
            value_str = f'"{value_str}"'
        parts.append(f"{key}={value_str}")
    metric_logger.info(" ".join(parts))


class PairState:
    """Menyimpan state trading untuk satu pair (candle, posisi, dll).

    market_type: "SPOT" atau "FUTURES" -- ditentukan per-pair sekarang,
    supaya SPOT dan FUTURES bisa jalan bersamaan dalam satu bot.
    exchange: "OKX" -- exchange mana pair ini ditradingkan (fork ini OKX-only).
    """

    def __init__(self, symbol: str, market_type: str, exchange: str = "OKX"):
        self.symbol = symbol
        self.market_type = market_type
        self.exchange = exchange
        self.df = pd.DataFrame()
        self.in_position = False
        self.position_side = None  # "LONG" atau "SHORT"
        self.entry_price = None
        self.stop_loss = None
        self.take_profit = None
        self.quantity = 0.0
        self.initial_quantity = 0.0  # quantity ASLI saat posisi dibuka, dipakai untuk hitung
                                       # porsi tiap level partial TP (self.quantity berkurang
                                       # tiap partial close, initial_quantity tetap)
        self.tp_levels = []  # list of dict {"price","fraction","hit","final"} kalau USE_MULTI_TP
        self.live_price = None
        # Sinyal terakhir dari generate_signal (BUY/SELL/HOLD) -- buat ticker
        # dashboard & status pair. Di-update tiap candle closed.
        self.last_signal = "HOLD"

        # --- Fitur Pro (break-even, trailing, cooldown) ---
        self.initial_risk_distance = None  # |entry-SL ASLI| saat posisi dibuka -- dipakai
                                             # sebagai satuan "R" untuk break-even & trailing,
                                             # TIDAK berubah walau SL sudah digeser
        self.breakeven_triggered = False    # cegah break-even dipicu berkali-kali per posisi
        self.entry_time = None               # datetime UTC saat posisi dibuka -- dasar cek umur posisi (reshuffle stall)
        self.last_sl_time = None            # datetime UTC terakhir kena SL -- dasar cooldown
        # Catatan (20 Sept 2026): field sl_reverse_streak (penghitung reverse
        # beruntun untuk fitur USE_SL_REVERSE) SUDAH DIHAPUS bersamaan dengan
        # try_sl_reverse() -- lihat catatan di dekat close_position() dan di
        # config.py untuk detail penghapusan fitur ini.

        # Marker posisi buka/tutup buat ditampilkan di grafik candlestick
        # dashboard (panah naik/turun persis di candle kejadiannya). Dibatasi
        # 100 entri terakhir -- cukup buat histori yang relevan ditampilkan,
        # tanpa numpuk memori tanpa batas kalau bot jalan berbulan-bulan.
        self.trade_markers = []

        # True kalau pair ini ditambah SAAT BOT JALAN lewat dashboard
        # (add_pair_live()) -- beda dari pair yang sudah dikonfigurasi sejak
        # awal di config.py. Dipakai buat tampilkan badge "BARU" di tabel
        # dashboard, murni penanda visual, tidak mempengaruhi logika trading.
        self.added_live = False


def _persist_config_changes(changes: dict):
    """Tulis perubahan setting strategi LANGSUNG ke file config.py di disk
    -- biar bertahan setelah bot restart. Pakai regex per-baris supaya
    komentar dan formatting lain di config.py TIDAK rusak.

    Dipanggil setiap kali apply_strategy_settings() berhasil menerapkan
    perubahan. Kalau gagal (misal file tidak bisa ditulis), cukup log
    warning -- bot tetap jalan dengan nilai yang sudah diubah di memori,
    hanya saja perubahan tidak akan bertahan di restart berikutnya."""
    if not changes:
        return
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.py")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        import re
        for key, value in changes.items():
            if isinstance(value, bool):
                new_val_str = str(value)
            elif isinstance(value, float):
                new_val_str = repr(value)
            elif isinstance(value, int):
                new_val_str = str(value)
            else:
                new_val_str = repr(value)

            pattern = re.compile(
                r'^(\s*' + re.escape(key) + r'\s*=\s*)([^\n#]+)(.*)',
            )
            found = False
            for i, line in enumerate(lines):
                m = pattern.match(line)
                if m:
                    lines[i] = f"{m.group(1)}{new_val_str}{m.group(3)}\n"
                    found = True
                    break
            if not found:
                logger.debug(f"_persist_config_changes: baris '{key}' tidak ditemukan di config.py, skip.")

        tmp_path = config_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        os.replace(tmp_path, config_path)
        logger.info(f"Config.py diperbarui di disk: {list(changes.keys())}")
    except Exception as e:
        logger.warning(f"Gagal tulis perubahan ke config.py: {e} -- perubahan hanya berlaku sampai restart.")


def fmt_price_log(price) -> str:
    """Format harga buat pesan log/event (logger.info & log_event) --
    JUMLAH DESIMAL ADAPTIF, bukan .2f tetap seperti sebelumnya.

    Bug lama: semua pesan log yang nampilin harga (candle closed, posisi
    dibuka/ditutup, partial TP, dry-run order) pakai format tetap
    f"{price:.2f}" -- untuk koin recehan (BOME, PEPE, BONK, NEIRO, dst
    yang harganya di bawah 1 sen) ini kepotong jadi '0.00', kelihatan
    kayak datanya rusak/nol padahal harganya beneran ada (misal BOME
    0.0008931 -> '0.00' di panel Aktivitas Terkini dashboard).

    >=1 tetap 2 desimal (konsisten sama tampilan lama buat BTC/SOL/dst),
    makin kecil harganya makin banyak desimal ditampilkan supaya tetap
    ada angka signifikan yang kelihatan."""
    if price is None:
        return "-"
    price = float(price)
    if price == 0:
        return "0.00"
    abs_p = abs(price)
    if abs_p >= 1:
        decimals = 2
    elif abs_p >= 0.01:
        decimals = 4
    elif abs_p >= 0.0001:
        decimals = 6
    else:
        decimals = 8
    return f"{price:.{decimals}f}"


class TradingBot:
    def __init__(self):
        # Waktu bot ini mulai jalan -- dicatat PALING AWAL (sebelum apapun
        # lain), dipakai dashboard buat hitung uptime "sudah berjalan berapa
        # lama". Reset ke 0 tiap kali proses di-restart (bukan akumulasi
        # lintas sesi kayak realized PnL -- uptime memang wajar mulai dari 0
        # lagi tiap restart, beda konsepnya dari PnL yang memang harus
        # nyambung).
        self.start_time = datetime.now(timezone.utc)

        # Mode "ALL MARKET" (opsional) -- kalau ada flag ALL_MARKET_*_OKX True
        # di config.py, timpa config.SYMBOLS_*_OKX dengan hasil fetch LANGSUNG
        # dari OKX di sini, SEBELUM validasi overlap di bawah (yang membaca
        # config.SYMBOLS_* apa adanya) -- kalau resolve_all_markets() dipanggil
        # SETELAH validasi, validasi itu bakal jalan dengan daftar manual yang
        # LAMA, bukan hasil fetch-nya. No-op (tidak melakukan apapun) kalau
        # semua flag ALL_MARKET_*_OKX False.
        market_loader.resolve_all_markets(config)

        okx_overlap = set(config.SYMBOLS_SPOT_OKX) & set(config.SYMBOLS_FUTURES_OKX)
        if okx_overlap:
            raise ValueError(
                f"Pair {sorted(okx_overlap)} ada di SYMBOLS_SPOT_OKX DAN SYMBOLS_FUTURES_OKX "
                "sekaligus. Hapus dari salah satu list di config.py."
            )

        # Endpoint kline FUTURES (SWAP) OKX cuma mendukung interval 1m ke atas --
        # BEDA dengan endpoint SPOT yang mendukung 1s. Kalau INTERVAL sub-menit
        # (mis. "1s") dipakai sementara SYMBOLS_FUTURES_OKX aktif, load histori
        # candle untuk SEMUA pair futures bisa gagal/tidak didukung endpoint-nya --
        # divalidasi di sini supaya errornya jelas dari awal, bukan kriptis dari
        # dalam load_history_for().
        if config.SYMBOLS_FUTURES_OKX and config.INTERVAL.endswith("s"):
            raise ValueError(
                f"INTERVAL='{config.INTERVAL}' tidak didukung untuk trading FUTURES (SWAP) di OKX "
                "(endpoint kline SWAP cuma mendukung 1m ke atas, beda dengan endpoint SPOT "
                "yang mendukung interval detik). SYMBOLS_FUTURES_OKX sedang aktif "
                f"({', '.join(config.SYMBOLS_FUTURES_OKX)}), jadi ganti INTERVAL ke '1m' atau lebih "
                "di config.py, atau kosongkan SYMBOLS_FUTURES_OKX kalau memang mau trading SPOT saja di interval detik."
            )

        # self.okx_client cuma dibuat kalau memang ada pair OKX yang
        # dikonfigurasi, supaya tidak maksa isi kredensial OKX kalau memang
        # tidak dipakai.
        self.okx_client = None
        has_okx_pairs = bool(config.SYMBOLS_SPOT_OKX or config.SYMBOLS_FUTURES_OKX)
        if has_okx_pairs:
            if not (config.OKX_API_KEY and config.OKX_API_SECRET and config.OKX_PASSWORD):
                raise ValueError(
                    "SYMBOLS_SPOT_OKX/SYMBOLS_FUTURES_OKX diisi tapi OKX_API_KEY/OKX_API_SECRET/"
                    "OKX_PASSWORD belum lengkap di .env. Generate API key di OKX > API "
                    "Management dan isi ketiganya, atau kosongkan SYMBOLS_SPOT_OKX/FUTURES_OKX kalau "
                    "memang belum mau pakai OKX."
                )
            self.okx_client = okx_client.OKXClient(
                config.OKX_API_KEY, config.OKX_API_SECRET, config.OKX_PASSWORD,
                demo_trading=config.OKX_DEMO_TRADING,
            )

        self.connected = False
        self.event_log = deque(maxlen=100)
        self.portfolio = []
        self.candles_cache = {}  # {symbol: [candle, ...]}
        self.pairs = {}
        for sym in config.SYMBOLS_SPOT_OKX:
            self.pairs[sym] = PairState(sym, "SPOT", exchange="OKX")
        for sym in config.SYMBOLS_FUTURES_OKX:
            self.pairs[sym] = PairState(sym, "FUTURES", exchange="OKX")

        # Muat pair yang PERNAH ditambah lewat dashboard di sesi
        # sebelumnya (add_pair_live()) -- PERSISTEN lintas restart,
        # TERPISAH dari config.SYMBOLS_*_OKX di atas. Kalau ada entri OKX
        # tapi self.okx_client belum sempat dibuat (config.py aslinya
        # tidak punya pair OKX sama sekali), coba buat sekarang; kalau
        # kredensial OKX ternyata tidak lengkap, SKIP entri itu dengan
        # peringatan -- JANGAN crash seluruh startup bot cuma gara-gara
        # satu entri live_pairs.json yang sudah tidak valid lagi. Entri
        # lama exchange=="BINANCE" (peninggalan dari sebelum fork OKX-only
        # ini dibuat) juga otomatis di-SKIP dengan peringatan tersendiri --
        # Binance sudah tidak didukung sama sekali di fork ini.
        for entry in state_writer.read_live_pairs():
            sym = entry.get("symbol", "")
            market_type = entry.get("market_type", "")
            exch = entry.get("exchange", "")
            if exch == "BINANCE":
                logger.warning(
                    f"Pair {sym or '?'} (dari live_pairs.json, exchange=BINANCE) di-SKIP -- "
                    "fork bot ini OKX-only, Binance sudah tidak didukung sama sekali."
                )
                continue
            if not sym or sym in self.pairs or market_type not in ("SPOT", "FUTURES") or exch != "OKX":
                continue
            if self.okx_client is None:
                if not (config.OKX_API_KEY and config.OKX_API_SECRET and config.OKX_PASSWORD):
                    logger.warning(
                        f"Pair {sym} (dari live_pairs.json sesi sebelumnya) di-SKIP -- "
                        "kredensial OKX belum lengkap di .env."
                    )
                    continue
                self.okx_client = okx_client.OKXClient(
                    config.OKX_API_KEY, config.OKX_API_SECRET, config.OKX_PASSWORD,
                    demo_trading=config.OKX_DEMO_TRADING,
                )
            pair = PairState(sym, market_type, exchange=exch)
            pair.added_live = True  # tetap tampilkan badge "BARU" di dashboard
            self.pairs[sym] = pair

        # Timeframe FUTURES -- mulai dari config.INTERVAL, tapi BISA diubah
        # runtime lewat dashboard (lihat request_interval_change()). Semua
        # kode di method LAIN pakai self.futures_interval, BUKAN config.INTERVAL
        # langsung, supaya perubahan dari dashboard benar-benar berlaku.
        # TERPISAH dari Spot (self.spot_interval) -- lihat _interval_for().
        self.futures_interval = config.INTERVAL
        self.spot_interval = config.SPOT_INTERVAL  # statis, belum ada live-switch buat ini
        self._last_candles_write = {}  # {symbol: timestamp} -- throttle update candle "live" (sedang berjalan) ke disk

        self._last_state_write = 0.0  # untuk throttle penulisan state.json (hindari overload disk I/O)
        self._empty_wallet_warned = {}  # {"OKX": bool} -- cegah spam log tiap 30 detik
        self.paused = False  # jeda GLOBAL (semua pair) -- dikontrol lewat dashboard (control.json)
        self.dry_run = config.DRY_RUN  # runtime state -- bisa diubah dari dashboard, config.DRY_RUN cuma default awal
        self._okx_last_candle_time = {}  # {symbol: open_time_ms terakhir yang sudah diproses} -- cegah duplikat polling

        # Realized PnL (dalam QUOTE_ASSET) terakumulasi tiap kali posisi ditutup --
        # ini profit/loss yang SUDAH terealisasi sejak bot ini dijalankan (reset
        # tiap restart, bukan diambil dari histori trade exchange).
        self.realized_pnl_quote = 0.0
        self.realized_pnl_by_market = {}  # {"SPOT": ..., "FUTURES": ...}
        self.last_unrealized_pnl_quote = 0.0  # cache dari save_state(), dipakai equity guard

        # Muat akumulasi realized PnL dari sesi SEBELUMNYA (pnl_history.json)
        # supaya tidak reset ke 0 tiap kali bot di-restart -- ini catatan
        # profit/loss jangka panjang, bukan cuma sesi yang sedang berjalan.
        _pnl_history = state_writer.read_pnl_history()
        self.realized_pnl_quote = _pnl_history["realized_pnl_quote"]
        self.realized_pnl_by_market = _pnl_history["realized_pnl_by_market"]
        if self.realized_pnl_quote != 0.0:
            logger.info(
                f"Realized PnL dari sesi sebelumnya dimuat: {self.realized_pnl_quote:+.4f} {config.QUOTE_ASSET}"
            )

        # Total waktu bot BERJALAN, TERAKUMULASI lintas SEMUA sesi (BEDA
        # dari self.start_time di atas, yang cuma buat hitung durasi SESI
        # INI SAJA). self.total_uptime_before_session = total detik dari
        # sesi-sesi SEBELUMNYA -- uptime TOTAL yang ditampilkan ke user =
        # ini + (waktu sekarang - self.start_time), dihitung tiap saat
        # butuh ditampilkan/disimpan (lihat _get_total_uptime_seconds()).
        self.total_uptime_before_session = _pnl_history.get("total_uptime_seconds", 0.0)
        if self.total_uptime_before_session > 0:
            _h = int(self.total_uptime_before_session // 3600)
            _m = int((self.total_uptime_before_session % 3600) // 60)
            logger.info(f"Total waktu berjalan dari sesi-sesi sebelumnya: {_h}j {_m}m")

        # Equity guard: saldo acuan buat hitung drawdown. None = belum
        # ditangkap (ditangkap sekali di refresh_portfolio() pertama).

        self.initial_balance = config.INITIAL_BALANCE if config.INITIAL_BALANCE > 0 else None

        mode = "DRY RUN (simulasi)" if self.dry_run else "LIVE (order nyata)"
        net = "OKX DEMO TRADING" if config.OKX_DEMO_TRADING else "MAINNET"
        logger.info(
            f"Bot diinisialisasi | Mode: {mode} | Jaringan: {net} | "
            f"OKX Spot: {', '.join(config.SYMBOLS_SPOT_OKX) or '-'} | "
            f"OKX Futures: {', '.join(config.SYMBOLS_FUTURES_OKX) or '-'}"
        )
        self.log_event(
            f"Bot diinisialisasi ({mode}, {net}) | {len(self.pairs)} pair total aktif",
            "info",
        )
        log_metric(
            "bot_started", mode="DRY_RUN" if self.dry_run else "LIVE", network=net,
            total_pairs=len(self.pairs), interval=self.futures_interval,
            okx_spot=len(config.SYMBOLS_SPOT_OKX), okx_futures=len(config.SYMBOLS_FUTURES_OKX),
            realized_pnl_loaded=round(self.realized_pnl_quote, 8),
        )
        telegram_notifier.notify_bot_started(config, mode, net, len(self.pairs))

        if not self.dry_run:
            for sym, pair in list(self.pairs.items()):
                if pair.market_type == "FUTURES":
                    self._setup_okx_futures(sym)

    def _setup_okx_futures(self, symbol: str):
        """Set leverage untuk satu pair Futures (SWAP) di OKX. Hanya saat LIVE."""
        try:
            self.okx_client.set_leverage(symbol, config.OKX_LEVERAGE, config.OKX_MARGIN_MODE)
            logger.info(f"[{symbol}] (OKX) Leverage diset {config.OKX_LEVERAGE}x ({config.OKX_MARGIN_MODE})")
        except Exception as e:
            logger.error(f"[{symbol}] (OKX) Gagal set leverage: {e}")

    # ---------- EVENT LOG & STATE (untuk dashboard) ----------
    def log_event(self, message: str, kind: str = "info"):
        self.event_log.appendleft({
            "time": datetime.now(timezone.utc).isoformat(),
            "message": message,
            "kind": kind,  # info, buy, sell, error
        })
        self.save_state(force=True)  # event penting (sinyal/order) selalu langsung ditulis

    def save_state(self, force: bool = False):
        """Tulis kondisi semua pair + portfolio ke state.json untuk dashboard.

        Di-throttle supaya tidak menulis ke disk di SETIAP tick harga (bisa
        sampai puluhan kali/detik dengan banyak pair) -- itu bikin I/O disk
        jadi bottleneck kalau ditulis SETIAP tick. Event penting (sinyal,
        order, dsb lewat log_event) selalu pakai force=True
        supaya tetap langsung muncul di dashboard tanpa delay.
        """
        now = time.time()
        if not force and (now - self._last_state_write) < 0.5:
            return
        self._last_state_write = now

        pairs_out = {}
        unrealized_pnl_quote = 0.0
        unrealized_pnl_by_market = {}
        for sym, pair in list(self.pairs.items()):
            price = pair.live_price
            macd_line = macd_signal = ema_trend = rsi_val = None
            ema_fast_x = ema_slow_x = None
            if len(pair.df) > 0:
                df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
                latest = df_ind.iloc[-1]
                # Kolom trigger yang ADA tergantung config.USE_EMA_CROSS_STRATEGY --
                # kalau True cuma ada ema_fast_x/ema_slow_x (BUKAN macd_line/signal),
                # kalau False sebaliknya. Cek dulu sebelum akses, supaya tidak
                # KeyError begitu USE_EMA_CROSS_STRATEGY di-toggle.
                if "macd_line" in latest and pd.notna(latest["macd_line"]):
                    macd_line = round(float(latest["macd_line"]), 6)
                if "macd_signal" in latest and pd.notna(latest["macd_signal"]):
                    macd_signal = round(float(latest["macd_signal"]), 6)
                if "ema_fast_x" in latest and pd.notna(latest["ema_fast_x"]):
                    ema_fast_x = round(float(latest["ema_fast_x"]), 6)
                if "ema_slow_x" in latest and pd.notna(latest["ema_slow_x"]):
                    ema_slow_x = round(float(latest["ema_slow_x"]), 6)
                if "ema_trend" in latest and pd.notna(latest["ema_trend"]):
                    ema_trend = round(float(latest["ema_trend"]), 4)
                if "rsi" in latest and pd.notna(latest["rsi"]):
                    rsi_val = round(float(latest["rsi"]), 2)
                if price is None:
                    price = float(latest["close"])

            pnl_pct = None
            pnl_quote = None
            if pair.in_position and price and pair.entry_price:
                if pair.position_side == "SHORT":
                    pnl_pct = round((pair.entry_price - price) / pair.entry_price * 100, 3)
                    pnl_quote = round((pair.entry_price - price) * pair.quantity, 4)
                else:
                    pnl_pct = round((price - pair.entry_price) / pair.entry_price * 100, 3)
                    pnl_quote = round((price - pair.entry_price) * pair.quantity, 4)
                unrealized_pnl_quote += pnl_quote
                unrealized_pnl_by_market[pair.market_type] = (
                    unrealized_pnl_by_market.get(pair.market_type, 0.0) + pnl_quote
                )

            liquidation_est = None
            if pair.market_type == "FUTURES" and pair.in_position and pair.entry_price:
                if pair.position_side == "SHORT":
                    liquidation_est = round(pair.entry_price * (1 + 1 / config.LEVERAGE), 2)
                else:
                    liquidation_est = round(pair.entry_price * (1 - 1 / config.LEVERAGE), 2)

            # Perubahan harga vs ~24 jam lalu (atau candle tertua kalau data < 24h).
            # Interval futures default 5m → 288 bar; spot 1h → 24 bar.
            price_change_pct = None
            if price and len(pair.df) >= 2:
                try:
                    interval = (
                        self.spot_interval if pair.market_type == "SPOT" else self.futures_interval
                    )
                    # parse "5m"/"1h"/"15m"
                    unit = interval[-1]
                    num = int(interval[:-1])
                    mins = num * (60 if unit == "h" else 1 if unit == "m" else 1)
                    bars_24h = max(1, int(24 * 60 / max(mins, 1)))
                    lookback = min(len(pair.df) - 1, bars_24h)
                    ref = float(pair.df.iloc[-1 - lookback]["close"])
                    if ref > 0:
                        price_change_pct = round((float(price) - ref) / ref * 100, 3)
                except Exception:
                    price_change_pct = None

            pairs_out[sym] = {
                "market_type": pair.market_type,
                "exchange": pair.exchange,
                "added_live": pair.added_live,
                "price": price,
                "price_change_pct_24h": price_change_pct,
                "last_signal": getattr(pair, "last_signal", "HOLD") or "HOLD",
                "macd_line": macd_line,
                "macd_signal": macd_signal,
                "ema_fast_x": ema_fast_x,
                "ema_slow_x": ema_slow_x,
                "ema_trend": ema_trend,
                "rsi": rsi_val,
                "in_position": pair.in_position,
                "position_side": pair.position_side,
                "entry_price": pair.entry_price,
                "stop_loss": pair.stop_loss,
                "take_profit": pair.take_profit,
                "quantity": pair.quantity,
                "initial_quantity": pair.initial_quantity,
                "tp_levels": pair.tp_levels,
                "pnl_pct": pnl_pct,
                "pnl_quote": pnl_quote,
                "liquidation_est": liquidation_est,
            }

        realized_pnl_by_market = {k: round(v, 4) for k, v in self.realized_pnl_by_market.items()}
        unrealized_pnl_by_market = {k: round(v, 4) for k, v in unrealized_pnl_by_market.items()}
        total_pnl_quote = round(self.realized_pnl_quote + unrealized_pnl_quote, 4)
        self.last_unrealized_pnl_quote = unrealized_pnl_quote  # cache untuk check_equity_guard()

        portfolio_quote_by_market = {
            p["market"]: p["amount"]
            for p in self.portfolio
            if p.get("asset") == config.QUOTE_ASSET and p.get("market")
        }
        if self.portfolio and self.portfolio[0].get("simulated"):
            # Breakdown per-market untuk saldo SIMULASI (DRY_RUN) -- OKX-only,
            # dulu ada cabang terpisah buat Binance (key "SPOT"/"FUTURES" tanpa
            # prefix) di sini, sudah dihapus bersih (lihat FORK OKX-ONLY di
            # kepala file).
            _has_okx_spot = any(p.exchange == "OKX" and p.market_type == "SPOT" for p in list(self.pairs.values()))
            _has_okx_futures = any(p.exchange == "OKX" and p.market_type == "FUTURES" for p in list(self.pairs.values()))
            _okx_sim_amount = self.portfolio[0]["amount"]

            portfolio_quote_by_market = {}
            if _has_okx_spot or _has_okx_futures:
                # OKX cuma punya SATU saldo unified (bukan wallet terpisah
                # spot/futures kayak Binance) -- digabung jadi SATU key
                # "OKX" (21 Sept 2026), bukan OKX_SPOT+OKX_FUTURES yang
                # dulu nilainya identik tapi kelihatan seperti 2 saldo
                # beda di dashboard (dan bikin portfolio_quote_total
                # double-count kalau dua-duanya aktif).
                portfolio_quote_by_market["OKX"] = _okx_sim_amount

        # Daftar pair per market_type dihitung dari self.pairs SEKARANG --
        # BUKAN dari config.SYMBOLS_*_OKX -- supaya pair yang ditambah/dihapus
        # LIVE lewat dashboard (add_pair_live() / remove_pair_live()) langsung
        # kelihatan di dashboard. config.SYMBOLS_* cuma daftar KONFIGURASI AWAL
        # saat bot start, bisa beda dari kondisi aktual sekarang begitu ada
        # pair yang ditambah/dihapus di tengah jalan.
        symbols_spot_okx = [s for s, p in list(self.pairs.items()) if p.exchange == "OKX" and p.market_type == "SPOT"]
        symbols_futures_okx = [s for s, p in list(self.pairs.items()) if p.exchange == "OKX" and p.market_type == "FUTURES"]

        state_writer.write_state({
            "symbols": list(self.pairs.keys()),
            # "symbols_spot"/"symbols_futures" (tanpa prefix) dulu daftar
            # Binance -- fork ini OKX-only, jadi SELALU kosong sekarang.
            # Tetap dikirim (bukan dihapus dari skema) untuk kompatibilitas
            # kalau ada dashboard frontend lama yang masih baca key ini.
            "symbols_spot": [],
            "symbols_futures": [],
            "symbols_spot_okx": symbols_spot_okx,
            "symbols_futures_okx": symbols_futures_okx,
            "interval": self.futures_interval,
            "spot_interval": self.spot_interval,
            "started_at": self.start_time.isoformat(),
            "total_uptime_before_session": self.total_uptime_before_session,
            "ema_fast_len": config.EMA_FAST_LEN,
            "ema_slow_len": config.EMA_SLOW_LEN,
            "ema_fast_len_futures": config.EMA_FAST_LEN_FUTURES,
            "ema_slow_len_futures": config.EMA_SLOW_LEN_FUTURES,
            "mode": "DRY_RUN" if self.dry_run else "LIVE",
            "network": "OKX DEMO TRADING" if config.OKX_DEMO_TRADING else "MAINNET",
            "quote_asset": config.QUOTE_ASSET,
            "simulated_balance": config.INITIAL_SIMULATED_BALANCE if self.dry_run else 0,
            # Flag strategi buat dashboard chart -- cuma overlay/oscillator
            # yang RELEVAN dengan strategy aktif yang ditampilkan.
            "chart_indicators": {
                "ema_cross": bool(config.USE_EMA_CROSS_STRATEGY),
                "ema_trend": bool(config.USE_TREND_FILTER),
                "ema_reversal": bool(config.USE_EARLY_REVERSAL),
                "rsi": bool(config.USE_RSI_FILTER),
                "adx": bool(config.USE_ADX_FILTER),
                "smi": bool(config.USE_SMII_FILTER),
                "macd": not bool(config.USE_EMA_CROSS_STRATEGY),
                "dpo": bool(getattr(config, "USE_DPO_FILTER", False)),
                "smc": bool(config.USE_SMC_FILTER or config.USE_SMC_ORDER_BLOCK_FILTER or config.USE_SMC_FVG_FILTER),
                # Scope market per indikator (config.*_MARKET_SCOPE: "ALL"/"SPOT"/
                # "FUTURES") -- dikirim ke dashboard supaya SMII/SMC bisa
                # disembunyikan dari chart begitu pair yang dipilih market_type-nya
                # TIDAK sesuai scope (misal SMC_MARKET_SCOPE="SPOT" tapi yang
                # dipilih pair Futures -- filter itu memang tidak dipakai buat
                # gate sinyal pair itu, jadi overlay-nya jangan ikut ditampilkan,
                # bisa menyesatkan kalau dikira memang berpengaruh).
                "smi_scope": str(getattr(config, "SMII_MARKET_SCOPE", "ALL") or "ALL").upper(),
                "smc_scope": str(getattr(config, "SMC_MARKET_SCOPE", "ALL") or "ALL").upper(),
                # Spread indicators di oscillator panel
                "ema_cross_spread": bool(getattr(config, "USE_EMA_CROSS_SPREAD_FILTER", False)),
                "price_spread": bool(getattr(config, "USE_PRICE_SPREAD_FILTER", False)),
                "ema_cross_spread_min": float(getattr(config, "EMA_CROSS_SPREAD_MIN_PCT", 0.0) or 0.0),
                "price_spread_min": float(getattr(config, "PRICE_SPREAD_MIN_PCT", 0.0) or 0.0),
            },
            "rsi_filter": {
                "enabled": config.USE_RSI_FILTER,
                "period": config.RSI_PERIOD,
                "overbought": config.RSI_OVERBOUGHT,
                "oversold": config.RSI_OVERSOLD,
            },
            "realized_pnl": round(self.realized_pnl_quote, 4),
            "realized_pnl_by_market": realized_pnl_by_market,
            "unrealized_pnl": round(unrealized_pnl_quote, 4),
            "unrealized_pnl_by_market": unrealized_pnl_by_market,
            "total_pnl": total_pnl_quote,
            "leverage": config.LEVERAGE if symbols_futures_okx else None,
            "margin_type": config.OKX_MARGIN_MODE if symbols_futures_okx else None,
            "connected": self.connected,
            "portfolio": self.portfolio,
            "portfolio_quote_by_market": portfolio_quote_by_market,
            "portfolio_quote_total": round(sum(portfolio_quote_by_market.values()), 4),
            "paused": self.paused,
            "dry_run": self.dry_run,
            "pairs": pairs_out,
            "events": list(self.event_log),
        })

    # ---------- PORTFOLIO (saldo akun, di-refresh berkala) ----------
    def _append_okx_balance(self, portfolio: list, quote_by_market: dict, ok_markets: set):
        """Tambahkan saldo OKX (kalau ada pair OKX yang AKTIF SEKARANG di
        self.pairs) ke list portfolio.

        Dicek dari self.pairs (BUKAN config.SYMBOLS_SPOT_OKX/FUTURES_OKX
        statis) supaya pair OKX yang ditambah LIVE lewat dashboard
        (add_pair_live()) langsung ikut ke-refresh saldonya juga."""
        quote = config.QUOTE_ASSET
        has_spot_okx = any(p.exchange == "OKX" and p.market_type == "SPOT" for p in list(self.pairs.values()))
        has_futures_okx = any(p.exchange == "OKX" and p.market_type == "FUTURES" for p in list(self.pairs.values()))
        if has_spot_okx or has_futures_okx:
            try:
                amount = self.okx_client.get_balance(quote)
                # OKX unified balance -- SATU entry "OKX" saja (21 Sept
                # 2026), bukan digandakan jadi OKX_SPOT+OKX_FUTURES dengan
                # nilai identik cuma karena kedua market_type aktif.
                portfolio.append({"asset": quote, "amount": amount, "market": "OKX", "exchange": "OKX"})
                quote_by_market["OKX"] = amount
                ok_markets.add("OKX")
            except Exception as e:
                logger.error(f"Gagal ambil saldo OKX: {e}")
                self.log_event(f"Gagal ambil saldo OKX: {e}", "error")

    @staticmethod
    def _reset_pair_position_fields(pair: "PairState"):
        """Reset field posisi ke flat TANPA eksekusi order dan TANPA hitung
        PnL -- beda dari close_position() yang memang menutup posisi
        BENERAN di exchange. Dipakai KHUSUS oleh sync_okx_positions() untuk
        membersihkan posisi HANTU (bot kira ada posisi, tapi exchange
        bilang tidak ada -- lihat docstring sync_okx_positions())."""
        pair.in_position = False
        pair.position_side = None
        pair.entry_price = None
        pair.stop_loss = None
        pair.take_profit = None
        pair.quantity = 0.0
        pair.initial_quantity = 0.0
        pair.tp_levels = []
        pair.initial_risk_distance = None
        pair.breakeven_triggered = False
        pair.entry_time = None

    def sync_okx_positions(self):
        """Cocokkan status in_position INTERNAL bot dengan posisi ASLI yang
        beneran ada di OKX (FUTURES/SWAP saja -- SPOT tidak punya endpoint
        "posisi" di OKX, cuma saldo wallet biasa, jadi tidak direkonsiliasi
        di sini). Dipanggil dari portfolio_loop() tiap 30 detik, DAN sekali
        lagi langsung saat mode diubah ke LIVE dari dashboard (lihat
        control_loop()) supaya tidak perlu nunggu sampai 30 detik. TIDAK
        jalan sama sekali kalau dry_run=True (posisi simulasi memang tidak
        punya wujud asli di exchange, tidak ada yang perlu dicocokkan).

        LATAR BELAKANG (21 Sept 2026): ditemukan bug nyata -- pas mode
        diganti dari DRY RUN ke LIVE lewat dashboard, posisi SIMULASI yang
        masih "terbuka" di state internal (in_position=True dengan
        quantity/entry FIKTIF) tidak pernah dibersihkan atau dicek ulang.
        Bot lanjut mengira dia pegang posisi itu -- padahal di exchange
        tidak ada apa-apa -- dan begitu SL/TP/reshuffle kena, bot akan
        coba kirim ORDER ASLI untuk menutup posisi yang sebenarnya tidak
        pernah ada (execute_order() cuma cek self.dry_run SAAT ITU JUGA,
        bukan saat posisi dibuka). Fungsi ini menutup celah itu.

        Sekalian menangani arah SEBALIKNYA: kalau user buka posisi MANUAL
        langsung di OKX (di luar bot), posisi itu otomatis DIADOPSI --
        dipasangkan SL/TP pakai parameter risk DEFAULT dari config, PERSIS
        seperti kalau bot sendiri yang baru buka posisi itu -- supaya tidak
        dibiarkan telanjang tanpa perlindungan SL/TP sama sekali.

        DIPERLUAS (21 Sept 2026, malam): awalnya bagian "adopsi posisi
        asing" di atas CUMA mengecek pair yang MEMANG SUDAH dipantau bot
        (5 slot aktif hasil scanner). Ternyata user buka posisi manual di
        simbol yang SAMA SEKALI belum pernah dipantau bot (XRP-USDT-SWAP,
        di luar 5 pair aktif saat itu) -- posisi itu jadi tidak kedeteksi
        SAMA SEKALI karena loop lama cuma iterasi self.pairs yang sudah
        ada. Sekarang get_positions() dicek untuk SEMUA simbol SWAP yang
        beneran punya posisi terbuka di akun, TERLEPAS dari status
        pantauan bot -- simbol yang belum ada di self.pairs otomatis
        ditambahkan (via jalur yang sama dengan add_pair_live(): riwayat
        candle dimuat, disimpan ke live_pairs.json supaya bertahan lewat
        restart) baru diadopsi SL/TP-nya. Leverage exchange-nya SENGAJA
        TIDAK disentuh/diubah (lihat komentar di bagian adopsi simbol baru
        di bawah) -- beda dari add_pair_live() versi dashboard biasa yang
        memang set leverage default untuk pair BARU (belum ada posisi).
        """
        if self.dry_run:
            return
        if self.okx_client is None:
            return

        try:
            real_positions = self.okx_client.get_positions(inst_type="SWAP")
        except Exception as e:
            logger.error(f"Gagal sinkronisasi posisi OKX: {e}")
            return

        real_by_symbol = {}
        for p in real_positions:
            try:
                pos_qty_contracts = float(p.get("pos", 0) or 0)
            except (TypeError, ValueError):
                continue
            if pos_qty_contracts == 0:
                continue
            inst_id = p.get("instId")
            if not inst_id:
                continue
            try:
                avg_px = float(p.get("avgPx", 0) or 0)
            except (TypeError, ValueError):
                avg_px = 0.0
            pos_side_raw = (p.get("posSide") or "net").lower()
            if pos_side_raw == "long":
                side = "LONG"
            elif pos_side_raw == "short":
                side = "SHORT"
            else:
                side = "LONG" if pos_qty_contracts > 0 else "SHORT"

            # KONVERSI CONTRACTS -> KOIN (ditemukan 21 Sept 2026, lihat
            # docstring okx_client.get_instrument_spec()): field "pos" dari
            # OKX untuk SWAP SELALU dalam CONTRACTS, bukan jumlah koin
            # langsung -- harus dikali ctVal instrument itu dulu supaya
            # pair.quantity konsisten dengan satuan yang dipakai di seluruh
            # kode bot lainnya (koin asli, sama seperti
            # risk_manager.calculate_position_size()). Tanpa ini, PnL yang
            # ditampilkan dashboard untuk posisi hasil sync/adopsi salah
            # total (contoh nyata: XRP qty asli 27 tapi kebaca 0.27 --
            # PnL keitung ~100x lebih kecil dari sebenarnya). Kalau gagal
            # ambil spec, qty dipakai APA ADANYA (fallback ct_val=1) supaya
            # fungsi ini tidak crash -- tapi hasilnya kemungkinan salah
            # satuan, makanya error di-log dengan jelas.
            qty_coin = self._contracts_to_coin_qty(inst_id, abs(pos_qty_contracts))
            if qty_coin is None:
                qty_coin = abs(pos_qty_contracts)

            real_by_symbol[inst_id] = {"side": side, "qty": qty_coin, "entry": avg_px}

        # ---- 1) Pair yang SUDAH dipantau bot (5 slot aktif scanner, dll) ----
        futures_pairs = {
            sym: p for sym, p in list(self.pairs.items())
            if p.exchange == "OKX" and p.market_type == "FUTURES"
        }

        for sym, pair in futures_pairs.items():
            real = real_by_symbol.get(sym)

            if pair.in_position and not real:
                # HANTU -- bot kira ada posisi, exchange bilang tidak ada.
                msg = (
                    f"[{sym}] Posisi internal ({pair.position_side}, qty {pair.quantity}) TIDAK "
                    f"ADA di exchange -- dianggap posisi hantu (kemungkinan sisa simulasi DRY RUN "
                    f"yang belum dibersihkan sebelum ganti ke LIVE) dan dibersihkan TANPA kirim "
                    f"order apapun."
                )
                logger.warning(msg)
                self.log_event(msg, "error")
                log_metric(
                    "phantom_position_cleared", symbol=sym, exchange="OKX", market_type="FUTURES",
                    side=pair.position_side, qty=pair.quantity, entry=pair.entry_price,
                )
                telegram_notifier.send_telegram(config, f"⚠️ {msg}")
                self._reset_pair_position_fields(pair)
                continue

            if real and not pair.in_position:
                # DITEMUKAN posisi ASLI yang bot tidak tahu (kemungkinan
                # dibuka manual) -- adopsi & pasang SL/TP otomatis pakai
                # parameter risk default, PERSIS seperti open_position().
                side = real["side"]
                entry = real["entry"]
                qty = real["qty"]
                stop_loss = risk_manager.calculate_stop_loss(entry, side, None, None, None)
                take_profit = risk_manager.calculate_take_profit(entry, side, stop_loss)

                pair.in_position = True
                pair.position_side = side
                pair.entry_price = entry
                pair.stop_loss = stop_loss
                pair.take_profit = take_profit
                pair.quantity = qty
                pair.initial_quantity = qty
                pair.tp_levels = risk_manager.calculate_tp_levels(entry, stop_loss, side) if config.USE_MULTI_TP else []
                pair.initial_risk_distance = abs(entry - stop_loss) if stop_loss is not None else None
                pair.breakeven_triggered = False
                pair.entry_time = datetime.now(timezone.utc)

                msg = (
                    f"[{sym}] Posisi {side} ASLI ditemukan di exchange (bukan dari bot ini, "
                    f"kemungkinan dibuka manual) | Entry: {entry} | Qty: {qty} -- diadopsi & "
                    f"dipasang SL {stop_loss} / TP {take_profit} otomatis pakai parameter risk "
                    f"default (bukan rencana risk kamu sendiri kalau ada -- cek ulang manual "
                    f"kalau perlu)."
                )
                logger.warning(msg)
                self.log_event(msg, "error")
                log_metric(
                    "foreign_position_adopted", symbol=sym, exchange="OKX", market_type="FUTURES",
                    side=side, entry=entry, qty=qty, sl=stop_loss, tp=take_profit,
                )
                telegram_notifier.send_telegram(config, f"🔍 {msg}")
                continue

            if real and pair.in_position:
                # Sama-sama ada -- cek quantity meleset jauh (manual
                # nambah/kurangi posisi tanpa sepengetahuan bot) dan
                # sinkronkan ke nilai exchange (SL/TP yang sudah ada TIDAK
                # diutak-atik di sini, cuma quantity-nya).
                qty_diff_pct = (
                    abs(real["qty"] - pair.quantity) / pair.quantity * 100 if pair.quantity else 100
                )
                if qty_diff_pct > 1:
                    msg = (
                        f"[{sym}] Quantity posisi internal ({pair.quantity}) beda dari exchange "
                        f"({real['qty']}) -- disinkronkan ke nilai exchange. Kemungkinan ada "
                        f"penambahan/pengurangan manual di luar bot."
                    )
                    logger.warning(msg)
                    self.log_event(msg, "error")
                    old_qty = pair.quantity
                    pair.quantity = real["qty"]
                    if pair.initial_quantity < pair.quantity:
                        pair.initial_quantity = pair.quantity
                    log_metric(
                        "position_quantity_synced", symbol=sym, exchange="OKX", market_type="FUTURES",
                        old_qty=old_qty, new_qty=pair.quantity,
                    )

                # FIX 21 Sept 2026: entry_price internal JUGA bisa meleset dari
                # avgPx ASLI di exchange -- open_position() dulu selalu catat
                # entry_price dari harga sinyal/candle SAAT order dikirim,
                # BUKAN dari harga fill order yang BENERAN terjadi (order
                # response OKX cuma konfirmasi "order placed", tidak langsung
                # kasih avgPx). Untuk coin volatile/tipis likuiditasnya,
                # selisihnya bisa signifikan (kejadian nyata: NEIRO-USDT-SWAP,
                # entry tercatat 0.00009778 vs avgPx ASLI ~0.0001056 -- beda
                # ~8%, bikin PnL yang ditampilkan dashboard SALAH ARAH dari
                # kondisi sebenarnya). Sync ini jadi jaring pengaman kalau
                # koreksi di open_position() (fetch avgPx langsung setelah
                # order) gagal/lewat, ATAU utk posisi lama yang sudah
                # terlanjur salah sebelum fix ini di-deploy. SL/TP TIDAK
                # di-recalculate ulang (konsisten dgn perlakuan quantity di
                # atas) -- cuma entry_price yang dikoreksi supaya PnL akurat.
                if real["entry"] > 0 and pair.entry_price:
                    entry_diff_pct = abs(real["entry"] - pair.entry_price) / pair.entry_price * 100
                    if entry_diff_pct > 0.5:
                        msg = (
                            f"[{sym}] Entry price posisi internal ({pair.entry_price}) beda dari "
                            f"avgPx ASLI exchange ({real['entry']}) -- disinkronkan (selisih "
                            f"{entry_diff_pct:.2f}%, kemungkinan slippage order market saat "
                            f"dibuka). SL/TP TIDAK diubah."
                        )
                        logger.warning(msg)
                        self.log_event(msg, "error")
                        old_entry = pair.entry_price
                        pair.entry_price = real["entry"]
                        log_metric(
                            "position_entry_price_synced", symbol=sym, exchange="OKX", market_type="FUTURES",
                            old_entry=old_entry, new_entry=pair.entry_price, diff_pct=round(entry_diff_pct, 4),
                        )

        # ---- 2) Posisi ASLI di simbol yang bot BELUM PERNAH pantau sama
        # sekali (di luar pair aktif scanner saat ini) -- ditambahkan atas
        # permintaan user 21 Sept 2026 malam, supaya SEMUA posisi terbuka
        # di akun OKX otomatis dilindungi, bukan cuma yang kebetulan sedang
        # dipilih scanner sebagai salah satu dari 5 slot aktif.
        unknown_symbols = set(real_by_symbol.keys()) - set(self.pairs.keys())
        for sym in unknown_symbols:
            real = real_by_symbol[sym]
            try:
                pair = PairState(sym, "FUTURES", exchange="OKX")
                pair.added_live = True
                self.pairs[sym] = pair
                self.load_history_for(sym)
                self.save_candles(sym)
            except Exception as e:
                logger.error(f"[{sym}] Gagal memuat riwayat candle saat adopsi posisi asing (simbol baru): {e}")
                self.pairs.pop(sym, None)
                continue

            # SENGAJA TIDAK panggil _setup_okx_futures() di sini -- itu akan
            # MENGUBAH leverage posisi yang SUDAH terbuka ke leverage default
            # bot (config.OKX_LEVERAGE), padahal posisi ini dibuka manual dan
            # mungkin sengaja pakai leverage BEDA (contoh: user pakai 5x,
            # default bot bisa saja beda). Leverage exchange dibiarkan APA
            # ADANYA -- bot cuma memasang SL/TP internal yang mengikuti
            # harga, sama sekali tidak menyentuh setting leverage exchange.
            side = real["side"]
            entry = real["entry"]
            qty = real["qty"]
            stop_loss = risk_manager.calculate_stop_loss(entry, side, None, None, None)
            take_profit = risk_manager.calculate_take_profit(entry, side, stop_loss)

            pair.in_position = True
            pair.position_side = side
            pair.entry_price = entry
            pair.stop_loss = stop_loss
            pair.take_profit = take_profit
            pair.quantity = qty
            pair.initial_quantity = qty
            pair.tp_levels = risk_manager.calculate_tp_levels(entry, stop_loss, side) if config.USE_MULTI_TP else []
            pair.initial_risk_distance = abs(entry - stop_loss) if stop_loss is not None else None
            pair.breakeven_triggered = False
            pair.entry_time = datetime.now(timezone.utc)

            # Simpan ke live_pairs.json supaya pair ini TETAP dipantau walau
            # bot di-restart -- sama seperti add_pair_live(), BEDA dari
            # config.SYMBOLS_*_OKX yang statis (cuma berubah kalau file
            # config.py sendiri diedit manual).
            try:
                live_pairs = state_writer.read_live_pairs()
                if not any(lp.get("symbol") == sym for lp in live_pairs):
                    live_pairs.append({"symbol": sym, "market_type": "FUTURES", "exchange": "OKX"})
                    state_writer.write_live_pairs(live_pairs)
            except Exception as e:
                logger.error(f"[{sym}] Gagal simpan ke live_pairs.json setelah adopsi: {e}")

            msg = (
                f"[{sym}] Posisi {side} ASLI ditemukan di exchange pada SYMBOL YANG BELUM "
                f"PERNAH DIPANTAU bot (dibuka manual, di luar pair aktif scanner) | Entry: "
                f"{entry} | Qty: {qty} -- pair ditambahkan otomatis ke daftar pantau, diadopsi "
                f"& dipasang SL {stop_loss} / TP {take_profit} otomatis pakai parameter risk "
                f"default. Leverage TIDAK diubah (tetap sesuai yang kamu set manual di exchange)."
            )
            logger.warning(msg)
            self.log_event(msg, "error")
            log_metric(
                "foreign_position_adopted", symbol=sym, exchange="OKX", market_type="FUTURES",
                side=side, entry=entry, qty=qty, sl=stop_loss, tp=take_profit, new_pair=True,
            )
            telegram_notifier.send_telegram(config, f"🔍 {msg}")

        if unknown_symbols:
            self.save_state(force=True)

    def refresh_portfolio(self):
        """Refresh saldo OKX (SPOT & FUTURES/SWAP, wallet terpisah kalau
        kedua pasar aktif -- digabung dalam satu list, dibedakan lewat
        field "market").

        QUOTE_ASSET selalu dimasukkan untuk tiap market yang aktif walau
        saldonya 0 -- supaya wallet kosong langsung KELIHATAN di dashboard,
        bukan diam-diam hilang dari list. Kalau ada market aktif dengan
        saldo quote 0, langsung di-log sebagai peringatan.

        PENTING: "saldo 0" dan "fetch saldo gagal" adalah dua masalah
        berbeda dan tidak boleh dilaporkan dengan pesan yang sama -- kalau
        request ke OKX gagal (network/rate-limit/dsb), _check_empty_wallets
        HANYA dipanggil untuk market yang berhasil di-fetch (lihat `ok_markets`),
        supaya tidak salah menyimpulkan "saldo kosong" padahal sebenarnya
        request-nya yang gagal.
        """
        if self.dry_run:
            simulated_balance = config.INITIAL_SIMULATED_BALANCE + self.realized_pnl_quote
            self.portfolio = [{
                "asset": config.QUOTE_ASSET, "amount": simulated_balance,
                "simulated": True, "exchange": "OKX",
            }]
            self.save_state()
            return

        quote = config.QUOTE_ASSET
        portfolio = []
        quote_by_market = {}
        ok_markets = set()
        self._append_okx_balance(portfolio, quote_by_market, ok_markets)
        self.portfolio = portfolio
        self._check_empty_wallets(quote, quote_by_market, ok_markets)
        self.check_equity_guard()
        self.save_state()

    def check_equity_guard(self):
        """Auto-pause SELURUH bot kalau drawdown dari saldo awal sudah
        melewati config.MAX_DRAWDOWN_PCT. Saldo awal ditangkap SEKALI di
        siklus refresh_portfolio() pertama (atau dari config.INITIAL_BALANCE
        kalau diisi manual, bukan 0). Posisi yang SUDAH terbuka tetap
        dipantau SL/TP seperti biasa selama dijeda (sama seperti toggle
        pause manual dari dashboard) -- cuma entry baru yang berhenti."""
        if not config.USE_EQUITY_GUARD:
            return

        current_quote_total = sum(
            p["amount"] for p in self.portfolio if p.get("asset") == config.QUOTE_ASSET
        )

        if self.initial_balance is None:
            if current_quote_total > 0:
                self.initial_balance = current_quote_total
                logger.info(f"Equity guard: saldo acuan ditangkap = {self.initial_balance:.4f} {config.QUOTE_ASSET}")
                self.log_event(f"Equity guard aktif, saldo acuan: {self.initial_balance:.2f} {config.QUOTE_ASSET}", "info")
            return  # belum ada acuan, belum bisa hitung drawdown siklus ini

        if self.initial_balance <= 0:
            return

        current_equity = self.initial_balance + self.realized_pnl_quote + self.last_unrealized_pnl_quote
        drawdown_pct = (self.initial_balance - current_equity) / self.initial_balance * 100

        if drawdown_pct >= config.MAX_DRAWDOWN_PCT and not self.paused:
            self.paused = True
            state_writer.write_control({"paused": True})  # sinkron ke dashboard, cegah control_loop nge-unpause balik
            msg = (
                f"EQUITY GUARD AKTIF: drawdown {drawdown_pct:.2f}% >= batas {config.MAX_DRAWDOWN_PCT}% -- "
                f"bot DIJEDA OTOMATIS. Equity: {current_equity:.2f} / Acuan: {self.initial_balance:.2f} {config.QUOTE_ASSET}"
            )
            logger.error(msg)
            self.log_event(msg, "error")
            log_metric(
                "equity_guard_triggered", drawdown_pct=round(drawdown_pct, 4),
                current_equity=round(current_equity, 4), initial_balance=round(self.initial_balance, 4),
                max_drawdown_pct=config.MAX_DRAWDOWN_PCT,
            )
            telegram_notifier.notify_equity_guard(config, drawdown_pct, config.MAX_DRAWDOWN_PCT, current_equity, config.QUOTE_ASSET)

    def _check_empty_wallets(self, quote: str, quote_by_market: dict, ok_markets: set):
        """Peringatkan sekali per siklus refresh kalau ada market aktif
        dengan saldo QUOTE_ASSET = 0 -- ini penyebab paling umum order
        'dilewati: saldo tidak cukup' padahal total akun sebenarnya ada isi
        di wallet lain (misal Spot ada isi, tapi Futures kosong).
        Pakai edge-trigger (self._empty_wallet_warned) supaya cuma muncul
        SEKALI di log saat kondisinya berubah, bukan tiap 30 detik selama
        wallet-nya tetap kosong.

        `ok_markets` = market yang fetch saldonya BERHASIL siklus ini --
        market yang fetch-nya gagal dilewati di sini (errornya sudah
        dilaporkan terpisah di refresh_portfolio), supaya tidak salah
        melaporkan 'saldo 0' padahal sebenarnya request-nya yang gagal."""
        for market in ("OKX",):
            if market not in ok_markets:
                continue
            is_empty = quote_by_market.get(market, 0) <= 0
            was_warned = self._empty_wallet_warned.get(market, False)
            if is_empty and not was_warned:
                self.log_event(f"Saldo {quote} di wallet {market} saat ini 0 -- order {market} akan dilewati.", "error")
            self._empty_wallet_warned[market] = is_empty

    def portfolio_loop(self):
        while True:
            self.refresh_portfolio()
            self.sync_okx_positions()
            self._persist_uptime()
            time.sleep(30)

    def discord_listener_loop(self):
        """Dengarkan perintah MASUK dari Discord (bot token + prefix, default !).
        Pola sama Telegram: tulis ke control.json, balas di channel Discord.
        Keamanan: kalau DISCORD_ALLOWED_USER_IDS diisi, HANYA user itu yang boleh.

        PENTING (ditambahkan 20 Sept 2026, sentralisasi listener Discord):
        di mode multi-user, instance ini HARUS diset
        DISCORD_LISTEN_FOR_COMMANDS=false (dilakukan otomatis oleh
        multi_bot_orchestrator.py) supaya TIDAK ikut connect ke Discord
        Gateway pakai DISCORD_BOT_TOKEN yang SAMA dengan instance user
        lain -- itu akan bentrok. Listener terpusatnya ada di
        discord_dispatcher.py (proses terpisah, SATU-SATUNYA yang boleh
        pegang koneksi Discord di deployment multi-user). Default True,
        jadi perilaku single-user lama TIDAK BERUBAH."""
        if not config.DISCORD_LISTEN_FOR_COMMANDS:
            logger.info(
                "DISCORD_LISTEN_FOR_COMMANDS=False -- instance ini TIDAK mendengarkan "
                "pesan Discord masuk (mode multi-user, listener terpusat di discord_dispatcher.py). "
                "Notifikasi KELUAR tetap berfungsi normal."
            )
            return

        def on_command(text, reply_fn, author_id=None):
            self._handle_telegram_command(text, reply=reply_fn)

        try:
            discord_notifier.run_command_listener(config, on_command)
        except Exception as e:
            logger.warning(f"Discord listener berhenti: {e}")

    def telegram_listener_loop(self):
        """Dengarkan PERINTAH MASUK dari Telegram (long polling getUpdates)
        SELAMA bot ini sedang jalan -- /status, /pause, /resume, /shutdown.

        PENTING: ini CUMA bisa mengontrol bot yang SUDAH JALAN. Thread ini
        sendiri baru hidup setelah bot.py start -- kalau proses bot mati
        total, TIDAK ADA yang mendengarkan pesan Telegram sama sekali,
        jadi TIDAK BISA "menghidupkan" bot dari kondisi mati lewat Telegram.

        Perintah yang diterima ditulis ke control.json PAKAI MEKANISME
        YANG SAMA dengan tombol dashboard -- diproses control_loop() di
        siklus 2 detik berikutnya, BUKAN dieksekusi langsung di sini.

        Keamanan: CUMA menerima perintah dari chat_id yang PERSIS sama
        dengan config.TELEGRAM_CHAT_ID -- pesan dari chat manapun selain
        itu diabaikan total (di-log sebagai peringatan)."""
        if not (config.USE_TELEGRAM_ALERTS and config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID):
            return  # tidak dikonfigurasi -- thread ini tidak melakukan apapun
        if not config.TELEGRAM_LISTEN_FOR_COMMANDS:
            logger.info(
                "TELEGRAM_LISTEN_FOR_COMMANDS=False -- instance ini TIDAK mendengarkan "
                "pesan masuk (mode multi-user, listener terpusat di central_dispatcher.py). "
                "Notifikasi KELUAR tetap berfungsi normal."
            )
            return

        logger.info("Telegram command listener siap, menunggu perintah...")
        offset = None
        allowed_chat_id = str(config.TELEGRAM_CHAT_ID)

        while True:
            try:
                updates = telegram_notifier.get_telegram_updates(config, offset=offset, timeout=25)
                for update in updates:
                    offset = update["update_id"] + 1

                    callback_query = update.get("callback_query")
                    if callback_query:
                        cb_chat_id = str(callback_query.get("message", {}).get("chat", {}).get("id", ""))
                        if cb_chat_id != allowed_chat_id:
                            logger.warning(f"Callback Telegram dari chat_id TIDAK DIKENAL ({cb_chat_id}) diabaikan.")
                            continue
                        self._handle_telegram_callback(callback_query)
                        continue

                    message = update.get("message", {})
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    text = message.get("text", "")

                    if chat_id != allowed_chat_id:
                        logger.warning(f"Perintah Telegram dari chat_id TIDAK DIKENAL ({chat_id}) diabaikan.")
                        continue

                    self._handle_telegram_command(text)
            except Exception as e:
                logger.warning(f"Telegram listener error: {e}")
                time.sleep(5)

    # ---------- MENU TELEGRAM: inline keyboard, TANPA perlu ketik ---------
    def _build_main_menu(self):
        uptime_sec = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
        hours, rem = divmod(uptime_sec, 3600)
        minutes = rem // 60
        # Total/Realized/Unrealized dihitung dengan cara yang SAMA PERSIS
        # dengan hero PnL di dashboard (lihat _calc_unrealized_pnl()) --
        # supaya angka di sini TIDAK PERNAH beda dengan yang tampil di
        # dashboard, walau sebelumnya menu ini cuma nampilin Realized saja.
        unrealized = self._calc_unrealized_pnl()
        total_pnl = self.realized_pnl_quote + unrealized
        text = (
            f"🤖 <b>Menu Bot</b>\n"
            f"Mode: {'DRY_RUN' if self.dry_run else 'LIVE'} | {'DIJEDA' if self.paused else 'AKTIF'}\n"
            f"Uptime: {hours}j {minutes}m | Posisi: {self.count_open_positions()}\n"
            f"Total PnL: {total_pnl:+.2f} {config.QUOTE_ASSET} "
            f"(Realized {self.realized_pnl_quote:+.2f} | Unrealized {unrealized:+.2f})"
        )
        pause_label = "▶️ Resume" if self.paused else "⏸ Pause"
        kb = telegram_notifier.build_inline_keyboard([
            [("📊 Status", "m_status"), (pause_label, "m_pause")],
            [("📋 Pairs", "m_pairs"), ("⚙️ Strategi", "m_strategy")],
            [("⏻ Shutdown", "m_shutdown")],
        ])
        return text, kb

    def _build_pairs_menu(self):
        rows = []
        if self.pairs:
            text = "📋 <b>Pilih pair:</b>\n🟢=LONG 🔴=SHORT ⚪=flat"
            for sym, pair in list(self.pairs.items()):
                if pair.in_position:
                    icon = "🟢" if pair.position_side == "LONG" else "🔴"
                else:
                    icon = "⚪"
                rows.append([(f"{icon} {sym}", f"m_pair:{sym}")])
        else:
            text = "Tidak ada pair aktif sama sekali."
        rows.append([("➕ Tambah Pair", "m_addpair"), ("🗑️ Hapus Pair", "m_removepair")])
        rows.append([("⬅️ Kembali", "m_main")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_addpair_market_menu(self, exchange: str):
        """Fork OKX-only: langkah pilih exchange DIHILANGKAN (dulu
        _build_addpair_exchange_menu() di sini, sudah dihapus) -- cuma ada
        satu exchange sekarang, jadi menu "Tambah Pair" langsung lompat ke
        sini (lihat routing "m_addpair" di _handle_telegram_callback())."""
        text = f"➕ <b>Tambah Pair</b> -- {exchange}\nPilih jenis market:"
        kb = telegram_notifier.build_inline_keyboard([
            [("Spot", f"m_addpair_mkt:{exchange}:SPOT"), ("Futures", f"m_addpair_mkt:{exchange}:FUTURES")],
            [("⬅️ Kembali", "m_pairs")],
        ])
        return text, kb

    def _build_addpair_symbol_menu(self, exchange: str, market_type: str):
        """Ambil daftar pair BENERAN ADA di exchange (SAMA fungsi yang
        dipakai dropdown dashboard, lihat _fetch_available_pairs()) --
        user tinggal TAP, TIDAK PERLU ketik simbol sama sekali. Cuma
        tampilkan 12 PALING LIKUID (volume 24 jam tertinggi) biar menu
        tombol tidak kepanjangan/susah di-scroll di HP."""
        try:
            symbols = _fetch_available_pairs(exchange, market_type, max_symbols=12)
        except Exception as e:
            logger.warning(f"Gagal ambil daftar pair buat menu Telegram ({exchange}/{market_type}): {e}")
            text = f"Gagal ambil daftar pair dari {exchange} ({market_type}). Coba lagi sebentar."
            kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_addpair")]])
            return text, kb

        if not symbols:
            text = f"Tidak ada pair ditemukan buat {exchange} ({market_type})."
            kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_addpair")]])
            return text, kb

        text = f"➕ <b>Tambah Pair</b> -- {exchange}/{market_type}\nDiurutkan volume 24 jam, tap buat pilih:"
        rows = []
        for i in range(0, len(symbols), 2):
            row_symbols = symbols[i:i + 2]
            rows.append([(s, f"m_addpair_do:{exchange}:{market_type}:{s}") for s in row_symbols])
        rows.append([("⬅️ Kembali", f"m_addpair_ex:{exchange}")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_removepair_menu(self):
        if not self.pairs:
            text = "Tidak ada pair aktif buat dihapus."
            kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_pairs")]])
            return text, kb
        text = "🗑️ <b>Hapus Pair</b>\nTap pair yang mau dihapus:\n⚠️ Pair dengan posisi terbuka akan DITOLAK otomatis."
        rows = []
        for sym, pair in list(self.pairs.items()):
            icon = "🟢" if (pair.in_position and pair.position_side == "LONG") else ("🔴" if pair.in_position else "⚪")
            rows.append([(f"{icon} {sym}", f"m_removepair_do:{sym}")])
        rows.append([("⬅️ Kembali", "m_pairs")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_pair_actions_menu(self, symbol: str):
        pair = self.pairs.get(symbol)
        if not pair:
            return f"{symbol} tidak ditemukan.", telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_pairs")]])
        if pair.in_position:
            text = (
                f"<b>{symbol}</b> ({pair.exchange}/{pair.market_type})\n"
                f"Posisi: {pair.position_side}\n"
                f"Entry: {pair.entry_price}\n"
                f"SL: {pair.stop_loss}\nTP: {pair.take_profit}"
            )
            rows = [[("❌ Tutup Posisi", f"m_close:{symbol}")], [("⬅️ Kembali", "m_pairs")]]
        else:
            text = f"<b>{symbol}</b> ({pair.exchange}/{pair.market_type})\nTidak ada posisi terbuka."
            rows = [
                [("🟢 Buka LONG", f"m_open:{symbol}:LONG"), ("🔴 Buka SHORT", f"m_open:{symbol}:SHORT")],
                [("⬅️ Kembali", "m_pairs")],
            ]
        return text, telegram_notifier.build_inline_keyboard(rows)

    # ---- Definisi step per parameter angka (berapa besar tap +/-) ----
    _PARAM_STEPS = _SCHEMA_PARAM_STEPS

    def _build_strategy_menu(self):
        """Menu strategi utama -- pintu masuk ke 3 sub-menu."""
        text = "⚙️ <b>Strategi</b>\nPilih bagian yang mau diubah:"
        kb = telegram_notifier.build_inline_keyboard([
            [("🔘 Filter ON/OFF", "m_s_filters")],
            [("🔢 Parameter Filter", "m_s_params")],
            [("⚖️ Risiko & Posisi", "m_s_risk")],
            [("⬅️ Kembali", "m_main")],
        ])
        return text, kb

    def _build_filters_menu(self):
        """Filter bool: toggle ON/OFF satu tap."""
        text = "🔘 <b>Filter ON/OFF</b>\nTap buat toggle:"
        rows = []
        for key, (py_type, label, group) in STRATEGY_SETTINGS_SCHEMA.items():
            if group != "filter":
                continue
            val = getattr(config, key, None)
            icon = "✅" if val else "❌"
            rows.append([(f"{icon} {label}", f"m_tf:{key}")])
        rows.append([("⬅️ Kembali", "m_strategy")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_params_menu(self):
        """Parameter angka filter -- tap setting buat masuk ke editor +/-."""
        text = "🔢 <b>Parameter Filter</b>\nTap setting buat ubah:"
        rows = []
        for key, (py_type, label, group) in STRATEGY_SETTINGS_SCHEMA.items():
            if group != "filter_param":
                continue
            val = getattr(config, key, "—")
            rows.append([(f"{label}: {val}", f"m_edit:{key}")])
        rows.append([("⬅️ Kembali", "m_strategy")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_risk_menu(self):
        """Manajemen risiko -- bool dan angka dicampur, dipisah per baris."""
        text = "⚖️ <b>Risiko & Posisi</b>\nTap setting buat ubah atau toggle:"
        rows = []
        for key, (py_type, label, group) in STRATEGY_SETTINGS_SCHEMA.items():
            if group != "risk":
                continue
            val = getattr(config, key, None)
            if py_type == bool:
                icon = "✅" if val else "❌"
                rows.append([(f"{icon} {label}", f"m_tf:{key}")])
            else:
                rows.append([(f"{label}: {val}", f"m_edit:{key}")])
        rows.append([("⬅️ Kembali", "m_strategy")])
        return text, telegram_notifier.build_inline_keyboard(rows)

    def _build_edit_menu(self, key: str):
        """Editor nilai angka -- tombol +/- buat ubah, tanpa ketik apapun.
        Tap + atau - langsung APPLY ke pending_strategy_settings, menu
        di-render ulang menampilkan nilai BARU, sehingga user bisa tap
        berkali-kali untuk mendapatkan nilai yang pas."""
        schema = STRATEGY_SETTINGS_SCHEMA.get(key)
        if not schema:
            return "Setting tidak dikenali.", telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_strategy")]])

        py_type, label, group = schema
        val = getattr(config, key, 0)
        step = self._PARAM_STEPS.get(key, 1)

        # Format nilai biar rapi (float tampil 2-4 desimal kalau perlu)
        if py_type == float:
            val_str = f"{val:.4g}"
        else:
            val_str = str(val)

        text = (
            f"🔢 <b>{label}</b>\n"
            f"Nilai sekarang: <code>{val_str}</code>\n"
            f"Step per tap: <code>{step}</code>\n\n"
            f"Tap ➕/➖ buat ubah nilai.\n"
            f"Perubahan aktif setelah beberapa detik (lewat control_loop)."
        )
        back_cb = "m_s_params" if group == "filter_param" else "m_s_risk"
        kb = telegram_notifier.build_inline_keyboard([
            [(f"➖ -{step}", f"m_adj:{key}:down"), (f"➕ +{step}", f"m_adj:{key}:up")],
            [("⬅️ Kembali", back_cb)],
        ])
        return text, kb

    # Pasangan EMA fast/slow yang harus tetap fast < slow -- dicek tiap
    # kali salah satu digeser lewat +/- di Telegram (dashboard sudah
    # divalidasi di request_ema_params_change()/_futures() lewat widget
    # khusus, tapi Telegram cuma punya editor generik +/- per-key jadi
    # validasinya ditaruh di sini).
    _EMA_PAIR_VALIDATION = {
        "EMA_FAST_LEN": ("EMA_SLOW_LEN", "fast"),
        "EMA_SLOW_LEN": ("EMA_FAST_LEN", "slow"),
        "EMA_FAST_LEN_FUTURES": ("EMA_SLOW_LEN_FUTURES", "fast"),
        "EMA_SLOW_LEN_FUTURES": ("EMA_FAST_LEN_FUTURES", "slow"),
    }

    def _apply_numeric_adjustment(self, key: str, direction: str) -> tuple:
        """Hitung nilai baru (current ± step), pastikan tidak negatif kalau
        tidak masuk akal, lalu tulis ke control.json buat dieksekusi
        control_loop() -- SAMA persis mekanisme /set, cuma nilai sudah
        dihitung di sini (bukan user ketik angkanya sendiri).

        Return (ok: bool, msg: str atau None) -- msg diisi kalau perubahan
        DITOLAK (bukan diterapkan) supaya caller bisa nampilin alasannya
        lewat popup Telegram, bukan diam-diam gagal."""
        schema = STRATEGY_SETTINGS_SCHEMA.get(key)
        if not schema:
            return False, "Setting tidak dikenali."
        py_type, _, _ = schema
        current = getattr(config, key, 0)
        step = self._PARAM_STEPS.get(key, 1)
        if direction == "up":
            new_val = round(current + step, 8)
        else:
            new_val = round(current - step, 8)
            # Jaga batas bawah -- sebagian besar param angka tidak boleh negatif
            if new_val < 0 and key not in ("DPO_MIN_PCT",):
                new_val = 0.0

        pair_info = self._EMA_PAIR_VALIDATION.get(key)
        if pair_info:
            pair_key, role = pair_info
            pair_val = getattr(config, pair_key, None)
            if pair_val is not None:
                if role == "fast" and new_val >= pair_val:
                    return False, f"Ditolak: {key} ({new_val}) harus lebih KECIL dari {pair_key} ({pair_val})."
                if role == "slow" and new_val <= pair_val:
                    return False, f"Ditolak: {key} ({new_val}) harus lebih BESAR dari {pair_key} ({pair_val})."

        # Update config LANGSUNG supaya menu di-render ulang menampilkan
        # nilai BARU (bukan tunggu control_loop eksekusi dulu) -- aman
        # karena control_loop akan mengkonfirmasi ke nilai yang sama.
        setattr(config, key, py_type(new_val))
        control = state_writer.read_control()
        pending = control.get("pending_strategy_settings") or {}
        pending[key] = str(new_val)
        control["pending_strategy_settings"] = pending
        # FIX BUG 21 Sep 2026 (lihat penjelasan lengkap di control_loop()):
        # sinkronkan requested_ema_params/_futures juga kalau key ini EMA
        # fast/slow -- cegah control_loop() mengira ada permintaan BARU
        # dari dashboard buat balikin ke snapshot lama di siklus berikutnya.
        if key in ("EMA_FAST_LEN", "EMA_SLOW_LEN"):
            control["requested_ema_params"] = {"fast": config.EMA_FAST_LEN, "slow": config.EMA_SLOW_LEN}
        elif key in ("EMA_FAST_LEN_FUTURES", "EMA_SLOW_LEN_FUTURES"):
            control["requested_ema_params_futures"] = {"fast": config.EMA_FAST_LEN_FUTURES, "slow": config.EMA_SLOW_LEN_FUTURES}
        state_writer.write_control(control)
        return True, None

    def _handle_telegram_callback(self, callback_query: dict):
        """Proses SATU tap tombol inline keyboard -- rute berdasarkan
        callback_data, EDIT pesan yang sama di tempat (bukan kirim pesan
        baru tiap navigasi), dan WAJIB answer_callback_query() supaya
        tombol yang ditap tidak loading spinner selamanya di HP user.

        Sama seperti perintah teks (/pause, /set, dll), aksi yang mengubah
        state bot CUMA nulis ke control.json -- eksekusi sebenarnya tetap
        lewat control_loop(). Karena itu, state terbaru (self.paused,
        config.XXX) BISA BELUM ke-update persis saat menu di-render ulang
        (baru diproses control_loop beberapa detik lagi) -- kalau menu
        kelihatan belum berubah, tap sekali lagi beres (aksi ini idempotent,
        aman diulang)."""
        data = callback_query.get("data", "")
        cq_id = callback_query.get("id")
        message = callback_query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")

        def render(text, kb):
            if chat_id and message_id:
                telegram_notifier.edit_telegram_message(config, chat_id, message_id, text, kb)
            telegram_notifier.answer_callback_query(config, cq_id)

        try:
            if data == "m_main":
                render(*self._build_main_menu())
            elif data == "m_status":
                text, _ = self._build_main_menu()
                kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_main")]])
                render(text, kb)
            elif data == "m_pause":
                control = state_writer.read_control()
                control["paused"] = not self.paused
                state_writer.write_control(control)
                render(*self._build_main_menu())
            elif data == "m_pairs":
                render(*self._build_pairs_menu())
            elif data.startswith("m_pair:"):
                symbol = data.split(":", 1)[1]
                render(*self._build_pair_actions_menu(symbol))
            elif data == "m_addpair":
                render(*self._build_addpair_market_menu("OKX"))
            elif data.startswith("m_addpair_ex:"):
                exchange = data.split(":", 1)[1]
                render(*self._build_addpair_market_menu(exchange))
            elif data.startswith("m_addpair_mkt:"):
                _, exchange, market_type = data.split(":")
                render(*self._build_addpair_symbol_menu(exchange, market_type))
            elif data.startswith("m_addpair_do:"):
                _, exchange, market_type, symbol = data.split(":", 3)
                ok, msg = self.add_pair_live(symbol, market_type, exchange, source="telegram")
                text = f"{'✅' if ok else '❌'} {msg}"
                kb = telegram_notifier.build_inline_keyboard([[("⬅️ Ke daftar pair", "m_pairs")]])
                render(text, kb)
            elif data == "m_removepair":
                render(*self._build_removepair_menu())
            elif data.startswith("m_removepair_do:"):
                symbol = data.split(":", 1)[1]
                ok, msg = self.remove_pair_live(symbol)
                text = f"{'✅' if ok else '❌'} {msg}"
                kb = telegram_notifier.build_inline_keyboard([[("⬅️ Ke daftar pair", "m_pairs")]])
                render(text, kb)
            elif data.startswith("m_open:"):
                _, symbol, side = data.split(":")
                if symbol not in self.pairs:
                    telegram_notifier.answer_callback_query(config, cq_id, text="Pair tidak ditemukan.")
                    return
                control = state_writer.read_control()
                pending = control.get("pending_manual_open", [])
                pending.append({"symbol": symbol, "side": side})
                control["pending_manual_open"] = pending
                state_writer.write_control(control)
                text = f"📥 Permintaan buka {side} {symbol} diterima, diproses beberapa detik lagi."
                kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_pairs")]])
                render(text, kb)
            elif data.startswith("m_close:"):
                symbol = data.split(":", 1)[1]
                if symbol not in self.pairs or not self.pairs[symbol].in_position:
                    telegram_notifier.answer_callback_query(config, cq_id, text="Tidak ada posisi terbuka.")
                    return
                control = state_writer.read_control()
                pending = control.get("pending_manual_close", [])
                pending.append(symbol)
                control["pending_manual_close"] = pending
                state_writer.write_control(control)
                text = f"📤 Permintaan tutup posisi {symbol} diterima, diproses beberapa detik lagi."
                kb = telegram_notifier.build_inline_keyboard([[("⬅️ Kembali", "m_pairs")]])
                render(text, kb)
            elif data == "m_strategy":
                render(*self._build_strategy_menu())
            elif data in ("m_filters", "m_s_filters"):
                render(*self._build_filters_menu())
            elif data == "m_s_params":
                render(*self._build_params_menu())
            elif data == "m_s_risk":
                render(*self._build_risk_menu())
            elif data.startswith("m_edit:"):
                key = data.split(":", 1)[1]
                render(*self._build_edit_menu(key))
            elif data.startswith("m_adj:"):
                _, key, direction = data.split(":")
                ok, msg = self._apply_numeric_adjustment(key, direction)
                text, kb = self._build_edit_menu(key)
                if chat_id and message_id:
                    telegram_notifier.edit_telegram_message(config, chat_id, message_id, text, kb)
                telegram_notifier.answer_callback_query(config, cq_id, text=(None if ok else msg))
            elif data.startswith("m_tf:"):
                key = data.split(":", 1)[1]
                if key not in STRATEGY_SETTINGS_SCHEMA:
                    telegram_notifier.answer_callback_query(config, cq_id, text="Setting tidak dikenali.")
                    return
                current_val = getattr(config, key, False)
                control = state_writer.read_control()
                pending = control.get("pending_strategy_settings") or {}
                pending[key] = not current_val
                control["pending_strategy_settings"] = pending
                state_writer.write_control(control)
                render(*self._build_filters_menu())
            elif data == "m_shutdown":
                text = "⚠️ <b>Yakin mau matikan bot?</b>\nSemua posisi terbuka TIDAK akan dipantau lagi sampai dijalankan ulang manual."
                kb = telegram_notifier.build_inline_keyboard([[("✅ Ya, matikan", "m_shutdownok"), ("❌ Batal", "m_main")]])
                render(text, kb)
            elif data == "m_shutdownok":
                control = state_writer.read_control()
                control["shutdown_requested"] = True
                state_writer.write_control(control)
                render("⏻ Bot akan dimatikan (diproses beberapa detik lagi).", None)
            else:
                telegram_notifier.answer_callback_query(config, cq_id)
        except Exception as e:
            logger.warning(f"Gagal proses callback Telegram: {e}")
            telegram_notifier.answer_callback_query(config, cq_id, text="Terjadi kesalahan, coba lagi.")

    def _handle_telegram_command(self, text: str, reply=None):
        """Proses SATU perintah remote (Telegram/Discord) yang sudah lolos
        verifikasi. reply: callable(msg) opsional -- kalau None, balas lewat
        Telegram. Perintah yang mengubah state bot CUMA menulis ke
        control.json -- eksekusi lewat control_loop()."""
        raw_text = text.strip()
        cmd = raw_text.lower()

        def _reply(msg: str):
            if reply is not None:
                reply(msg)
            else:
                telegram_notifier.send_telegram(config, msg)

        if cmd in ("/status", "status"):
            uptime_sec = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
            hours, rem = divmod(uptime_sec, 3600)
            minutes = rem // 60
            # Sama seperti _build_main_menu() -- tampilkan Total (Realized +
            # Unrealized), BUKAN Realized saja, supaya angka /status SAMA
            # PERSIS dengan hero "Total PnL" di dashboard (sebelumnya beda
            # karena /status cuma nampilin Realized, dashboard nampilin Total).
            unrealized = self._calc_unrealized_pnl()
            total_pnl = self.realized_pnl_quote + unrealized
            _reply(f"📊 <b>Status Bot</b>\n"
                f"Mode: {'DRY_RUN' if self.dry_run else 'LIVE'}\n"
                f"Kondisi: {'DIJEDA' if self.paused else 'AKTIF'}\n"
                f"Uptime: {hours}j {minutes}m\n"
                f"Posisi terbuka: {self.count_open_positions()}\n"
                f"Total PnL: {total_pnl:+.2f} {config.QUOTE_ASSET}\n"
                f"  • Realized: {self.realized_pnl_quote:+.2f} {config.QUOTE_ASSET}\n"
                f"  • Unrealized: {unrealized:+.2f} {config.QUOTE_ASSET}",
            )
        elif cmd == "/pause":
            control = state_writer.read_control()
            control["paused"] = True
            state_writer.write_control(control)
            _reply("⏸ Bot akan DIJEDA (diproses beberapa detik lagi).")
        elif cmd == "/resume":
            control = state_writer.read_control()
            control["paused"] = False
            state_writer.write_control(control)
            _reply("▶️ Bot akan DIAKTIFKAN kembali (diproses beberapa detik lagi).")
        elif cmd == "/shutdown confirm":
            control = state_writer.read_control()
            control["shutdown_requested"] = True
            state_writer.write_control(control)
            _reply("⏻ Bot akan DIMATIKAN (diproses beberapa detik lagi).")
        elif cmd == "/shutdown":
            _reply("⚠️ Untuk mematikan bot, kirim PERSIS: /shutdown confirm")
        elif cmd == "/pairs":
            if not self.pairs:
                _reply("Tidak ada pair aktif sama sekali.")
            else:
                lines = ["📋 <b>Pair aktif sekarang:</b>"]
                for sym, pair in list(self.pairs.items()):
                    status = f"{pair.position_side} terbuka" if pair.in_position else "flat"
                    lines.append(f"{sym} ({pair.exchange}/{pair.market_type}) -- {status}")
                lines.append("\nBuka posisi: /open SYMBOL LONG_atau_SHORT confirm\nTutup posisi: /close SYMBOL confirm")
                _reply("\n".join(lines))
        elif raw_text.lower().startswith("/open "):
            # Sama seperti manual_open dashboard -- WAJIB kata "confirm" di
            # akhir, sama alasannya (ini order NYATA kalau lagi LIVE).
            parts = raw_text.split()
            if len(parts) < 3 or parts[-1].lower() != "confirm":
                _reply("Format: /open SYMBOL LONG_atau_SHORT confirm\nContoh: /open BTCUSDT LONG confirm\n\nWajib akhiri dengan kata 'confirm' -- ini aksi trading beneran.",
                )
            else:
                symbol = parts[1].upper()
                side = parts[2].upper()
                if side not in ("LONG", "SHORT"):
                    _reply(f"Side harus LONG atau SHORT, dapat '{parts[2]}'.")
                elif symbol not in self.pairs:
                    _reply(f"{symbol} tidak ada di daftar pair aktif. Ketik /pairs buat lihat daftarnya.")
                else:
                    control = state_writer.read_control()
                    pending = control.get("pending_manual_open", [])
                    pending.append({"symbol": symbol, "side": side})
                    control["pending_manual_open"] = pending
                    state_writer.write_control(control)
                    _reply(f"📥 Permintaan buka {side} {symbol} diterima, diproses beberapa detik lagi.")
        elif raw_text.lower().startswith("/close "):
            parts = raw_text.split()
            if len(parts) < 2 or parts[-1].lower() != "confirm":
                _reply("Format: /close SYMBOL confirm\nContoh: /close BTCUSDT confirm\n\nWajib akhiri dengan kata 'confirm'.",
                )
            else:
                symbol = parts[1].upper()
                if symbol not in self.pairs:
                    _reply(f"{symbol} tidak ada di daftar pair aktif. Ketik /pairs buat lihat daftarnya.")
                elif not self.pairs[symbol].in_position:
                    _reply(f"{symbol} tidak sedang punya posisi terbuka.")
                else:
                    control = state_writer.read_control()
                    pending = control.get("pending_manual_close", [])
                    pending.append(symbol)
                    control["pending_manual_close"] = pending
                    state_writer.write_control(control)
                    _reply(f"📤 Permintaan tutup posisi {symbol} diterima, diproses beberapa detik lagi.")
        elif cmd == "/settings":
            # Ringkasan filter ON/OFF SAJA (grup "filter") -- kalau nampilin
            # semua 47 setting termasuk angka-angka parameter, pesannya
            # kepanjangan buat satu bubble chat Telegram.
            lines = ["⚙️ <b>Filter konfirmasi aktif sekarang:</b>"]
            for key, (py_type, label, group) in STRATEGY_SETTINGS_SCHEMA.items():
                if group != "filter":
                    continue
                val = getattr(config, key, None)
                lines.append(f"{'✅' if val else '❌'} {label}")
            lines.append("\nUbah pakai: /set KEY NILAI\nContoh: /set USE_RSI_FILTER false\nContoh: /set RSI_OVERBOUGHT 80")
            _reply("\n".join(lines))
        elif cmd.startswith("/set "):
            # Pakai raw_text (BUKAN cmd yang sudah di-lowercase) buat parsing
            # -- nama setting di STRATEGY_SETTINGS_SCHEMA semuanya UPPERCASE,
            # jadi case aslinya penting buat validasi (walau kita upper() lagi
            # di bawah biar user tidak perlu peduli besar-kecil huruf).
            parts = raw_text.split(maxsplit=2)
            if len(parts) != 3:
                _reply("Format: /set KEY NILAI\nContoh: /set RSI_OVERBOUGHT 80\nContoh: /set USE_RSI_FILTER false\n\nKetik /settings buat lihat daftar filter yang bisa diubah.",
                )
            else:
                _, key_raw, value_str = parts
                key = key_raw.strip().upper()
                if key not in STRATEGY_SETTINGS_SCHEMA:
                    _reply(f"Setting '{key}' tidak dikenali/tidak diizinkan diubah. Ketik /settings buat lihat daftarnya.")
                else:
                    control = state_writer.read_control()
                    pending = control.get("pending_strategy_settings") or {}
                    pending[key] = value_str  # apply_strategy_settings() yang urus konversi tipe dari string
                    control["pending_strategy_settings"] = pending
                    state_writer.write_control(control)
                    _, label, _ = STRATEGY_SETTINGS_SCHEMA[key]
                    _reply(f"⚙️ {label} akan diubah jadi '{value_str}' (diproses beberapa detik lagi).")
        elif cmd in ("/menu", "/start"):
            text, kb = self._build_main_menu()
            if reply is not None:
                # Discord tidak pakai inline keyboard Telegram -- kirim ringkasan teks
                _reply(text + "\n\nKetik !help untuk daftar perintah Discord.")
            else:
                telegram_notifier.send_telegram_keyboard(config, text, kb)
        elif cmd == "/help":
            help_tg = (
                "🤖 <b>Perintah tersedia:</b>\n"
                "/menu - buka menu tombol\n"
                "/status - status bot\n"
                "/pause / /resume - jeda / lanjut\n"
                "/pairs - daftar pair aktif\n"
                "/open SYMBOL LONG/SHORT confirm\n"
                "/close SYMBOL confirm\n"
                "/settings - filter ON/OFF\n"
                "/set KEY NILAI\n"
                "/shutdown confirm\n"
            )
            help_dc = (
                "🤖 **Perintah Discord** (prefix `!`):\n"
                "`!status` — status bot\n"
                "`!pause` / `!resume` — jeda / lanjut\n"
                "`!pairs` — daftar pair aktif\n"
                "`!open SYMBOL LONG/SHORT confirm`\n"
                "`!close SYMBOL confirm`\n"
                "`!settings` — filter ON/OFF\n"
                "`!set KEY NILAI`\n"
                "`!shutdown confirm` — matikan bot\n"
                "`!help` — bantuan ini\n\n"
                "Catatan: bot harus sedang jalan."
            )
            _reply(help_dc if reply is not None else help_tg)
        else:
            _reply(f"Perintah tidak dikenali: {text}\nKetik /help untuk daftar perintah.")

    def okx_poll_loop(self):
        """Loop background: poll REST API OKX berkala untuk semua pair OKX --
        harga live tiap OKX_POLL_INTERVAL_SECONDS, dan deteksi candle baru
        yang sudah CLOSED (field 'confirm'=='1' dari OKX, beda dari Binance
        yang punya flag 'x' eksplisit di payload WebSocket-nya). Dipakai
        sebagai pengganti WebSocket untuk OKX -- lihat penjelasan lengkap
        kenapa REST polling yang dipilih di docstring okx_client.py."""
        # PENTING: TIDAK exit dini walau belum ada pair OKX sama sekali saat
        # bot start -- loop ini harus TETAP HIDUP (cuma sleep terus) supaya
        # bisa langsung mulai polling begitu ada pair OKX yang ditambahkan
        # LIVE lewat add_pair_live() nanti (lihat dashboard "tambah pair").
        logger.info(f"Polling OKX siap (interval {config.OKX_POLL_INTERVAL_SECONDS}s), menunggu pair OKX aktif...")

        while True:
            # Daftar pair OKX dihitung ULANG tiap iterasi (bukan sekali di
            # awal) -- supaya pair yang ditambah/dihapus LIVE lewat dashboard
            # langsung ikut/berhenti dipoll di siklus berikutnya, tanpa
            # perlu restart thread ini sama sekali.
            okx_symbols = [sym for sym, p in list(self.pairs.items()) if p.exchange == "OKX"]
            if not okx_symbols or self.okx_client is None:
                time.sleep(config.OKX_POLL_INTERVAL_SECONDS)
                continue

            for sym in okx_symbols:
                try:
                    pair = self.pairs[sym]
                    interval = self._interval_for(pair)
                    bar = self._to_okx_bar(interval)

                    price = self.okx_client.get_ticker_price(sym)
                    self.pairs[sym].live_price = price

                    # Update candle TERAKHIR yang masih berjalan biar chart di
                    # dashboard ikut bergerak, bukan diam sampai candle CLOSED.
                    if len(pair.df) > 0:
                        last_idx = pair.df.index[-1]
                        pair.df.loc[last_idx, "close"] = price
                        if price > pair.df.loc[last_idx, "high"]:
                            pair.df.loc[last_idx, "high"] = price
                        if price < pair.df.loc[last_idx, "low"]:
                            pair.df.loc[last_idx, "low"] = price
                        now = time.time()
                        last_write = self._last_candles_write.get(sym, 0)
                        if now - last_write >= 3:
                            self._last_candles_write[sym] = now
                            self.save_candles(sym)

                    self.save_state()

                    raw = self.okx_client.get_candles(sym, bar=bar, limit=3)
                    closed = next((k for k in raw if len(k) > 8 and k[8] == "1"), None)
                    if closed is None:
                        continue

                    open_time = int(closed[0])
                    if self._okx_last_candle_time.get(sym) == open_time:
                        continue  # candle ini sudah pernah diproses

                    self._okx_last_candle_time[sym] = open_time
                    candle_dict = {
                        "t": open_time,
                        "o": closed[1], "h": closed[2], "l": closed[3], "c": closed[4],
                        "v": closed[5],
                        "T": open_time + self._interval_to_ms(interval),
                    }
                    self.on_new_candle(sym, candle_dict)
                except Exception as e:
                    logger.error(f"[{sym}] (OKX) Gagal polling: {e}")
            time.sleep(config.OKX_POLL_INTERVAL_SECONDS)

    # ---------- GANTI TIMEFRAME SAAT BOT JALAN (dari dashboard) ----------
    VALID_INTERVALS = ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

    def request_interval_change(self, new_interval: str) -> tuple:
        """Ganti timeframe FUTURES SAAT BOT SEDANG JALAN (dipanggil dari
        dashboard, bukan cuma edit config.py lalu restart). CUMA
        mempengaruhi pair FUTURES -- pair SPOT punya timeframe SENDIRI
        (self.spot_interval, dari config.SPOT_INTERVAL) yang TIDAK
        terpengaruh sama sekali oleh perubahan ini.

        Reload riwayat candle pair FUTURES SAJA dengan interval baru --
        TIDAK ADA reconnect apa pun yang perlu dipaksa (fork ini tidak
        punya WebSocket sama sekali): okx_poll_loop() otomatis pakai
        interval BARU di siklus polling berikutnya lewat _interval_for(),
        cukup baca self.futures_interval yang sudah diupdate di bawah.

        Posisi yang SEDANG TERBUKA TIDAK TERPENGARUH -- entry/SL/TP/quantity
        tetap sama persis, cuma data candle & sinyal KE DEPAN yang pakai
        timeframe baru.

        Return: (berhasil: bool, pesan: str)
        """
        new_interval = new_interval.strip()

        if new_interval not in self.VALID_INTERVALS:
            return False, f"Interval '{new_interval}' tidak dikenali. Pilihan valid: {', '.join(self.VALID_INTERVALS)}"

        # Sama seperti validasi startup di __init__: interval sub-menit ('1s')
        # cuma didukung endpoint kline SPOT, BUKAN Futures (SWAP) OKX.
        if new_interval.endswith("s") and config.SYMBOLS_FUTURES_OKX:
            return False, (
                f"Interval '{new_interval}' tidak didukung untuk pair FUTURES. "
                f"Kosongkan SYMBOLS_FUTURES_OKX dulu kalau mau pakai interval sub-menit."
            )

        old_interval = self.futures_interval
        if new_interval == old_interval:
            return True, f"Sudah pakai interval {new_interval}, tidak ada perubahan."

        logger.info(f"Mengganti timeframe FUTURES: {old_interval} -> {new_interval}")
        self.log_event(f"Mengganti timeframe Futures {old_interval} -> {new_interval}, memuat ulang riwayat candle...", "info")

        self.futures_interval = new_interval
        self._okx_last_candle_time = {}  # reset -- candle lama beda timeframe, tidak relevan lagi utk dedup

        # Muat ulang riwayat cuma pair FUTURES -- pair SPOT tidak perlu
        # direload sama sekali karena timeframe-nya (self.spot_interval)
        # tidak berubah oleh method ini.
        for sym, pair in list(self.pairs.items()):
            if pair.market_type == "FUTURES":
                self.load_history_for(sym)
                self.save_candles(sym)

        logger.info(f"Timeframe Futures berhasil diganti ke {new_interval}")
        self.log_event(f"Timeframe Futures sekarang: {new_interval}", "info")
        log_metric("interval_changed", old_interval=old_interval, new_interval=new_interval, source="dashboard")
        telegram_notifier.send_telegram(config, f"⏱ Timeframe Futures diganti: {old_interval} → {new_interval}")
        self.save_state(force=True)
        return True, f"Timeframe Futures berhasil diganti ke {new_interval}"

    def request_leverage_change(self, new_leverage: int) -> tuple:
        """Ganti leverage SAAT BOT SEDANG JALAN -- BEDA dari setting
        strategi biasa (RSI/ADX/dll) karena leverage BUKAN cuma angka buat
        kalkulasi internal, tapi HARUS benar-benar dikirim ke API OKX
        (set_leverage) supaya nyata berlaku di posisi BARU ke depan.

        Diterapkan ke SEMUA pair FUTURES yang lagi aktif. Posisi yang
        SEDANG TERBUKA TIDAK TERPENGARUH -- leverage baru cuma berlaku
        buat entry BERIKUTNYA (mengubah leverage di tengah posisi terbuka
        itu berisiko/bisa ditolak exchange, jadi sengaja tidak dilakukan
        di sini).

        Return: (berhasil: bool, pesan: str)"""
        try:
            new_leverage = int(new_leverage)
        except (TypeError, ValueError):
            return False, f"Leverage harus berupa angka bulat, dapat '{new_leverage}'."
        if not (1 <= new_leverage <= 125):
            return False, f"Leverage harus antara 1-125, dapat {new_leverage}."

        old_leverage = config.LEVERAGE
        if new_leverage == old_leverage:
            return True, f"Sudah pakai leverage {new_leverage}x, tidak ada perubahan."

        applied_symbols = []
        failed_symbols = []
        for sym, pair in list(self.pairs.items()):
            if pair.market_type != "FUTURES":
                continue
            try:
                self.okx_client.set_leverage(sym, new_leverage, config.OKX_MARGIN_MODE)
                applied_symbols.append(sym)
            except Exception as e:
                logger.error(f"[{sym}] Gagal ganti leverage ke {new_leverage}x: {e}")
                failed_symbols.append(sym)

        # config.LEVERAGE dipakai bareng buat kalkulasi liquidation estimate
        # & position sizing (lihat save_state()), config.OKX_LEVERAGE yang
        # benar-benar dikirim ke API OKX -- update KEDUANYA supaya konsisten.
        config.LEVERAGE = new_leverage
        config.OKX_LEVERAGE = new_leverage

        logger.info(f"Leverage diganti {old_leverage}x -> {new_leverage}x | Berhasil: {applied_symbols} | Gagal: {failed_symbols}")
        self.log_event(f"Leverage diganti ke {new_leverage}x ({len(applied_symbols)} pair berhasil" + (f", {len(failed_symbols)} gagal" if failed_symbols else "") + ")", "info")
        log_metric("leverage_changed", old_leverage=old_leverage, new_leverage=new_leverage, applied=len(applied_symbols), failed=len(failed_symbols), source="dashboard")
        telegram_notifier.send_telegram(
            config,
            f"⚙️ Leverage diganti: {old_leverage}x → {new_leverage}x ({len(applied_symbols)} pair berhasil"
            + (f", {len(failed_symbols)} gagal" if failed_symbols else "") + ")",
        )
        self.save_state(force=True)

        if failed_symbols and not applied_symbols:
            return False, f"Gagal ganti leverage untuk SEMUA pair: {', '.join(failed_symbols)}"
        msg = f"Leverage berhasil diganti ke {new_leverage}x untuk {len(applied_symbols)} pair."
        if failed_symbols:
            msg += f" ({len(failed_symbols)} pair gagal: {', '.join(failed_symbols)})"
        return True, msg

    # ---------- GANTI PARAMETER EMA FAST/SLOW SAAT BOT JALAN (dari dashboard) ----------
    def apply_strategy_settings(self, settings: dict) -> tuple:
        """Terapkan PERUBAHAN BANYAK setting strategi sekaligus, SAAT BOT
        SEDANG JALAN, dari dashboard (Menu Strategy) -- tanpa perlu restart
        atau edit config.py manual. Cuma setting yang ADA di
        STRATEGY_SETTINGS_SCHEMA yang diizinkan (whitelist) -- mencegah
        perubahan sembarangan ke config yang TIDAK dimaksudkan buat
        di-edit lewat sini (misal API key/credentials).

        Efeknya LANGSUNG berlaku di pengecekan sinyal candle BERIKUTNYA
        (strategy.py baca config.XXX FRESH tiap dipanggil, tidak di-cache)
        -- SAMA PERSIS mekanismenya dengan request_ema_params_change() yang
        sudah ada. Posisi yang SEDANG TERBUKA TIDAK TERPENGARUH sama sekali
        oleh perubahan filter ENTRY (RSI/ADX/SMC/dll) -- cuma mempengaruhi
        entry BARU ke depan. Beberapa setting risk management (trailing,
        break-even, dll) BISA mempengaruhi posisi terbuka -- ini WAJAR dan
        memang tujuannya kalau kamu sengaja matikan/nyalakan itu.

        Return: (berhasil: bool, pesan: str -- ringkasan apa saja yang berubah)
        """
        applied = []
        rejected = []

        for key, raw_value in settings.items():
            if key not in STRATEGY_SETTINGS_SCHEMA:
                rejected.append(f"{key} (bukan setting yang diizinkan)")
                continue
            expected_type, label, _group = STRATEGY_SETTINGS_SCHEMA[key]
            try:
                if expected_type is bool:
                    # Terima True/False asli, ATAU string "true"/"false" dari form HTML
                    value = raw_value if isinstance(raw_value, bool) else str(raw_value).strip().lower() == "true"
                else:
                    value = expected_type(raw_value)
            except (ValueError, TypeError):
                rejected.append(f"{key} (nilai '{raw_value}' tidak valid buat tipe {expected_type.__name__})")
                continue

            old_value = getattr(config, key, None)
            setattr(config, key, value)
            applied.append(f"{label}: {old_value} -> {value}")

        if applied:
            logger.info(f"Menu Strategy: {len(applied)} setting diubah dari dashboard -- {'; '.join(applied)}")
            self.log_event(f"Strategi diubah dari dashboard ({len(applied)} setting): {', '.join(a.split(':')[0] for a in applied)}", "info")
            log_metric("strategy_settings_changed", count=len(applied), source="dashboard")
            telegram_notifier.send_telegram(
                config,
                f"⚙️ Menu Strategy diubah ({len(applied)} setting):\n" + "\n".join(applied),
            )
            self.save_state(force=True)
            # Tulis ke config.py di DISK supaya perubahan bertahan setelah restart
            _persist_config_changes({k: getattr(config, k) for k in settings if k in STRATEGY_SETTINGS_SCHEMA and k not in [r.split(' ')[0] for r in rejected]})

        if rejected:
            logger.warning(f"Menu Strategy: {len(rejected)} setting DITOLAK -- {'; '.join(rejected)}")

        if not applied and not rejected:
            return False, "Tidak ada setting yang dikirim."
        if not applied:
            return False, f"Semua setting ditolak: {'; '.join(rejected)}"

        msg = f"{len(applied)} setting berhasil diterapkan."
        if rejected:
            msg += f" ({len(rejected)} ditolak: {'; '.join(rejected)})"
        return True, msg

    def request_ema_params_change(self, ema_fast: int, ema_slow: int) -> tuple:
        """Ganti EMA_FAST_LEN/EMA_SLOW_LEN SAAT BOT SEDANG JALAN, tanpa perlu
        restart atau reload riwayat candle sama sekali -- BEDA dari ganti
        timeframe (yang perlu reload+reconnect), karena EMA cuma dihitung
        ULANG dari candle yang SUDAH ADA (pair.df) tiap kali
        strategy.compute_indicators() dipanggil. Cukup ubah nilai
        config.EMA_FAST_LEN/EMA_SLOW_LEN, efeknya langsung berlaku di
        pengecekan sinyal candle BERIKUTNYA.

        Posisi yang SEDANG TERBUKA TIDAK TERPENGARUH SAMA SEKALI -- EMA
        fast/slow cuma dipakai buat TRIGGER entry baru, bukan buat
        SL/TP/trailing posisi yang sudah ada.

        Return: (berhasil: bool, pesan: str)
        """
        try:
            ema_fast = int(ema_fast)
            ema_slow = int(ema_slow)
        except (ValueError, TypeError):
            return False, "EMA_FAST_LEN dan EMA_SLOW_LEN harus berupa angka bulat."

        if ema_fast < 1 or ema_slow < 1:
            return False, "EMA_FAST_LEN dan EMA_SLOW_LEN harus lebih besar dari 0."
        if ema_fast >= ema_slow:
            return False, f"EMA_FAST_LEN ({ema_fast}) harus lebih KECIL dari EMA_SLOW_LEN ({ema_slow})."
        if ema_slow > 500:
            return False, "EMA_SLOW_LEN terlalu besar (maks 500) -- kemungkinan salah ketik."

        old_fast, old_slow = config.EMA_FAST_LEN, config.EMA_SLOW_LEN
        if ema_fast == old_fast and ema_slow == old_slow:
            return True, f"Sudah pakai EMA {ema_fast}/{ema_slow}, tidak ada perubahan."

        config.EMA_FAST_LEN = ema_fast
        config.EMA_SLOW_LEN = ema_slow

        msg = f"EMA diganti dari {old_fast}/{old_slow} ke {ema_fast}/{ema_slow} lewat dashboard."
        logger.info(msg)
        self.log_event(msg, "info")
        log_metric("ema_params_changed", old_fast=old_fast, old_slow=old_slow, new_fast=ema_fast, new_slow=ema_slow, source="dashboard")
        telegram_notifier.send_telegram(config, f"📈 {msg}")
        self.save_state(force=True)
        return True, msg

    def request_ema_params_change_futures(self, ema_fast: int, ema_slow: int) -> tuple:
        """Sama persis seperti request_ema_params_change(), tapi buat
        EMA_FAST_LEN_FUTURES/EMA_SLOW_LEN_FUTURES (trigger EMA Cross Futures,
        default 20/50) -- dipisah dari versi Spot karena dua pasangan EMA
        ini independen (pair Futures aktif jauh lebih banyak sekarang
        dibanding Spot, makanya butuh widget validasi sendiri juga)."""
        try:
            ema_fast = int(ema_fast)
            ema_slow = int(ema_slow)
        except (ValueError, TypeError):
            return False, "EMA_FAST_LEN_FUTURES dan EMA_SLOW_LEN_FUTURES harus berupa angka bulat."

        if ema_fast < 1 or ema_slow < 1:
            return False, "EMA_FAST_LEN_FUTURES dan EMA_SLOW_LEN_FUTURES harus lebih besar dari 0."
        if ema_fast >= ema_slow:
            return False, f"EMA_FAST_LEN_FUTURES ({ema_fast}) harus lebih KECIL dari EMA_SLOW_LEN_FUTURES ({ema_slow})."
        if ema_slow > 500:
            return False, "EMA_SLOW_LEN_FUTURES terlalu besar (maks 500) -- kemungkinan salah ketik."

        old_fast, old_slow = config.EMA_FAST_LEN_FUTURES, config.EMA_SLOW_LEN_FUTURES
        if ema_fast == old_fast and ema_slow == old_slow:
            return True, f"Sudah pakai EMA Futures {ema_fast}/{ema_slow}, tidak ada perubahan."

        config.EMA_FAST_LEN_FUTURES = ema_fast
        config.EMA_SLOW_LEN_FUTURES = ema_slow

        msg = f"EMA Futures diganti dari {old_fast}/{old_slow} ke {ema_fast}/{ema_slow} lewat dashboard."
        logger.info(msg)
        self.log_event(msg, "info")
        log_metric("ema_params_futures_changed", old_fast=old_fast, old_slow=old_slow, new_fast=ema_fast, new_slow=ema_slow, source="dashboard")
        telegram_notifier.send_telegram(config, f"📈 {msg}")
        self.save_state(force=True)
        return True, msg

    # ---------- TAMBAH/HAPUS PAIR SAAT BOT JALAN (dari dashboard) ----------
    def add_pair_live(self, symbol: str, market_type: str, exchange: str, score: float = None, source: str = "dashboard") -> tuple:
        """Tambah SATU pair baru ke bot yang SEDANG JALAN, tanpa restart --
        OKX otomatis ke-pickup okx_poll_loop() di siklus berikutnya (maks
        OKX_POLL_INTERVAL_SECONDS detik), tidak perlu aksi khusus (tidak
        ada socket/koneksi apa pun yang perlu dibuka per-pair, beda dari
        versi Binance lama yang harus subscribe WebSocket baru di sini).

        Return: (berhasil: bool, pesan: str)
        """
        symbol = symbol.strip()
        market_type = market_type.strip().upper()
        exchange = exchange.strip().upper()

        if not symbol:
            return False, "Symbol tidak boleh kosong."
        if market_type not in ("SPOT", "FUTURES"):
            return False, f"market_type harus 'SPOT' atau 'FUTURES', dapat '{market_type}'."
        if exchange != "OKX":
            return False, f"exchange harus 'OKX' (fork ini OKX-only), dapat '{exchange}'."
        if symbol in self.pairs:
            return False, f"{symbol} sudah aktif ditradingkan, tidak bisa ditambah dobel."

        if self.okx_client is None:
            # Belum ada pair OKX sama sekali sebelumnya (self.okx_client
            # cuma dibuat di __init__ kalau SYMBOLS_SPOT_OKX/FUTURES_OKX
            # awalnya tidak kosong) -- coba buat sekarang kalau kredensial
            # OKX ternyata sudah diisi di .env, walau awalnya tidak dipakai.
            if not (config.OKX_API_KEY and config.OKX_API_SECRET and config.OKX_PASSWORD):
                return False, (
                    "Pair OKX pertama kamu -- tapi OKX_API_KEY/OKX_API_SECRET/OKX_PASSWORD "
                    "belum lengkap di .env. Isi dulu ketiganya, baru coba tambah pair OKX lagi."
                )
            self.okx_client = okx_client.OKXClient(
                config.OKX_API_KEY, config.OKX_API_SECRET, config.OKX_PASSWORD,
                demo_trading=config.OKX_DEMO_TRADING,
            )
            logger.info("okx_client baru dibuat (pair OKX pertama ditambahkan secara live).")

        pair = PairState(symbol, market_type, exchange=exchange)
        pair.added_live = True
        self.pairs[symbol] = pair

        try:
            self.load_history_for(symbol)
            self.save_candles(symbol)
        except Exception as e:
            del self.pairs[symbol]
            return False, f"Gagal muat riwayat candle {symbol}: {e}"

        if not self.dry_run and market_type == "FUTURES":
            self._setup_okx_futures(symbol)

        msg = f"{symbol} ({exchange}/{market_type}) ditambahkan ke bot -- {len(self.pairs)} pair aktif total."
        logger.info(msg)
        self.log_event(msg, "info")
        log_metric("pair_added", symbol=symbol, exchange=exchange, market_type=market_type, source=source, score=(round(score, 2) if score is not None else None))
        telegram_notifier.send_telegram(config, f"➕ {msg}")

        # Simpan ke live_pairs.json supaya pair ini TETAP ADA walau bot
        # di-restart -- BEDA dari config.SYMBOLS_*_OKX yang statis (cuma
        # berubah kalau file config.py sendiri diedit manual).
        live_pairs = state_writer.read_live_pairs()
        live_pairs.append({"symbol": symbol, "market_type": market_type, "exchange": exchange})
        state_writer.write_live_pairs(live_pairs)

        self.save_state(force=True)
        return True, msg

    def remove_pair_live(self, symbol: str) -> tuple:
        """Hapus SATU pair dari bot yang SEDANG JALAN, tanpa restart.
        DITOLAK kalau pair itu SEDANG PUNYA POSISI TERBUKA -- tutup dulu
        posisinya (manual atau tunggu SL/TP kena) sebelum bisa dihapus,
        supaya tidak ada posisi yang "ditinggal" tanpa pemantauan SL/TP.

        Return: (berhasil: bool, pesan: str)
        """
        symbol = symbol.strip()
        if symbol not in self.pairs:
            return False, f"{symbol} tidak ada di daftar pair aktif."

        pair = self.pairs[symbol]
        if pair.in_position:
            return False, (
                f"{symbol} SEDANG PUNYA POSISI TERBUKA ({pair.position_side}) -- "
                "tutup dulu posisinya sebelum bisa dihapus dari daftar trading, "
                "supaya SL/TP tetap terpantau sampai posisi benar-benar closed."
            )

        # Tidak perlu aksi eksplisit apapun buat "berhenti memantau" --
        # okx_poll_loop() menghitung ulang daftar pair OKX dari self.pairs
        # tiap iterasi, begitu symbol-nya dihapus dari self.pairs di bawah,
        # otomatis berhenti ke-poll (beda dari versi Binance lama yang
        # harus eksplisit stop_socket() per-pair di sini).

        del self.pairs[symbol]
        self.candles_cache.pop(symbol, None)
        self._okx_last_candle_time.pop(symbol, None)

        msg = f"{symbol} dihapus dari bot -- {len(self.pairs)} pair aktif tersisa."
        logger.info(msg)
        self.log_event(msg, "info")
        log_metric("pair_removed", symbol=symbol, source="dashboard")
        telegram_notifier.send_telegram(config, f"🗑️ {msg}")

        # Hapus juga dari live_pairs.json kalau memang pernah ditambah
        # lewat dashboard -- kalau TIDAK ada di situ (pair dari config.py
        # asli), tidak ada yang perlu dihapus, aman-aman saja (filter
        # simpel, tidak perlu tahu asal-usul pair ini).
        live_pairs = state_writer.read_live_pairs()
        live_pairs = [p for p in live_pairs if p.get("symbol") != symbol]
        state_writer.write_live_pairs(live_pairs)

        self.save_state(force=True)
        return True, msg

    # ---------- BUKA POSISI MANUAL (dari dashboard, di luar sinyal strategi) ----------
    def manual_open_position(self, symbol: str, side: str) -> tuple:
        """Buka posisi MANUAL dipicu dari dashboard -- BUKAN dari sinyal
        strategi otomatis (EMA cross/MACD). SL/TP/quantity tetap dihitung
        pakai logika risk_manager.py yang SAMA PERSIS dengan entry otomatis
        (ATR-based atau persentase, sesuai USE_ATR_RISK) -- cuma KAPAN
        masuk-nya yang user pilih sendiri, bukan nunggu crossover.

        TETAP menghormati MAX_CONCURRENT_POSITIONS (batas risiko keras,
        tidak bisa dilewati sekalipun manual) -- TAPI TIDAK menghormati jam
        trading (USE_TRADE_HOURS) atau cooldown setelah SL, karena aturan
        itu didesain buat menahan entry OTOMATIS, sementara entry manual
        adalah keputusan sadar user kapanpun dia klik tombolnya.

        Return: (berhasil: bool, pesan: str)
        """
        symbol = symbol.strip()
        side = side.strip().upper()

        if symbol not in self.pairs:
            return False, f"{symbol} tidak ada di daftar pair aktif."
        if side not in ("LONG", "SHORT"):
            return False, f"side harus 'LONG' atau 'SHORT', dapat '{side}'."

        pair = self.pairs[symbol]
        if pair.in_position:
            return False, f"{symbol} sudah punya posisi terbuka ({pair.position_side}) -- tidak bisa dobel."
        if side == "SHORT" and not (pair.market_type == "FUTURES" and config.ALLOW_SHORT):
            return False, f"{symbol} tidak bisa SHORT (Spot tidak mendukung short, atau ALLOW_SHORT=False di config.py)."
        if len(pair.df) == 0:
            return False, f"{symbol} belum ada data candle sama sekali -- coba lagi sebentar."

        if config.MAX_CONCURRENT_POSITIONS is not None:
            if self.count_open_positions() >= config.MAX_CONCURRENT_POSITIONS:
                return False, f"Sudah mencapai batas MAX_CONCURRENT_POSITIONS ({config.MAX_CONCURRENT_POSITIONS})."

        latest = pair.df.iloc[-1]
        price = pair.live_price if pair.live_price is not None else float(latest["close"])
        candle_low, candle_high = float(latest["low"]), float(latest["high"])

        atr_val = None
        if config.USE_ATR_RISK or config.USE_TRAILING_STOP:
            try:
                df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
                latest_atr = df_ind.iloc[-1].get("atr")
                if pd.notna(latest_atr):
                    atr_val = float(latest_atr)
            except Exception as e:
                logger.warning(f"[{symbol}] Gagal hitung ATR untuk entry manual: {e}")

        self.open_position(symbol, pair, side, price, candle_low, candle_high, atr_val)

        if not pair.in_position:
            # open_position() bisa gagal DIAM-DIAM (return lebih awal, qty<=0)
            # kalau saldo tidak cukup -- dicek balik di sini biar caller tahu pasti.
            return False, f"Gagal buka posisi {symbol} -- kemungkinan saldo tidak cukup (lihat log aktivitas)."

        msg = f"{symbol} {side} dibuka MANUAL dari dashboard @ {price}."
        self.log_event(msg, "info")
        log_metric("manual_position_opened", symbol=symbol, side=side, source="dashboard")
        self.save_state(force=True)
        return True, msg

    def manual_close_position(self, symbol: str) -> tuple:
        """Tutup posisi yang SEDANG TERBUKA secara manual dari dashboard --
        SATU-SATUNYA cara menutup posisi Spot yang dibuka TANPA SL/TP
        (config.USE_SPOT_SLTP=False), karena posisi semacam itu memang
        sengaja TIDAK PERNAH ditutup otomatis oleh apapun (SL, TP, sinyal
        SELL, trailing, break-even, early reversal -- semuanya di-skip).

        Berguna juga buat posisi NORMAL (yang punya SL/TP) kalau user
        mau keluar lebih cepat dari jadwal otomatisnya.

        Return: (berhasil: bool, pesan: str)
        """
        symbol = symbol.strip()
        if symbol not in self.pairs:
            return False, f"{symbol} tidak ada di daftar pair aktif."

        pair = self.pairs[symbol]
        if not pair.in_position:
            return False, f"{symbol} tidak sedang punya posisi terbuka."

        price = pair.live_price
        if price is None and len(pair.df) > 0:
            price = float(pair.df.iloc[-1]["close"])
        if price is None:
            return False, f"{symbol} belum ada data harga sama sekali -- coba lagi sebentar."

        side = pair.position_side
        self.close_position(symbol, pair, price, "MANUAL_CLOSE")

        msg = f"{symbol} ({side}) ditutup MANUAL dari dashboard @ {price}."
        self.log_event(msg, "info")
        log_metric("manual_position_closed", symbol=symbol, side=side, source="dashboard")
        self.save_state(force=True)
        return True, msg

    def _get_total_uptime_seconds(self) -> float:
        """Total waktu bot berjalan, TERAKUMULASI lintas SEMUA sesi (bukan
        cuma sesi yang sedang berjalan sekarang)."""
        this_session = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        return self.total_uptime_before_session + max(0.0, this_session)

    def _persist_uptime(self):
        """Simpan total uptime SEKARANG ke pnl_history.json -- dipanggil
        berkala (portfolio_loop, tiap 30 detik) DAN sekali lagi tepat
        sebelum shutdown, supaya kalaupun bot crash/mati mendadak (bukan
        shutdown rapi lewat dashboard/Telegram), uptime yang sudah
        terkumpul tidak hilang lebih dari ~30 detik terakhir."""
        state_writer.write_pnl_history(
            self.realized_pnl_quote, self.realized_pnl_by_market,
            total_uptime_seconds=self._get_total_uptime_seconds(),
        )

    def reset_stats(self) -> tuple:
        """Reset PnL, total uptime, DAN saldo simulasi (yang otomatis ikut
        ke-reset karena rumusnya = INITIAL_SIMULATED_BALANCE +
        realized_pnl_quote) balik ke kondisi awal -- dipicu dari dashboard.

        TIDAK mempengaruhi posisi yang SEDANG TERBUKA sama sekali (SL/TP/
        entry semuanya tetap seperti apa adanya) -- ini CUMA reset
        angka statistik akumulasi, bukan aksi trading."""
        old_pnl = self.realized_pnl_quote
        old_uptime = self._get_total_uptime_seconds()

        self.realized_pnl_quote = 0.0
        self.realized_pnl_by_market = {}
        self.total_uptime_before_session = 0.0
        self.start_time = datetime.now(timezone.utc)  # uptime sesi ini juga ikut mulai dari 0

        state_writer.write_pnl_history(0.0, {}, total_uptime_seconds=0.0)

        msg = (
            f"Statistik di-reset dari dashboard -- PnL {old_pnl:+.2f} {config.QUOTE_ASSET} "
            f"dan uptime {old_uptime/3600:.1f} jam dikembalikan ke 0."
        )
        logger.info(msg)
        self.log_event(msg, "info")
        log_metric("stats_reset", old_pnl=round(old_pnl, 8), old_uptime_seconds=round(old_uptime, 1), source="dashboard")
        telegram_notifier.send_telegram(config, f"↺ {msg}")
        self.save_state(force=True)
        return True, msg

    # ---------- KONTROL AKTIF/DIJEDA (dari toggle di dashboard) ----------
    def control_loop(self):
        """Baca control.json secara berkala (ditulis lewat route Flask yang
        digabung ke proses ini -- lihat run()) dan sinkronkan status paused
        & dry_run. Dicek tiap 2 detik -- cukup responsif tapi tidak
        membebani I/O."""
        while True:
            try:
                control = state_writer.read_control()

                # Shutdown SELALU dicek PALING AWAL dan langsung dieksekusi --
                # tidak perlu proses field lain kalau memang mau mati.
                if bool(control.get("shutdown_requested", False)):
                    logger.info("Bot diminta SHUTDOWN dari dashboard.")
                    self.log_event("Bot dimatikan dari dashboard...", "error")
                    log_metric("bot_shutdown", source="dashboard", realized_pnl_total=round(self.realized_pnl_quote, 8))
                    telegram_notifier.notify_bot_shutdown(config, self.realized_pnl_quote, config.QUOTE_ASSET)
                    self._persist_uptime()  # simpan uptime TERAKHIR sebelum proses benar-benar mati
                    self.connected = False
                    self.save_state(force=True)
                    if config.USE_MARKET_SCANNER:
                        market_scanner.stop_scanner()
                    time.sleep(1)  # kasih waktu log & state ke-flush ke disk sebelum proses benar-benar mati
                    os._exit(0)  # matikan SELURUH proses -- semua thread lain (daemon) ikut mati bersamaan

                new_paused = bool(control.get("paused", False))
                if new_paused != self.paused:
                    self.paused = new_paused
                    if self.paused:
                        msg = "Bot DIJEDA lewat dashboard -- sinyal baru tidak akan dieksekusi (posisi terbuka tetap dipantau SL/TP)."
                    else:
                        msg = "Bot DIAKTIFKAN kembali lewat dashboard."
                    logger.info(msg)
                    self.log_event(msg, "error" if self.paused else "info")
                    log_metric("bot_paused" if self.paused else "bot_resumed", source="dashboard")
                    telegram_notifier.send_telegram(config, f"{'⏸' if self.paused else '▶️'} {msg}")
                    self.save_state(force=True)

                new_dry_run = bool(control.get("dry_run", self.dry_run))
                if new_dry_run != self.dry_run:
                    if new_dry_run:
                        # LIVE -> DRY_RUN: arah aman, langsung berlaku, tidak perlu setup apa pun.
                        self.dry_run = True
                        msg = "Mode diubah ke DRY RUN (simulasi) lewat dashboard -- order baru tidak lagi nyata."
                        logger.info(msg)
                        self.log_event(msg, "info")
                        log_metric("mode_changed", new_mode="DRY_RUN", source="dashboard")
                        telegram_notifier.send_telegram(config, f"🧪 {msg}")
                        self.save_state(force=True)
                    else:
                        # DRY_RUN -> LIVE: sinkronkan posisi DULU (butuh dry_run
                        # sudah False -- sync_okx_positions() langsung return
                        # kalau masih dry_run=True), BARU verifikasi leverage.
                        #
                        # URUTAN INI PENTING (diperbaiki 21 Sept 2026, revisi
                        # dari versi sebelumnya yang verifikasi leverage DULU
                        # baru sync): kalau leverage diverifikasi/direset SEBELUM
                        # sync_okx_positions() jalan, SEMUA pair (termasuk yang
                        # sebenarnya sudah punya posisi terbuka hasil adopsi
                        # sesi sebelumnya, misal lewat live_pairs.json) masih
                        # kebaca in_position=False di titik ini -- PairState baru
                        # SELALU mulai flat, baru sync_okx_positions() yang
                        # mengisi in_position=True lagi. Akibatnya pengecekan
                        # "not pair.in_position" di bawah TIDAK EFEKTIF menyaring
                        # pair yang sudah live -- leverage-nya tetap kereset ke
                        # config.OKX_LEVERAGE (kebetulan tidak kelihatan
                        # dampaknya untuk XRP karena config.OKX_LEVERAGE == 5x
                        # == leverage manual user, tapi ini murni kebetulan).
                        # Dengan sync_okx_positions() dipanggil LEBIH DULU,
                        # in_position sudah benar-benar akurat SEBELUM loop
                        # leverage jalan.
                        self.dry_run = False
                        self.sync_okx_positions()

                        self.log_event("Mengaktifkan LIVE lewat dashboard -- memverifikasi leverage/margin dulu...", "error")
                        for sym, pair in list(self.pairs.items()):
                            if pair.market_type == "FUTURES" and not pair.in_position:
                                # Skip pair yang SUDAH punya posisi terbuka (misal hasil
                                # adopsi posisi manual via sync_okx_positions() barusan)
                                # -- reset leverage ke config.OKX_LEVERAGE di sini
                                # SEHARUSNYA cuma buat pair yang MASIH FLAT (calon entry
                                # baru), bukan menimpa leverage posisi yang sudah berjalan
                                # dengan leverage pilihan user sendiri saat buka manual.
                                self._setup_okx_futures(sym)
                        msg = "Mode diubah ke LIVE (order nyata) lewat dashboard."
                        logger.info(msg)
                        self.log_event(msg, "error")
                        log_metric("mode_changed", new_mode="LIVE", source="dashboard")
                        telegram_notifier.send_telegram(config, f"💰 {msg}")
                        self.save_state(force=True)

                # Ganti timeframe (dari dashboard) -- selalu tulis balik
                # self.futures_interval SEKARANG ke control.json (baik
                # berhasil MAUPUN gagal validasi), supaya tidak terus dicoba
                # ulang tiap 2 detik kalau memang request-nya invalid.
                requested_interval = control.get("requested_interval")
                if requested_interval and requested_interval != self.futures_interval:
                    self.log_event(f"Permintaan ganti timeframe ke '{requested_interval}' diterima dari dashboard.", "info")
                    ok, msg = self.request_interval_change(requested_interval)
                    if not ok:
                        logger.error(f"Gagal ganti timeframe: {msg}")
                        self.log_event(f"Gagal ganti timeframe: {msg}", "error")
                    control["requested_interval"] = self.futures_interval
                    state_writer.write_control(control)

                requested_leverage = control.get("requested_leverage")
                if requested_leverage and requested_leverage != config.LEVERAGE:
                    self.log_event(f"Permintaan ganti leverage ke {requested_leverage}x diterima dari dashboard.", "info")
                    ok, msg = self.request_leverage_change(requested_leverage)
                    if not ok:
                        logger.error(f"Gagal ganti leverage: {msg}")
                        self.log_event(f"Gagal ganti leverage: {msg}", "error")
                    control["requested_leverage"] = config.LEVERAGE
                    state_writer.write_control(control)

                # Ganti EMA_FAST_LEN/EMA_SLOW_LEN (dari dashboard) -- pola sama
                # kayak ganti timeframe di atas: selalu tulis balik nilai
                # SEKARANG ke control.json biar tidak terus dicoba ulang
                # kalau memang gagal validasi.
                requested_ema = control.get("requested_ema_params")
                if requested_ema and (requested_ema.get("fast") != config.EMA_FAST_LEN or requested_ema.get("slow") != config.EMA_SLOW_LEN):
                    self.log_event(
                        f"Permintaan ganti EMA ke {requested_ema.get('fast')}/{requested_ema.get('slow')} diterima dari dashboard.",
                        "info",
                    )
                    ok, msg = self.request_ema_params_change(requested_ema.get("fast"), requested_ema.get("slow"))
                    if not ok:
                        logger.error(f"Gagal ganti EMA: {msg}")
                        self.log_event(f"Gagal ganti EMA: {msg}", "error")
                    control["requested_ema_params"] = {"fast": config.EMA_FAST_LEN, "slow": config.EMA_SLOW_LEN}
                    state_writer.write_control(control)

                # Sama seperti di atas, tapi buat EMA_FAST_LEN_FUTURES/EMA_SLOW_LEN_FUTURES.
                requested_ema_fut = control.get("requested_ema_params_futures")
                if requested_ema_fut and (requested_ema_fut.get("fast") != config.EMA_FAST_LEN_FUTURES or requested_ema_fut.get("slow") != config.EMA_SLOW_LEN_FUTURES):
                    self.log_event(
                        f"Permintaan ganti EMA Futures ke {requested_ema_fut.get('fast')}/{requested_ema_fut.get('slow')} diterima dari dashboard.",
                        "info",
                    )
                    ok, msg = self.request_ema_params_change_futures(requested_ema_fut.get("fast"), requested_ema_fut.get("slow"))
                    if not ok:
                        logger.error(f"Gagal ganti EMA Futures: {msg}")
                        self.log_event(f"Gagal ganti EMA Futures: {msg}", "error")
                    control["requested_ema_params_futures"] = {"fast": config.EMA_FAST_LEN_FUTURES, "slow": config.EMA_SLOW_LEN_FUTURES}
                    state_writer.write_control(control)

                pending_strategy_settings = control.get("pending_strategy_settings")
                if pending_strategy_settings:
                    self.log_event(f"Permintaan ubah Menu Strategy ({len(pending_strategy_settings)} setting) diterima dari dashboard.", "info")
                    ok, msg = self.apply_strategy_settings(pending_strategy_settings)
                    if not ok:
                        logger.error(f"Gagal ubah Menu Strategy: {msg}")
                        self.log_event(f"Gagal ubah Menu Strategy: {msg}", "error")
                    control["pending_strategy_settings"] = None
                    # PENTING -- FIX BUG 21 Sep 2026: sinkronkan requested_ema_params/
                    # _futures ke config SEKARANG juga di sini. EMA_FAST_LEN/SLOW (Spot
                    # & Futures) BISA ikut diubah lewat pending_strategy_settings ini
                    # (tombol +/- Menu Strategy Telegram -- lihat _apply_numeric_adjustment
                    # di bot.py & bot_launcher.py), BUKAN cuma lewat /api/set_ema_params
                    # dashboard yang sebelumnya satu-satunya penulis requested_ema_params.
                    # Tanpa sinkronisasi ini, requested_ema_params tetap BEKU di nilai lama
                    # (sejak startup/terakhir dashboard ubah EMA), dan blok drift-check EMA
                    # di ATAS salah kira ada permintaan BARU dari dashboard buat balikin ke
                    # nilai lama itu di siklus control_loop BERIKUTNYA -- akibatnya perubahan
                    # EMA lewat Telegram ke-REVERT SENDIRI dalam ~1 siklus (~2 detik) tanpa
                    # sebab jelas. Terbukti di log: "Menu Strategy: ... EMA Cross Spot --
                    # Fast: 9 -> 11" diikuti "EMA diganti dari 11/21 ke 9/21 lewat dashboard"
                    # cuma beberapa detik kemudian -- itu BUKAN aksi dashboard beneran,
                    # itu control_loop membalikkan sendiri ke snapshot basi.
                    control["requested_ema_params"] = {"fast": config.EMA_FAST_LEN, "slow": config.EMA_SLOW_LEN}
                    control["requested_ema_params_futures"] = {"fast": config.EMA_FAST_LEN_FUTURES, "slow": config.EMA_SLOW_LEN_FUTURES}
                    state_writer.write_control(control)

                # Tambah/hapus pair (dari dashboard) -- BEDA pola dari
                # timeframe/EMA di atas: ini antrian aksi SEKALI JALAN
                # (bukan "current setting" yang disinkronkan), jadi setelah
                # diproses antriannya langsung DIKOSONGKAN (bukan ditulis
                # balik ke nilai "sekarang").
                pending_adds = control.get("pending_add_pairs", [])
                if pending_adds:
                    for item in pending_adds:
                        symbol = item.get("symbol", "")
                        self.log_event(f"Permintaan tambah pair {symbol} diterima dari dashboard.", "info")
                        ok, msg = self.add_pair_live(symbol, item.get("market_type", ""), item.get("exchange", ""), source="dashboard")
                        if not ok:
                            logger.error(f"Gagal tambah pair {symbol}: {msg}")
                            self.log_event(f"Gagal tambah pair {symbol}: {msg}", "error")
                    control["pending_add_pairs"] = []
                    state_writer.write_control(control)

                pending_removes = control.get("pending_remove_pairs", [])
                if pending_removes:
                    for symbol in pending_removes:
                        self.log_event(f"Permintaan hapus pair {symbol} diterima dari dashboard.", "info")
                        ok, msg = self.remove_pair_live(symbol)
                        if not ok:
                            logger.error(f"Gagal hapus pair {symbol}: {msg}")
                            self.log_event(f"Gagal hapus pair {symbol}: {msg}", "error")
                    control["pending_remove_pairs"] = []
                    state_writer.write_control(control)

                pending_manual = control.get("pending_manual_open", [])
                if pending_manual:
                    for item in pending_manual:
                        symbol = item.get("symbol", "")
                        side = item.get("side", "")
                        self.log_event(f"Permintaan buka posisi manual {symbol} {side} diterima dari dashboard.", "info")
                        ok, msg = self.manual_open_position(symbol, side)
                        if not ok:
                            logger.error(f"Gagal buka posisi manual {symbol}: {msg}")
                            self.log_event(f"Gagal buka posisi manual {symbol}: {msg}", "error")
                    control["pending_manual_open"] = []
                    state_writer.write_control(control)

                pending_close = control.get("pending_manual_close", [])
                if pending_close:
                    for symbol in pending_close:
                        self.log_event(f"Permintaan tutup posisi manual {symbol} diterima dari dashboard.", "info")
                        ok, msg = self.manual_close_position(symbol)
                        if not ok:
                            logger.error(f"Gagal tutup posisi manual {symbol}: {msg}")
                            self.log_event(f"Gagal tutup posisi manual {symbol}: {msg}", "error")
                    control["pending_manual_close"] = []
                    state_writer.write_control(control)

                if bool(control.get("reset_stats_requested", False)):
                    self.log_event("Permintaan reset statistik diterima dari dashboard.", "info")
                    self.reset_stats()
                    control["reset_stats_requested"] = False
                    state_writer.write_control(control)
            except Exception as e:
                logger.debug(f"Gagal baca control.json: {e}")
            time.sleep(2)

    # ---------- DATA HISTORIS ----------
    def load_history(self):
        for sym in list(self.pairs):
            self.load_history_for(sym)

    def check_startup_entry(self, symbol: str, pair: "PairState"):
        """Buka posisi LANGSUNG sesuai arah tren SEKARANG (EMA cepat vs
        lambat, ATAU MACD line vs signal kalau USE_EMA_CROSS_STRATEGY=False)
        -- dipanggil SEKALI per pair, tepat setelah riwayat candle dimuat
        saat bot BARU START. TIDAK menunggu cross baru terjadi -- beda dari
        alur normal (on_new_candle -> generate_signal) yang emang perlu
        cross BARU buat trigger entry.

        Cuma aktif kalau config.USE_ENTRY_ON_STARTUP=True DAN pair itu
        belum ada posisi terbuka. TETAP lewat try_open_position() seperti
        biasa -- semua pengaman risiko (MAX_CONCURRENT_POSITIONS, saldo,
        dll, yang dicek di dalam open_position()) tetap berlaku penuh.

        Pair SPOT SELALU DILEWATI (diabaikan) -- fitur ini cuma berlaku
        untuk Futures. Alasannya: entry startup ini murni ikut tren
        SEKARANG tanpa filter konfirmasi apapun, sementara Spot biasanya
        dipakai buat hold lebih lama (apalagi kalau USE_SPOT_SLTP=False,
        "buy and hold") -- entry paksa tanpa konfirmasi kurang cocok buat
        gaya holding, beda dengan Futures yang memang untuk trading aktif."""
        if pair.market_type == "SPOT":
            return
        if not config.USE_ENTRY_ON_STARTUP or pair.in_position:
            return
        if len(pair.df) < 2:
            return

        df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
        curr = df_ind.iloc[-1]

        if config.USE_EMA_CROSS_STRATEGY:
            fast_val, slow_val = curr.get("ema_fast_x"), curr.get("ema_slow_x")
        else:
            fast_val, slow_val = curr.get("macd_line"), curr.get("macd_signal")

        if pd.isna(fast_val) or pd.isna(slow_val) or fast_val == slow_val:
            return  # data belum cukup, atau PERSIS sama (netral, tidak ada arah jelas)

        side = "LONG" if fast_val > slow_val else "SHORT"
        if side == "SHORT" and not (pair.market_type == "FUTURES" and config.ALLOW_SHORT):
            return  # tidak bisa short di pair ini -- diamkan saja, tidak paksa LONG yang salah arah

        latest = pair.df.iloc[-1]
        price = pair.live_price if pair.live_price is not None else float(latest["close"])

        atr_val = None
        if config.USE_ATR_RISK or config.USE_TRAILING_STOP:
            atr_series = df_ind.get("atr")
            if atr_series is not None and pd.notna(atr_series.iloc[-1]):
                atr_val = float(atr_series.iloc[-1])

        logger.info(f"[{symbol}] Entry OTOMATIS saat startup -- tren SEKARANG: {side} (EMA cepat {'>' if side == 'LONG' else '<'} EMA lambat)")
        self.log_event(f"[{symbol}] Entry otomatis saat bot start (tren sekarang: {side})", "info")
        self.try_open_position(symbol, pair, side, price, float(latest["low"]), float(latest["high"]), atr_val)

    @staticmethod
    def _to_okx_bar(interval: str) -> str:
        """Konversi format interval gaya Binance (self.futures_interval, misal '1m',
        '4h', '1d') ke format 'bar' yang dipakai OKX. Untuk menit sama persis
        (huruf kecil), tapi jam/hari/minggu di OKX pakai HURUF BESAR -- beda
        dari Binance yang selalu huruf kecil. Salah konversi di sini bikin
        request candle OKX ditolak/salah tanpa pesan error yang jelas."""
        if interval.endswith(("h", "d", "w")):
            return interval[:-1] + interval[-1].upper()
        return interval  # menit ('1m', '5m', dst) sama persis di kedua exchange

    def load_history_for(self, sym: str):
        """Muat riwayat candle untuk SATU symbol saja, sesuai market_type
        pair itu SAAT INI (dipakai saat start awal maupun setelah switch
        pasar SPOT<->FUTURES, supaya candle yang dipakai strategi selalu
        dari pasar yang benar)."""
        pair = self.pairs[sym]
        interval = self._interval_for(pair)
        try:
            bar = self._to_okx_bar(interval)
            raw = self.okx_client.get_candles(sym, bar=bar, limit=config.KLINE_HISTORY)
            raw = list(reversed(raw))  # OKX kirim TERBARU DULU -- reverse supaya urutan waktu naik
            rows = [{
                "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                "close_time": int(k[0]) + self._interval_to_ms(interval),
            } for k in raw]
            pair.df = pd.DataFrame(rows)
            logger.info(f"[{sym}] Riwayat {len(pair.df)} candle berhasil dimuat ({interval}).")
        except Exception as e:
            logger.error(f"[{sym}] Gagal memuat riwayat candle: {e}")

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        """Konversi '1m'/'5m'/'1h'/'1d' dll ke durasi milidetik -- dipakai
        untuk hitung close_time candle OKX (yang tidak dikirim eksplisit
        oleh API-nya)."""
        unit = interval[-1]
        value = int(interval[:-1])
        multiplier = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
        return value * multiplier.get(unit, 60_000)

    # ---------- SALDO ----------
    def get_balance(self, asset: str, market_type: str, exchange: str = "OKX") -> float:
        """market_type: 'SPOT' atau 'FUTURES' (fork ini OKX-only, jadi
        exchange praktis selalu 'OKX' -- parameter dipertahankan supaya
        signature tetap kompatibel dengan caller yang meneruskan pair.exchange)."""
        if self.dry_run:
            # Saldo simulasi BENERAN naik/turun sesuai PnL yang sudah
            # terealisasi -- BUKAN flat config.INITIAL_SIMULATED_BALANCE
            # terus-terusan. self.realized_pnl_quote sendiri persisten
            # lintas restart (pnl_history.json), jadi saldo simulasi ini
            # otomatis ikut "keinget" juga tanpa perlu disimpan terpisah.
            return config.INITIAL_SIMULATED_BALANCE + self.realized_pnl_quote
        try:
            return self.okx_client.get_balance(asset)
        except Exception as e:
            logger.error(f"Gagal ambil saldo OKX: {e}")
            return 0.0

    def _interval_for(self, pair: "PairState") -> str:
        """Timeframe yang berlaku untuk SATU pair -- Spot dan Futures
        TERPISAH TOTAL (self.spot_interval vs self.futures_interval).
        Dipakai di SEMUA tempat yang butuh tahu interval candle sebuah
        pair (load histori, subscribe WebSocket, polling OKX, dll) --
        JANGAN pernah baca self.futures_interval/spot_interval langsung
        di kode lain, selalu lewat method ini supaya konsisten."""
        return self.spot_interval if pair.market_type == "SPOT" else self.futures_interval

    def count_open_positions(self) -> int:
        return sum(1 for p in list(self.pairs.values()) if p.in_position)

    def _calc_unrealized_pnl(self) -> float:
        """Total unrealized PnL (floating, dalam QUOTE_ASSET) dari SEMUA
        posisi yang sedang terbuka SEKARANG -- SUMBER TUNGGAL yang dipakai
        BARENG oleh dashboard (save_state()) DAN status/menu Telegram,
        supaya kedua tempat itu SELALU menghitung angka yang SAMA PERSIS
        (rumus dan sumber harga identik: pair.live_price, fallback ke
        close candle terakhir) -- tidak akan pernah "kelihatan beda"
        lagi antara Telegram dan dashboard.

        BEDA dari loop di save_state(): helper ini SENGAJA tidak menghitung
        indikator (macd/ema/dst) per pair -- cuma butuh entry_price, side,
        quantity, dan harga sekarang -- jadi jauh lebih ringan dipanggil
        kapan saja (termasuk tiap kali user ketik /status di Telegram),
        tanpa perlu strategy.compute_indicators() yang lebih berat."""
        total = 0.0
        for pair in list(self.pairs.values()):
            if not (pair.in_position and pair.entry_price):
                continue
            price = pair.live_price
            if price is None and len(pair.df) > 0:
                price = float(pair.df.iloc[-1]["close"])
            if not price:
                continue
            if pair.position_side == "SHORT":
                total += (pair.entry_price - price) * pair.quantity
            else:
                total += (price - pair.entry_price) * pair.quantity
        return total

    # ---------- KONVERSI KOIN <-> CONTRACTS (khusus SWAP/FUTURES OKX) ----------
    def _coin_qty_to_contracts(self, symbol: str, coin_qty: float):
        """Konversi jumlah KOIN (base asset, misal 27 XRP) ke CONTRACTS OKX
        -- unit asli yang dipakai OKX untuk order/posisi SWAP, lihat
        docstring okx_client.get_instrument_spec() untuk latar belakang
        lengkap kenapa konversi ini WAJIB ADA (tanpa ini, order FUTURES
        yang dikirim salah ukuran total).

        Dibulatkan KE BAWAH ke kelipatan lot_sz terdekat -- supaya order
        yang benar-benar dikirim TIDAK PERNAH melebihi budget risiko yang
        sudah dihitung risk_manager (round-up bisa bikin posisi sedikit
        lebih besar dari yang dimaksud).

        Return None kalau gagal ambil spesifikasi instrument, ATAU hasil
        konversi di bawah ukuran minimum kontrak exchange (order memang
        tidak valid untuk dieksekusi -- caller harus batalkan order,
        BUKAN memaksa kirim ukuran yang salah).
        """
        try:
            spec = self.okx_client.get_instrument_spec(symbol)
        except Exception as e:
            logger.error(f"[{symbol}] Gagal ambil spesifikasi instrument OKX (ctVal/lotSz): {e}")
            return None

        ct_val = spec["ct_val"]
        lot_sz = spec["lot_sz"]
        min_sz = spec["min_sz"]
        if ct_val <= 0 or lot_sz <= 0:
            logger.error(f"[{symbol}] Spesifikasi instrument tidak valid (ct_val={ct_val}, lot_sz={lot_sz}).")
            return None

        contracts_raw = coin_qty / ct_val
        # +1e-9 -- jaga-jaga floating point error (contoh nyata: 0.29/0.01
        # di Python menghasilkan 28.999999999999996, bukan 29.0 pas --
        # tanpa epsilon ini, int() akan salah membulatkan ke BAWAH satu
        # step lebih jauh dari seharusnya).
        steps = int(contracts_raw / lot_sz + 1e-9)  # floor -- coin_qty & lot_sz selalu positif di sini
        contracts = steps * lot_sz
        if contracts < min_sz:
            return None

        # Bulatkan presisi float ala 0.30000000000000004 -- jumlah desimal
        # ikut jumlah desimal lot_sz (contoh lot_sz=0.01 -> 2 desimal).
        lot_str = f"{lot_sz:.10f}".rstrip("0")
        decimals = len(lot_str.split(".")[1]) if "." in lot_str else 0
        contracts = round(contracts, decimals)
        return contracts

    def _contracts_to_coin_qty(self, symbol: str, contracts: float):
        """Kebalikan dari _coin_qty_to_contracts() -- dipakai saat MEMBACA
        posisi ASLI dari OKX (get_positions() mengembalikan field "pos"
        dalam CONTRACTS) supaya pair.quantity internal konsisten dalam
        KOIN, sama seperti yang dihasilkan risk_manager.calculate_position_size()
        untuk posisi yang dibuka bot sendiri. Return None kalau gagal ambil
        spesifikasi instrument -- caller HARUS menangani ini (jangan
        menganggap qty 1:1 dengan contracts, itu salah satuan)."""
        try:
            spec = self.okx_client.get_instrument_spec(symbol)
        except Exception as e:
            logger.error(f"[{symbol}] Gagal ambil spesifikasi instrument OKX (ctVal) saat baca posisi: {e}")
            return None
        return contracts * spec["ct_val"]

    # ---------- EKSEKUSI ORDER ----------
    def execute_order(self, symbol: str, side: str, quantity: float, price: float,
                       market_type: str, exchange: str = "OKX", reduce_only: bool = False):
        market_label = f"[{exchange}:{market_type}]"
        if self.dry_run:
            logger.info(f"[DRY_RUN]{market_label} {side} {quantity:.6f} {symbol} @ ~{fmt_price_log(price)}")
            return {"status": "SIMULATED", "side": side, "quantity": quantity, "price": price}

        try:
            # OKX pakai 'buy'/'sell' huruf kecil, dan tdMode beda tergantung
            # Spot ('cash') vs Futures/SWAP ('cross'/'isolated').
            td_mode = config.OKX_MARGIN_MODE if market_type == "FUTURES" else "cash"

            sz = quantity
            if market_type == "FUTURES":
                # PENTING (ditemukan 21 Sept 2026): quantity di sini SELALU
                # dalam KOIN (hasil risk_manager.calculate_position_size()
                # atau pair.quantity yang sudah disamakan satuannya), tapi
                # OKX SWAP mewajibkan "sz" dalam CONTRACTS -- HARUS
                # dikonversi dulu (lihat _coin_qty_to_contracts() dan
                # docstring get_instrument_spec() untuk detail lengkap).
                # Sebelum fix ini, quantity koin dikirim MENTAH-MENTAH
                # sebagai contracts -- inilah AKAR PENYEBAB order live yang
                # berulang kali ditolak OKX dengan "All operations failed"
                # untuk SATS/PEPE/dll sepanjang sesi ini (ukurannya jadi
                # jutaan kali lebih besar dari maksimum yang diizinkan).
                contracts = self._coin_qty_to_contracts(symbol, quantity)
                if contracts is None:
                    logger.error(
                        f"[{symbol}] Order dibatalkan -- gagal konversi {quantity:.6f} koin ke "
                        f"contracts OKX (spec instrument tidak didapat, atau hasil di bawah "
                        f"ukuran minimum kontrak exchange)."
                    )
                    return None
                sz = contracts

            order = self.okx_client.place_order(
                symbol, side.lower(), str(sz if market_type == "FUTURES" else round(quantity, 6)), td_mode=td_mode,
                reduce_only=(reduce_only and market_type == "FUTURES"),
            )
            extra = f" ({sz} contracts, qty asli {quantity:.6f} koin)" if market_type == "FUTURES" else ""
            if reduce_only and market_type == "FUTURES":
                extra += " [reduceOnly]"
            logger.info(f"Order EKSEKUSI {market_label} [{symbol}]: {order}{extra}")
            return order
        except Exception as e:
            logger.error(f"[{symbol}] (OKX) Gagal eksekusi order: {e}")
            return None

    # ---------- CANDLE CHART (untuk dashboard) ----------
    def save_candles(self, symbol: str):
        pair = self.pairs[symbol]
        if len(pair.df) == 0:
            return

        # Hitung indikator dulu di SELURUH riwayat (pair.df), BUKAN cuma
        # window yang mau ditampilkan -- EMA butuh "pemanasan" dari histori
        # sebelumnya, kalau dihitung cuma dari window kecil nilainya beda
        # dari yang BENAR-BENAR dipakai bot buat ambil keputusan sinyal.
        try:
            indicators_full = strategy.compute_indicators(pair.df, market_type=pair.market_type)
        except Exception as e:
            logger.debug(f"[{symbol}] Gagal hitung indikator untuk overlay chart: {e}")
            indicators_full = pair.df

        merged = pair.df.copy()
        # Hanya sertakan kolom indikator yang dipakai strategy aktif --
        # jangan kirim overlay yang dimatikan di config (chart jadi bersih).
        wanted_cols = []
        if config.USE_EMA_CROSS_STRATEGY:
            wanted_cols.extend(["ema_fast_x", "ema_slow_x"])
        else:
            wanted_cols.extend(["macd_line", "macd_signal"])
        if config.USE_TREND_FILTER:
            wanted_cols.append("ema_trend")
        if config.USE_EARLY_REVERSAL:
            wanted_cols.extend(["ema_reversal_fast", "ema_reversal_slow"])
        if config.USE_RSI_FILTER:
            wanted_cols.append("rsi")
        if config.USE_ADX_FILTER:
            wanted_cols.append("adx")
        if config.USE_SMII_FILTER:
            wanted_cols.extend(["smi", "smi_signal"])
        # ema_fast/slow tetap dihitung di compute_indicators untuk chart
        # referensi visual walau trigger MACD -- tetap sertakan kalau ada
        if "ema_fast_x" in indicators_full.columns and "ema_fast_x" not in wanted_cols:
            wanted_cols.append("ema_fast_x")
        if "ema_slow_x" in indicators_full.columns and "ema_slow_x" not in wanted_cols:
            wanted_cols.append("ema_slow_x")
        for col in wanted_cols:
            if col in indicators_full.columns:
                merged[col] = indicators_full[col]

        # Lapisan pertahanan KEDUA (selain dedup di on_new_candle): urutkan
        # berdasarkan open_time dan buang duplikat sebelum dikirim ke
        # dashboard. drop_duplicates(keep="last") mempertahankan versi
        # PALING BARU kalau ada open_time yang sama (kandle yang di-update),
        # bukan versi lama -- supaya harga OHLC yang ditampilkan tetap yang
        # paling akurat/terkini. Timestamp duplikat/tidak berurutan bikin
        # lightweight-charts di dashboard gagal total render grafik.
        recent = (
            merged.tail(300)
            .drop_duplicates(subset="open_time", keep="last")
            .sort_values("open_time")
            .tail(150)
        )

        self.candles_cache[symbol] = []
        for _, row in recent.iterrows():
            candle = {
                "time": int(row["open_time"] / 1000),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            # Overlay indikator -- CUMA disertakan kalau memang ada nilainya
            # (kolom ini eksis tergantung config.USE_EMA_CROSS_STRATEGY /
            # USE_TREND_FILTER, dan bisa NaN di awal-awal candle sebelum
            # cukup data buat "pemanasan" EMA).
            if "ema_fast_x" in row.index and pd.notna(row["ema_fast_x"]):
                candle["ema_fast"] = round(float(row["ema_fast_x"]), 8)
            if "ema_slow_x" in row.index and pd.notna(row["ema_slow_x"]):
                candle["ema_slow"] = round(float(row["ema_slow_x"]), 8)
            if "ema_trend" in row.index and pd.notna(row["ema_trend"]):
                candle["ema_trend"] = round(float(row["ema_trend"]), 8)
            if "ema_reversal_fast" in row.index and pd.notna(row["ema_reversal_fast"]):
                candle["ema_reversal_fast"] = round(float(row["ema_reversal_fast"]), 8)
            if "ema_reversal_slow" in row.index and pd.notna(row["ema_reversal_slow"]):
                candle["ema_reversal_slow"] = round(float(row["ema_reversal_slow"]), 8)
            if "rsi" in row.index and pd.notna(row["rsi"]):
                candle["rsi"] = round(float(row["rsi"]), 4)
            if "adx" in row.index and pd.notna(row["adx"]):
                candle["adx"] = round(float(row["adx"]), 4)
            if "smi" in row.index and pd.notna(row["smi"]):
                candle["smi"] = round(float(row["smi"]), 4)
            if "smi_signal" in row.index and pd.notna(row["smi_signal"]):
                candle["smi_signal"] = round(float(row["smi_signal"]), 4)
            if "macd_line" in row.index and pd.notna(row["macd_line"]):
                candle["macd_line"] = round(float(row["macd_line"]), 6)
            if "macd_signal" in row.index and pd.notna(row["macd_signal"]):
                candle["macd_signal"] = round(float(row["macd_signal"]), 6)
            # Spread indicators (% harga) -- buat oscillator dashboard
            close_v = float(row["close"]) if pd.notna(row.get("close")) else 0.0
            ef = row["ema_fast_x"] if "ema_fast_x" in row.index else None
            es = row["ema_slow_x"] if "ema_slow_x" in row.index else None
            if close_v > 0 and ef is not None and pd.notna(ef) and es is not None and pd.notna(es):
                candle["ema_cross_spread"] = round(abs(float(ef) - float(es)) / close_v * 100, 5)
            if close_v > 0 and es is not None and pd.notna(es):
                candle["price_spread"] = round(abs(close_v - float(es)) / close_v * 100, 5)
            self.candles_cache[symbol].append(candle)

        state_writer.write_candles(self.candles_cache)

    # ---------- BUKA / TUTUP POSISI (LONG maupun SHORT) ----------
    def open_position(self, symbol: str, pair: "PairState", side: str, price: float,
                       candle_low: float = None, candle_high: float = None, atr: float = None):
        """side: 'LONG' atau 'SHORT'.

        candle_low/candle_high/atr: dari candle sinyal, dipakai risk_manager
        kalau config.USE_ATR_RISK=True untuk hitung SL berbasis struktur
        candle + volatilitas (bukan persentase tetap). Diabaikan otomatis
        kalau USE_ATR_RISK=False.
        """
        # Spot bisa dikonfigurasi TANPA SL/TP sama sekali (config.USE_SPOT_SLTP=False)
        # -- beli sesuai sinyal, lalu ditahan terus, TIDAK PERNAH ditutup
        # otomatis. Futures TIDAK terpengaruh, selalu tetap pakai SL/TP
        # normal (leverage bikin Futures tanpa SL sangat berisiko likuidasi).
        skip_sltp = pair.market_type == "SPOT" and not config.USE_SPOT_SLTP

        if skip_sltp:
            stop_loss = None
            take_profit = None
            # actual_sl_pct di bawah CUMA dipakai buat HITUNG UKURAN POSISI
            # (position sizing) -- bukan order SL beneran, jadi aman pakai
            # STOP_LOSS_PCT statis sebagai acuan sizing walau SL asli tidak ada.
            actual_sl_pct = config.STOP_LOSS_PCT
        else:
            stop_loss = risk_manager.calculate_stop_loss(price, side, candle_low, candle_high, atr)
            take_profit = risk_manager.calculate_take_profit(price, side, stop_loss)
            # Position sizing ikut jarak SL SEBENARNYA (bukan STOP_LOSS_PCT statis)
            # supaya risiko per-trade tetap konsisten walau SL-nya dari ATR yang
            # jaraknya berubah-ubah tiap sinyal.
            actual_sl_pct = abs(price - stop_loss) / price * 100 if price else config.STOP_LOSS_PCT

        balance = self.get_balance(config.QUOTE_ASSET, pair.market_type, pair.exchange)
        qty = risk_manager.calculate_position_size(balance, price, actual_sl_pct)
        if qty <= 0:
            logger.warning(f"[{symbol}] Ukuran posisi = 0, order dilewati (saldo tidak cukup?).")
            self.log_event(f"[{symbol}] Order dilewati: saldo tidak cukup", "error")
            return

        order_side = "BUY" if side == "LONG" else "SELL"
        order_result = self.execute_order(symbol, order_side, qty, price, pair.market_type, pair.exchange)

        # execute_order() return None kalau order LIVE beneran gagal (exception
        # ditangkap di dalamnya, misal saldo kurang / OKX API error) -- DRY_RUN
        # selalu return dict {"status": "SIMULATED", ...} jadi tidak pernah None
        # di sini. Kalau gagal, JANGAN catat posisi sebagai terbuka: sebelumnya
        # bot tetap set in_position=True walau order-nya gagal, membuat posisi
        # "hantu" yang baru dibersihkan sync_okx_positions() di siklus berikutnya
        # (~30 detik kemudian). Dicegat di sini supaya tidak perlu menunggu itu.
        if order_result is None:
            logger.warning(f"[{symbol}] Order {side} GAGAL dieksekusi di exchange -- posisi TIDAK dicatat, sinyal dilewati.")
            self.log_event(f"[{symbol}] Order {side} gagal dieksekusi (lihat log error di atas) -- posisi dibatalkan", "error")
            return

        pair.in_position = True
        pair.position_side = side

        # FIX 21 Sept 2026: cari avgPx ASLI dari exchange segera setelah order
        # berhasil, alih-alih langsung pakai `price` (harga sinyal/candle SAAT
        # order dikirim, sebelum tau hasil fill sebenarnya). Order response
        # place_order() cuma konfirmasi "order placed", TIDAK langsung kasih
        # avgPx -- baru kelihatan lewat get_positions() setelah order match di
        # exchange. Untuk coin volatile/tipis likuiditasnya, selisihnya bisa
        # signifikan (kejadian nyata: NEIRO-USDT-SWAP, harga sinyal 0.00009778
        # vs avgPx ASLI ~0.0001056 -- beda ~8%, PnL yang ditampilkan jadi SALAH
        # ARAH dari kondisi sebenarnya). Kalau fetch gagal/posisi belum
        # kelihatan, fallback ke `price` seperti biasa -- SL/TP TETAP dihitung
        # dari `price`/`stop_loss` di atas, TIDAK digeser (posisi risiko yang
        # sudah direncanakan saat sinyal tetap dipakai, cuma entry_price yang
        # dikoreksi supaya PnL yang ditampilkan akurat). sync_okx_positions()
        # jadi jaring pengaman kalau koreksi di sini gagal/lewat.
        entry_price = price
        if pair.market_type == "FUTURES" and not self.dry_run:
            try:
                real_positions = self.okx_client.get_positions(inst_type="SWAP")
                for p in real_positions:
                    if p.get("instId") == symbol:
                        real_avg_px = float(p.get("avgPx", 0) or 0)
                        if real_avg_px > 0:
                            diff_pct = abs(real_avg_px - price) / price * 100 if price else 0
                            if diff_pct > 0.5:
                                logger.warning(
                                    f"[{symbol}] Entry price dikoreksi dari harga sinyal {price} ke "
                                    f"avgPx ASLI exchange {real_avg_px} (selisih {diff_pct:.2f}%, "
                                    f"kemungkinan slippage order market). SL/TP tetap dihitung dari "
                                    f"harga sinyal, tidak digeser."
                                )
                            entry_price = real_avg_px
                        break
            except Exception as e:
                logger.error(f"[{symbol}] Gagal ambil avgPx asli setelah buka posisi (pakai harga sinyal sbg fallback): {e}")

        pair.entry_price = entry_price
        pair.stop_loss = stop_loss
        pair.take_profit = take_profit
        pair.quantity = qty
        pair.initial_quantity = qty
        pair.tp_levels = risk_manager.calculate_tp_levels(price, stop_loss, side) if (config.USE_MULTI_TP and not skip_sltp) else []
        pair.initial_risk_distance = abs(price - stop_loss) if stop_loss is not None else None  # None kalau skip_sltp -- manage_trade_management() otomatis skip juga
        pair.breakeven_triggered = False
        pair.entry_time = datetime.now(timezone.utc)

        if skip_sltp:
            logger.info(f"[{symbol}] Posisi {side} dibuka TANPA SL/TP (USE_SPOT_SLTP=False) | Entry: {pair.entry_price} -- ditahan sampai ditutup manual.")
        else:
            logger.info(f"[{symbol}] Posisi {side} dibuka | Entry: {pair.entry_price} | SL: {pair.stop_loss} | TP: {pair.take_profit}")
        kind = "buy" if side == "LONG" else "sell"
        tp_info = f" | {len(pair.tp_levels)} level TP bertingkat" if pair.tp_levels else ""
        sl_tp_text = f"SL {pair.stop_loss} | TP {pair.take_profit}{tp_info}" if pair.stop_loss is not None else "TANPA SL/TP -- ditahan sampai ditutup manual"
        self.log_event(f"[{symbol}] {side} dibuka @ {fmt_price_log(pair.entry_price)} | {sl_tp_text}", kind)
        log_metric(
            "position_opened", symbol=symbol, exchange=pair.exchange, market_type=pair.market_type,
            side=side, entry=pair.entry_price, sl=pair.stop_loss, tp=pair.take_profit, qty=qty,
            tp_levels=len(pair.tp_levels), dry_run=self.dry_run,
        )
        telegram_notifier.notify_position_opened(config, symbol, side, pair.entry_price, pair.stop_loss, pair.take_profit, self.dry_run)
        self._add_trade_marker(pair, "open_long" if side == "LONG" else "open_short", pair.entry_price, f"{side} {pair.entry_price:.4g}")

    def _add_trade_marker(self, pair: "PairState", marker_type: str, price: float, text: str):
        """Catat satu marker posisi buka/tutup, buat ditampilkan sebagai
        panah di grafik candlestick dashboard persis di candle kejadiannya.

        marker_type: 'open_long' | 'open_short' | 'close' | 'partial_tp'
        Waktu marker diambil dari candle TERAKHIR di pair.df (yang baru saja
        diproses on_new_candle() saat method ini dipanggil) -- BUKAN
        datetime.now(), supaya marker-nya presisi nempel di candle yang
        benar walau ada delay pemrosesan sekecil apapun."""
        if len(pair.df) == 0:
            return
        open_time_ms = pair.df.iloc[-1]["open_time"]
        pair.trade_markers.append({
            "time": int(open_time_ms / 1000),
            "type": marker_type,
            "price": round(float(price), 8),
            "text": text,
        })
        if len(pair.trade_markers) > 100:
            pair.trade_markers = pair.trade_markers[-100:]

        # Persist SEMUA pair (bukan cuma yang baru diubah) ke markers.json,
        # sama seperti pola candles.json -- supaya Flask route /api/markers
        # bisa baca langsung dari file (tidak punya akses ke instance bot
        # yang sedang jalan, cuma bisa komunikasi lewat file).
        all_markers = {sym: p.trade_markers for sym, p in list(self.pairs.items())}
        state_writer.write_markers(all_markers)

    @staticmethod
    def _calc_round_trip_fee(entry_price: float, exit_price: float, quantity: float) -> float:
        """Hitung TOTAL fee (entry + exit, keduanya taker -- bot pakai
        market order) untuk SATU porsi posisi yang ditutup. Dipanggil di
        close_position() DAN partial_close_position() -- kalau posisi
        ditutup bertahap (multi-TP), fee dihitung PRORATA per porsi yang
        ditutup di titik itu, totalnya di akhir = SAMA PERSIS dengan kalau
        ditutup sekaligus (matematis konsisten, tidak dobel hitung).

        Return: total fee dalam quote asset (USDT), SELALU POSITIF
        (dikurangi dari pnl_quote oleh caller, bukan ditambah)."""
        fee_pct = getattr(config, "TAKER_FEE_PCT", 0.05) / 100
        entry_notional = entry_price * quantity
        exit_notional = exit_price * quantity
        return entry_notional * fee_pct + exit_notional * fee_pct

    def close_position(self, symbol: str, pair: "PairState", price: float, reason: str) -> bool:
        """Tutup SISA posisi sepenuhnya (dipakai untuk stop-loss, TP final,
        reverse, atau sinyal exit -- bukan untuk partial TP intermediate,
        lihat partial_close_position() untuk itu).

        Return True kalau BENERAN tertutup (order berhasil ATAU dry_run),
        False kalau order LIVE gagal dieksekusi di exchange.

        PENTING (ditemukan 21 Sept 2026, simetris dengan fix serupa di
        open_position()): SEBELUM fix ini, fungsi ini SELALU melanjutkan
        merealisasi PnL + menandai pair.in_position=False APAPUN hasil
        execute_order() -- kalau order close LIVE gagal (misal exchange
        menolak), bot tetap MENGANGGAP posisi sudah tertutup secara
        internal padahal di exchange MASIH TERBUKA. Konsekuensi nyata yang
        sempat terjadi: realized_pnl_quote tercemar oleh PnL yang TIDAK
        PERNAH benar-benar terealisasi, notifikasi Telegram salah bilang
        "ditutup", dan posisi baru "ditemukan lagi" oleh sync_okx_positions()
        di siklus berikutnya seolah-olah itu posisi asing baru (SL/TP
        dihitung ulang dari nol). Sekarang: kalau order gagal, TIDAK ADA
        state yang diubah sama sekali (in_position, entry, SL/TP, quantity
        semuanya dibiarkan APA ADANYA) -- caller (lihat check_partial_exits/
        check_early_reversal/reverse-entry di on_new_candle) WAJIB cek nilai
        balik ini sebelum menganggap posisi sudah flat."""
        order_side = "SELL" if pair.position_side == "LONG" else "BUY"
        order_result = self.execute_order(
            symbol, order_side, pair.quantity, price, pair.market_type, pair.exchange, reduce_only=True,
        )
        if order_result is None:
            msg = (
                f"[{symbol}] Order CLOSE ({reason}) GAGAL dieksekusi di exchange -- posisi "
                f"TETAP dianggap terbuka (TIDAK ADA PnL yang direalisasi, TIDAK ADA state yang "
                f"diubah). Akan dicoba lagi candle berikutnya, atau disinkronkan ulang oleh "
                f"sync_okx_positions()."
            )
            logger.error(msg)
            self.log_event(msg, "error")
            return False

        # Hitung profit/loss yang baru saja terealisasi dari SISA posisi ini
        # (bukan initial_quantity -- kalau sebagian sudah di-partial-close
        # sebelumnya, PnL dari bagian itu sudah masuk realized_pnl duluan
        # lewat partial_close_position, jangan dihitung dobel di sini).
        if pair.position_side == "SHORT":
            pnl_quote = (pair.entry_price - price) * pair.quantity
        else:
            pnl_quote = (price - pair.entry_price) * pair.quantity
        fee_paid = self._calc_round_trip_fee(pair.entry_price, price, pair.quantity)
        pnl_quote -= fee_paid
        self.realized_pnl_quote += pnl_quote
        self.realized_pnl_by_market[pair.market_type] = (
            self.realized_pnl_by_market.get(pair.market_type, 0.0) + pnl_quote
        )
        state_writer.write_pnl_history(self.realized_pnl_quote, self.realized_pnl_by_market)

        logger.info(
            f"[{symbol}] Posisi {pair.position_side} ditutup ({reason}) | "
            f"Entry: {pair.entry_price} -> Exit: {price} | "
            f"PnL (setelah fee {fee_paid:.4f}): {pnl_quote:+.4f} {config.QUOTE_ASSET}"
        )
        kind = "sell" if pair.position_side == "LONG" else "buy"
        self.log_event(
            f"[{symbol}] {pair.position_side} ditutup ({reason}) @ {fmt_price_log(price)} | "
            f"PnL {pnl_quote:+.2f} {config.QUOTE_ASSET}",
            kind,
        )
        pnl_pct = round((pnl_quote / (pair.entry_price * pair.quantity)) * 100, 4) if pair.entry_price and pair.quantity else None
        log_metric(
            "position_closed", symbol=symbol, exchange=pair.exchange, market_type=pair.market_type,
            side=pair.position_side, reason=reason, entry=pair.entry_price, exit=price,
            qty=pair.quantity, pnl_quote=round(pnl_quote, 8), pnl_pct=pnl_pct, dry_run=self.dry_run,
        )
        telegram_notifier.notify_position_closed(
            config, symbol, pair.position_side, reason, pair.entry_price, price,
            pnl_quote, pnl_pct, config.QUOTE_ASSET, self.dry_run,
        )
        self._add_trade_marker(pair, "close", price, f"{reason} {pnl_quote:+.2f}")

        if reason == "STOP_LOSS":
            pair.last_sl_time = datetime.now(timezone.utc)

        # Catatan (20 Sept 2026): fitur reverse otomatis setelah SL kena
        # (USE_SL_REVERSE / try_sl_reverse()) SUDAH DIHAPUS atas permintaan
        # langsung -- dulu sudah default MATI (USE_SL_REVERSE=False) sejak
        # awal dibuat, jadi penghapusan ini tidak mengubah perilaku trading
        # yang sedang berjalan, cuma membersihkan kode yang tidak dipakai.

        pair.in_position = False
        pair.position_side = None
        pair.entry_price = None
        pair.stop_loss = None
        pair.take_profit = None
        pair.quantity = 0.0
        pair.initial_quantity = 0.0
        pair.tp_levels = []
        pair.initial_risk_distance = None
        pair.breakeven_triggered = False
        pair.entry_time = None
        return True

    def partial_close_position(self, symbol: str, pair: "PairState", price: float, level: dict) -> bool:
        """Tutup SEBAGIAN posisi di satu level TP intermediate (bukan level
        final -- level final selalu lewat close_position() supaya tidak ada
        dust tersisa akibat pembulatan).

        Return True kalau BENERAN tertutup (order berhasil ATAU dry_run),
        False kalau order LIVE gagal -- caller (check_partial_exits()) WAJIB
        membatalkan level["hit"]=True yang sudah di-set SEBELUM memanggil
        fungsi ini kalau hasilnya False, supaya level TP itu dicoba lagi
        candle berikutnya alih-alih dilewati permanen (lihat fix simetris
        di close_position(), latar belakang sama: 21 Sept 2026)."""
        close_qty = round(pair.initial_quantity * level["fraction"], 8)
        close_qty = min(close_qty, pair.quantity)  # jaga-jaga tidak melebihi sisa yang ada
        if close_qty <= 0:
            return False

        order_side = "SELL" if pair.position_side == "LONG" else "BUY"
        order_result = self.execute_order(
            symbol, order_side, close_qty, price, pair.market_type, pair.exchange, reduce_only=True,
        )
        if order_result is None:
            msg = (
                f"[{symbol}] Order Partial TP GAGAL dieksekusi di exchange -- quantity & PnL "
                f"TIDAK diubah, level TP ini akan dicoba lagi candle berikutnya."
            )
            logger.error(msg)
            self.log_event(msg, "error")
            return False

        if pair.position_side == "SHORT":
            pnl_quote = (pair.entry_price - price) * close_qty
        else:
            pnl_quote = (price - pair.entry_price) * close_qty
        fee_paid = self._calc_round_trip_fee(pair.entry_price, price, close_qty)
        pnl_quote -= fee_paid
        self.realized_pnl_quote += pnl_quote
        self.realized_pnl_by_market[pair.market_type] = (
            self.realized_pnl_by_market.get(pair.market_type, 0.0) + pnl_quote
        )
        state_writer.write_pnl_history(self.realized_pnl_quote, self.realized_pnl_by_market)

        pair.quantity = round(pair.quantity - close_qty, 8)

        logger.info(
            f"[{symbol}] Partial TP kena @ {fmt_price_log(price)} | Tutup {close_qty} "
            f"({level['fraction']*100:.0f}%) | PnL (setelah fee {fee_paid:.4f}): {pnl_quote:+.4f} {config.QUOTE_ASSET} | "
            f"Sisa quantity: {pair.quantity}"
        )
        kind = "sell" if pair.position_side == "LONG" else "buy"
        self.log_event(
            f"[{symbol}] Partial TP @ {fmt_price_log(price)} - tutup {level['fraction']*100:.0f}% posisi | "
            f"PnL {pnl_quote:+.2f} {config.QUOTE_ASSET} | Sisa: {pair.quantity}",
            kind,
        )
        log_metric(
            "partial_tp_hit", symbol=symbol, exchange=pair.exchange, market_type=pair.market_type,
            side=pair.position_side, level_price=level["price"], fraction=level["fraction"],
            close_qty=close_qty, remaining_qty=pair.quantity, pnl_quote=round(pnl_quote, 8), dry_run=self.dry_run,
        )
        self._add_trade_marker(pair, "partial_tp", price, f"TP {level['fraction']*100:.0f}%")
        return True

    def check_partial_exits(self, symbol: str, pair: "PairState", price: float) -> Optional[str]:
        """Cek exit untuk satu candle: stop-loss (prioritas tertinggi, tutup
        SEMUA sisa), lalu level TP satu-satu urut dari yang paling dekat.
        Return alasan exit ("STOP_LOSS"/"TAKE_PROFIT_FINAL"/dst) kalau
        posisi FULL CLOSED (truthy, dipakai caller buat tahu APA alasannya),
        atau None kalau masih ada sisa posisi terbuka (termasuk setelah
        partial, None juga falsy jadi kode caller lama `if check_partial_exits(...)`
        tetap jalan sama seperti sebelum diubah dari bool).

        Kalau pair.stop_loss is None -- posisi Spot yang sengaja dibuka
        TANPA SL/TP (config.USE_SPOT_SLTP=False) -- fungsi ini TIDAK
        melakukan apapun sama sekali, posisi dibiarkan terbuka selamanya
        sampai ditutup manual lewat manual_close_position()."""
        if pair.stop_loss is None:
            return None

        # Stop-loss selalu dicek DULU -- membatalkan semua TP level yang
        # belum kena, apapun mode TP-nya (single atau bertingkat).
        hit_sl = (price <= pair.stop_loss) if pair.position_side == "LONG" else (price >= pair.stop_loss)
        if hit_sl:
            self.close_position(symbol, pair, price, "STOP_LOSS")
            return "STOP_LOSS"

        if not config.USE_MULTI_TP or not pair.tp_levels:
            # Fallback ke logika lama: satu TP final saja, seluruh posisi ditutup sekaligus.
            exit_reason = risk_manager.should_exit(
                price, pair.entry_price, pair.stop_loss, pair.take_profit, pair.position_side
            )
            if exit_reason != "HOLD":
                self.close_position(symbol, pair, price, exit_reason)
                return exit_reason
            return None

        # Cek tiap level TP urut dari yang paling dekat entry -- begitu satu
        # level belum kena, level setelahnya (lebih jauh) pasti juga belum.
        for i, level in enumerate(pair.tp_levels):
            if level["hit"]:
                continue

            # Level TERAKHIR (final) bisa "dilepas" dari target harga tetap
            # begitu level SEBELUMNYA (kedua-dari-terakhir) sudah kena --
            # sisa posisi dibiarkan ikut tren, exit-nya diserahkan ke
            # check_early_reversal() (dipanggil SEBELUM method ini tiap
            # candle) atau SL, bukan lagi harga TP tetap. Cuma berlaku
            # kalau ada MINIMAL 2 level TP (level tunggal tetap pakai
            # target harga tetap seperti biasa, tidak ada apa-apa buat
            # "dilepas" duluan).
            if (
                level.get("final") and config.USE_TREND_EXIT_FINAL_TP
                and len(pair.tp_levels) >= 2 and pair.tp_levels[i - 1]["hit"]
            ):
                break

            hit = (price >= level["price"]) if pair.position_side == "LONG" else (price <= level["price"])
            if not hit:
                break
            level["hit"] = True

            if level.get("final"):
                self.close_position(symbol, pair, price, "TAKE_PROFIT_FINAL")
                return "TAKE_PROFIT_FINAL"
            if not self.partial_close_position(symbol, pair, price, level):
                # Order partial TP gagal di exchange -- batalkan level["hit"]=True
                # yang sudah di-set di atas supaya level ini dicoba lagi candle
                # berikutnya, bukan dilewati permanen (fix 21 Sept 2026).
                level["hit"] = False

        return None

    def manage_trade_management(self, symbol: str, pair: "PairState", price: float, atr_val: float):
        """Break-even dan trailing stop -- dipanggil TIAP candle SEBELUM
        check_partial_exits(), supaya SL yang baru dipindah langsung
        berlaku dievaluasi di candle yang sama (misal candle ini juga yang
        memicu breakeven DAN sekaligus menyentuh level barunya)."""
        if not pair.in_position or not pair.initial_risk_distance or pair.initial_risk_distance <= 0:
            return

        # --- BREAK EVEN: pindah SL ke dekat entry begitu profit capai BREAK_EVEN_AT_R ---
        if config.USE_BREAK_EVEN and not pair.breakeven_triggered:
            if pair.position_side == "LONG":
                profit_r = (price - pair.entry_price) / pair.initial_risk_distance
            else:
                profit_r = (pair.entry_price - price) / pair.initial_risk_distance

            if profit_r >= config.BREAK_EVEN_AT_R:
                new_sl = (
                    pair.entry_price + config.BREAK_EVEN_OFFSET if pair.position_side == "LONG"
                    else pair.entry_price - config.BREAK_EVEN_OFFSET
                )
                # cuma pindah kalau memang lebih menguntungkan dari SL sekarang (SL tidak boleh mundur)
                improves = (
                    (pair.position_side == "LONG" and new_sl > pair.stop_loss) or
                    (pair.position_side == "SHORT" and new_sl < pair.stop_loss)
                )
                if improves:
                    old_sl = pair.stop_loss
                    pair.stop_loss = risk_manager._round_price(new_sl)
                    pair.breakeven_triggered = True
                    logger.info(f"[{symbol}] Break-even @ {profit_r:.2f}R: SL {old_sl} -> {pair.stop_loss}")
                    self.log_event(f"[{symbol}] SL dipindah ke breakeven @ {pair.stop_loss} (profit {profit_r:.2f}R)", "info")
                    log_metric(
                        "breakeven_triggered", symbol=symbol, side=pair.position_side,
                        old_sl=old_sl, new_sl=pair.stop_loss, profit_r=round(profit_r, 4),
                    )

        # --- TRAILING STOP: aktif SETELAH level TP pertama kena (butuh USE_MULTI_TP) ---
        if config.USE_TRAILING_STOP and pair.tp_levels and atr_val is not None and pd.notna(atr_val):
            first_level_hit = pair.tp_levels[0]["hit"]
            if first_level_hit:
                trail_distance = atr_val * config.TRAILING_ATR_MULT
                if pair.position_side == "LONG":
                    new_sl = risk_manager._round_price(price - trail_distance)
                    if new_sl > pair.stop_loss:  # trailing SL cuma boleh naik, tidak pernah mundur
                        old_sl = pair.stop_loss
                        pair.stop_loss = new_sl
                        logger.info(f"[{symbol}] Trailing stop: SL naik ke {pair.stop_loss}")
                        log_metric("trailing_stop_updated", symbol=symbol, side="LONG", old_sl=old_sl, new_sl=pair.stop_loss)
                else:
                    new_sl = risk_manager._round_price(price + trail_distance)
                    if new_sl < pair.stop_loss:  # trailing SL cuma boleh turun (SHORT)
                        old_sl = pair.stop_loss
                        pair.stop_loss = new_sl
                        logger.info(f"[{symbol}] Trailing stop: SL turun ke {pair.stop_loss}")
                        log_metric("trailing_stop_updated", symbol=symbol, side="SHORT", old_sl=old_sl, new_sl=pair.stop_loss)

    def check_early_reversal(self, symbol: str, pair: "PairState", price: float,
                              candle_low: float, candle_high: float, atr_val: float) -> bool:
        """Pantau EMA reversal dini (config.EMA_REVERSAL_FAST_LEN/SLOW_LEN).
        TERPISAH dari EMA_FAST_LEN/EMA_SLOW_LEN (trigger ENTRY BARU).

        Cuma dipantau saat POSISI SEDANG TERBUKA. Trigger = CROSS EMA
        reversal melawan arah posisi (tanpa syarat spread/jarak minimal).
        Saat cross terdeteksi: tutup posisi, lalu reverse ke arah lawan
        (kalau market mengizinkan SHORT).

        Return True kalau terjadi reversal/exit protektif -- caller HARUS
        skip proses candle ini lebih lanjut."""
        if not config.USE_EARLY_REVERSAL or not pair.in_position:
            return False
        if pair.stop_loss is None:
            # Spot tanpa SL/TP -- tidak pernah ditutup otomatis, termasuk reversal.
            return False
        if len(pair.df) < 2:
            return False

        df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
        if "ema_reversal_fast" not in df_ind.columns:
            return False

        prev = df_ind.iloc[-2]
        curr = df_ind.iloc[-1]
        for col in ("ema_reversal_fast", "ema_reversal_slow"):
            if pd.isna(prev.get(col)) or pd.isna(curr.get(col)):
                return False

        # FIX 21 Sept 2026: syarat jarak minimal EMA reversal diaktifkan lagi
        # (config.EMA_REVERSAL_SPREAD_MIN_PCT) -- SEBELUMNYA ini murni "cross
        # sederhana" tanpa threshold sama sekali, dan data live membuktikan
        # itu hampir selalu salah: dari 91 kejadian EARLY_REVERSAL (periode
        # EMA_REVERSAL 5/13 lama), cuma 4 yang untung (win rate 4.4%) --
        # tanda mekanisme ini bereaksi ke wobble/noise candle-by-candle,
        # bukan reversal tren yang benar-benar meyakinkan.
        #
        # Sekarang reversal dianggap TERKONFIRMASI kalau EMA reversal fast
        # SUDAH di sisi lawan arah posisi DAN jaraknya (relatif harga) >=
        # EMA_REVERSAL_SPREAD_MIN_PCT -- BUKAN lagi wajib persis di candle
        # cross terjadi. Kalau baru cross tapi jaraknya masih tipis, belum
        # trigger candle ini -- ditunggu sampai jaraknya cukup lebar di
        # candle berikutnya (atau malah tidak pernah lebar & sinyal balik
        # lagi duluan, yang berarti memang TIDAK trigger sama sekali --
        # ini justru perilaku yang diinginkan, bukan bug). Karena begitu
        # reversal benar2 dieksekusi posisi langsung berganti arah, kondisi
        # utk sisi LAMA otomatis tidak pernah dicek lagi -- jadi tidak ada
        # risiko trigger berulang utk reversal yang sama.
        #
        # Dengan EMA_REVERSAL_SPREAD_MIN_PCT=0.0 (nilai lama/default),
        # perilaku ini degradasi balik PERSIS seperti cross sederhana
        # sebelumnya -- backward compatible, tidak mengubah apa pun sampai
        # nilainya benar-benar diisi > 0 (lewat menu Strategi dashboard/
        # Telegram, sudah ada di STRATEGY_SETTINGS_SCHEMA sejak awal).
        min_rev_spread = float(getattr(config, "EMA_REVERSAL_SPREAD_MIN_PCT", 0.0) or 0.0)
        ema_rev_spread_pct = (
            abs(curr["ema_reversal_fast"] - curr["ema_reversal_slow"]) / price * 100
            if price else 0.0
        )
        confirmed_down = (
            curr["ema_reversal_fast"] < curr["ema_reversal_slow"]
            and ema_rev_spread_pct >= min_rev_spread
        )
        confirmed_up = (
            curr["ema_reversal_fast"] > curr["ema_reversal_slow"]
            and ema_rev_spread_pct >= min_rev_spread
        )
        label = f"EMA reversal cross {config.EMA_REVERSAL_FAST_LEN}/{config.EMA_REVERSAL_SLOW_LEN}"
        if min_rev_spread > 0:
            label += f" (jarak >= {min_rev_spread}%)"

        if pair.position_side == "LONG" and confirmed_down:
            can_short = pair.market_type == "FUTURES" and config.ALLOW_SHORT
            logger.info(f"[{symbol}] {label} BEARISH -- tren balik terdeteksi saat LONG.")
            self.log_event(
                f"[{symbol}] Tren balik ({label}) -- "
                f"{'reverse ke SHORT' if can_short else 'tutup posisi (tidak bisa short)'}.",
                "info",
            )
            closed_ok = self.close_position(symbol, pair, price, "EARLY_REVERSAL")
            if can_short and closed_ok:
                self.try_open_position(symbol, pair, "SHORT", price, candle_low, candle_high, atr_val)
            elif can_short and not closed_ok:
                logger.error(
                    f"[{symbol}] Reverse ke SHORT dibatalkan -- order tutup posisi LONG "
                    "gagal di exchange, posisi lama TETAP terbuka & tetap dipantau."
                )
            return True

        if pair.position_side == "SHORT" and confirmed_up:
            logger.info(f"[{symbol}] {label} BULLISH -- tren balik terdeteksi saat SHORT.")
            self.log_event(f"[{symbol}] Tren balik ({label}) -- reverse ke LONG.", "info")
            closed_ok = self.close_position(symbol, pair, price, "EARLY_REVERSAL")
            if closed_ok:
                self.try_open_position(symbol, pair, "LONG", price, candle_low, candle_high, atr_val)
            else:
                logger.error(
                    f"[{symbol}] Reverse ke LONG dibatalkan -- order tutup posisi SHORT "
                    "gagal di exchange, posisi lama TETAP terbuka & tetap dipantau."
                )
            return True

        return False

    # Catatan (20 Sept 2026): method try_sl_reverse() (reverse otomatis
    # SETELAH stop-loss kena, config.USE_SL_REVERSE) SUDAH DIHAPUS atas
    # permintaan langsung. Fitur ini defaultnya sudah MATI (USE_SL_REVERSE
    # = False) sejak pertama dibuat dan tidak pernah diaktifkan di
    # produksi, jadi penghapusan ini TIDAK mengubah perilaku trading yang
    # sedang berjalan -- murni membersihkan kode yang tidak dipakai.
    # TERPISAH dari check_early_reversal() di atas (EMA reversal cross
    # SEBELUM harga menyentuh SL, config.USE_EARLY_REVERSAL) -- fitur itu
    # TETAP ADA dan tidak tersentuh oleh perubahan ini.

    def is_within_trade_hours(self) -> bool:
        """Cek apakah SEKARANG masuk jam yang diizinkan buka posisi BARU
        (config.TRADE_HOURS_START/END, dalam WIB/UTC+7). Tidak mempengaruhi
        exit (SL/TP tetap jalan 24 jam demi keamanan modal) -- cuma
        menahan ENTRY baru di luar jam tersebut."""
        if not config.USE_TRADE_HOURS:
            return True
        wib_hour = (datetime.now(timezone.utc) + timedelta(hours=7)).hour
        start, end = config.TRADE_HOURS_START, config.TRADE_HOURS_END
        if start <= end:
            return start <= wib_hour < end
        return wib_hour >= start or wib_hour < end  # jam yang melewati tengah malam, misal 22 -> 8

    def is_symbol_in_cooldown(self, pair: "PairState") -> bool:
        """True kalau symbol ini baru saja kena Stop Loss dan masih dalam
        periode jeda (config.COOLDOWN_AFTER_SL_MIN), jadi entry baru
        ditahan dulu supaya tidak langsung entry ulang di kondisi yang
        baru saja terbukti salah arah."""
        if not config.COOLDOWN_AFTER_SL_MIN or pair.last_sl_time is None:
            return False
        elapsed_min = (datetime.now(timezone.utc) - pair.last_sl_time).total_seconds() / 60
        return elapsed_min < config.COOLDOWN_AFTER_SL_MIN

    def entry_allowed(self, symbol: str, pair: "PairState") -> tuple:
        """Cek semua syarat sebelum entry BARU boleh diproses (dipanggil
        untuk entry fresh MAUPUN reverse posisi, keduanya menambah risiko
        baru). Return (allowed: bool, reason: str kalau ditolak)."""
        if not self.is_within_trade_hours():
            return False, f"di luar jam trading ({config.TRADE_HOURS_START}-{config.TRADE_HOURS_END} WIB)"
        if self.is_symbol_in_cooldown(pair):
            remaining = config.COOLDOWN_AFTER_SL_MIN - (datetime.now(timezone.utc) - pair.last_sl_time).total_seconds() / 60
            return False, f"cooldown setelah SL, sisa ~{remaining:.0f} menit"
        return True, ""

    def reverse_allowed(self, symbol: str, pair: "PairState") -> tuple:
        """Syarat TAMBAHAN khusus utk REVERSE_TO_LONG/REVERSE_TO_SHORT (posisi
        terbuka dibalik ke arah lawan), TERPISAH dari entry_allowed() di atas
        (yang cuma cek jam trading & cooldown SL, dan tetap berlaku juga utk
        reverse -- ini tambahan, bukan pengganti).

        FIX 21 Sept 2026: sebelumnya reverse dieksekusi begitu saja begitu
        generate_signal() balik arah, pakai ambang spread YANG SAMA dengan
        entry baru dari flat (config.EMA_CROSS_SPREAD_MIN_PCT) -- padahal
        membalik posisi (tutup posisi lama + buka posisi lawan sekaligus)
        risikonya lebih besar drpd entry biasa (kena 2x biaya/slippage, dan
        kalau sinyal cuma noise tipis, posisi bisa bolak-balik terus alias
        whipsaw). Sekarang reverse butuh EMA-cross-spread JAUH LEBIH LEBAR
        drpd ambang entry biasa (config.REVERSE_EMA_SPREAD_MIN_PCT, default
        2x lipat EMA_CROSS_SPREAD_MIN_PCT) -- entry baru dari FLAT TIDAK
        terpengaruh sama sekali, cuma PEMBALIKAN posisi yang diperketat.

        Return (allowed: bool, reason: str kalau ditolak)."""
        if len(pair.df) < 2:
            return True, ""
        df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
        if "ema_fast_x" not in df_ind.columns:
            return True, ""
        curr = df_ind.iloc[-1]
        ef, es, close = curr.get("ema_fast_x"), curr.get("ema_slow_x"), curr.get("close")
        if pd.isna(ef) or pd.isna(es) or not close:
            return True, ""
        ema_spread_pct = abs(float(ef) - float(es)) / float(close) * 100
        min_reverse_spread = float(getattr(config, "REVERSE_EMA_SPREAD_MIN_PCT", 0.0) or 0.0)
        if ema_spread_pct < min_reverse_spread:
            return False, (
                f"spread EMA {ema_spread_pct:.3f}% < ambang reverse {min_reverse_spread:.3f}% "
                f"(butuh sinyal lebih kuat utk membalik posisi)"
            )
        return True, ""

    def confirm_with_ai(self, symbol: str, pair: "PairState", side: str, price: float):
        """Filter tambahan (opsional) via Claude API. Return (allowed, reason)."""
        if not config.USE_AI_CONFIRMATION:
            return True, "Filter AI dimatikan"
        try:
            df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
            latest = df_ind.iloc[-1]
            context = {
                "symbol": symbol,
                "arah_diajukan": side,
                "harga": price,
                "macd_line": round(float(latest["macd_line"]), 6) if "macd_line" in latest and pd.notna(latest["macd_line"]) else None,
                "macd_signal": round(float(latest["macd_signal"]), 6) if "macd_signal" in latest and pd.notna(latest["macd_signal"]) else None,
                "ema_fast": round(float(latest["ema_fast_x"]), 6) if "ema_fast_x" in latest and pd.notna(latest["ema_fast_x"]) else None,
                "ema_slow": round(float(latest["ema_slow_x"]), 6) if "ema_slow_x" in latest and pd.notna(latest["ema_slow_x"]) else None,
                "ema_trend": round(float(latest["ema_trend"]), 4) if "ema_trend" in latest and pd.notna(latest["ema_trend"]) else None,
                "rsi": round(float(latest["rsi"]), 2) if "rsi" in latest and pd.notna(latest["rsi"]) else None,
                "adx": round(float(latest["adx"]), 2) if "adx" in latest and pd.notna(latest["adx"]) else None,
            }
            return ai_filter.confirm_signal(context)
        except Exception as e:
            logger.error(f"[{symbol}] Gagal siapkan konteks filter AI: {e}")
            return config.AI_FAIL_OPEN, f"Error internal ({e}), fallback ke sinyal teknikal asli"

    def try_open_position(self, symbol: str, pair: "PairState", side: str, price: float,
                           candle_low: float = None, candle_high: float = None, atr: float = None):
        """Buka posisi, tapi lewat filter AI dulu kalau diaktifkan."""
        allowed, reason = self.confirm_with_ai(symbol, pair, side, price)
        if config.USE_AI_CONFIRMATION:
            status = "DISETUJUI" if allowed else "DITOLAK"
            logger.info(f"[{symbol}] Filter AI ({side}): {status} - {reason}")
            self.log_event(f"[{symbol}] Filter AI {side}: {status} - {reason}", "info")

        if allowed:
            self.open_position(symbol, pair, side, price, candle_low, candle_high, atr)
        else:
            logger.info(f"[{symbol}] Entry {side} dibatalkan oleh filter AI: {reason}")

    # ---------- RESHUFFLE PAIR RUGI (19 Sept 2026, diubah jadi trigger SL 20 Sept 2026) ----------
    def _reshuffle_after_sl(self, symbol: str):
        """Dipanggil PERSIS setelah check_partial_exits() menutup posisi
        dengan alasan "STOP_LOSS" (posisi SUDAH closed di titik ini --
        fungsi ini CUMA mengurus pengeluaran pair dari daftar aktif, bukan
        menutup posisi apa pun). Kalau config.USE_PAIR_RESHUFFLE aktif dan
        pair ini BUKAN pair permanen, pair langsung dikeluarkan sepenuhnya
        dari daftar aktif lewat remove_pair_live() yang SUDAH ADA (hapus
        dari live_pairs.json, notifikasi Telegram, dst) -- supaya slotnya
        kosong dan otomatis diisi kandidat baru oleh market scanner di
        siklus berikutnya.

        Catatan (20 Sept 2026): sebelumnya reshuffle dipicu dari floating
        loss mencapai RESHUFFLE_LOSS_PCT% (independen dari SL asli, biasanya
        lebih cepat kena karena jaraknya tetap sementara SL berbasis ATR
        bisa lebih lebar/sempit). Atas permintaan langsung, trigger itu
        DIGANTI TOTAL jadi ini -- reshuffle sekarang HANYA terjadi kalau SL
        benar-benar kena, bukan lagi dari ambang floating loss terpisah.
        RESHUFFLE_LOSS_PCT sudah dihapus dari config.py, tidak dipakai lagi.

        Pair PERMANEN (config.SYMBOLS_SPOT_OKX/FUTURES_OKX -- misal
        BTC-USDT) TIDAK PERNAH direshuffle, walau SL-nya kena -- itu pair
        yang sengaja dipertahankan terus, bukan bagian rotasi otomatis
        scanner (sama definisi "permanen" yang dipakai
        market_scanner._permanent_symbols() buat mengecualikan pair ini
        dari hitungan slot SCANNER_MAX_ACTIVE_PAIRS_OKX)."""
        if not config.USE_PAIR_RESHUFFLE:
            return
        if symbol in market_scanner._permanent_symbols():
            return

        msg = (
            f"[{symbol}] Reshuffle: SL kena -- pair dikeluarkan dari daftar aktif "
            "supaya slotnya diisi kandidat baru oleh scanner."
        )
        logger.info(msg)
        self.log_event(msg, "info")
        ok, remove_msg = self.remove_pair_live(symbol)
        if not ok:
            logger.warning(f"[{symbol}] Reshuffle: gagal keluarkan pair dari daftar aktif: {remove_msg}")


    # ---------- RESHUFFLE PAIR MACET (umur posisi, 21 Sept 2026) ----------
    def check_tp_stall_reshuffle(self, symbol: str, pair: "PairState", price: float) -> bool:
        """Reshuffle berbasis UMUR POSISI -- TERPISAH dari _reshuffle_after_sl()
        di atas (yang trigger dari SL kena). Permintaan langsung: kalau
        posisi kepentok lama tidak maju-maju -- baik belum sampai TP level
        ke-3/4 (r3/r4), ATAUPUN sudah lewat r3 tapi macet di fase "ikut
        tren" nunggu reversal/SL (lihat USE_TREND_EXIT_FINAL_TP) -- slotnya
        kepakai terus padahal scanner mungkin punya kandidat lebih segar.

        Sengaja MURNI berbasis WAKTU sejak entry (config.TP_STALL_RESHUFFLE_HOURS),
        apapun level TP yang sudah/belum kena -- BUKAN dipicu dari level TP
        tertentu secara spesifik, supaya mencakup KEDUA skenario "macet" di
        atas sekaligus dengan satu mekanisme, bukan aturan terpisah per level.

        SYARAT TAMBAHAN (permintaan langsung): kalau posisi MASIH RUGI
        (harga belum balik ke level >= entry_price utk LONG / <= entry_price
        utk SHORT), TIDAK dipaksa tutup -- dibiarkan SL asli yang urus.
        Timeout ini CUMA berlaku buat posisi yang sudah minimal breakeven/
        untung tapi macet tidak maju-maju, BUKAN buat memotong rugi lebih
        awal dari SL yang sudah dihitung risk_manager.

        Dipanggil dari on_new_candle() SEBELUM check_early_reversal()/
        check_partial_exits() -- return True kalau posisi ditutup+
        direshuffle di panggilan ini (caller HARUS skip proses candle ini
        lebih lanjut, pola sama seperti check_early_reversal())."""
        if not config.USE_TP_STALL_RESHUFFLE or not pair.in_position:
            return False
        if pair.entry_time is None:
            return False
        if symbol in market_scanner._permanent_symbols():
            return False

        threshold = float(getattr(config, "TP_STALL_RESHUFFLE_HOURS", 0) or 0)
        if threshold <= 0:
            return False
        hours_open = (datetime.now(timezone.utc) - pair.entry_time).total_seconds() / 3600
        if hours_open < threshold:
            return False

        # Masih rugi (belum balik ke level >= entry) -- biarkan SL asli yang urus.
        at_or_above_entry = (
            price >= pair.entry_price if pair.position_side == "LONG" else price <= pair.entry_price
        )
        if not at_or_above_entry:
            return False

        msg = (
            f"[{symbol}] Reshuffle: posisi sudah {hours_open:.1f} jam (batas {threshold:.1f} jam) "
            "tanpa progres lebih lanjut -- ditutup & slot dikeluarkan supaya scanner cari kandidat baru."
        )
        logger.info(msg)
        self.log_event(msg, "info")
        closed_ok = self.close_position(symbol, pair, price, "RESHUFFLE_STALL")
        if not closed_ok:
            # Order tutup gagal di exchange -- JANGAN keluarkan pair dari
            # live_pairs.json, posisi asli masih terbuka & butuh tetap
            # dipantau (SL/TP). Return False supaya on_new_candle() lanjut
            # proses candle ini seperti biasa alih-alih menganggap sudah beres.
            logger.error(
                f"[{symbol}] Reshuffle (stall) dibatalkan -- order tutup posisi gagal di "
                "exchange, pair TETAP di daftar aktif & tetap dipantau."
            )
            return False

        if not config.USE_PAIR_RESHUFFLE:
            return True
        ok, remove_msg = self.remove_pair_live(symbol)
        if not ok:
            logger.warning(f"[{symbol}] Reshuffle (stall): gagal keluarkan pair dari daftar aktif: {remove_msg}")
        return True

    # ---------- LOGIKA UTAMA SAAT CANDLE BARU (per pair) ----------
    def on_new_candle(self, symbol: str, candle: dict):
        pair = self.pairs[symbol]
        new_row = {
            "open_time": candle["t"], "open": float(candle["o"]), "high": float(candle["h"]),
            "low": float(candle["l"]), "close": float(candle["c"]), "volume": float(candle["v"]),
            "close_time": candle["T"],
        }

        # Cegah candle DUPLIKAT (open_time sama persis) masuk ke pair.df.
        # Ini bisa terjadi kalau OKX re-kirim candle "closed" yang sama
        # lagi di siklus polling berikutnya (REST, bukan push) -- kalau
        # dibiarkan ke-append sebagai baris baru, candles.json jadi punya
        # timestamp duplikat/tidak berurutan, yang bikin lightweight-charts
        # di dashboard gagal total
        # render grafik (throw "Value is null" secara internal, lihat
        # tradingview/lightweight-charts#568). Kalau open_time sama dengan
        # candle TERAKHIR yang sudah ada, REPLACE baris itu, bukan nambah.
        if len(pair.df) > 0 and pair.df.iloc[-1]["open_time"] == new_row["open_time"]:
            pair.df.iloc[-1] = new_row
        else:
            pair.df = pd.concat([pair.df, pd.DataFrame([new_row])], ignore_index=True)
        pair.df = pair.df.tail(config.KLINE_HISTORY * 2).reset_index(drop=True)

        price = new_row["close"]
        pair.live_price = price
        self.save_candles(symbol)
        logger.info(f"[{symbol}] Candle closed @ {price}")
        self.log_event(f"[{symbol}] Candle closed @ {fmt_price_log(price)}", "info")

        # ATR candle ini -- dihitung kalau salah satu dari ATR risk, trailing
        # stop, atau filter volatilitas aktif (semuanya butuh nilai ATR).
        atr_val = None
        if config.USE_ATR_RISK or config.USE_TRAILING_STOP or config.USE_VOLATILITY_FILTER:
            df_ind = strategy.compute_indicators(pair.df, market_type=pair.market_type)
            latest_atr = df_ind.iloc[-1].get("atr")
            if pd.notna(latest_atr):
                atr_val = float(latest_atr)

        # Break-even & trailing SEBELUM cek exit -- supaya SL yang baru
        # dipindah langsung dievaluasi di candle yang sama kalau ternyata
        # candle ini juga yang menyentuhnya.
        if pair.in_position:
            self.manage_trade_management(symbol, pair, price, atr_val)

        # Reshuffle umur posisi (macet lama) -- SEBELUM reversal dini/SL-TP,
        # supaya begitu batas waktu kena, posisi langsung ditutup+direshuffle
        # tanpa proses candle ini lebih jauh.
        if pair.in_position:
            if self.check_tp_stall_reshuffle(symbol, pair, price):
                return

        # Cek reversal dini (EMA 9/21 default) SEBELUM cek SL/TP berbasis
        # harga -- proteksi lebih awal kalau tren sudah kelihatan balik,
        # tidak perlu nunggu harga sungguhan menyentuh level SL.
        if pair.in_position:
            if self.check_early_reversal(symbol, pair, price, new_row["low"], new_row["high"], atr_val):
                return

        # Cek exit dulu kalau sedang posisi (LONG maupun SHORT) -- SL prioritas
        # tertinggi, lalu TP (satu level final, atau bertingkat kalau
        # USE_MULTI_TP aktif). Return alasan exit HANYA kalau posisi full
        # closed (truthy); kalau cuma partial closed, tetap lanjut proses
        # candle ini seperti biasa (posisi masih terbuka, sisa quantity
        # tetap dipantau).
        # Catatan (20 Sept 2026): reverse otomatis setelah SL kena
        # (try_sl_reverse(), config.USE_SL_REVERSE) sudah dihapus -- lihat
        # penjelasan di dekat definisi method lama tersebut.
        if pair.in_position:
            exit_reason = self.check_partial_exits(symbol, pair, price)
            if exit_reason:
                # Reshuffle (20 Sept 2026): dulu dipicu dari ambang floating
                # loss terpisah (RESHUFFLE_LOSS_PCT), SEKARANG diganti total
                # jadi trigger SL asli -- begitu exit_reason == "STOP_LOSS",
                # pair langsung dikeluarkan dari daftar aktif (lihat
                # _reshuffle_after_sl() buat syarat lengkapnya: butuh
                # USE_PAIR_RESHUFFLE aktif dan pair BUKAN pair permanen).
                # Posisi SUDAH ditutup oleh check_partial_exits() di atas,
                # fungsi ini cuma mengurus pengeluaran pair-nya.
                if exit_reason == "STOP_LOSS":
                    self._reshuffle_after_sl(symbol)
                return

        # Cek sinyal strategi
        signal = strategy.generate_signal(pair.df, market_type=pair.market_type)
        pair.last_signal = signal  # selalu simpan (termasuk HOLD) buat ticker dashboard
        if signal != "HOLD":
            logger.info(f"[{symbol}] Sinyal: {signal}")
            self.log_event(f"[{symbol}] Sinyal: {signal}", "info")
            log_metric("signal_generated", symbol=symbol, exchange=pair.exchange, signal=signal, price=price)

        if self.paused:
            if signal in ("BUY", "SELL") and not pair.in_position:
                logger.info(f"[{symbol}] Sinyal {signal} diabaikan: bot sedang dijeda.")
                self.log_event(f"[{symbol}] Sinyal {signal} diabaikan: bot dijeda.", "info")
            self.save_state()
            return

        allow_short = pair.market_type == "FUTURES" and config.ALLOW_SHORT

        if config.MAX_CONCURRENT_POSITIONS is not None and not pair.in_position:
            if self.count_open_positions() >= config.MAX_CONCURRENT_POSITIONS:
                if signal in ("BUY", "SELL"):
                    logger.info(f"[{symbol}] Sinyal {signal} dilewati: sudah mencapai batas posisi bersamaan.")
                return

        # Catatan MAX_1_TRADE_PER_SYMBOL: arsitektur bot ini SELALU membatasi
        # 1 posisi aktif per symbol (PairState cuma nyimpen satu state posisi
        # sekaligus) -- jadi syarat ini otomatis terpenuhi terlepas dari nilai
        # config, tidak butuh pengecekan eksplisit tambahan di sini.

        # Syarat entry BARU (jam trading, cooldown setelah SL) -- cuma
        # menahan ENTRY, tidak pernah menahan exit/SL/TP di atas.
        candle_low, candle_high = new_row["low"], new_row["high"]

        if signal == "BUY":
            wants_new_entry = (pair.in_position and pair.position_side == "SHORT") or not pair.in_position
            if wants_new_entry:
                allowed, block_reason = self.entry_allowed(symbol, pair)
                if not allowed:
                    logger.info(f"[{symbol}] Sinyal BUY ditahan: {block_reason}")
                    self.log_event(f"[{symbol}] Sinyal BUY ditahan: {block_reason}", "info")
                elif pair.in_position and pair.position_side == "SHORT":
                    rev_ok, rev_reason = self.reverse_allowed(symbol, pair)
                    if not rev_ok:
                        logger.info(f"[{symbol}] Reverse ke LONG ditahan: {rev_reason}")
                        self.log_event(f"[{symbol}] Reverse ke LONG ditahan: {rev_reason}", "info")
                    else:
                        closed_ok = self.close_position(symbol, pair, price, "REVERSE_TO_LONG")
                        if closed_ok:
                            self.try_open_position(symbol, pair, "LONG", price, candle_low, candle_high, atr_val)
                        else:
                            logger.error(
                                f"[{symbol}] Reverse ke LONG dibatalkan -- order tutup posisi SHORT "
                                "gagal di exchange, posisi lama TETAP terbuka & tetap dipantau."
                            )
                else:
                    self.try_open_position(symbol, pair, "LONG", price, candle_low, candle_high, atr_val)
            # kalau sudah LONG, sinyal BUY diabaikan (tidak nambah posisi)

        elif signal == "SELL":
            if allow_short:
                wants_new_entry = (pair.in_position and pair.position_side == "LONG") or not pair.in_position
                if wants_new_entry:
                    allowed, block_reason = self.entry_allowed(symbol, pair)
                    if not allowed:
                        logger.info(f"[{symbol}] Sinyal SELL ditahan: {block_reason}")
                        self.log_event(f"[{symbol}] Sinyal SELL ditahan: {block_reason}", "info")
                    elif pair.in_position and pair.position_side == "LONG":
                        rev_ok, rev_reason = self.reverse_allowed(symbol, pair)
                        if not rev_ok:
                            logger.info(f"[{symbol}] Reverse ke SHORT ditahan: {rev_reason}")
                            self.log_event(f"[{symbol}] Reverse ke SHORT ditahan: {rev_reason}", "info")
                        else:
                            closed_ok = self.close_position(symbol, pair, price, "REVERSE_TO_SHORT")
                            if closed_ok:
                                self.try_open_position(symbol, pair, "SHORT", price, candle_low, candle_high, atr_val)
                            else:
                                logger.error(
                                    f"[{symbol}] Reverse ke SHORT dibatalkan -- order tutup posisi LONG "
                                    "gagal di exchange, posisi lama TETAP terbuka & tetap dipantau."
                                )
                    else:
                        self.try_open_position(symbol, pair, "SHORT", price, candle_low, candle_high, atr_val)
                # kalau sudah SHORT, sinyal SELL diabaikan
            else:
                if pair.in_position and pair.position_side == "LONG" and pair.stop_loss is not None:
                    # pair.stop_loss is None -- posisi Spot TANPA SL/TP
                    # (config.USE_SPOT_SLTP=False), TIDAK PERNAH ditutup
                    # otomatis oleh sinyal SELL sekalipun.
                    self.close_position(symbol, pair, price, "SIGNAL_SELL")
                # Spot atau ALLOW_SHORT=False: tidak bisa short, sinyal diabaikan kalau tidak ada posisi

        self.save_state()

    # ---------- RUN LOOP DENGAN AUTO-RECONNECT ----------
    def run(self):
        self.load_history()
        for sym in list(self.pairs):
            self.save_candles(sym)

        # Entry otomatis sesuai tren SEKARANG (kalau USE_ENTRY_ON_STARTUP=True)
        # -- dilakukan SEKALI di sini, SETELAH riwayat candle & saldo awal
        # (get_balance()/refresh_portfolio()) siap, SEBELUM loop utama mulai.
        if config.USE_ENTRY_ON_STARTUP:
            for sym, pair in list(self.pairs.items()):
                self.check_startup_entry(sym, pair)

        # PENTING: bersihkan sinyal kontrol BASI dari sesi SEBELUMNYA di
        # control.json sebelum control_loop mulai jalan -- kalau tidak,
        # sisa "shutdown_requested": true dari sesi lampau (misal user
        # sempat pakai tombol shutdown, tapi file-nya tidak kehapus) akan
        # langsung kebaca sebagai PERMINTAAN BARU begitu bot di-restart,
        # dan bot mati sendiri detik itu juga tanpa user minta apapun.
        _control = state_writer.read_control()
        if _control.get("shutdown_requested"):
            _control["shutdown_requested"] = False
            state_writer.write_control(_control)
            logger.info("Sinyal 'shutdown_requested' basi dari sesi sebelumnya dibersihkan saat startup.")
        if _control.get("requested_interval") and _control["requested_interval"] != self.futures_interval:
            # Sinkronkan ke interval SEKARANG (bukan proses ulang) -- cegah
            # reload candle tak terduga di startup gara-gara sisa permintaan
            # basi dari sesi sebelumnya yang belum sempat diproses/disinkronkan.
            _control["requested_interval"] = self.futures_interval
            state_writer.write_control(_control)
        if _control.get("requested_leverage") and _control["requested_leverage"] != config.LEVERAGE:
            _control["requested_leverage"] = config.LEVERAGE
            state_writer.write_control(_control)
        _stale_ema = _control.get("requested_ema_params")
        if _stale_ema and (_stale_ema.get("fast") != config.EMA_FAST_LEN or _stale_ema.get("slow") != config.EMA_SLOW_LEN):
            _control["requested_ema_params"] = {"fast": config.EMA_FAST_LEN, "slow": config.EMA_SLOW_LEN}
            state_writer.write_control(_control)
        _stale_ema_fut = _control.get("requested_ema_params_futures")
        if _stale_ema_fut and (_stale_ema_fut.get("fast") != config.EMA_FAST_LEN_FUTURES or _stale_ema_fut.get("slow") != config.EMA_SLOW_LEN_FUTURES):
            _control["requested_ema_params_futures"] = {"fast": config.EMA_FAST_LEN_FUTURES, "slow": config.EMA_SLOW_LEN_FUTURES}
            state_writer.write_control(_control)
        # Antrian tambah/hapus pair BASI dari sesi sebelumnya (kalau ada
        # sisa yang belum sempat diproses/dikosongkan) -- dikosongkan begitu
        # saja, JANGAN diproses ulang di sesi baru ini (permintaan itu milik
        # kondisi bot yang LAMA, bukan relevan lagi di start yang baru).
        if (
            _control.get("pending_add_pairs") or _control.get("pending_remove_pairs")
            or _control.get("pending_manual_open") or _control.get("pending_manual_close")
            or _control.get("reset_stats_requested") or _control.get("pending_strategy_settings")
        ):
            _control["pending_add_pairs"] = []
            _control["pending_remove_pairs"] = []
            _control["pending_manual_open"] = []
            _control["pending_manual_close"] = []
            _control["reset_stats_requested"] = False
            _control["pending_strategy_settings"] = None
            state_writer.write_control(_control)

        threading.Thread(target=self.portfolio_loop, daemon=True, name="Portfolio").start()
        threading.Thread(target=self.control_loop, daemon=True, name="Control").start()
        threading.Thread(target=self.telegram_listener_loop, daemon=True, name="TelegramListener").start()
        threading.Thread(target=self.discord_listener_loop, daemon=True, name="DiscordListener").start()

        # BUG ditemukan 20 Sept 2026: _dashboard_api_signal_detail._bot_ref
        # DICEK (hasattr) di endpoint /api/signal_detail TAPI TIDAK PERNAH
        # DI-SET di mana pun sebelumnya -- akibatnya market_type yang
        # dikirim ke strategy.get_signal_detail() SELALU None, bukan
        # market_type ASLI pair itu (SPOT/FUTURES). Efeknya: SEMUA nilai
        # parameter yang punya varian *_FUTURES (EMA_FAST/SLOW_LEN,
        # EMA_TREND, RSI_*, ADX_*, SMII_*, DPO_*, MACD_*) di panel "Status
        # Filter Candle Terakhir" dashboard SELALU pakai angka dasar/Spot,
        # BUKAN angka Futures-nya, untuk SEMUA pair termasuk yang Futures --
        # padahal trading yang SEBENARNYA (generate_signal()/save_candles()/
        # dll) sudah benar pakai pair.market_type asli. Jadi panel status
        # filter di dashboard bisa menampilkan label/ambang batas yang
        # BEDA dari yang benar-benar dipakai bot ambil keputusan. Baris di
        # bawah ini melengkapi assignment yang kelewatan itu.
        _dashboard_api_signal_detail._bot_ref = self

        threading.Thread(target=run_dashboard, daemon=True, name="Dashboard").start()

        if config.USE_MARKET_SCANNER:
            market_scanner.start_scanner(self)

        # ---------- HEARTBEAT UTAMA: OKX REST POLLING ----------
        # Fork OKX-only ini TIDAK PUNYA WebSocket sama sekali -- dulu
        # self.twm.join() (blocking call dari ThreadedWebsocketManager
        # Binance) yang menjaga proses ini tetap hidup selamanya di thread
        # utama. Sekarang heartbeat-nya adalah thread okx_poll_loop() (REST
        # polling OKX, sudah dimulai di sini) -- main thread cukup tidur
        # berkala dan memantau thread itu masih hidup, alih-alih blocking
        # join() ke sebuah koneksi WebSocket yang sudah tidak ada.
        self._okx_poll_thread = threading.Thread(target=self.okx_poll_loop, daemon=True, name="OKXPoll")
        self._okx_poll_thread.start()

        self.connected = True
        logger.info(
            f"Bot OKX-only aktif -- memantau {len(self.pairs)} pair via REST polling "
            f"(interval {config.OKX_POLL_INTERVAL_SECONDS}s)."
        )
        self.log_event(f"Bot aktif -- {len(self.pairs)} pair OKX dipantau (REST polling).", "info")

        try:
            while True:
                time.sleep(30)
                if not self._okx_poll_thread.is_alive():
                    # Praktis harusnya tidak pernah terjadi -- okx_poll_loop()
                    # sudah membungkus tiap symbol dengan try/except sendiri
                    # supaya loop-nya sendiri tidak pernah mati -- tapi dijaga
                    # di sini juga sebagai lapis pengaman terakhir, supaya bot
                    # tidak diam-diam berhenti memantau harga tanpa disadari
                    # kalau ternyata ada exception tak terduga yang lolos.
                    logger.error("Thread okx_poll_loop mati tak terduga -- restart otomatis.")
                    self.log_event("Thread polling OKX mati, restart otomatis...", "error")
                    self._okx_poll_thread = threading.Thread(target=self.okx_poll_loop, daemon=True, name="OKXPoll")
                    self._okx_poll_thread.start()
        except KeyboardInterrupt:
            logger.info("Bot dihentikan oleh user.")
            self.connected = False
            self.save_state()


# =====================================================================
# DASHBOARD WEB -- digabung jadi satu proses dengan bot (dulu file
# terpisah dashboard_server.py, dijalankan manual di terminal kedua).
# Sekarang jalan sebagai thread background di proses yang sama, lewat
# run_dashboard() yang dipanggil dari TradingBot.run() di atas.
#
# Route-nya TETAP baca/tulis file (state.json/candles.json/control.json)
# lewat state_writer -- bukan diubah ke akses memori langsung ke objek
# bot -- supaya perilakunya persis sama dengan sebelumnya (termasuk
# atomic write + lock per-file yang sudah menyelesaikan masalah candle
# grafik hilang), cuma sekarang jalan di proses yang sama, bukan proses
# terpisah yang harus dijalankan manual di terminal kedua.
# =====================================================================
# ---------- MENU STRATEGY: whitelist setting yang bisa diubah LIVE dari dashboard ----------
# Format: {nama_atribut_config: (tipe_python, label_tampilan, grup)}. HANYA
# setting di daftar ini yang boleh diubah lewat /api/update_strategy_settings
# -- mencegah dashboard (atau siapapun yang akses endpoint-nya) mengubah
# config SEMBARANGAN (misal API key, credentials, dll yang TIDAK ada di sini).
dashboard_app = Flask(__name__, static_folder="static", static_url_path="")

STATE_FILE = "state.json"
CANDLES_FILE = "candles.json"
LIQUIDATIONS_FILE = "liquidations.json"


@dashboard_app.after_request
def _add_no_cache_headers(response):
    """Cegah browser (atau proxy di jaringan) menyimpan cache respons
    /api/* -- endpoint ini datanya berubah tiap detik, kalau ke-cache
    browser bisa terus nampilin data BASI (misal candles.json versi lama
    sebelum bot di-restart) walau server sudah punya data baru."""
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
    return response


@dashboard_app.route("/")
def _dashboard_index():
    return send_from_directory(dashboard_app.static_folder, "index.html")


@dashboard_app.route("/api/state")
def _dashboard_api_state():
    if not os.path.exists(STATE_FILE):
        return jsonify({"ready": False, "message": "Menunggu bot mulai berjalan..."}), 200
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        data["ready"] = True
        return jsonify(data)
    except (json.JSONDecodeError, OSError):
        # File sedang ditulis ulang tepat saat dibaca, coba lagi sebentar lagi.
        return jsonify({"ready": False, "message": "Membaca data..."}), 200


@dashboard_app.route("/api/candles")
def _dashboard_api_candles():
    symbol = request.args.get("symbol")
    if not os.path.exists(CANDLES_FILE):
        return jsonify([] if symbol else {})
    try:
        with open(CANDLES_FILE) as f:
            data = json.load(f)
        if symbol:
            return jsonify(data.get(symbol, []))
        return jsonify(data)
    except (json.JSONDecodeError, OSError):
        return jsonify([] if symbol else {})


@dashboard_app.route("/api/signal_detail")
def _dashboard_api_signal_detail():
    """Status per-filter buat pair tertentu di candle TERAKHIR --
    dipakai dashboard buat tampilkan indikator mana yang LOLOS/GAGAL.
    Dihitung ulang tiap request dari candles.json (candle terbaru).
    Query param: symbol (wajib)"""
    symbol = request.args.get("symbol")
    if not symbol or not os.path.exists(CANDLES_FILE):
        return jsonify({"signal": "HOLD", "filters": []})
    try:
        with open(CANDLES_FILE) as f:
            data = json.load(f)
        candles = data.get(symbol, [])
        if len(candles) < 2:
            return jsonify({"signal": "HOLD", "filters": []})
        df = pd.DataFrame(candles)
        # Ambil market_type dari pairs yang aktif (kalau ada)
        market_type = None
        if hasattr(_dashboard_api_signal_detail, '_bot_ref'):
            pair = _dashboard_api_signal_detail._bot_ref.pairs.get(symbol)
            if pair:
                market_type = pair.market_type
        detail = strategy.get_signal_detail(df, market_type=market_type)
        return jsonify(detail)
    except Exception as e:
        logger.debug(f"signal_detail error {symbol}: {e}")
        return jsonify({"signal": "HOLD", "filters": []})


@dashboard_app.route("/api/smc_zones")
def _dashboard_api_smc_zones():
    """Zona SMC (Order Block + Fair Value Gap) TERBARU buat SATU pair --
    dihitung ULANG dari candles.json yang sudah di-cache (BUKAN akses
    live TradingBot instance -- Flask route di sini murni fungsi module-
    level, sama pola dengan /api/candles). Return beberapa zona PALING
    BARU tiap tipe (bukan semua yang pernah terdeteksi -- zona lama
    biasanya sudah tidak relevan/"mitigated")."""
    symbol = request.args.get("symbol")
    # market_type dikirim dashboard (dari state.json pairs[symbol].market_type)
    # -- dipakai buat cek SMC_MARKET_SCOPE di sini juga (bukan cuma disembunyikan
    # di frontend), supaya panggilan API langsung ke endpoint ini pun tetap
    # konsisten: kalau scope-nya tidak cocok dengan market_type pair yang
    # diminta, jangan hitung/kirim zona apa pun.
    market_type = (request.args.get("market_type") or "").upper()
    smc_scope = str(getattr(config, "SMC_MARKET_SCOPE", "ALL") or "ALL").upper()
    if market_type and smc_scope != "ALL" and smc_scope != market_type:
        return jsonify({"order_blocks": [], "fvgs": []})
    if not symbol or not os.path.exists(CANDLES_FILE):
        return jsonify({"order_blocks": [], "fvgs": []})
    try:
        with open(CANDLES_FILE) as f:
            data = json.load(f)
        candles = data.get(symbol, [])
        if len(candles) < 10:
            return jsonify({"order_blocks": [], "fvgs": []})

        df = pd.DataFrame(candles)
        obs = strategy.detect_order_blocks(
            df, impulse_threshold_pct=getattr(config, "SMC_OB_IMPULSE_PCT", 0.5),
            lookback=getattr(config, "SMC_OB_LOOKBACK", 50),
        )
        fvgs = strategy.detect_fair_value_gaps(df, lookback=getattr(config, "SMC_FVG_LOOKBACK", 50))

        # Cuma kirim 3 PALING BARU tiap tipe -- chart bakal terlalu ramai
        # kalau semua zona (bisa puluhan) digambar sekaligus.
        recent_obs = obs[-3:] if obs else []
        recent_fvgs = fvgs[-3:] if fvgs else []

        return jsonify({
            "order_blocks": [{"type": o["type"], "top": round(float(o["top"]), 8), "bottom": round(float(o["bottom"]), 8)} for o in recent_obs],
            "fvgs": [{"type": f["type"], "top": round(float(f["top"]), 8), "bottom": round(float(f["bottom"]), 8)} for f in recent_fvgs],
        })
    except Exception as e:
        logger.debug(f"Gagal hitung SMC zones buat {symbol}: {e}")
        return jsonify({"order_blocks": [], "fvgs": []})


@dashboard_app.route("/api/liquidations")
def _dashboard_api_liquidations():
    """Overlay 'Liquidasi Historis' -- dulu event liquidasi NYATA dari
    stream global Binance Futures (!forceOrder@arr, ditangkap lewat
    handle_liquidation_message() di TradingBot yang sekarang SUDAH
    DIHAPUS bersama semua kode Binance -- lihat FORK OKX-ONLY di kepala
    file). OKX tidak punya stream liquidasi publik yang setara dan fork
    ini belum mengimplementasikan penggantinya, jadi endpoint ini
    TETAP ADA (tidak menghapus skema API-nya) tapi SELALU balas list
    kosong sekarang -- LIQUIDATIONS_FILE tidak pernah ditulis lagi."""
    symbol = request.args.get("symbol")
    if not symbol or not os.path.exists(LIQUIDATIONS_FILE):
        return jsonify([])
    try:
        with open(LIQUIDATIONS_FILE) as f:
            data = json.load(f)
        return jsonify(data.get(symbol, []))
    except (json.JSONDecodeError, OSError):
        return jsonify([])


@dashboard_app.route("/api/markers")
def _dashboard_api_markers():
    """Marker posisi buka/tutup (panah di grafik candlestick), format siap
    pakai untuk lightweight-charts' series.setMarkers(). Dikonversi dari
    format internal {type, price, text} ke format library di sini (bukan
    di bot.py) supaya bot.py tidak perlu tahu detail rendering chart sama
    sekali -- murni pemisahan tanggung jawab data vs presentasi."""
    symbol = request.args.get("symbol")
    if not os.path.exists(state_writer.MARKERS_FILE):
        return jsonify([] if symbol else {})
    try:
        with open(state_writer.MARKERS_FILE) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return jsonify([] if symbol else {})

    def to_chart_marker(m):
        mtype = m.get("type")
        if mtype == "open_long":
            return {"time": m["time"], "position": "belowBar", "color": "#4ADE80", "shape": "arrowUp", "text": m.get("text", "")}
        if mtype == "open_short":
            return {"time": m["time"], "position": "aboveBar", "color": "#FB7185", "shape": "arrowDown", "text": m.get("text", "")}
        if mtype == "partial_tp":
            return {"time": m["time"], "position": "aboveBar", "color": "#D4A24C", "shape": "circle", "text": m.get("text", "")}
        # "close" (full close, SL atau TP final atau reverse/signal exit)
        return {"time": m["time"], "position": "aboveBar", "color": "#9089A8", "shape": "square", "text": m.get("text", "")}

    if symbol:
        return jsonify([to_chart_marker(m) for m in data.get(symbol, [])])
    return jsonify({sym: [to_chart_marker(m) for m in markers] for sym, markers in data.items()})


@dashboard_app.route("/api/control")
def _dashboard_api_control():
    return jsonify(state_writer.read_control())


@dashboard_app.route("/api/toggle", methods=["POST"])
def _dashboard_api_toggle():
    """Toggle status aktif/dijeda GLOBAL (semua pair). Bot membaca file ini
    berkala (maks 3 detik delay) lewat control_loop() dan menyesuaikan
    perilakunya. Arah dua-duanya aman -- tidak butuh konfirmasi tambahan,
    beda dengan toggle mode DRY_RUN/LIVE di bawah."""
    control = state_writer.read_control()
    control["paused"] = not bool(control.get("paused", False))
    state_writer.write_control(control)
    return jsonify(control)


@dashboard_app.route("/api/toggle_dry_run", methods=["POST"])
def _dashboard_api_toggle_dry_run():
    """Toggle mode DRY_RUN (simulasi) <-> LIVE (order nyata).

    DRY_RUN -> LIVE mengizinkan order NYATA dengan uang sungguhan, jadi
    endpoint ini WAJIB body JSON {"confirm": true} untuk arah itu --
    kalau tidak ada/false, request ditolak (400) supaya klik tidak sengaja
    di dashboard tidak bisa langsung mengaktifkan LIVE. Arah sebaliknya
    (LIVE -> DRY_RUN, menurunkan risiko) tidak butuh konfirmasi."""
    control = state_writer.read_control()
    current_dry_run = bool(control.get("dry_run", config.DRY_RUN))
    going_live = current_dry_run  # dry_run True -> mau jadi False (LIVE)

    if going_live and not bool((request.get_json(silent=True) or {}).get("confirm")):
        return jsonify({
            "error": "confirmation_required",
            "message": "Mengaktifkan LIVE butuh konfirmasi eksplisit -- kirim {\"confirm\": true}.",
        }), 400

    control["dry_run"] = not current_dry_run
    state_writer.write_control(control)
    return jsonify(control)


@dashboard_app.route("/api/set_interval", methods=["POST"])
def _dashboard_api_set_interval():
    """Minta ganti timeframe SAAT BOT SEDANG JALAN. Endpoint ini CUMA
    menulis permintaan ke control.json -- proses SEBENARNYA (reload
    riwayat candle, reconnect WebSocket) dijalankan oleh control_loop() di
    thread bot (lihat TradingBot.control_loop() dan
    TradingBot.request_interval_change()), BUKAN di sini, karena Flask
    handler ini tidak punya akses langsung ke instance TradingBot yang
    sedang jalan -- cuma bisa komunikasi lewat file, sama seperti toggle
    pause/dry_run.

    Body: {"interval": "5m"}
    Response CUMA konfirmasi permintaan DITERIMA, bukan konfirmasi
    berhasil -- cek /api/state (field "interval") beberapa detik kemudian
    buat lihat hasilnya, atau lihat log aktivitas di dashboard."""
    body = request.get_json(silent=True) or {}
    new_interval = str(body.get("interval", "")).strip()

    if new_interval not in TradingBot.VALID_INTERVALS:
        return jsonify({
            "error": "invalid_interval",
            "message": f"Interval '{new_interval}' tidak dikenali. Pilihan valid: {', '.join(TradingBot.VALID_INTERVALS)}",
        }), 400

    control = state_writer.read_control()
    control["requested_interval"] = new_interval
    state_writer.write_control(control)
    return jsonify({"status": "requested", "interval": new_interval})


@dashboard_app.route("/api/set_leverage", methods=["POST"])
def _dashboard_api_set_leverage():
    """Minta ganti leverage SAAT BOT SEDANG JALAN -- sama pola dengan
    /api/set_interval (cuma tulis ke control.json, diproses control_loop()
    lewat TradingBot.request_leverage_change()). Diterapkan ke SEMUA pair
    FUTURES aktif via API exchange asli (bukan cuma angka config).

    Body: {"leverage": 10}"""
    body = request.get_json(silent=True) or {}
    try:
        new_leverage = int(body.get("leverage"))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_params", "message": "Leverage harus berupa angka bulat."}), 400

    if not (1 <= new_leverage <= 125):
        return jsonify({"error": "invalid_params", "message": f"Leverage harus antara 1-125, dapat {new_leverage}."}), 400

    control = state_writer.read_control()
    control["requested_leverage"] = new_leverage
    state_writer.write_control(control)
    return jsonify({"status": "requested", "leverage": new_leverage})


@dashboard_app.route("/api/set_ema_params", methods=["POST"])
def _dashboard_api_set_ema_params():
    """Minta ganti EMA_FAST_LEN/EMA_SLOW_LEN SAAT BOT SEDANG JALAN. Sama
    seperti /api/set_interval, endpoint ini CUMA menulis permintaan ke
    control.json -- validasi & penerapan SEBENARNYA dilakukan
    TradingBot.control_loop() di thread bot sendiri.

    Body: {"ema_fast": 5, "ema_slow": 13}
    Validasi DASAR (tipe angka) dicek di sini supaya error jelas langsung
    tanpa nunggu 2 detik siklus control_loop; validasi LEBIH LANJUT (fast
    harus < slow, dst) tetap di TradingBot.request_ema_params_change()."""
    body = request.get_json(silent=True) or {}
    try:
        ema_fast = int(body.get("ema_fast"))
        ema_slow = int(body.get("ema_slow"))
    except (TypeError, ValueError):
        return jsonify({
            "error": "invalid_params",
            "message": "ema_fast dan ema_slow harus berupa angka bulat.",
        }), 400

    control = state_writer.read_control()
    control["requested_ema_params"] = {"fast": ema_fast, "slow": ema_slow}
    state_writer.write_control(control)
    return jsonify({"status": "requested", "ema_fast": ema_fast, "ema_slow": ema_slow})


@dashboard_app.route("/api/set_ema_params_futures", methods=["POST"])
def _dashboard_api_set_ema_params_futures():
    """Sama seperti /api/set_ema_params, tapi buat EMA_FAST_LEN_FUTURES/
    EMA_SLOW_LEN_FUTURES (trigger EMA Cross Futures, default 20/50) --
    field terpisah karena Spot & Futures independen satu sama lain.

    Body: {"ema_fast": 20, "ema_slow": 50}"""
    body = request.get_json(silent=True) or {}
    try:
        ema_fast = int(body.get("ema_fast"))
        ema_slow = int(body.get("ema_slow"))
    except (TypeError, ValueError):
        return jsonify({
            "error": "invalid_params",
            "message": "ema_fast dan ema_slow harus berupa angka bulat.",
        }), 400

    control = state_writer.read_control()
    control["requested_ema_params_futures"] = {"fast": ema_fast, "slow": ema_slow}
    state_writer.write_control(control)
    return jsonify({"status": "requested", "ema_fast": ema_fast, "ema_slow": ema_slow})


def _fetch_available_pairs(exchange: str, market_type: str, max_symbols: int = 150) -> list:
    """Ambil daftar pair BENERAN ADA di OKX, diurutkan volume 24 jam --
    dipakai menu tombol Telegram (TradingBot._handle_telegram_callback).
    Dulu juga dipakai endpoint dashboard /api/available_pairs, tapi
    endpoint itu sudah dihapus (20 Sept 2026, lihat catatan di bawah) --
    fungsi ini TETAP DIPERTAHANKAN karena Telegram masih memakainya.
    Parameter exchange dipertahankan di signature (selalu "OKX" di fork
    ini) supaya caller di _build_addpair_symbol_menu() tidak perlu diubah.
    Raise exception kalau gagal -- caller yang urus penanganan errornya
    masing-masing."""
    quote = getattr(config, "ALL_MARKET_QUOTE", "USDT")
    exclude_keywords = getattr(config, "ALL_MARKET_EXCLUDE_KEYWORDS", [])
    inst_type = "SPOT" if market_type == "SPOT" else "SWAP"
    return market_loader.fetch_okx_symbols(inst_type, quote, exclude_keywords, max_symbols=max_symbols)


# Catatan (20 Sept 2026): endpoint dashboard "/api/available_pairs" dan
# "/api/add_pair" SUDAH DIHAPUS atas permintaan langsung -- fungsi tambah
# pair manual dari dashboard tidak diperlukan lagi karena market scanner
# (market_scanner.py) sudah menambah pair Futures secara otomatis.
# _fetch_available_pairs() TETAP DIPERTAHANKAN (tidak dihapus) karena
# masih dipakai menu Telegram "➕ Tambah Pair" (lihat show_add_pair_menu()
# dkk) -- permintaan ini cuma soal dashboard, bukan Telegram. Struktur
# antrean control.json["pending_add_pairs"] juga tetap ada di
# control_loop() (sekarang jadi dead code tanpa pemanggil, aman
# dibiarkan) supaya tidak menyentuh alur scanner/Telegram yang masih
# aktif memakai add_pair_live() secara langsung.


@dashboard_app.route("/api/remove_pair", methods=["POST"])
def _dashboard_api_remove_pair():
    """Minta hapus SATU pair dari bot yang SEDANG JALAN. Cuma menulis ke
    antrian -- proses & validasi (termasuk PENOLAKAN kalau pair itu
    sedang punya posisi terbuka) dilakukan TradingBot.remove_pair_live()
    lewat control_loop().

    Body: {"symbol": "ADAUSDT"}"""
    body = request.get_json(silent=True) or {}
    symbol = str(body.get("symbol", "")).strip()
    if not symbol:
        return jsonify({"error": "invalid_params", "message": "symbol tidak boleh kosong."}), 400

    control = state_writer.read_control()
    pending = control.get("pending_remove_pairs", [])
    pending.append(symbol)
    control["pending_remove_pairs"] = pending
    state_writer.write_control(control)
    return jsonify({"status": "requested", "symbol": symbol})


@dashboard_app.route("/api/manual_open", methods=["POST"])
def _dashboard_api_manual_open():
    """Minta buka posisi MANUAL dari dashboard (di luar sinyal strategi
    otomatis). WAJIB body JSON {"confirm": true} -- ini aksi trading
    sungguhan (order NYATA kalau sedang LIVE), jadi butuh konfirmasi
    eksplisit sama seperti mengaktifkan mode LIVE, supaya klik tidak
    sengaja tidak bisa langsung buka posisi.

    Body: {"symbol": "BTCUSDT", "side": "LONG", "confirm": true}
    SL/TP/quantity dihitung otomatis pakai risk_manager.py (logika yang
    SAMA dengan entry strategi otomatis) -- lihat
    TradingBot.manual_open_position() untuk detailnya."""
    body = request.get_json(silent=True) or {}
    symbol = str(body.get("symbol", "")).strip()
    side = str(body.get("side", "")).strip().upper()

    if not symbol:
        return jsonify({"error": "invalid_params", "message": "symbol tidak boleh kosong."}), 400
    if side not in ("LONG", "SHORT"):
        return jsonify({"error": "invalid_params", "message": "side harus 'LONG' atau 'SHORT'."}), 400
    if not bool(body.get("confirm")):
        return jsonify({
            "error": "confirmation_required",
            "message": "Buka posisi manual butuh konfirmasi eksplisit -- kirim {\"confirm\": true}.",
        }), 400

    control = state_writer.read_control()
    pending = control.get("pending_manual_open", [])
    pending.append({"symbol": symbol, "side": side})
    control["pending_manual_open"] = pending
    state_writer.write_control(control)
    return jsonify({"status": "requested", "symbol": symbol, "side": side})


@dashboard_app.route("/api/manual_close", methods=["POST"])
def _dashboard_api_manual_close():
    """Minta tutup posisi yang SEDANG TERBUKA secara manual. Ini
    SATU-SATUNYA cara menutup posisi Spot yang dibuka TANPA SL/TP
    (config.USE_SPOT_SLTP=False), karena posisi begitu memang sengaja
    TIDAK PERNAH ditutup otomatis oleh apapun.

    WAJIB body JSON {"confirm": true} -- ini aksi trading sungguhan
    (order NYATA kalau sedang LIVE), sama seperti manual_open.

    Body: {"symbol": "BTCUSDT", "confirm": true}"""
    body = request.get_json(silent=True) or {}
    symbol = str(body.get("symbol", "")).strip()

    if not symbol:
        return jsonify({"error": "invalid_params", "message": "symbol tidak boleh kosong."}), 400
    if not bool(body.get("confirm")):
        return jsonify({
            "error": "confirmation_required",
            "message": "Tutup posisi manual butuh konfirmasi eksplisit -- kirim {\"confirm\": true}.",
        }), 400

    control = state_writer.read_control()
    pending = control.get("pending_manual_close", [])
    pending.append(symbol)
    control["pending_manual_close"] = pending
    state_writer.write_control(control)
    return jsonify({"status": "requested", "symbol": symbol})


@dashboard_app.route("/api/reset_stats", methods=["POST"])
def _dashboard_api_reset_stats():
    """Reset PnL, total uptime, dan saldo simulasi (otomatis ikut ke-reset
    karena rumusnya = INITIAL_SIMULATED_BALANCE + realized_pnl_quote)
    balik ke kondisi awal. TIDAK mempengaruhi posisi yang SEDANG TERBUKA.

    WAJIB body JSON {"confirm": true} -- ini AKSI IRREVERSIBLE (angka lama
    tidak bisa dikembalikan lagi setelah di-reset), butuh konfirmasi
    eksplisit sama seperti shutdown.

    Body: {"confirm": true}"""
    body = request.get_json(silent=True) or {}
    if not bool(body.get("confirm")):
        return jsonify({
            "error": "confirmation_required",
            "message": "Reset statistik butuh konfirmasi eksplisit -- kirim {\"confirm\": true}. Aksi ini TIDAK BISA dibatalkan.",
        }), 400

    control = state_writer.read_control()
    control["reset_stats_requested"] = True
    state_writer.write_control(control)
    return jsonify({"status": "requested"})


@dashboard_app.route("/api/strategy_settings", methods=["GET"])
def _dashboard_api_get_strategy_settings():
    """Daftar SEMUA setting strategi yang bisa diubah lewat Menu Strategy
    di dashboard, LENGKAP dengan nilai config SEKARANG, tipe data, label
    tampilan, dan grupnya -- dipakai buat generate form di frontend secara
    otomatis (jadi kalau ada setting baru ditambah di STRATEGY_SETTINGS_SCHEMA,
    otomatis muncul juga di dashboard tanpa perlu ubah kode frontend)."""
    result = []
    for key, (py_type, label, group) in STRATEGY_SETTINGS_SCHEMA.items():
        type_str = "bool" if py_type is bool else ("int" if py_type is int else "float")
        default_step = 1 if type_str == "int" else (0.01 if type_str == "float" else None)
        result.append({
            "key": key,
            "label": label,
            "group": group,
            "type": type_str,
            "value": getattr(config, key, None),
            # step per tap +/- di dashboard -- ambil dari PARAM_STEPS (SAMA
            # persis dengan step di menu Telegram) supaya konsisten antara
            # dashboard dan Telegram; fallback ke default kalau key belum
            # ada di PARAM_STEPS (mis. bool tidak butuh step).
            "step": _SCHEMA_PARAM_STEPS.get(key, default_step),
        })
    return jsonify({"settings": result})


@dashboard_app.route("/api/update_strategy_settings", methods=["POST"])
def _dashboard_api_update_strategy_settings():
    """Kirim perubahan BANYAK setting strategi sekaligus dari Menu Strategy
    di dashboard -- diproses control_loop() lewat TradingBot.apply_strategy_settings(),
    yang cuma menerima key dari STRATEGY_SETTINGS_SCHEMA (whitelist).

    Body: {"settings": {"USE_RSI_FILTER": false, "RSI_OVERBOUGHT": 70, ...}}"""
    body = request.get_json(silent=True) or {}
    settings = body.get("settings")
    if not isinstance(settings, dict) or not settings:
        return jsonify({"error": "invalid_params", "message": "Body harus berisi {\"settings\": {...}} dengan minimal 1 setting."}), 400

    unknown_keys = [k for k in settings if k not in STRATEGY_SETTINGS_SCHEMA]
    if unknown_keys:
        return jsonify({
            "error": "invalid_params",
            "message": f"Setting tidak dikenali (tidak ada di whitelist): {', '.join(unknown_keys)}",
        }), 400

    control = state_writer.read_control()
    control["pending_strategy_settings"] = settings
    state_writer.write_control(control)
    return jsonify({"status": "requested", "count": len(settings)})


@dashboard_app.route("/api/shutdown", methods=["POST"])
def _dashboard_api_shutdown():
    """Matikan SELURUH proses bot dari dashboard. WAJIB body JSON
    {"confirm": true} -- tanpa itu ditolak (400), supaya klik tidak
    sengaja tidak bisa langsung mematikan bot begitu saja.

    PERINGATAN yang perlu dipahami user: mematikan bot berarti TIDAK ADA
    LAGI yang memantau SL/TP untuk posisi yang sedang terbuka -- posisi
    itu TETAP ada di exchange (kalau LIVE), tapi bot tidak akan
    menutupnya otomatis lagi sampai dijalankan ulang manual.

    Endpoint ini cuma menulis permintaan ke control.json -- proses
    shutdown SEBENARNYA (log, save_state, stop WebSocket, os._exit)
    dijalankan oleh TradingBot.control_loop(), yang jalan di thread bot
    itu sendiri (bukan thread Flask ini)."""
    body = request.get_json(silent=True) or {}
    if not bool(body.get("confirm")):
        return jsonify({
            "error": "confirmation_required",
            "message": "Shutdown butuh konfirmasi eksplisit -- kirim {\"confirm\": true}.",
        }), 400

    control = state_writer.read_control()
    control["shutdown_requested"] = True
    state_writer.write_control(control)
    return jsonify({"status": "shutdown_requested"})


def run_dashboard():
    """Dipanggil sebagai thread background dari TradingBot.run(). Flask dev
    server memang tidak disarankan untuk trafik produksi tinggi, tapi ini
    dashboard monitoring lokal (satu operator, polling tiap 2 detik) jadi
    cukup memadai -- sama seperti dashboard_server.py yang dulu terpisah.

    Port BISA diatur lewat env var DASHBOARD_PORT -- default TETAP 5000
    (tidak mengubah perilaku single-user yang sudah ada). Dibutuhkan buat
    mode multi-user (lihat multi_bot_orchestrator.py) supaya tiap user
    dapat port dashboard SENDIRI, tidak bentrok satu sama lain."""
    port = int(os.getenv("DASHBOARD_PORT", "5000"))
    dashboard_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


def _launch_gui_window(dashboard_url: str = "http://localhost:5000"):
    """Buka window desktop NATIVE (bukan tab browser) menampilkan dashboard
    -- dipanggil dari main block kalau dijalankan dengan flag '--gui'.

    Bot (semua thread-nya) sudah dijalankan LEBIH DULU di background
    sebelum fungsi ini dipanggil -- fungsi ini CUMA jendela tampilan,
    persis kayak browser biasa tapi tanpa 'baju' browser-nya.

    Import 'webview' sengaja LAZY (baru di-import di sini, bukan di atas
    file) supaya mode headless (default, tanpa '--gui') TIDAK PERNAH butuh
    pywebview ter-install -- penting untuk deploy di VPS/server tanpa
    display sama sekali (lihat catatan di README soal ini)."""
    import webview

    print(f"Mengecek dashboard di {dashboard_url} ...")
    ready = False
    for _ in range(20):
        try:
            resp = requests.get(dashboard_url, timeout=1)
            if resp.status_code == 200:
                ready = True
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)

    if not ready:
        logger.warning(f"Dashboard belum merespons setelah 20 detik, tetap coba buka window ({dashboard_url}).")

    webview.create_window(
        title="Trading Bot Monitor",
        url=dashboard_url,
        width=1000,
        height=680,
        min_size=(760, 500),
        resizable=True,  # tetap bisa dibesarkan manual kalau user mau
        confirm_close=True,  # tanya konfirmasi dulu sebelum window ditutup
    )
    webview.start()

    # Window sudah ditutup user -- ini dianggap "tutup aplikasi", jadi bot
    # ikut dimatikan juga (bukan cuma window-nya doang, biar tidak ada
    # proses nyangkut tak terlihat di background tanpa disadari user).
    logger.info("Window desktop ditutup -- mematikan bot juga (aplikasi tunggal).")
    os._exit(0)


if __name__ == "__main__":
    bot = TradingBot()

    if "--gui" in sys.argv:
        # Mode GABUNGAN: bot jalan di background thread, window native
        # dibuka di foreground (thread utama -- wajib untuk GUI di macOS).
        logger.info("Mode GUI: membuka window desktop native, bot jalan di background...")
        threading.Thread(target=bot.run, daemon=True).start()
        # Pakai DASHBOARD_PORT dari .env (bukan hardcode 5000) -- Bot OKX
        # sengaja dijalankan di port beda (5001) supaya tidak bentrok kalau
        # bot Binance+OKX yang satu lagi jalan bersamaan di komputer yang sama.
        _gui_port = os.getenv("DASHBOARD_PORT", "5000")
        _launch_gui_window(f"http://localhost:{_gui_port}")
    else:
        # Mode headless (DEFAULT, PERILAKU LAMA TIDAK BERUBAH) -- WAJIB
        # dipertahankan persis seperti ini untuk kompatibilitas VPS/server
        # tanpa display, dan siapa saja yang masih mau akses lewat browser
        # biasa dari HP/komputer lain (lihat catatan Tailscale/VPS di README).
        logger.info("Dashboard tergabung di proses ini -- buka http://localhost:5000")
        logger.info("(Mau window desktop native? Jalankan: python bot.py --gui)")
        bot.run()
