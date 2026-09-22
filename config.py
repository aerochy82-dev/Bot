"""
Konfigurasi Bot Trading.
Isi API key di file .env (lihat .env.example), JANGAN hardcode di sini
dan JANGAN commit file .env ke git.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ====== 1. API CREDENTIALS ======
# FORK OKX-ONLY (21 Sept 2026): BINANCE_API_KEY/BINANCE_API_SECRET,
# USE_TESTNET, BINANCE_RECV_WINDOW, dan USE_PORTFOLIO_MARGIN sudah DIHAPUS
# dari sini -- semuanya konsep khusus Binance (Client python-binance,
# testnet Binance, Portfolio Margin akun Binance) yang sudah tidak
# dipakai kode apa pun lagi di fork ini (lihat bot.py). Kalau butuh
# Binance lagi, pakai bot.py yang asli di folder "bot".
OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_PASSWORD = os.getenv("OKX_PASSWORD", "")

DRY_RUN = False

# True = OKX Demo Trading (dana virtual). WAJIB generate API key TERPISAH
# khusus buat Demo Trading (toggle 'Demo Trading' sebelum generate key di
# OKX) -- key live dan key demo dua sistem yang berbeda total, tidak bisa
# saling dipakai (nyoba pakai key salah -> error code 50101).
OKX_DEMO_TRADING = False  # key yang dipakai adalah key akun LIVE OKX -- DRY_RUN=True
                            # tetap mencegah order NYATA terkirim, jadi tetap aman selama itu masih True

# ====== 2. MODE GLOBAL ======
LEVERAGE = 5
# MARGIN_TYPE ("CROSSED"/"ISOLATED", format Binance) sudah DIHAPUS -- fork
# ini pakai OKX_MARGIN_MODE ("cross"/"isolated") di bawah untuk semuanya.
ALLOW_SHORT = True

# Fee exchange -- dipakai analisa_pair.py buat hitung dampak fee terhadap
# rugi SL/profit TP (BELUM dipakai bot.py buat position sizing/PnL --
# murni informasi analisa saat ini). Default sesuai tier standar Binance &
# OKX Futures (sama persis: maker 0.02%, taker 0.05%) -- sesuaikan kalau
# kamu punya VIP tier/diskon BNB/OKB yang beda.
TAKER_FEE_PCT = 0.05   # per SISI (entry ATAU exit) -- bot pakai market order = taker
MAKER_FEE_PCT = 0.02   # per sisi, kalau nanti ada order type limit

OKX_LEVERAGE = 5
OKX_MARGIN_MODE = "cross"   # "cross" atau "isolated"
OKX_POLL_INTERVAL_SECONDS = 5   # OKX dipantau lewat REST polling, bukan WebSocket

# ====== 3. PASAR & SYMBOL ======
# Format symbol OKX: "BTC-USDT" untuk Spot, "BTC-USDT-SWAP" untuk Futures perpetual.
#
# Pair PERMANEN (manual, di luar market scanner) dikosongkan atas permintaan
# langsung -- supaya bot 100% mengandalkan market scanner otomatis untuk
# pair Futures, tanpa ada pair yang "dikunci" manual sejak startup.
# FORK OKX-ONLY (21 Sept 2026): SYMBOLS_SPOT_BINANCE/SYMBOLS_FUTURES_BINANCE/
# ALL_SYMBOLS_BINANCE/BINANCE_POLL_FALLBACK_INTERVAL_SECONDS sudah DIHAPUS
# -- Binance sudah tidak ada sama sekali lagi di kode ini (bukan cuma
# dikosongkan), lihat bot.py.
SYMBOLS_SPOT_OKX = ["BTC-USDT"]   # satu-satunya pair permanen yang TERSISA (Spot OKX)
SYMBOLS_FUTURES_OKX = []

ALL_SYMBOLS_OKX = SYMBOLS_SPOT_OKX + SYMBOLS_FUTURES_OKX

# ====== 3b. MODE "ALL MARKET" (opsional -- override list manual di atas) ======
# True = pair untuk market itu diambil OTOMATIS dari OKX saat bot start
# (semua pair TRADING dengan quote asset ALL_MARKET_QUOTE), bukan dari list
# manual SYMBOLS_*_OKX di atas. List manual di atas TETAP HARUS ada
# (dipakai kalau flag-nya False), tapi DIABAIKAN untuk market yang
# flag-nya True. (ALL_MARKET_SPOT_BINANCE/ALL_MARKET_FUTURES_BINANCE sudah
# DIHAPUS di fork OKX-only ini.)
#
# PERINGATAN: OKX Futures (SWAP) sendiri ada ratusan pair. Trading semua
# pair sekaligus = banyak request REST polling, exposure lebih liar, dan
# lebih rawan kena rate-limit exchange. ALL_MARKET_MAX_SYMBOLS di bawah
# WAJIB dipakai buat batasi jumlahnya kecuali kamu benar-benar tahu resikonya.
ALL_MARKET_SPOT_OKX = False
ALL_MARKET_FUTURES_OKX = False

ALL_MARKET_QUOTE = "USDT"   # cuma ambil pair dengan quote asset ini
# Kata kunci di BASE ASSET yang di-skip (token leverage spot Binance,
# misal BTCUPUSDT/BTCDOWNUSDT -- bukan token yang mau ditradingkan normal).
ALL_MARKET_EXCLUDE_KEYWORDS = ["UP", "DOWN", "BULL", "BEAR"]
# Batas jumlah pair yang diambil PER market (None = tanpa batas -- TIDAK
# disarankan, lihat peringatan di atas). Kalau diisi angka, pair dipilih
# berdasarkan volume 24 jam TERBESAR (paling likuid duluan).
ALL_MARKET_MAX_SYMBOLS = 30

INTERVAL = "15m"   # dipakai untuk pair FUTURES OKX -- BISA diganti live lewat
                    # tombol timeframe di dashboard (cuma berlaku buat Futures)
                    # (diubah dari default lama "5m" -> "15m", 20 Sept 2026,
                    # permintaan langsung -- nilai default ini BARU berlaku
                    # buat pair FUTURES yang di-load ULANG saat bot start,
                    # lihat catatan riwayat perubahan project soal ini)
SPOT_INTERVAL = "1h"   # timeframe TERPISAH khusus pair SPOT OKX -- lebih
                         # tinggi dari Futures karena Spot biasanya buat posisi lebih
                         # panjang. STATIS (belum ada tombol live-switch buat ini di
                         # dashboard, cuma bisa diubah lewat config.py + restart bot)
KLINE_HISTORY = 500
MAX_CONCURRENT_POSITIONS = 10   # Batas jumlah posisi terbuka bersamaan (digabung Spot+Futures+
                                   # kedua exchange). Diset ke 10 (20 Sept 2026, permintaan langsung)
                                   # untuk membatasi eksposur total sekarang karena USE_TRADE_HOURS
                                   # sudah dimatikan (trading 24 jam). Sebelumnya None (tanpa batas)
                                   # -- lihat riwayat: awalnya 1, lalu dinaikkan ke None karena posisi
                                   # Spot (USE_SPOT_SLTP=False) tidak pernah nutup sendiri sehingga
                                   # bisa mengunci slot itu SELAMANYA dan bikin SEMUA sinyal Futures
                                   # ke-block. Kalau isu itu muncul lagi dengan batas 10, pertimbangkan
                                   # exclude posisi Spot dari hitungan count_open_positions() alih-alih
                                   # menaikkan batas ini lagi. Pengaman risiko lain
                                   # (MAX_POSITION_PCT per trade, RISK_PER_TRADE_PCT, USE_EQUITY_GUARD)
                                   # TETAP aktif dan TIDAK berubah oleh setting ini.
# WS_MAX_QUEUE_SIZE (parameter ThreadedWebsocketManager Binance) sudah
# DIHAPUS -- fork ini tidak punya WebSocket sama sekali.

# ====== 4. STRATEGI EMA CROSS + ATR (dari Pine Script) ======
# USE_EMA_CROSS_STRATEGY=True pakai trigger EMA_FAST_LEN/EMA_SLOW_LEN cross
# (gaya Pine Script kamu). False = tetap pakai trigger MACD_FAST/SLOW/SIGNAL
# di bagian 8 di bawah.
USE_EMA_CROSS_STRATEGY = True
EMA_FAST_LEN = 9
EMA_SLOW_LEN = 21

# EMA CROSS KHUSUS FUTURES (INTERVAL=15m) -- dipisah dari Spot (1h) di atas
# karena timeframe beda jauh (15m jauh lebih cepat ganti candle daripada 1h).
# Sama nilainya dengan Spot dulu (belum diubah) -- tuning lebih lanjut bisa
# lewat dashboard/menu Strategi Telegram. strategy.py otomatis pakai nilai
# ini untuk pair market_type=="FUTURES", fallback ke EMA_FAST_LEN/SLOW_LEN
# biasa (Spot) kalau var ini dihapus.
EMA_FAST_LEN_FUTURES = 21
EMA_SLOW_LEN_FUTURES = 55
CONFIRM_CANDLE = True# candle di titik cross harus searah (close>open utk LONG, dst)

# Filter EMA Cross Spread + Price Spread.
# Kalau KEDUA-DUANYA True (dan USE_EMA_CROSS_STRATEGY=True):
#   EMA cross event DI-LEWATI (EMA hanya acuan hitung jarak).
#   Trigger sinyal dari perbandingan nilai:
#     ema_spread < price_spread  → BUY
#     ema_spread > price_spread  → SELL
#     ema_spread ≈ price_spread  → HOLD
#   (di bawah min masing-masing juga HOLD = masih bersentuhan/menempel)
# Kalau hanya salah satu True: tetap mode klasik (trigger = EMA/MACD cross).
USE_EMA_CROSS_SPREAD_FILTER = True
EMA_CROSS_SPREAD_MIN_PCT = 0.12# % minimal |EMA_fast-EMA_slow|/harga

# FIX 21 Sept 2026: ambang KHUSUS utk REVERSE_TO_LONG/REVERSE_TO_SHORT
# (membalik posisi yang sedang terbuka ke arah lawan), TERPISAH dan LEBIH
# KETAT dari EMA_CROSS_SPREAD_MIN_PCT di atas (yang tetap dipakai apa
# adanya utk entry BARU dari flat). Membalik posisi menutup 1 posisi +
# membuka 1 posisi lawan sekaligus (2x biaya/slippage), jadi butuh sinyal
# yang jauh lebih meyakinkan drpd entry biasa supaya tidak whipsaw bolak-
# balik kena noise tipis. Default 2x lipat EMA_CROSS_SPREAD_MIN_PCT.
# Lihat TradingBot.reverse_allowed() di bot.py.
REVERSE_EMA_SPREAD_MIN_PCT = 0.24# % minimal |EMA_fast-EMA_slow|/harga KHUSUS utk reverse

USE_PRICE_SPREAD_FILTER = True
PRICE_SPREAD_MIN_PCT = 0.08# % minimal |close-EMA_slow|/harga

# USE_ATR_RISK=True pakai SL dari low/high candle sinyal +/- ATR, TP dari
# RISK_REWARD x jarak risiko (bagian ini). False = pakai SL/TP persentase
# tetap (STOP_LOSS_PCT/TAKE_PROFIT_PCT di bagian 9).
USE_ATR_RISK = True
ATR_LEN = 14
# FIX 21 Sept 2026: dilebarkan dari 1.0 -> 1.5 (SL ~50% lebih jauh dari
# sebelumnya) supaya tidak gampang kena stop akibat noise/wick candle
# sesaat. TP ikut melebar proporsional (tetap rasio 1:RISK_REWARD di
# bawah) sehingga risk/reward ratio TIDAK berubah, cuma jaraknya lebih
# lebar keduanya.
ATR_MULT_SL = 1.5
RISK_REWARD = 2.0

# ====== 5. TP BERTINGKAT (partial close custom, bukan equal-split) ======
# USE_MULTI_TP=True: posisi ditutup bertahap sesuai TP_LEVELS (kelipatan R)
# dan TP_PERCENTS (persen porsi ditutup di level itu, HARUS total 100).
# Beda dari versi equal-split sebelumnya -- di sini porsinya bisa custom
# per level (misal TP1 cuma tutup 30%, bukan otomatis dibagi rata).
USE_MULTI_TP = True
TP_LEVELS = [1.0, 2.0, 3.0, 4.0]   # 1R, 2R, 3R, 4R
TP_PERCENTS = [25, 25, 25, 25]     # HARUS total 100 -- 4 level rata (25% tiap level)

# Level TP TERAKHIR (final) bisa diganti dari "target harga tetap" jadi
# "ikuti tren sampai balik arah" -- begitu level KEDUA DARI TERAKHIR (TP2
# kalau TP_LEVELS ada 3 level) kena, target harga tetap buat level
# terakhir DIABAIKAN, sisa posisi dibiarkan "jalan" dan CUMA ditutup kalau
# EMA reversal (config.USE_EARLY_REVERSAL) balik arah, atau SL kena.
# Sebelum level kedua-dari-terakhir kena, semua tetap seperti biasa
# (target harga tetap + deteksi reversal dini dua-duanya tetap aktif).
USE_TREND_EXIT_FINAL_TP = True

# ====== 6. FITUR PRO ======
USE_BREAK_EVEN = True
BREAK_EVEN_AT_R = 1.0       # pindah SL ke breakeven begitu profit capai 1R
BREAK_EVEN_OFFSET = 0.0005  # buffer kecil di atas/bawah entry, biar tidak ke-sambar noise persis di harga entry

USE_TRAILING_STOP = True
TRAILING_ATR_MULT = 1.0     # jarak trailing = ATR x 1.0, aktif SETELAH TP1 kena

# EMA REVERSAL DINI -- indikator TERPISAH dari EMA_FAST_LEN/EMA_SLOW_LEN
# (yang dipakai buat trigger ENTRY BARU). Cuma dipantau SAAT SUDAH ADA
# POSISI TERBUKA -- kalau EMA reversal ini cross MELAWAN arah posisi
# (tanda tren sudah balik), bot langsung REVERSE (tutup + buka arah
# berlawanan) SEBELUM harga sempat menyentuh SL -- proteksi lebih awal
# daripada nunggu SL kena harga.
USE_EARLY_REVERSAL = True

# Entry OTOMATIS saat bot BARU DINYALAKAN -- buka posisi LANGSUNG sesuai
# arah tren SEKARANG (EMA cepat vs lambat), TANPA nunggu cross baru
# terjadi. Berguna kalau market sudah trending lama SEBELUM bot ini
# dijalankan -- tanpa fitur ini, bot cuma diam nunggu cross baru muncul,
# padahal tren yang sudah berjalan bisa jadi sudah cukup jelas arahnya.
# TETAP menghormati semua pengaman risiko yang ada (MAX_CONCURRENT_POSITIONS,
# jam trading, saldo, dll) lewat try_open_position()/open_position() --
# cuma SYARAT masuknya yang beda (tren sekarang, bukan cross baru).
USE_ENTRY_ON_STARTUP = False# dimatikan
EMA_REVERSAL_FAST_LEN = 12    # diubah dari 5 (20 Sept 2026, permintaan langsung) --
                              # periode lebih panjang supaya early-reversal tidak
                              # terlalu sensitif/false-trigger, mengurangi keluhan
                              # "terlalu sering reverse [lalu kena] SL"
EMA_REVERSAL_SLOW_LEN = 26   # diubah dari 13, alasan sama seperti di atas
# FIX 21 Sept 2026: diaktifkan lagi sebagai syarat jarak minimal reversal
# (lihat check_early_reversal() di bot.py untuk implementasi lengkap) --
# data live membuktikan cross sederhana tanpa threshold (0.0) hampir
# selalu salah (win rate cuma 4.4% dari 91 kejadian EARLY_REVERSAL periode
# EMA_REVERSAL 5/13 lama). Nilai 0.05 dipilih sepadan dengan
# PRICE_SPREAD_MIN_PCT (0.05) di atas -- ambang longgar, cuma menyaring
# cross yang benar-benar tipis/noise, bukan menahan reversal yang wajar.
# Bisa diubah kapan saja lewat menu Strategi dashboard/Telegram (sudah
# ada di STRATEGY_SETTINGS_SCHEMA), tidak perlu edit file ini lagi.
EMA_REVERSAL_SPREAD_MIN_PCT = 0.2

MAX_1_TRADE_PER_SYMBOL = False# 1 symbol cuma boleh 1 posisi terbuka bersamaan
COOLDOWN_AFTER_SL_MIN = 0# dimatikan -- boleh entry lagi langsung tanpa jeda setelah SL kena

# Catatan (20 Sept 2026): fitur reverse otomatis setelah stop-loss kena
# (USE_SL_REVERSE / SL_REVERSE_MAX_CONSECUTIVE / try_sl_reverse() di
# bot.py) SUDAH DIHAPUS atas permintaan langsung. Fitur ini defaultnya
# sudah MATI sejak pertama dibuat dan tidak pernah diaktifkan di produksi,
# jadi penghapusan ini TIDAK mengubah perilaku trading yang sedang
# berjalan -- murni membersihkan kode yang tidak dipakai. TERPISAH dari
# USE_EARLY_REVERSAL di atas (reverse SEBELUM harga menyentuh SL, dari EMA
# reversal cross) -- fitur itu TETAP ADA dan tidak tersentuh.

USE_TRADE_HOURS = False
TRADE_HOURS_START = 8   # jam mulai boleh entry baru (WIB, 24h format)
TRADE_HOURS_END = 22    # jam berhenti boleh entry baru (WIB)

USE_EQUITY_GUARD = True
MAX_DRAWDOWN_PCT = 10.0# bot PAUSE OTOMATIS kalau saldo turun >X% dari INITIAL_BALANCE
INITIAL_BALANCE = 0        # 0 = auto ambil saldo saat bot start sebagai acuan

USE_VOLATILITY_FILTER = True
MIN_ATR_PCT = 0.03# skip sinyal kalau ATR% di bawah ini (market terlalu sepi) -- diturunkan
                       # dari 0.15 setelah diagnose_signals.py ETHUSDT nunjukin ATR% asli pair
                       # ini di 1m berkisar 0.032%-0.1227%, jauh di bawah 0.15% lama (100% candle
                       # ke-block). Kalau nanti ganti timeframe/tambah pair harga jauh beda,
                       # jalankan diagnose_signals.py lagi buat cek ulang -- angka ini bukan
                       # cocok universal buat semua kondisi.
MAX_ATR_PCT = 2.0# skip sinyal kalau ATR% di atas ini (market terlalu liar/rawan whipsaw) --
                       # TIDAK diubah, 0% candle ETHUSDT kena batas ini di diagnostik

# Ambang ATR% KHUSUS FUTURES -- dipisah dari Spot di atas karena candle 15m
# (Futures) wajar punya ATR% per-candle beda karakter dari 1h (Spot). Sama
# nilainya dengan Spot dulu, tuning lanjut lewat dashboard/Telegram.
MIN_ATR_PCT_FUTURES = 0.03
MAX_ATR_PCT_FUTURES = 2.0

# WS_RECONNECT_DELAY / MAX_WS_ERRORS (Binance WebSocket only) sudah
# DIHAPUS -- kedua-duanya sebenarnya sudah tidak dipakai kode manapun
# bahkan sebelum fork ini dibuat (dead config lama).

# ====== 7. FILTER TAMBAHAN (EMA_TREND, RSI, ADX, Volume) ======
USE_TREND_FILTER = True# dimatikan
EMA_TREND = 50
# EMA Tren KHUSUS FUTURES (15m) -- dipisah dari Spot (1h) di atas, alasan
# timeframe sama seperti EMA_FAST_LEN_FUTURES/dst. Sama nilainya dulu.
EMA_TREND_FUTURES = 100


# Filter Smart Money Concepts (SMC) -- TAMBAHAN konfirmasi arah, BUKAN
# pengganti EMA cross (trigger entry utama tetap sama). Sinyal BUY/SELL
# dari EMA cross cuma dieksekusi kalau SEARAH struktur market (Break of
# Structure paling baru) -- kalau berlawanan, sinyal itu diabaikan (HOLD).
# lihat strategy.get_smc_structure_bias() untuk detail cara kerjanya.
USE_SMC_FILTER = True# AKTIF -- tapi lihat SMC_MARKET_SCOPE di bawah, cuma berlaku buat Spot
SMC_SWING_LOOKBACK = 10# dinaikkan dari 5 -- makin besar = butuh lebih banyak candle
                            # kiri-kanan buat dianggap swing high/low, jadi cuma swing yang
                            # BENERAN signifikan yang kehitung (kurang sensitif ke noise harga kecil)
                          # (makin besar = struktur lebih "kasar"/jangka panjang,
                          # makin kecil = lebih sensitif/banyak swing terdeteksi)

# Order Block -- candle TERAKHIR berlawanan arah sebelum pergerakan
# impulsif (>= SMC_OB_IMPULSE_PCT%) searah sinyal. Sinyal cuma lolos
# kalau harga SEKARANG ada di/dekat zona OB relevan PALING BARU.
USE_SMC_ORDER_BLOCK_FILTER = True# AKTIF -- sama, dibatasi SMC_MARKET_SCOPE
SMC_OB_IMPULSE_PCT = 0.5     # % pergerakan candle berikutnya buat dianggap "impulsif"
SMC_OB_LOOKBACK = 50# cuma cari OB dalam N candle terakhir
SMC_OB_TOLERANCE_PCT = 65.0# dilonggarkan lagi dari 40 -- FVG dimatikan, OB jadi filter SMC paling ketat sekarang

# Fair Value Gap (FVG) -- celah/imbalance harga pada pola 3-candle
# berurutan. Sama konsepnya kayak Order Block di atas, independen aktifnya.
USE_SMC_FVG_FILTER = True# AKTIF -- sama, dibatasi SMC_MARKET_SCOPE

# Batasi SEMUA filter SMC di atas (Struktur/OB/FVG) CUMA berlaku buat
# market tertentu -- "ALL" (default, berlaku ke semua), "SPOT", atau
# "FUTURES". Berguna karena SMC butuh candle yang "bersih" (kurang
# cocok di timeframe kecil/noise seperti Futures 15m kamu) -- Spot yang
# 1 jam jauh lebih cocok buat deteksi struktur/BOS yang akurat.
SMC_MARKET_SCOPE = "SPOT"
SMC_FVG_LOOKBACK = 50
SMC_FVG_TOLERANCE_PCT = 100.0# dilonggarkan lagi dari 70 -- masih paling ketat (+5) di diagnostik terakhir

USE_RSI_FILTER = True
RSI_PERIOD = 14
RSI_OVERBOUGHT = 75.0
RSI_OVERSOLD = 25.0
# RSI KHUSUS FUTURES (15m) -- dipisah dari Spot (1h) di atas, alasan timeframe
# sama. Sama nilainya dulu, tuning lanjut lewat dashboard/Telegram.
RSI_PERIOD_FUTURES = 14
RSI_OVERBOUGHT_FUTURES = 75.0
RSI_OVERSOLD_FUTURES = 25.0

# SMII (SMI Ergodic Indicator) -- indikator momentum yang di-smooth DUA
# KALI (double-smoothed EMA), sama dengan indikator bawaan TradingView
# "SMII"/"SMI Ergodic Indicator". Sinyal BUY/SELL cuma lolos kalau SEARAH
# momentum SMI relatif ke garis sinyalnya sendiri -- default TAMBAHAN
# konfirmasi, BUKAN pengganti RSI/filter momentum lain yang sudah ada.
USE_SMII_FILTER = True# AKTIF
SMII_LONG_LENGTH = 20# "Panjang Pembelian" di TradingView
SMII_SHORT_LENGTH = 5# "Panjang Penjualan" di TradingView
SMII_SIGNAL_LENGTH = 5# "Panjang Garis Sinyal" di TradingView
# SMII KHUSUS FUTURES (15m) -- dipisah dari Spot (1h), sama nilainya dulu.
SMII_LONG_LENGTH_FUTURES = 20
SMII_SHORT_LENGTH_FUTURES = 5
SMII_SIGNAL_LENGTH_FUTURES = 5
# Batasi filter SMII di atas CUMA berlaku buat market tertentu -- "ALL"
# (default, berlaku ke semua), "SPOT", atau "FUTURES". Sama pola dengan
# SMC_MARKET_SCOPE di atas. Default "ALL" -- TIDAK ADA perubahan perilaku
# dari sebelumnya (toggle USE_SMII_FILTER tetap berlaku ke semua market
# sampai scope ini sengaja dipersempit).
SMII_MARKET_SCOPE = "SPOT"

# Filter DPO (Detrended Price Oscillator) -- TAMBAHAN konfirmasi arah,
# sama pola dengan SMII di atas. Sinyal BUY cuma lolos kalau DPO positif
# (harga overbought jangka pendek relatif ke tren SMA-nya), SELL cuma
# lolos kalau DPO negatif (oversold). Threshold dalam % dari harga
# (DPO_MIN_PCT), bukan angka absolut, biar konsisten antar pair.
USE_DPO_FILTER = True# AKTIF
DPO_PERIOD = 20
DPO_MIN_PCT = 0.0# 0 = cukup di sisi yang benar dari nol, tidak perlu jarak signifikan
# DPO KHUSUS FUTURES (15m) -- dipisah dari Spot (1h), sama nilainya dulu.
DPO_PERIOD_FUTURES = 20
DPO_MIN_PCT_FUTURES = 0.0
# Batasi filter DPO di atas CUMA berlaku buat market tertentu -- "ALL"
# (default, berlaku ke semua), "SPOT", atau "FUTURES". Sama pola dengan
# SMC_MARKET_SCOPE/SMII_MARKET_SCOPE. Default "ALL" -- TIDAK ADA perubahan
# perilaku dari sebelumnya.
DPO_MARKET_SCOPE = "SPOT"

USE_ADX_FILTER = True
ADX_PERIOD = 14
ADX_MIN_TREND = 15.0
# ADX KHUSUS FUTURES (15m) -- dipisah dari Spot (1h), sama nilainya dulu.
ADX_PERIOD_FUTURES = 14
ADX_MIN_TREND_FUTURES = 15.0

USE_VOLUME_FILTER = False# dimatikan -- terbukti jadi salah satu filter yang paling sering
                             # menolak sinyal (lihat hasil diagnose_signals.py)
VOLUME_MA_PERIOD = 20
VOLUME_MIN_RATIO = 1.0
# Rasio Volume KHUSUS FUTURES (15m) -- dipisah dari Spot (1h), sama nilainya
# dulu. VOLUME_MA_PERIOD (panjang moving average-nya) TETAP SAMA untuk
# kedua market, cuma ambang rasionya yang dipisah.
VOLUME_MIN_RATIO_FUTURES = 1.0

# ====== 8. MACD (dipakai kalau USE_EMA_CROSS_STRATEGY=False) ======
# Nilai default ini dipakai untuk pair SPOT (SPOT_INTERVAL=1h) -- SENGAJA
# tidak diubah dari sebelumnya.
MACD_FAST = 8
MACD_SLOW = 17
MACD_SIGNAL = 6

# MACD KHUSUS FUTURES (INTERVAL=15m) -- periode lebih pendek dari yang Spot
# di atas supaya lebih responsif di candle 5 menit yang cepat berganti.
# Kombinasi 6/13/5 ini umum dipakai buat scalping/day-trading timeframe
# rendah (kira-kira setengah dari 12/26/9 standar, mirip proporsi MACD_FAST/
# SLOW/SIGNAL Spot di atas tapi sedikit lebih cepat lagi). strategy.py
# otomatis pakai nilai ini untuk pair market_type=="FUTURES", dan tetap
# fallback ke MACD_FAST/SLOW/SIGNAL biasa (Spot) kalau var ini dihapus.
MACD_FAST_FUTURES = 6
MACD_SLOW_FUTURES = 13
MACD_SIGNAL_FUTURES = 5

# ====== 9. MANAJEMEN RISIKO ======
RISK_PER_TRADE_PCT = 25.01# dinaikkan dari 5.0 sesuai permintaan -- ini basis perhitungan
                          # sizing utama dari jarak SL (makin lebar SL, makin kecil posisi,
                          # rugi NOMINAL per trade tetap konsisten sekitar 10% saldo per SL
                          # kena). Berlaku SAMA untuk Spot maupun Futures (tidak dipisah
                          # seperti MAX_POSITION_PCT_SPOT/FUTURES di bawah).
                          # MAX_POSITION_PCT_SPOT (10%) / MAX_POSITION_PCT_FUTURES (80%)
                          # tetap jadi plafon darurat kalau SL kebetulan sangat sempit.

# Saldo AWAL simulasi mode DRY_RUN -- saldo yang BENERAN dipakai sekarang
# = INITIAL_SIMULATED_BALANCE + realized_pnl_quote (PnL yang sudah
# terealisasi, persisten lintas restart lewat pnl_history.json). Beda
# dari sebelumnya yang selalu flat $1000 terlepas dari hasil trading --
# sekarang saldo simulasi BENERAN naik/turun sesuai performa bot, dan
# TETAP KEINGET walau bot di-restart. Reset lewat tombol "Reset Statistik"
# di dashboard kalau mau mulai dari 0 lagi.
INITIAL_SIMULATED_BALANCE = 100.0
STOP_LOSS_PCT = 1.5     # dipakai kalau USE_ATR_RISK=False

# Khusus market SPOT (TIDAK berlaku untuk Futures) -- False = posisi Spot
# dibuka TANPA SL/TP sama sekali (beli sesuai sinyal, lalu ditahan terus).
# Tidak akan pernah ditutup otomatis oleh SL/TP, sinyal SELL, trailing,
# break-even, ATAU early reversal -- SATU-SATUNYA cara menutupnya adalah
# manual lewat tombol "Tutup Posisi" di dashboard. Cocok buat strategi
# akumulasi/"buy the dip" jangka panjang, BUKAN untuk trading aktif biasa.
# Futures TIDAK terpengaruh sama sekali oleh setting ini (selalu tetap
# pakai SL/TP normal, karena leverage bikin Futures tanpa SL sangat
# berisiko likuidasi).
USE_SPOT_SLTP = False# Spot mode "buy and hold" -- beli sesuai sinyal, TANPA SL/TP,
                         # tidak pernah ditutup otomatis. Cuma bisa ditutup manual dari dashboard.
TAKE_PROFIT_PCT = 2.5   # dipakai kalau USE_ATR_RISK=False

# MAX_POSITION_PCT sekarang DIPISAH per market type (sebelumnya satu angka
# 25% berlaku sama untuk Spot maupun Futures) -- ini batas KERAS plafon
# darurat ukuran posisi (dari saldo di market itu), yang menang duluan dari
# perhitungan risiko berbasis jarak SL (RISK_PER_TRADE_PCT=10%, SAMA untuk
# KEDUANYA, tidak dipisah) kalau plafon ini lebih kecil.
#
# CATATAN PENTING soal MAX_POSITION_PCT_SPOT: sebelumnya di-set 10% --
# PERSIS SAMA dengan RISK_PER_TRADE_PCT (10%). Secara matematis itu berarti
# plafon SELALU menang untuk SL berapa pun yang realistis (rumus sizing
# baru memakai RISK_PER_TRADE_PCT murni kalau jarak SL > 100%, yang
# mustahil dalam praktik) -- jadi posisi Spot akan SELALU persis 10% saldo,
# RISK_PER_TRADE_PCT jadi tidak berpengaruh sama sekali untuk Spot. Sekarang
# dinaikkan ke 25% supaya ada jarak dari RISK_PER_TRADE_PCT (posisi Spot
# masih akan sering ke-cap di plafon untuk SL sempit khas ATR -- itu memang
# wajar secara matematis, lihat penjelasan di bawah -- tapi setidaknya
# tidak SELALU 100% identik seperti sebelumnya).
#
# PENTING dipahami: untuk SL SEMPIT (misal 1.5%, umum untuk ATR_MULT_SL=1.0
# di pair mainstream), rumus risk-based (risk_amount/jarak_SL%) akan SELALU
# menghasilkan angka yang jauh lebih besar dari plafon berapa pun yang
# masuk akal -- ini BUKAN bug, melainkan konsekuensi matematis wajar:
# me-risk-kan 10% saldo dengan jarak stop cuma 1.5% BUTUH notional ~667%
# saldo. Plafon (MAX_POSITION_PCT_SPOT/FUTURES) memang DISENGAJA jadi
# "rem darurat" untuk kasus ini -- menaikkan plafon lebih tinggi lagi
# BUKAN membuat RISK_PER_TRADE_PCT "lebih berlaku", melainkan LANGSUNG
# menaikkan eksposur dollar riil per trade. Risiko dollar SEBENARNYA saat
# plafon yang menang = ukuran_posisi% x jarak_SL% (jauh lebih kecil dari
# angka RISK_PER_TRADE_PCT itu sendiri) -- jadi walau labelnya "risiko per
# trade 10%", risiko dollar riil untuk SL sempit jauh lebih kecil dari itu
# selama plafon masih moderat. RISK_PER_TRADE_PCT baru benar-benar jadi
# penentu utama kalau jarak SL kebetulan LEBAR (di atas threshold
# 100 x RISK_PER_TRADE_PCT / plafon -- sekitar 40% untuk Spot sekarang,
# 12.5% untuk Futures).
MAX_POSITION_PCT_SPOT = 25.0# plafon Spot per trade -- naik dari 10% supaya beda dari RISK_PER_TRADE_PCT
MAX_POSITION_PCT_FUTURES = 90.0# plafon Futures per trade -- 80% saldo Futures (tidak diubah)
# PERINGATAN KERAS: 80% saldo Futures dikombinasikan dengan LEVERAGE=5x
# (lihat bagian atas) berarti NOTIONAL POSISI bisa mencapai ~4x SALDO
# FUTURES dalam SATU trade -- eksposur yang sangat besar, dan bisa
# menyentuh batas likuidasi jauh lebih cepat dibanding plafon lama (25%,
# notional ~1.25x saldo). MAX_CONCURRENT_POSITIONS sudah dibatasi ke 10
# (20 Sept 2026) yang memberi batas praktis untuk total eksposur -- tapi
# per-posisi masih bisa besar (80% saldo x leverage 5x). Selama
# DRY_RUN=True tidak ada risiko dana nyata, tapi WAJIB ditinjau ulang
# sebelum mematikan DRY_RUN.
QUOTE_ASSET = "USDT"

# ====== 10. FILTER KONFIRMASI CLAUDE (opsional) ======
USE_AI_CONFIRMATION = False
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
AI_MODEL = "claude-sonnet-5"
AI_FAIL_OPEN = True

# ====== 11. TELEGRAM & LOGGING ======
# ====== MULTI-USER (multi-tenant) -- FONDASI, lihat user_manager.py ======
# MASTER_ENCRYPTION_KEY -- WAJIB diisi string RAHASIA & PANJANG di .env kalau
# USE_MULTI_USER=True. Dipakai buat enkripsi/dekripsi SEMUA kredensial
# exchange milik SEMUA user terdaftar -- KALAU HILANG/BERUBAH, SEMUA
# KREDENSIAL TERSIMPAN JADI TIDAK BISA DIBACA LAGI SELAMANYA. Backup di
# tempat aman TERPISAH dari server.
MASTER_ENCRYPTION_KEY = os.getenv("MASTER_ENCRYPTION_KEY", "")

# Kalau bot ini dijalankan sebagai SALAH SATU dari banyak instance
# multi-user (lihat multi_bot_orchestrator.py), SEMUA instance berbagi
# TELEGRAM_TOKEN yang SAMA -- Telegram cuma izinkan SATU "pendengar"
# pesan masuk per token (getUpdates konsumsi update sekali pakai). Kalau
# banyak instance sama-sama dengar, pesan user bisa "kecuri" ke instance
# user lain. Set False di instance yang di-orchestrate multi-user (biar
# CUMA central_dispatcher.py yang dengar pesan masuk) -- notifikasi
# KELUAR (posisi dibuka/ditutup, dll) TETAP jalan normal, cuma listener
# PESAN MASUK yang dimatikan. Default True (perilaku single-user lama,
# TIDAK BERUBAH kalau bot dijalankan sendirian seperti biasa).
TELEGRAM_LISTEN_FOR_COMMANDS = os.getenv("TELEGRAM_LISTEN_FOR_COMMANDS", "true").lower() != "false"

# Sama seperti TELEGRAM_LISTEN_FOR_COMMANDS di atas, TAPI untuk Discord --
# ditambahkan 20 Sept 2026 sebagai bagian dari sentralisasi listener
# Discord (lihat discord_dispatcher.py BARU) supaya instance bot per-user
# (multi_bot_orchestrator.py) TIDAK masing-masing connect ke Discord
# Gateway pakai DISCORD_BOT_TOKEN yang SAMA -- itu akan bentrok persis
# seperti masalah token contention Telegram yang sudah lebih dulu
# diselesaikan lewat central_dispatcher.py. Default True (perilaku lama
# TIDAK BERUBAH kalau bot dijalankan sendirian/single-user seperti biasa).
DISCORD_LISTEN_FOR_COMMANDS = os.getenv("DISCORD_LISTEN_FOR_COMMANDS", "true").lower() != "false"

# URL publik dashboard_gateway.py -- dikirim ke user di pesan sukses
# registrasi (registration_flow.py). Isi dengan alamat SERVER kamu yang
# sebenarnya (bukan localhost) kalau mau user beneran bisa akses dari
# HP/komputer mereka sendiri.
DASHBOARD_GATEWAY_URL = os.getenv("DASHBOARD_GATEWAY_URL", "http://localhost:8000")
USE_MULTI_USER = False  # default MATI -- bot tetap single-user/single-akun seperti biasa

# ====== MARKET SCANNER (auto-pilih pair dari hasil scan sinyal) ======
# Scan top N pair terliquid di OKX Futures (SWAP), jalankan semua filter
# strategi, pilih pair yang ada sinyal terkuat buat di-entry.
USE_MARKET_SCANNER = True        # aktifkan untuk mulai scan otomatis
SCANNER_TOP_N_SYMBOLS = 20       # berapa pair teratas (by volume 24j) yang di-scan
# Batas jumlah pair hasil scan yang boleh aktif sekaligus. (Riwayat: dulu
# ada SCANNER_MAX_ACTIVE_PAIRS_BINANCE terpisah di sini karena scanner-nya
# multi-exchange -- fork OKX-only ini sudah menghapusnya, cuma OKX yang
# tersisa jadi cukup satu angka.)
# Catatan: ini CUMA membatasi pair hasil SCANNER (Futures). Pair Spot OKX
# (SYMBOLS_SPOT_OKX di atas, saat ini cuma BTC-USDT) di LUAR cakupan ini --
# scanner tidak pernah menyentuh Spot sama sekali (lihat run_scanner_cycle()
# di market_scanner.py, cuma scan symbol FUTURES).
SCANNER_MAX_ACTIVE_PAIRS_OKX = 5# maksimal pair Futures OKX dari hasil scan
SCANNER_INTERVAL_MINUTES = 15    # seberapa sering scanner jalan (dalam menit)
SCANNER_CANDLE_LIMIT = 100       # jumlah candle history yang diambil buat hitung indikator
SCANNER_ROTATE_OUT = True        # kalau True: pair yang sinyalnya HILANG otomatis dihapus
                                  # (cuma kalau tidak ada posisi terbuka di pair itu)
# Berapa SIKLUS SCANNER berturut-turut (tiap siklus = SCANNER_INTERVAL_MINUTES
# menit) sebuah pair harus HOLD (sinyal tidak aktif) dulu SEBELUM dirotasi
# keluar -- ditambahkan 20 Sept 2026 ("untuk pair kondisi hold tapi tidak
# open posisi lakukan resuffle") supaya pair yang baru masuk atau cuma
# sesaat HOLD tidak langsung dibuang di siklus pertama. Nilai 2 = pair harus
# HOLD terus 2 siklus scan berturut-turut (untuk INTERVAL Futures 15m yang
# sama dengan SCANNER_INTERVAL_MINUTES sekarang, kira-kira setara 2 candle)
# baru dirotasi. Naikkan angka ini kalau masih terasa terlalu cepat dibuang,
# turunkan (minimal 1, sama seperti perilaku lama) kalau mau lebih agresif.
SCANNER_ROTATE_OUT_HOLD_CYCLES = 2

# Reshuffle pair rugi (19 Sept 2026, trigger diganti 20 Sept 2026): begitu
# SL sebuah pair hasil scanner (yang PUNYA posisi terbuka) benar-benar
# KENA, pair itu langsung dihapus dari daftar aktif, supaya slotnya kosong
# dan otomatis diisi kandidat baru oleh market scanner di siklus berikutnya
# (maks SCANNER_INTERVAL_MINUTES menit lagi) -- bukan cuma ditutup normal
# lalu dibiarkan tetap di daftar pantau.
# (Sebelumnya trigger-nya ambang floating loss terpisah, RESHUFFLE_LOSS_PCT,
# independen dari SL asli -- atas permintaan langsung diganti total jadi
# trigger SL asli, RESHUFFLE_LOSS_PCT sudah dihapus, tidak dipakai lagi.)
# Pair PERMANEN (SYMBOLS_SPOT_OKX/SYMBOLS_FUTURES_OKX di atas -- misal
# BTC-USDT) TIDAK PERNAH direshuffle walau SL-nya kena,
# itu pair yang sengaja dipertahankan terus, bukan bagian rotasi otomatis
# scanner.
USE_PAIR_RESHUFFLE = True

# Reshuffle KEDUA (21 Sept 2026, terpisah dari USE_PAIR_RESHUFFLE/SL di
# atas): berbasis UMUR POSISI, bukan SL. Kalau posisi (bukan pair
# permanen) sudah terbuka >= TP_STALL_RESHUFFLE_HOURS jam TAPI belum
# full-close -- baik belum sampai TP level ke-3/4 (r3/r4), ATAUPUN
# sudah lewat r3 tapi macet di fase "ikut tren" (USE_TREND_EXIT_FINAL_TP)
# nunggu reversal/SL -- posisi ditutup paksa & pair-nya dikeluarkan dari
# daftar aktif (lewat USE_PAIR_RESHUFFLE di atas) supaya slotnya diisi
# kandidat baru oleh scanner. HANYA berlaku kalau posisi minimal sudah
# breakeven/untung (harga >= entry utk LONG / <= entry utk SHORT) --
# kalau masih rugi, TIDAK dipaksa tutup, dibiarkan SL asli yang urus.
# Lihat TradingBot.check_tp_stall_reshuffle() di bot.py.
USE_TP_STALL_RESHUFFLE = True
TP_STALL_RESHUFFLE_HOURS = 3.0# jam -- batas umur posisi sebelum dianggap macet
                          # sampai fitur multi-user selesai dibangun penuh & diaktifkan sengaja

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
USE_TELEGRAM_ALERTS = True    # AKTIF -- pastikan TELEGRAM_TOKEN/CHAT_ID sudah diisi
                                # panduan setup lengkap di docstring telegram_notifier.py
USE_DISCORD_ALERTS = True
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
# Perintah masuk Discord (BOT token -- BEDA dari webhook).
# Setup: Discord Developer Portal -> New Application -> Bot -> Reset Token
# -> Message Content Intent ON -> invite bot ke server dengan permission
# Read Messages + Send Messages. Lalu isi di .env:
#   DISCORD_BOT_TOKEN=...
#   DISCORD_ALLOWED_USER_IDS=123456789,987654321   (opsional, kosong=semua)
USE_DISCORD_COMMANDS = os.getenv("USE_DISCORD_COMMANDS", "true").lower() != "false"
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_COMMAND_PREFIX = os.getenv("DISCORD_COMMAND_PREFIX", "!")
DISCORD_ALLOWED_USER_IDS = [
    x.strip() for x in os.getenv("DISCORD_ALLOWED_USER_IDS", "").split(",") if x.strip()
]

LOG_FILE = "trading_bot.log"
LOG_LEVEL = "INFO"
LOG_WIPE_HOURS = 1   # wipe penuh trading_bot.log tiap N jam, cegah membengkak

# Log TERSTRUKTUR (format logfmt: key=value dipisah spasi) untuk event-event
# trading penting (posisi dibuka/ditutup, sinyal, PnL, dll) -- TERPISAH dari
# LOG_FILE di atas yang isinya kalimat manusia biasa. Ditulis ke file SENDIRI,
# TIDAK ikut di-wipe otomatis (beda tujuan -- ini buat dianalisa/diarsipkan,
# bukan cuma debug sesaat), jadi kamu yang atur sendiri kapan mau dibersihkan.
METRICS_LOG_FILE = "metrics.log"
