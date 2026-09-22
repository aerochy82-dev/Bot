"""
Modul strategi: MACD Crossover + filter Tren (EMA200), ADX, RSI, dan Volume.

Logika dasar (trigger):
- MACD line memotong ke ATAS signal line  -> kandidat sinyal BUY
- MACD line memotong ke BAWAH signal line -> kandidat sinyal SELL
MACD (turunan dari EMA cepat/lambat yang di-smooth lagi) lebih halus
dibanding EMA crossover mentah, jadi cross-nya cenderung lebih jarang
false/whipsaw dibanding EMA_FAST vs EMA_SLOW langsung.

Filter tambahan (mengurangi false-signal, dicek berurutan -- kalau satu
gagal, sinyal langsung jadi HOLD, tidak perlu cek filter berikutnya):
1. TREND (EMA200): sinyal BUY hanya diambil kalau harga close di ATAS
   EMA200 (bias tren naik), sinyal SELL hanya kalau harga close di BAWAH
   EMA200 (bias tren turun). Ini mencegah bot entry melawan tren besar --
   MACD cross yang searah tren jauh lebih reliable daripada yang melawan
   tren.
2. ADX: sinyal HANYA diambil kalau ADX >= ADX_MIN_TREND (market memang
   trending). ADX rendah berarti market sedang sideways -> cross di
   kondisi ini sering "whipsaw" (bolak-balik), jadi sinyal diabaikan.
3. RSI: sinyal BUY ditolak kalau RSI sudah overbought, sinyal SELL
   ditolak kalau RSI sudah oversold -> menghindari entry telat di ujung
   pergerakan.
4. VOLUME: sinyal ditolak kalau volume candle saat ini di bawah rata-rata
   (volume_ma * VOLUME_MIN_RATIO) -> menghindari entry di liquidity
   rendah / kemungkinan fakeout yang gampang dibalik.

Semua filter (TREND/ADX/RSI/VOLUME) bisa dimatikan satu-satu lewat
config.py kalau mau mengurangi jumlah trade yang di-skip.
"""

import numpy as np
import pandas as pd

import config


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI standar (Wilder's smoothing)."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    # Edge case: avg_loss = 0 (misal uptrend sempurna tanpa penurunan sama
    # sekali) -> RS -> tak hingga, RSI harusnya 100, bukan NaN dari div-by-zero.
    rsi = rsi.where(avg_loss != 0, 100.0)
    # Edge case: harga benar-benar flat (avg_gain = avg_loss = 0) -> netral 50.
    rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), 50.0)

    return rsi


def compute_dpo(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Detrended Price Oscillator (DPO) -- menghilangkan tren jangka
    panjang dari harga, biar kelihatan siklus naik-turun jangka pendeknya
    saja. BEDA dari indikator momentum biasa (RSI/MACD/SMI) -- DPO
    sengaja TIDAK dipakai buat sinyal real-time murni, lebih ke
    identifikasi overbought/oversold jangka pendek relatif ke tren SMA-nya.

    Rumus standar: DPO = harga close [period//2 + 1 periode yang lalu]
    dikurangi SMA(period) SEKARANG.

    DPO positif -> harga (di titik itu) di atas tren SMA -> overbought
    jangka pendek. DPO negatif -> di bawah tren -> oversold jangka pendek."""
    shift = period // 2 + 1
    sma = df["close"].rolling(window=period).mean()
    return df["close"].shift(shift) - sma


def compute_smi_ergodic(df: pd.DataFrame, long_length: int = 20, short_length: int = 5,
                         signal_length: int = 5) -> tuple:
    """SMI Ergodic Indicator (William Blau) -- indikator momentum yang
    ngukur posisi harga close SEKARANG relatif terhadap pergerakannya,
    di-smooth DUA KALI (double-smoothed EMA) biar lebih halus dari
    RSI/Stochastic biasa. SAMA dengan indikator "SMII" bawaan TradingView.

    long_length/short_length: dua tahap smoothing buat momentum-nya
    sendiri (selisih close antar candle) DAN buat momentum absolutnya.
    signal_length: EMA dari SMI itu sendiri, jadi garis pembanding buat
    deteksi cross (mirip garis sinyal MACD).

    Return (smi, smi_signal) -- dua pd.Series."""
    momentum = df["close"].diff()
    abs_momentum = momentum.abs()

    m1 = momentum.ewm(span=long_length, adjust=False).mean()
    m2 = m1.ewm(span=short_length, adjust=False).mean()

    a1 = abs_momentum.ewm(span=long_length, adjust=False).mean()
    a2 = a1.ewm(span=short_length, adjust=False).mean()

    smi = 100 * (m2 / a2.replace(0, np.nan))
    smi = smi.fillna(0.0)  # a2=0 (harga benar-benar flat) -> momentum netral, bukan NaN

    smi_signal = smi.ewm(span=signal_length, adjust=False).mean()

    return smi, smi_signal


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR (Average True Range) standar, Wilder's smoothing. Dipakai baik
    untuk filter ADX (butuh ATR sebagai komponen internal) maupun untuk
    SL/TP berbasis volatilitas (USE_ATR_RISK)."""
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ADX standar (Wilder's smoothing) dari kolom high/low/close."""
    high, low, close = df["high"], df["low"], df["close"]

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = compute_atr(df, period)
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr.replace(0, np.nan)

    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return adx


def compute_macd(close: pd.Series, fast: int, slow: int, signal: int):
    """MACD standar: macd_line = EMA_fast - EMA_slow, signal_line = EMA(macd_line)."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _mp(market_type, base_key):
    """Resolusi nilai parameter STRATEGI yang bisa dipisah Spot/Futures.

    Kalau market_type=="FUTURES" DAN ada variant "<base_key>_FUTURES" di
    config.py, pakai nilai itu. Selain itu (Spot, atau market_type None
    dari caller lama yang belum tahu market_type-nya) pakai <base_key>
    biasa (Spot) -- SAMA PERSIS perilaku lama kalau var _FUTURES belum
    ada, jadi aman dipakai di mana saja tanpa mengubah pemanggilan lama.

    Dipakai buat EMA cross (EMA_FAST_LEN/SLOW_LEN), EMA Tren, RSI, ADX,
    ATR% (volatilitas), Rasio Volume, SMII, dan DPO -- supaya semua
    parameter itu bisa dituning terpisah antara Spot (candle 1 jam) dan
    Futures (candle 5 menit) yang karakter pergerakannya beda jauh, sama
    seperti MACD_FAST_FUTURES/dst yang sudah ada duluan. Lihat juga
    compute_indicators()/generate_signal()/get_signal_detail() di bawah.
    """
    if market_type == "FUTURES":
        return getattr(config, f"{base_key}_FUTURES", getattr(config, base_key))
    return getattr(config, base_key)


def compute_indicators(df: pd.DataFrame, market_type=None) -> pd.DataFrame:
    """Hitung indikator trigger (MACD atau EMA cross, tergantung config),
    EMA tren, plus RSI/ADX/volume/ATR kalau dibutuhkan.

    market_type: "FUTURES" atau "SPOT" (opsional). Kalau diisi "FUTURES",
    SEMUA parameter berikut ini pakai variant _FUTURES-nya (lewat helper
    _mp() di atas): EMA_FAST_LEN/SLOW_LEN (trigger EMA cross), EMA_TREND,
    RSI_PERIOD, ADX_PERIOD, SMII_LONG/SHORT/SIGNAL_LENGTH, DPO_PERIOD, dan
    MACD_FAST/SLOW/SIGNAL -- semuanya dituning buat candle 5 menit yang
    jauh lebih cepat ganti daripada candle 1 jam punya Spot. Kalau bukan
    "FUTURES" (termasuk None, dipanggil dari kode lama yang belum tahu
    market_type-nya), tetap pakai nilai biasa (Spot) seperti sebelumnya,
    jadi pemanggilan lama tanpa argumen ini tidak akan rusak. _mp()/getattr
    dipakai supaya tetap aman kalau suatu saat var _FUTURES dihapus dari
    config.py (fallback otomatis ke yang Spot)."""
    df = df.copy()

    # EMA_FAST_LEN/EMA_SLOW_LEN dihitung SELALU -- dipakai dashboard buat
    # garis overlay "EMA Cepat"/"EMA Lambat" di chart candlestick (referensi
    # visual), TERPISAH dari trigger sinyal beneran. Sebelumnya dua kolom
    # ini CUMA dihitung kalau USE_EMA_CROSS_STRATEGY=True, jadi begitu
    # strategi dipindah ke MACD, garisnya hilang total dari chart. Trigger
    # sinyal aktual tetap ikut config.USE_EMA_CROSS_STRATEGY seperti biasa
    # di generate_signal()/get_signal_detail() -- ini murni nambah data
    # buat divisualisasikan, tidak mengubah logika entry/exit sama sekali.
    df["ema_fast_x"] = df["close"].ewm(span=_mp(market_type, "EMA_FAST_LEN"), adjust=False).mean()
    df["ema_slow_x"] = df["close"].ewm(span=_mp(market_type, "EMA_SLOW_LEN"), adjust=False).mean()

    if not config.USE_EMA_CROSS_STRATEGY:
        macd_fast = _mp(market_type, "MACD_FAST")
        macd_slow = _mp(market_type, "MACD_SLOW")
        macd_signal_len = _mp(market_type, "MACD_SIGNAL")
        df["macd_line"], df["macd_signal"], df["macd_hist"] = compute_macd(
            df["close"], macd_fast, macd_slow, macd_signal_len
        )

    if config.USE_TREND_FILTER:
        df["ema_trend"] = df["close"].ewm(span=_mp(market_type, "EMA_TREND"), adjust=False).mean()

    # EMA reversal dini -- TERPISAH dari ema_fast_x/ema_slow_x (trigger entry
    # baru) di atas. Cuma dipakai bot.py buat pantau posisi yang SUDAH
    # TERBUKA, bukan buat sinyal entry. Lihat TradingBot.check_early_reversal().
    if config.USE_EARLY_REVERSAL:
        df["ema_reversal_fast"] = df["close"].ewm(span=config.EMA_REVERSAL_FAST_LEN, adjust=False).mean()
        df["ema_reversal_slow"] = df["close"].ewm(span=config.EMA_REVERSAL_SLOW_LEN, adjust=False).mean()

    if config.USE_RSI_FILTER:
        df["rsi"] = compute_rsi(df["close"], _mp(market_type, "RSI_PERIOD"))

    if config.USE_SMII_FILTER:
        df["smi"], df["smi_signal"] = compute_smi_ergodic(
            df, _mp(market_type, "SMII_LONG_LENGTH"), _mp(market_type, "SMII_SHORT_LENGTH"),
            _mp(market_type, "SMII_SIGNAL_LENGTH")
        )

    if config.USE_DPO_FILTER:
        df["dpo"] = compute_dpo(df, _mp(market_type, "DPO_PERIOD"))

    # Kolom "atr" dipakai untuk TIGA hal yang independen: SL/TP berbasis ATR
    # (USE_ATR_RISK), trailing stop (USE_TRAILING_STOP), dan filter
    # volatilitas (USE_VOLATILITY_FILTER) -- dihitung sekali kalau SALAH SATU
    # dari ketiganya aktif. ADX punya perhitungan ATR internal terpisah
    # (dengan ADX_PERIOD) di dalam compute_adx() di atas -- sengaja tidak
    # digabung supaya tidak salah pakai periode kalau ATR_LEN != ADX_PERIOD.
    if config.USE_ATR_RISK or config.USE_TRAILING_STOP or config.USE_VOLATILITY_FILTER:
        df["atr"] = compute_atr(df, config.ATR_LEN)

    if config.USE_ADX_FILTER:
        df["adx"] = compute_adx(df, _mp(market_type, "ADX_PERIOD"))

    if config.USE_VOLUME_FILTER:
        df["volume_ma"] = df["volume"].rolling(window=config.VOLUME_MA_PERIOD, min_periods=1).mean()

    return df


# ---------- SMART MONEY CONCEPTS (SMC) -- filter TAMBAHAN, bukan pengganti ----------
# EMA cross tetap trigger UTAMA (lihat generate_signal() di bawah) -- SMC di
# sini cuma dipakai sebagai KONFIRMASI ARAH, sama seperti filter Trend/RSI/
# ADX/Volume/Volatilitas yang sudah ada. Implementasi disederhanakan (bukan
# full SMC dengan Order Block/Fair Value Gap/liquidity sweep), fokus ke
# konsep paling inti: struktur market via swing high/low + Break of
# Structure (BOS) buat tentukan bias bullish/bearish SEKARANG.

def detect_swing_points(df: pd.DataFrame, lookback: int = 5):
    """Deteksi swing high/low sederhana -- satu titik dianggap swing high
    kalau high-nya PALING TINGGI dibanding `lookback` candle di kiri DAN
    kanannya (swing low sebaliknya, PALING RENDAH). Return dua list
    (posisi_index_integer, harga) terpisah untuk swing high dan swing low."""
    highs = df["high"].values
    lows = df["low"].values
    n = len(df)
    swing_highs, swing_lows = [], []
    for i in range(lookback, n - lookback):
        window_h = highs[i - lookback:i + lookback + 1]
        window_l = lows[i - lookback:i + lookback + 1]
        if highs[i] == window_h.max():
            swing_highs.append((i, highs[i]))
        if lows[i] == window_l.min():
            swing_lows.append((i, lows[i]))
    return swing_highs, swing_lows


def get_smc_structure_bias(df: pd.DataFrame, lookback: int = 5) -> str:
    """Tentukan bias struktur market SEKARANG -- 'bullish', 'bearish',
    atau 'neutral' (data belum cukup buat nentuin struktur sama sekali) --
    berdasarkan Break of Structure (BOS) yang PALING BARU terjadi.

    Cara kerja: kumpulkan semua swing high & swing low yang terdeteksi,
    lalu buat masing-masing titik, cek APAKAH dan KAPAN closing price
    PERTAMA KALI menembusnya SETELAH titik itu terbentuk (BOS bullish
    kalau menembus swing high, bearish kalau menembus swing low). Bias
    yang dipakai adalah dari BOS yang waktunya PALING BARU di antara semua
    itu -- itulah arah struktur market yang paling relevan SEKARANG."""
    swing_highs, swing_lows = detect_swing_points(df, lookback=lookback)
    if not swing_highs or not swing_lows:
        return "neutral"

    closes = df["close"].values
    n = len(df)
    points = [(pos, level, "high") for pos, level in swing_highs] + \
             [(pos, level, "low") for pos, level in swing_lows]

    most_recent_break_pos = -1
    most_recent_break_kind = "neutral"

    for pos, level, kind in points:
        for j in range(pos + 1, n):
            if kind == "high" and closes[j] > level:
                if j > most_recent_break_pos:
                    most_recent_break_pos = j
                    most_recent_break_kind = "bullish"
                break
            if kind == "low" and closes[j] < level:
                if j > most_recent_break_pos:
                    most_recent_break_pos = j
                    most_recent_break_kind = "bearish"
                break

    return most_recent_break_kind


def detect_order_blocks(df: pd.DataFrame, impulse_threshold_pct: float = 0.5, lookback: int = 50) -> list:
    """Deteksi Order Block (OB) sederhana -- candle TERAKHIR yang berlawanan
    arah SEBELUM pergerakan kuat (impulsif) searah candle berikutnya:
    - OB BULLISH: candle BEARISH (merah), diikuti candle berikutnya naik
      kuat (>= impulse_threshold_pct%) -- ini "zona demand" institusional.
    - OB BEARISH: candle BULLISH (hijau), diikuti candle berikutnya turun
      kuat -- "zona supply".

    Cuma nyari di `lookback` candle terakhir (OB yang terlalu lama biasanya
    sudah tidak relevan/"mitigated"). Return list makin BARU makin di akhir,
    tiap entri: {"index": posisi_integer, "type": "bullish"/"bearish",
    "top": harga_tertinggi_candle, "bottom": harga_terendah_candle}."""
    obs = []
    n = len(df)
    opens = df["open"].values
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values

    start = max(0, n - lookback)
    for i in range(start, n - 1):
        if closes[i] == 0:
            continue
        next_move_pct = (closes[i + 1] - closes[i]) / closes[i] * 100
        is_bearish_candle = closes[i] < opens[i]
        is_bullish_candle = closes[i] > opens[i]

        if is_bearish_candle and next_move_pct >= impulse_threshold_pct:
            obs.append({"index": i, "type": "bullish", "top": highs[i], "bottom": lows[i]})
        elif is_bullish_candle and next_move_pct <= -impulse_threshold_pct:
            obs.append({"index": i, "type": "bearish", "top": highs[i], "bottom": lows[i]})

    return obs


def detect_fair_value_gaps(df: pd.DataFrame, lookback: int = 50) -> list:
    """Deteksi Fair Value Gap (FVG) -- celah/imbalance harga pada pola 3
    candle berurutan:
    - FVG BULLISH: low candle ke-3 > high candle ke-1 (harga "meloncat"
      naik, meninggalkan celah yang belum pernah "diisi" ulang).
    - FVG BEARISH: high candle ke-3 < low candle ke-1 (celah ke bawah).

    Cuma nyari di `lookback` candle terakhir. Return list, tiap entri:
    {"index": posisi_integer (candle TENGAH dari pola 3-candle),
    "type": "bullish"/"bearish", "top": batas_atas_celah,
    "bottom": batas_bawah_celah}."""
    fvgs = []
    n = len(df)
    highs = df["high"].values
    lows = df["low"].values

    start = max(1, n - lookback)
    for i in range(start, n - 1):
        if lows[i + 1] > highs[i - 1]:
            fvgs.append({"index": i, "type": "bullish", "top": lows[i + 1], "bottom": highs[i - 1]})
        elif highs[i + 1] < lows[i - 1]:
            fvgs.append({"index": i, "type": "bearish", "top": lows[i - 1], "bottom": highs[i + 1]})

    return fvgs


def _price_near_zone(price: float, zone_top: float, zone_bottom: float, tolerance_pct: float) -> bool:
    """Cek apakah `price` ada DI DALAM zona [bottom, top], ATAU meleset
    tidak lebih dari `tolerance_pct`% dari lebar zona itu -- dipakai buat
    OB/FVG karena harga jarang PERSIS balik ke zona, biasanya cukup
    "mendekat". Kalau zona tidak punya lebar sama sekali (top==bottom,
    kasus langka), toleransi dihitung dari persentase harga zona itu sendiri."""
    if zone_bottom > zone_top:
        zone_top, zone_bottom = zone_bottom, zone_top
    zone_height = zone_top - zone_bottom
    margin = zone_height * (tolerance_pct / 100) if zone_height > 0 else zone_top * (tolerance_pct / 100)
    return (zone_bottom - margin) <= price <= (zone_top + margin)


def get_smc_order_block_confirmation(df: pd.DataFrame, side: str, impulse_threshold_pct: float = 0.5,
                                      lookback: int = 50, tolerance_pct: float = 20) -> bool:
    """True kalau harga SEKARANG ada di dalam/dekat Order Block yang
    RELEVAN buat sinyal `side` ('BUY' perlu OB bullish/demand zone,
    'SELL' perlu OB bearish/supply zone) -- pakai OB PALING BARU yang
    ketemu. Kalau TIDAK ADA order block relevan sama sekali dalam
    lookback, return True (data belum cukup BUKAN alasan buat blokir
    sinyal, sama seperti bias 'neutral' di filter struktur)."""
    obs = detect_order_blocks(df, impulse_threshold_pct=impulse_threshold_pct, lookback=lookback)
    wanted_type = "bullish" if side == "BUY" else "bearish"
    relevant = [ob for ob in obs if ob["type"] == wanted_type]
    if not relevant:
        return True
    current_price = df["close"].iloc[-1]
    most_recent = relevant[-1]
    return _price_near_zone(current_price, most_recent["top"], most_recent["bottom"], tolerance_pct)


def get_smc_fvg_confirmation(df: pd.DataFrame, side: str, lookback: int = 50, tolerance_pct: float = 20) -> bool:
    """Sama seperti get_smc_order_block_confirmation() tapi buat Fair
    Value Gap -- True kalau harga SEKARANG di dalam/dekat FVG relevan
    PALING BARU, atau kalau tidak ada FVG relevan sama sekali (tidak
    memblokir sinyal cuma karena data belum cukup)."""
    fvgs = detect_fair_value_gaps(df, lookback=lookback)
    wanted_type = "bullish" if side == "BUY" else "bearish"
    relevant = [f for f in fvgs if f["type"] == wanted_type]
    if not relevant:
        return True
    current_price = df["close"].iloc[-1]
    most_recent = relevant[-1]
    return _price_near_zone(current_price, most_recent["top"], most_recent["bottom"], tolerance_pct)


def generate_signal(df: pd.DataFrame, market_type: str = None, indicators_df=None) -> str:
    """
    Tentukan sinyal berdasarkan candle terakhir yang sudah closed.
    Return: "BUY", "SELL", atau "HOLD"

    market_type: opsional -- "SPOT" atau "FUTURES". Dibutuhkan KHUSUS
    buat filter SMC (Struktur/OB/FVG), yang bisa diatur buat CUMA
    berlaku di Spot lewat config.SMC_MARKET_SCOPE (lihat di bawah).
    Kalau None (caller lama yang belum di-update), filter SMC berlaku
    ke SEMUA market seperti biasa -- TIDAK ADA perubahan perilaku.

    indicators_df: opsional -- DataFrame yang SUDAH lewat compute_indicators().
    Dipakai scanner supaya tidak hitung indikator 2x (deteksi cross + filter).
    """
    if indicators_df is not None:
        df = indicators_df
    else:
        if len(df) < 2:
            return "HOLD"
        df = compute_indicators(df, market_type=market_type)
    if len(df) < 2:
        return "HOLD"
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    # ---- Trigger sinyal ----
    # Mode SPREAD (kedua filter ON): EMA cross tidak dipakai sebagai event
    # sesaat, tapi arah sinyal tetap DITENTUKAN oleh arah cross EMA
    # TERAKHIR (sign dari ema_fast_x - ema_slow_x -- kalau EMA cepat masih
    # di atas EMA lambat, cross terakhir yang terjadi adalah bullish, dst).
    # "Spread" (jarak harga vs EMA lambat dibanding jarak EMA cepat vs EMA
    # lambat) cuma dipakai buat mengukur SEBERAPA JAUH harga sudah kabur
    # dari EMA lambat SEARAH cross terakhir itu -- bukan buat menentukan
    # arahnya sendiri. Sebelumnya arah BUY/SELL diambil murni dari
    # perbandingan dua nilai abs() (ema_spread vs price_spread) TANPA
    # melihat tanda/arahnya sama sekali -- akibatnya breakout turun yang
    # kuat pun bisa salah tercatat sebagai BUY, karena abs() menghilangkan
    # informasi arah. Sekarang: BUY hanya mungkin kalau EMA cepat > EMA
    # lambat (cross terakhir bullish) DAN harga juga di sisi yang sama
    # (di atas EMA lambat); SELL sebaliknya. Kalau harga sudah menyeberang
    # ke sisi berlawanan dari cross terakhir, itu HOLD (bukan otomatis
    # dibalik ke sinyal lawan) -- pembalikan sungguhan butuh cross EMA
    # baru, bukan cuma perbandingan jarak.
    use_ecs = bool(getattr(config, "USE_EMA_CROSS_SPREAD_FILTER", False))
    use_ps = bool(getattr(config, "USE_PRICE_SPREAD_FILTER", False))
    use_spread_trigger = use_ecs and use_ps and bool(config.USE_EMA_CROSS_STRATEGY)

    if use_spread_trigger:
        close = float(curr["close"]) if curr.get("close") else 0.0
        ef = curr.get("ema_fast_x")
        es = curr.get("ema_slow_x")
        if close <= 0 or not pd.notna(ef) or not pd.notna(es):
            return "HOLD"
        ef = float(ef)
        es = float(es)

        # Signed, BUKAN abs() -- tandanya dipakai buat regime arah di bawah.
        ema_diff = ef - es
        price_diff = close - es
        ema_spread_pct = abs(ema_diff) / close * 100
        price_spread_pct = abs(price_diff) / close * 100

        min_ecs = float(getattr(config, "EMA_CROSS_SPREAD_MIN_PCT", 0.0) or 0.0)
        min_ps = float(getattr(config, "PRICE_SPREAD_MIN_PCT", 0.0) or 0.0)
        # Masih di zona "bersentuhan"/menempel → HOLD dulu
        if ema_spread_pct < min_ecs or price_spread_pct < min_ps:
            return "HOLD"

        # Regime arah = sisi cross EMA TERAKHIR, dan harga harus di sisi
        # yang SAMA (bukan sudah menyeberang balik).
        if ema_diff > 0 and price_diff > 0:
            regime = "BUY"
        elif ema_diff < 0 and price_diff < 0:
            regime = "SELL"
        else:
            return "HOLD"

        # Baru fire kalau harga sudah kabur cukup jauh dari EMA lambat
        # dibanding EMA cepat (breakout searah regime) -- toleransi
        # kesamaan (% absolut) supaya tidak fire di beda tipis/noise.
        equal_eps = max(0.005, min(min_ecs, min_ps) * 0.25)
        delta = ema_spread_pct - price_spread_pct
        if delta >= -equal_eps:
            return "HOLD"

        signal = regime
    else:
        # Mode klasik: trigger dari event EMA cross / MACD cross
        if config.USE_EMA_CROSS_STRATEGY:
            crossed_up = prev["ema_fast_x"] <= prev["ema_slow_x"] and curr["ema_fast_x"] > curr["ema_slow_x"]
            crossed_down = prev["ema_fast_x"] >= prev["ema_slow_x"] and curr["ema_fast_x"] < curr["ema_slow_x"]

            if config.CONFIRM_CANDLE:
                if crossed_up and curr["close"] <= curr["open"]:
                    crossed_up = False
                if crossed_down and curr["close"] >= curr["open"]:
                    crossed_down = False
        else:
            crossed_up = prev["macd_line"] <= prev["macd_signal"] and curr["macd_line"] > curr["macd_signal"]
            crossed_down = prev["macd_line"] >= prev["macd_signal"] and curr["macd_line"] < curr["macd_signal"]

        if crossed_up:
            signal = "BUY"
        elif crossed_down:
            signal = "SELL"
        else:
            return "HOLD"

        # Filter spread klasik (threshold saja) -- bukan trigger
        if use_ecs and config.USE_EMA_CROSS_STRATEGY:
            close = float(curr["close"]) if curr.get("close") else 0.0
            ef = curr.get("ema_fast_x")
            es = curr.get("ema_slow_x")
            if close > 0 and pd.notna(ef) and pd.notna(es):
                ema_spread_pct = abs(float(ef) - float(es)) / close * 100
                min_spread = float(getattr(config, "EMA_CROSS_SPREAD_MIN_PCT", 0.0) or 0.0)
                if ema_spread_pct < min_spread:
                    return "HOLD"

        if use_ps:
            close = float(curr["close"]) if curr.get("close") else 0.0
            ef = curr.get("ema_fast_x")
            es = curr.get("ema_slow_x")
            ref = es
            if ref is None or (isinstance(ref, float) and pd.isna(ref)):
                ref = curr.get("ema_trend")
            min_ps = float(getattr(config, "PRICE_SPREAD_MIN_PCT", 0.0) or 0.0)
            if close > 0:
                if config.USE_EMA_CROSS_STRATEGY and pd.notna(ef) and pd.notna(es):
                    lo = min(float(ef), float(es))
                    hi = max(float(ef), float(es))
                    pad = close * (min_ps / 100.0)
                    if (lo - pad) <= close <= (hi + pad):
                        return "HOLD"
                if ref is not None and pd.notna(ref):
                    price_spread_pct = abs(close - float(ref)) / close * 100
                    if price_spread_pct < min_ps:
                        return "HOLD"

    # Filter TREND: sinyal harus searah bias EMA200
    if config.USE_TREND_FILTER:
        trend_val = curr.get("ema_trend")
        if pd.notna(trend_val):
            if signal == "BUY" and curr["close"] < trend_val:
                return "HOLD"
            if signal == "SELL" and curr["close"] > trend_val:
                return "HOLD"

    # Filter SMC (Smart Money Concepts): sinyal harus searah bias struktur
    # market (Break of Structure paling baru) -- TAMBAHAN konfirmasi, BUKAN
    # trigger utama (EMA cross di atas tetap yang menentukan KAPAN entry).
    # Bias 'neutral' (data belum cukup) TIDAK memblokir sinyal -- cuma
    # bias yang JELAS BERLAWANAN yang memblokir.
    #
    # smc_applies: config.SMC_MARKET_SCOPE ("ALL"/"SPOT"/"FUTURES") batasi
    # SEMUA filter SMC di bawah CUMA berlaku buat market_type tertentu --
    # default "ALL" (perilaku lama, berlaku ke semua market, TIDAK ADA
    # perubahan kalau caller tidak kasih market_type/scope masih default).
    smc_scope = getattr(config, "SMC_MARKET_SCOPE", "ALL")
    smc_applies = smc_scope == "ALL" or market_type is None or market_type == smc_scope

    if config.USE_SMC_FILTER and smc_applies:
        bias = get_smc_structure_bias(df, lookback=config.SMC_SWING_LOOKBACK)
        if signal == "BUY" and bias == "bearish":
            return "HOLD"
        if signal == "SELL" and bias == "bullish":
            return "HOLD"

    # Filter SMC Order Block: sinyal cuma dieksekusi kalau harga SEKARANG
    # ada di/dekat zona Order Block yang relevan (demand buat BUY, supply
    # buat SELL) -- konfirmasi entry bukan di sembarang tempat, tapi di
    # zona yang institusional-nya "pernah bertindak". Independen dari
    # filter struktur di atas, bisa aktif salah satu atau keduanya.
    if config.USE_SMC_ORDER_BLOCK_FILTER and smc_applies:
        if not get_smc_order_block_confirmation(
            df, signal, impulse_threshold_pct=config.SMC_OB_IMPULSE_PCT,
            lookback=config.SMC_OB_LOOKBACK, tolerance_pct=config.SMC_OB_TOLERANCE_PCT,
        ):
            return "HOLD"

    # Filter SMC Fair Value Gap: sama konsepnya kayak Order Block, tapi
    # zonanya dari celah/imbalance harga (gap 3-candle), bukan dari candle
    # tunggal sebelum gerakan impulsif.
    if config.USE_SMC_FVG_FILTER and smc_applies:
        if not get_smc_fvg_confirmation(
            df, signal, lookback=config.SMC_FVG_LOOKBACK, tolerance_pct=config.SMC_FVG_TOLERANCE_PCT,
        ):
            return "HOLD"

    # Filter ADX: abaikan sinyal kalau market tidak cukup trending
    if config.USE_ADX_FILTER:
        adx_val = curr.get("adx")
        if pd.notna(adx_val) and adx_val < _mp(market_type, "ADX_MIN_TREND"):
            return "HOLD"

    # Filter RSI: hindari entry di zona jenuh (overbought/oversold)
    if config.USE_RSI_FILTER:
        rsi_val = curr.get("rsi")
        if pd.notna(rsi_val):
            if signal == "BUY" and rsi_val > _mp(market_type, "RSI_OVERBOUGHT"):
                return "HOLD"
            if signal == "SELL" and rsi_val < _mp(market_type, "RSI_OVERSOLD"):
                return "HOLD"

    # Filter SMII (SMI Ergodic Indicator): sinyal harus SEARAH momentum
    # SMI relatif ke garis sinyalnya SENDIRI (SMI > signal = momentum
    # bullish, SMI < signal = bearish) -- mirip konsep cross MACD, tapi
    # dari indikator yang di-smooth dua kali (lebih halus dari RSI biasa).
    #
    # smii_applies: config.SMII_MARKET_SCOPE ("ALL"/"SPOT"/"FUTURES") batasi
    # filter ini CUMA berlaku buat market_type tertentu -- default "ALL"
    # (berlaku ke semua, TIDAK ADA perubahan kalau caller tidak kasih
    # market_type/scope masih default). Sama pola dengan smc_applies di atas.
    smii_scope = getattr(config, "SMII_MARKET_SCOPE", "ALL")
    smii_applies = smii_scope == "ALL" or market_type is None or market_type == smii_scope

    if config.USE_SMII_FILTER and smii_applies:
        smi_val = curr.get("smi")
        smi_signal_val = curr.get("smi_signal")
        if pd.notna(smi_val) and pd.notna(smi_signal_val):
            if signal == "BUY" and smi_val < smi_signal_val:
                return "HOLD"
            if signal == "SELL" and smi_val > smi_signal_val:
                return "HOLD"

    # Filter DPO (Detrended Price Oscillator): sinyal cuma lolos kalau DPO
    # ada di sisi yang SEARAH (positif = overbought jangka pendek buat
    # konfirmasi BUY, negatif = oversold buat konfirmasi SELL). Dihitung
    # dalam % dari harga (config.DPO_MIN_PCT) biar konsisten antar pair,
    # bukan angka absolut.
    #
    # dpo_applies: config.DPO_MARKET_SCOPE, sama polanya dengan SMII/SMC
    # di atas -- default "ALL".
    dpo_scope = getattr(config, "DPO_MARKET_SCOPE", "ALL")
    dpo_applies = dpo_scope == "ALL" or market_type is None or market_type == dpo_scope

    if config.USE_DPO_FILTER and dpo_applies:
        dpo_val = curr.get("dpo")
        dpo_min_pct = _mp(market_type, "DPO_MIN_PCT")
        if pd.notna(dpo_val) and curr["close"]:
            dpo_pct = dpo_val / curr["close"] * 100
            if signal == "BUY" and dpo_pct < dpo_min_pct:
                return "HOLD"
            if signal == "SELL" and dpo_pct > -dpo_min_pct:
                return "HOLD"

    # Filter VOLUME: hindari entry di volume rendah (rawan fakeout)
    if config.USE_VOLUME_FILTER:
        vol_ma = curr.get("volume_ma")
        if pd.notna(vol_ma) and vol_ma > 0:
            if curr["volume"] < vol_ma * _mp(market_type, "VOLUME_MIN_RATIO"):
                return "HOLD"

    # Filter VOLATILITAS: skip kalau market terlalu sepi (ATR% di bawah
    # MIN_ATR_PCT, rawan sinyal palsu/tidak ada momentum) ATAU terlalu liar
    # (ATR% di atas MAX_ATR_PCT, rawan whipsaw/SL kena sebelum sempat profit).
    if config.USE_VOLATILITY_FILTER:
        atr_val = curr.get("atr")
        if pd.notna(atr_val) and curr["close"] > 0:
            atr_pct = (atr_val / curr["close"]) * 100
            if atr_pct < _mp(market_type, "MIN_ATR_PCT") or atr_pct > _mp(market_type, "MAX_ATR_PCT"):
                return "HOLD"

    return signal


def get_signal_detail(df: pd.DataFrame, market_type: str = None) -> dict:
    """Evaluasi SEMUA filter satu per satu dan return status tiap filter
    -- dipakai dashboard buat tampilkan indikator mana yang LOLOS/GAGAL
    di candle terakhir. Tidak mengubah perilaku trading sama sekali.

    Return dict: {
        "signal": "BUY"/"SELL"/"HOLD",
        "filters": [
            {"key": "EMA_CROSS", "label": "EMA Cross", "status": "pass"/"fail"/"skip"/"inactive",
             "value": "...", "enabled": True/False},
            ...
        ]
    }
    status:
        "pass"     -- filter aktif DAN kondisi terpenuhi
        "fail"     -- filter aktif DAN kondisi TIDAK terpenuhi (blokir sinyal)
        "skip"     -- filter aktif tapi data tidak cukup / tidak relevan
        "inactive" -- filter dimatikan di config
    """
    if len(df) < 2:
        return {"signal": "HOLD", "filters": []}

    df_ind = compute_indicators(df, market_type=market_type)
    prev = df_ind.iloc[-2]
    curr = df_ind.iloc[-1]
    filters = []

    def _add(key, label, enabled, status, value="", scope_skip=False):
        # scope_skip=True -- filter ini "skip" BUKAN karena data belum
        # cukup, tapi karena market_type pair yang dievaluasi memang di
        # luar *_MARKET_SCOPE-nya (SMII/DPO/SMC). Dashboard pakai flag ini
        # buat SEMBUNYIKAN kartu filter ini sama sekali dari panel "Status
        # Filter Candle Terakhir" (bukan cuma ditandai skip abu-abu) --
        # konsisten dengan overlay chart yang juga disembunyikan untuk
        # kasus yang sama.
        filters.append({"key": key, "label": label, "enabled": enabled,
                        "status": status if enabled else "inactive", "value": value,
                        "scope_skip": bool(scope_skip)})

    # ---- TRIGGER ----
    # Mode spread (kedua filter ON): EMA hanya acuan jarak; sinyal dari
    # perbandingan ema_spread vs price_spread. Mode klasik: EMA/MACD cross.
    use_ecs = bool(getattr(config, "USE_EMA_CROSS_SPREAD_FILTER", False))
    use_ps = bool(getattr(config, "USE_PRICE_SPREAD_FILTER", False))
    use_spread_trigger = use_ecs and use_ps and bool(config.USE_EMA_CROSS_STRATEGY)

    close = float(curr["close"]) if curr.get("close") else 0.0
    ef, es = curr.get("ema_fast_x"), curr.get("ema_slow_x")
    ecs_pct = None
    ps_pct = None
    ema_diff_signed = None
    price_diff_signed = None
    if close > 0 and pd.notna(ef) and pd.notna(es):
        ema_diff_signed = float(ef) - float(es)
        price_diff_signed = close - float(es)
        ecs_pct = abs(ema_diff_signed) / close * 100
        ps_pct = abs(price_diff_signed) / close * 100

    signal = "HOLD"
    has_cross = False
    crossed_up = crossed_down = False

    if use_spread_trigger:
        # Sama persis dengan generate_signal(): arah = sisi cross EMA
        # TERAKHIR (tanda ema_diff_signed), bukan cuma mana yang lebih
        # besar antar dua abs(). Lihat komentar detail di generate_signal().
        cross_dir = "BUY" if (ema_diff_signed is not None and ema_diff_signed > 0) else \
                    ("SELL" if (ema_diff_signed is not None and ema_diff_signed < 0) else "—")
        _add("EMA_CROSS", f"EMA {_mp(market_type, 'EMA_FAST_LEN')}/{_mp(market_type, 'EMA_SLOW_LEN')} (regime dari cross terakhir)",
             True, "pass", f"cross terakhir: {cross_dir}")
        _add("CONFIRM_CANDLE", "Konfirmasi Arah Candle", False, "inactive",
             "tidak dipakai di mode spread-trigger")
        if ecs_pct is None or ps_pct is None:
            signal = "HOLD"
        else:
            min_ecs = float(getattr(config, "EMA_CROSS_SPREAD_MIN_PCT", 0.0) or 0.0)
            min_ps = float(getattr(config, "PRICE_SPREAD_MIN_PCT", 0.0) or 0.0)
            equal_eps = max(0.005, min(min_ecs, min_ps) * 0.25)
            if ecs_pct < min_ecs or ps_pct < min_ps:
                signal = "HOLD"
            elif ema_diff_signed > 0 and price_diff_signed > 0 and (ecs_pct - ps_pct) < -equal_eps:
                signal = "BUY"
            elif ema_diff_signed < 0 and price_diff_signed < 0 and (ecs_pct - ps_pct) < -equal_eps:
                signal = "SELL"
            else:
                signal = "HOLD"
    elif config.USE_EMA_CROSS_STRATEGY:
        crossed_up = prev["ema_fast_x"] <= prev["ema_slow_x"] and curr["ema_fast_x"] > curr["ema_slow_x"]
        crossed_down = prev["ema_fast_x"] >= prev["ema_slow_x"] and curr["ema_fast_x"] < curr["ema_slow_x"]
        has_cross = crossed_up or crossed_down
        cross_dir = "BUY" if crossed_up else ("SELL" if crossed_down else "—")
        _add("EMA_CROSS", f"EMA {_mp(market_type, 'EMA_FAST_LEN')}/{_mp(market_type, 'EMA_SLOW_LEN')} Cross",
             True, "pass" if has_cross else "fail", cross_dir)
        signal = "BUY" if crossed_up else ("SELL" if crossed_down else "HOLD")
        if not config.USE_EMA_CROSS_STRATEGY:
            pass
        elif has_cross and config.CONFIRM_CANDLE:
            candle_ok = ((signal == "BUY" and curr["close"] > curr["open"]) or
                         (signal == "SELL" and curr["close"] < curr["open"]))
            _add("CONFIRM_CANDLE", "Konfirmasi Arah Candle", True,
                 "pass" if candle_ok else "fail",
                 "Hijau" if curr["close"] > curr["open"] else "Merah")
            if not candle_ok:
                signal = "HOLD"
        elif has_cross:
            _add("CONFIRM_CANDLE", "Konfirmasi Arah Candle", False, "inactive")
        else:
            _add("CONFIRM_CANDLE", "Konfirmasi Arah Candle", config.CONFIRM_CANDLE, "skip")
    else:
        _mf = _mp(market_type, "MACD_FAST")
        _ms = _mp(market_type, "MACD_SLOW")
        _msig = _mp(market_type, "MACD_SIGNAL")
        crossed_up = prev["macd_line"] <= prev["macd_signal"] and curr["macd_line"] > curr["macd_signal"]
        crossed_down = prev["macd_line"] >= prev["macd_signal"] and curr["macd_line"] < curr["macd_signal"]
        has_cross = crossed_up or crossed_down
        cross_dir = "BUY" if crossed_up else ("SELL" if crossed_down else "—")
        _add("MACD_CROSS", f"MACD {_mf}/{_ms}/{_msig} Cross",
             True, "pass" if has_cross else "fail", cross_dir)
        signal = "BUY" if crossed_up else ("SELL" if crossed_down else "HOLD")
        _add("CONFIRM_CANDLE", "Konfirmasi Arah Candle", False, "inactive",
             "tidak berlaku untuk trigger MACD")

    # ---- EMA CROSS SPREAD / PRICE SPREAD ----
    min_ecs = float(getattr(config, "EMA_CROSS_SPREAD_MIN_PCT", 0.0) or 0.0)
    min_ps = float(getattr(config, "PRICE_SPREAD_MIN_PCT", 0.0) or 0.0)
    if use_ecs and config.USE_EMA_CROSS_STRATEGY:
        if ecs_pct is not None:
            ecs_ok = ecs_pct >= min_ecs
            _add("EMA_CROSS_SPREAD", "EMA Cross Spread", True,
                 "pass" if ecs_ok else "fail",
                 f"{ecs_pct:.3f}% (min {min_ecs}%)")
            if not use_spread_trigger and not ecs_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("EMA_CROSS_SPREAD", "EMA Cross Spread", True, "skip", "data kurang")
    else:
        _add("EMA_CROSS_SPREAD", "EMA Cross Spread", False, "inactive")

    if use_ps:
        if ps_pct is not None:
            ps_ok = ps_pct >= min_ps
            _add("PRICE_SPREAD", "Price Spread", True,
                 "pass" if ps_ok else "fail",
                 f"{ps_pct:.3f}% (min {min_ps}%)")
            if not use_spread_trigger and not ps_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("PRICE_SPREAD", "Price Spread", True, "skip", "data kurang")
    else:
        _add("PRICE_SPREAD", "Price Spread", False, "inactive")

    if use_spread_trigger and ecs_pct is not None and ps_pct is not None:
        equal_eps = max(0.005, min(min_ecs, min_ps) * 0.25)
        if abs(ecs_pct - ps_pct) <= equal_eps:
            cmp = "="
        elif ecs_pct < ps_pct:
            cmp = "<"
        else:
            cmp = ">"
        _add("SPREAD_COMPARE", "EMA Spread vs Price Spread", True,
             "pass" if signal in ("BUY", "SELL") else "fail",
             f"EMA {ecs_pct:.3f}% {cmp} Price {ps_pct:.3f}% → {signal}")


    # ---- TREND FILTER ----
    if config.USE_TREND_FILTER:
        tv = curr.get("ema_trend")
        if pd.notna(tv):
            trend_ok = (signal == "BUY" and curr["close"] >= tv) or \
                       (signal == "SELL" and curr["close"] <= tv) or signal == "HOLD"
            _add("TREND", f"Filter Tren (EMA {_mp(market_type, 'EMA_TREND')})", True,
                 "pass" if trend_ok else "fail",
                 f"{'di atas' if curr['close'] >= tv else 'di bawah'} EMA Tren")
            if not trend_ok:
                signal = "HOLD"
        else:
            _add("TREND", f"Filter Tren (EMA {_mp(market_type, 'EMA_TREND')})", True, "skip", "data kurang")
    else:
        _add("TREND", f"Filter Tren (EMA {_mp(market_type, 'EMA_TREND')})", False, "inactive")

    # ---- ADX ----
    if config.USE_ADX_FILTER:
        adx = curr.get("adx")
        adx_min_trend = _mp(market_type, "ADX_MIN_TREND")
        if pd.notna(adx):
            adx_ok = adx >= adx_min_trend
            _add("ADX", "Filter ADX", True,
                 "pass" if adx_ok else "fail",
                 f"{adx:.1f} (min {adx_min_trend})")
            if not adx_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("ADX", "Filter ADX", True, "skip")
    else:
        _add("ADX", "Filter ADX", False, "inactive")

    # ---- RSI ----
    if config.USE_RSI_FILTER:
        rsi = curr.get("rsi")
        rsi_oversold = _mp(market_type, "RSI_OVERSOLD")
        rsi_overbought = _mp(market_type, "RSI_OVERBOUGHT")
        if pd.notna(rsi):
            rsi_ok = rsi_oversold <= rsi <= rsi_overbought
            _add("RSI", "Filter RSI", True,
                 "pass" if rsi_ok else "fail",
                 f"{rsi:.1f} ({rsi_oversold}–{rsi_overbought})")
            if not rsi_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("RSI", "Filter RSI", True, "skip")
    else:
        _add("RSI", "Filter RSI", False, "inactive")

    # ---- SMII (scope) ----
    smii_scope = getattr(config, "SMII_MARKET_SCOPE", "ALL")
    smii_applies = smii_scope == "ALL" or market_type is None or market_type == smii_scope
    smii_label = f"Filter SMII (scope: {smii_scope})"

    if config.USE_SMII_FILTER:
        if smii_applies:
            smi = curr.get("smi"); smi_sig = curr.get("smi_signal")
            if pd.notna(smi) and pd.notna(smi_sig):
                smi_ok = (signal == "BUY" and smi >= smi_sig) or \
                         (signal == "SELL" and smi <= smi_sig) or signal == "HOLD"
                _add("SMII", smii_label, True,
                     "pass" if smi_ok else "fail",
                     f"SMI {smi:.2f} / Sig {smi_sig:.2f}")
                if not smi_ok:
                    signal = "HOLD"
            else:
                _add("SMII", smii_label, True, "skip")
        else:
            _add("SMII", smii_label, True, "skip", f"tidak berlaku di {market_type}", scope_skip=True)
    else:
        _add("SMII", smii_label, False, "inactive")

    # ---- DPO (scope) ----
    dpo_scope = getattr(config, "DPO_MARKET_SCOPE", "ALL")
    dpo_applies = dpo_scope == "ALL" or market_type is None or market_type == dpo_scope
    dpo_label = f"Filter DPO (scope: {dpo_scope})"

    if config.USE_DPO_FILTER:
        if dpo_applies:
            dpo = curr.get("dpo")
            dpo_min_pct = _mp(market_type, "DPO_MIN_PCT")
            if pd.notna(dpo) and curr["close"]:
                dpo_pct = dpo / curr["close"] * 100
                dpo_ok = (signal == "BUY" and dpo_pct >= dpo_min_pct) or \
                         (signal == "SELL" and dpo_pct <= -dpo_min_pct) or signal == "HOLD"
                _add("DPO", dpo_label, True,
                     "pass" if dpo_ok else "fail",
                     f"{dpo_pct:+.3f}% (min ±{dpo_min_pct}%)")
                if not dpo_ok:
                    signal = "HOLD"
            else:
                _add("DPO", dpo_label, True, "skip")
        else:
            _add("DPO", dpo_label, True, "skip", f"tidak berlaku di {market_type}", scope_skip=True)
    else:
        _add("DPO", dpo_label, False, "inactive")

    # ---- SMC (scope) ----
    smc_scope = getattr(config, "SMC_MARKET_SCOPE", "ALL")
    smc_applies = smc_scope == "ALL" or market_type is None or market_type == smc_scope
    smc_label = f"(scope: {smc_scope})"

    if config.USE_SMC_FILTER:
        if smc_applies:
            bias = get_smc_structure_bias(df, lookback=config.SMC_SWING_LOOKBACK)
            smc_ok = not (signal == "BUY" and bias == "bearish") and \
                     not (signal == "SELL" and bias == "bullish")
            _add("SMC", f"SMC Struktur {smc_label}", True,
                 "pass" if smc_ok else "fail", f"Bias: {bias}")
            if not smc_ok:
                signal = "HOLD"
        else:
            _add("SMC", f"SMC Struktur {smc_label}", True, "skip",
                 f"tidak berlaku di {market_type}", scope_skip=True)
    else:
        _add("SMC", f"SMC Struktur {smc_label}", False, "inactive")

    # ---- VOLUME ----
    if config.USE_VOLUME_FILTER:
        vol_ma = curr.get("volume_ma")
        vol_min_ratio = _mp(market_type, "VOLUME_MIN_RATIO")
        if pd.notna(vol_ma) and vol_ma > 0:
            vol_ok = curr["volume"] >= vol_ma * vol_min_ratio
            _add("VOLUME", "Filter Volume", True,
                 "pass" if vol_ok else "fail",
                 f"{curr['volume']:.0f} (min {vol_ma * vol_min_ratio:.0f})")
            if not vol_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("VOLUME", "Filter Volume", True, "skip")
    else:
        _add("VOLUME", "Filter Volume", False, "inactive")

    # ---- VOLATILITAS ----
    if config.USE_VOLATILITY_FILTER:
        atr = curr.get("atr")
        min_atr_pct = _mp(market_type, "MIN_ATR_PCT")
        max_atr_pct = _mp(market_type, "MAX_ATR_PCT")
        if pd.notna(atr) and curr["close"] > 0:
            atr_pct = atr / curr["close"] * 100
            vol_ok = min_atr_pct <= atr_pct <= max_atr_pct
            _add("VOLATILITY", "Filter Volatilitas (ATR%)", True,
                 "pass" if vol_ok else "fail",
                 f"{atr_pct:.3f}% ({min_atr_pct}–{max_atr_pct}%)")
            if not vol_ok and signal != "HOLD":
                signal = "HOLD"
        else:
            _add("VOLATILITY", "Filter Volatilitas (ATR%)", True, "skip")
    else:
        _add("VOLATILITY", "Filter Volatilitas (ATR%)", False, "inactive")

    return {"signal": signal, "filters": filters}
