"""
strategy_schema.py
STRATEGY_SETTINGS_SCHEMA -- definisi tunggal yang dipakai BERSAMA oleh
bot.py (whitelist /set, menu strategi Telegram, panel dashboard) DAN
bot_launcher.py (menu strategi di launcher). Dipisah ke file sendiri
supaya launcher bisa import tanpa circular import ke bot.py.

Format tiap entry: KEY -> (python_type, label_tampilan, grup)
Grup: "filter", "filter_param", "risk"
"""

STRATEGY_SETTINGS_SCHEMA = {
    # --- Filter konfirmasi (ON/OFF) ---
    "USE_EMA_CROSS_STRATEGY": (bool, "Trigger: EMA Cross (ON) / MACD Cross (OFF)", "filter"),
    "USE_TREND_FILTER": (bool, "Filter Tren (EMA 50)", "filter"),
    "USE_RSI_FILTER": (bool, "Filter RSI", "filter"),
    "USE_ADX_FILTER": (bool, "Filter ADX", "filter"),
    "USE_VOLUME_FILTER": (bool, "Filter Volume", "filter"),
    "USE_VOLATILITY_FILTER": (bool, "Filter Volatilitas (ATR%)", "filter"),
    "USE_SMC_FILTER": (bool, "Filter SMC Struktur/BOS", "filter"),
    "USE_SMC_ORDER_BLOCK_FILTER": (bool, "Filter SMC Order Block", "filter"),
    "USE_SMC_FVG_FILTER": (bool, "Filter SMC Fair Value Gap", "filter"),
    "USE_SMII_FILTER": (bool, "Filter SMII (SMI Ergodic)", "filter"),
    "USE_DPO_FILTER": (bool, "Filter DPO", "filter"),
    "CONFIRM_CANDLE": (bool, "Konfirmasi Arah Candle", "filter"),
    "USE_EMA_CROSS_SPREAD_FILTER": (bool, "Filter EMA Cross Spread (bersentuhan=HOLD)", "filter"),
    "USE_PRICE_SPREAD_FILTER": (bool, "Filter Price Spread (bersentuhan=HOLD)", "filter"),

    # --- Parameter numerik filter ---
    # Spot dan Futures dipisah untuk sebagian besar parameter di bawah
    # (kecuali EMA_CROSS_SPREAD_MIN_PCT/PRICE_SPREAD_MIN_PCT/SMC_* yang
    # TETAP SATU nilai global) -- alasannya candle Spot (1h) dan Futures
    # (5m) punya karakter pergerakan beda jauh, sama seperti MACD_FAST/
    # SLOW/SIGNAL vs MACD_FAST_FUTURES/dst yang sudah lebih dulu dipisah.
    # Lihat strategy._mp() untuk cara resolusinya.
    "EMA_FAST_LEN": (int, "EMA Cross Spot -- Fast", "filter_param"),
    "EMA_SLOW_LEN": (int, "EMA Cross Spot -- Slow", "filter_param"),
    "EMA_FAST_LEN_FUTURES": (int, "EMA Cross Futures -- Fast", "filter_param"),
    "EMA_SLOW_LEN_FUTURES": (int, "EMA Cross Futures -- Slow", "filter_param"),
    "EMA_TREND": (int, "Panjang EMA Tren (Spot)", "filter_param"),
    "EMA_TREND_FUTURES": (int, "Panjang EMA Tren (Futures)", "filter_param"),
    "EMA_CROSS_SPREAD_MIN_PCT": (float, "Min jarak antar EMA (%) — di bawah = bersentuhan", "filter_param"),
    "REVERSE_EMA_SPREAD_MIN_PCT": (float, "Min Jarak EMA Khusus Reverse (%)", "risk"),
    "PRICE_SPREAD_MIN_PCT": (float, "Min jarak harga ke EMA (%) — di bawah = menempel", "filter_param"),
    "RSI_PERIOD": (int, "Periode RSI (Spot)", "filter_param"),
    "RSI_OVERBOUGHT": (float, "RSI Overbought (Spot)", "filter_param"),
    "RSI_OVERSOLD": (float, "RSI Oversold (Spot)", "filter_param"),
    "RSI_PERIOD_FUTURES": (int, "Periode RSI (Futures)", "filter_param"),
    "RSI_OVERBOUGHT_FUTURES": (float, "RSI Overbought (Futures)", "filter_param"),
    "RSI_OVERSOLD_FUTURES": (float, "RSI Oversold (Futures)", "filter_param"),
    "ADX_PERIOD": (int, "Periode ADX (Spot)", "filter_param"),
    "ADX_MIN_TREND": (float, "ADX Minimal (Spot, trending)", "filter_param"),
    "ADX_PERIOD_FUTURES": (int, "Periode ADX (Futures)", "filter_param"),
    "ADX_MIN_TREND_FUTURES": (float, "ADX Minimal (Futures, trending)", "filter_param"),
    "MIN_ATR_PCT": (float, "ATR% Minimal (Spot)", "filter_param"),
    "MAX_ATR_PCT": (float, "ATR% Maksimal (Spot)", "filter_param"),
    "MIN_ATR_PCT_FUTURES": (float, "ATR% Minimal (Futures)", "filter_param"),
    "MAX_ATR_PCT_FUTURES": (float, "ATR% Maksimal (Futures)", "filter_param"),
    "VOLUME_MIN_RATIO": (float, "Rasio Volume Minimal (Spot)", "filter_param"),
    "VOLUME_MIN_RATIO_FUTURES": (float, "Rasio Volume Minimal (Futures)", "filter_param"),
    "SMII_LONG_LENGTH": (int, "SMII Panjang Pembelian (Spot)", "filter_param"),
    "SMII_SHORT_LENGTH": (int, "SMII Panjang Penjualan (Spot)", "filter_param"),
    "SMII_SIGNAL_LENGTH": (int, "SMII Panjang Sinyal (Spot)", "filter_param"),
    "SMII_LONG_LENGTH_FUTURES": (int, "SMII Panjang Pembelian (Futures)", "filter_param"),
    "SMII_SHORT_LENGTH_FUTURES": (int, "SMII Panjang Penjualan (Futures)", "filter_param"),
    "SMII_SIGNAL_LENGTH_FUTURES": (int, "SMII Panjang Sinyal (Futures)", "filter_param"),
    "DPO_PERIOD": (int, "Periode DPO (Spot)", "filter_param"),
    "DPO_MIN_PCT": (float, "DPO Minimal (%) (Spot)", "filter_param"),
    "DPO_PERIOD_FUTURES": (int, "Periode DPO (Futures)", "filter_param"),
    "DPO_MIN_PCT_FUTURES": (float, "DPO Minimal (%) (Futures)", "filter_param"),
    "SMC_SWING_LOOKBACK": (int, "SMC Swing Lookback", "filter_param"),
    "SMC_OB_LOOKBACK": (int, "SMC Order Block Lookback (candle)", "filter_param"),
    "SMC_OB_TOLERANCE_PCT": (float, "SMC Order Block Toleransi (%)", "filter_param"),
    "SMC_FVG_LOOKBACK": (int, "SMC FVG Lookback (candle)", "filter_param"),
    "SMC_FVG_TOLERANCE_PCT": (float, "SMC FVG Toleransi (%)", "filter_param"),

    # --- MACD (trigger sinyal utama kalau USE_EMA_CROSS_STRATEGY=False) ---
    # Spot dan Futures dipisah karena config.py memang punya periode beda
    # untuk masing-masing (lihat strategy.py get_trigger_indicator()).
    "MACD_FAST": (int, "MACD Spot -- Fast", "filter_param"),
    "MACD_SLOW": (int, "MACD Spot -- Slow", "filter_param"),
    "MACD_SIGNAL": (int, "MACD Spot -- Signal", "filter_param"),
    "MACD_FAST_FUTURES": (int, "MACD Futures -- Fast", "filter_param"),
    "MACD_SLOW_FUTURES": (int, "MACD Futures -- Slow", "filter_param"),
    "MACD_SIGNAL_FUTURES": (int, "MACD Futures -- Signal", "filter_param"),

    # --- Manajemen posisi & risiko ---
    "USE_EARLY_REVERSAL": (bool, "Reversal Dini", "risk"),
    "EMA_REVERSAL_SPREAD_MIN_PCT": (float, "Jarak Minimal Reversal (%)", "risk"),
    "USE_ENTRY_ON_STARTUP": (bool, "Entry Otomatis Saat Startup", "risk"),
    "USE_TREND_EXIT_FINAL_TP": (bool, "TP Terakhir Ikuti Tren", "risk"),
    "USE_BREAK_EVEN": (bool, "Break-Even", "risk"),
    "USE_TRAILING_STOP": (bool, "Trailing Stop", "risk"),
    "USE_MULTI_TP": (bool, "TP Bertingkat", "risk"),
    "USE_TRADE_HOURS": (bool, "Batasi Jam Trading", "risk"),
    "USE_EQUITY_GUARD": (bool, "Equity Guard", "risk"),
    "USE_SPOT_SLTP": (bool, "Spot Pakai SL/TP (OFF=Buy&Hold)", "risk"),
    "RISK_REWARD": (float, "Risk:Reward Ratio", "risk"),
    "ATR_MULT_SL": (float, "SL = ATR x", "risk"),
    "RISK_PER_TRADE_PCT": (float, "Risiko per Trade (%)", "risk"),
    "MAX_POSITION_PCT_SPOT": (float, "Maks Posisi Spot (% saldo)", "risk"),
    "MAX_POSITION_PCT_FUTURES": (float, "Maks Posisi Futures (% saldo)", "risk"),
    "COOLDOWN_AFTER_SL_MIN": (int, "Cooldown Setelah SL (menit)", "risk"),
    "MAX_DRAWDOWN_PCT": (float, "Batas Drawdown Equity Guard (%)", "risk"),
    "USE_PAIR_RESHUFFLE": (bool, "Reshuffle Pair Rugi saat SL Kena (scanner)", "risk"),
    "USE_TP_STALL_RESHUFFLE": (bool, "Reshuffle Pair Macet (umur posisi)", "risk"),
    "TP_STALL_RESHUFFLE_HOURS": (float, "Batas Umur Posisi Macet (jam)", "risk"),
    "SCANNER_MAX_ACTIVE_PAIRS_OKX": (int, "Slot Aktif Scanner (OKX Futures)", "risk"),
}

# Step per tap +/- di editor angka Telegram
PARAM_STEPS = {
    "EMA_FAST_LEN": 1, "EMA_SLOW_LEN": 1,
    "EMA_FAST_LEN_FUTURES": 1, "EMA_SLOW_LEN_FUTURES": 1,
    "EMA_TREND": 1, "EMA_TREND_FUTURES": 1,
    "RSI_PERIOD": 1, "RSI_OVERBOUGHT": 1.0, "RSI_OVERSOLD": 1.0,
    "RSI_PERIOD_FUTURES": 1, "RSI_OVERBOUGHT_FUTURES": 1.0, "RSI_OVERSOLD_FUTURES": 1.0,
    "ADX_PERIOD": 1, "ADX_MIN_TREND": 1.0,
    "ADX_PERIOD_FUTURES": 1, "ADX_MIN_TREND_FUTURES": 1.0,
    "MIN_ATR_PCT": 0.01, "MAX_ATR_PCT": 0.1,
    "MIN_ATR_PCT_FUTURES": 0.01, "MAX_ATR_PCT_FUTURES": 0.1,
    "VOLUME_MIN_RATIO": 0.1, "VOLUME_MIN_RATIO_FUTURES": 0.1,
    "SMII_LONG_LENGTH": 1, "SMII_SHORT_LENGTH": 1, "SMII_SIGNAL_LENGTH": 1,
    "SMII_LONG_LENGTH_FUTURES": 1, "SMII_SHORT_LENGTH_FUTURES": 1, "SMII_SIGNAL_LENGTH_FUTURES": 1,
    "DPO_PERIOD": 1, "DPO_MIN_PCT": 0.01,
    "DPO_PERIOD_FUTURES": 1, "DPO_MIN_PCT_FUTURES": 0.01,
    "SMC_SWING_LOOKBACK": 1, "SMC_OB_LOOKBACK": 5, "SMC_OB_TOLERANCE_PCT": 0.05,
    "SMC_FVG_LOOKBACK": 5, "SMC_FVG_TOLERANCE_PCT": 0.05, "RISK_REWARD": 0.25, "ATR_MULT_SL": 0.1,
    "RISK_PER_TRADE_PCT": 0.01, "MAX_POSITION_PCT_SPOT": 1.0, "MAX_POSITION_PCT_FUTURES": 5.0,
    "COOLDOWN_AFTER_SL_MIN": 5, "MAX_DRAWDOWN_PCT": 1.0,
    "EMA_REVERSAL_SPREAD_MIN_PCT": 0.05,
    "TP_STALL_RESHUFFLE_HOURS": 0.5,
    "SCANNER_MAX_ACTIVE_PAIRS_OKX": 1,
    "EMA_CROSS_SPREAD_MIN_PCT": 0.01,
    "REVERSE_EMA_SPREAD_MIN_PCT": 0.01,
    "PRICE_SPREAD_MIN_PCT": 0.01,
    "MACD_FAST": 1, "MACD_SLOW": 1, "MACD_SIGNAL": 1,
    "MACD_FAST_FUTURES": 1, "MACD_SLOW_FUTURES": 1, "MACD_SIGNAL_FUTURES": 1,
}
