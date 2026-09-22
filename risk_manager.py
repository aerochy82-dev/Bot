"""
Modul manajemen risiko.
Menentukan ukuran posisi dan level stop-loss / take-profit.
Mendukung posisi LONG maupun SHORT (SHORT hanya relevan untuk Futures).

Dua mode SL/TP:
- Persentase tetap (default): STOP_LOSS_PCT/TAKE_PROFIT_PCT dari config.py.
- Berbasis ATR (USE_ATR_RISK=True): SL diletakkan di low/high candle sinyal
  +/- ATR*ATR_MULT_SL, TP = jarak_SL x RISK_REWARD. Position sizing otomatis
  menyesuaikan ke jarak SL SEBENARNYA (bukan STOP_LOSS_PCT statis) supaya
  risiko per-trade tetap konsisten walau jarak SL berubah-ubah tiap sinyal.
"""

import config


def _round_price(price: float) -> float:
    """Bulatkan harga berbasis JUMLAH ANGKA PENTING (significant figures),
    BUKAN jumlah desimal tetap seperti sebelumnya (8 desimal tetap, semua
    harga dipotong ke 8 angka di belakang koma tanpa peduli magnitude-nya).

    FIX 21 Sept 2026: pembulatan 8-desimal-tetap runtuh total untuk token
    berharga sangat kecil (misal SATS-USDT-SWAP di kisaran 0.000000011 =
    1.1e-08). Pada harga sekecil itu, 8-desimal-tetap cuma menyisakan satu
    digit signifikan (kadang malah membulatkan ke 0.00000001 utk banyak
    harga berbeda sekaligus) -- akibatnya SL dan TP yang seharusnya
    berbeda malah collapse jadi angka SAMA PERSIS, atau urutannya jadi
    terbalik (TP di bawah entry utk posisi LONG, dst). Ini pernah kejadian
    nyata: SATS-USDT-SWAP partial_tp_hit dengan profit yang mencurigakan
    kecil/instan karena SL/TP sudah salah hitung dari awal.

    Solusi: bulatkan ke N angka penting (default 10) dihitung dari
    magnitude harga itu sendiri, bukan dari posisi titik desimal. Untuk
    harga "normal" (misal 43210.123456 atau 1.2345), hasilnya PRAKTIS SAMA
    seperti pembulatan 8-desimal-tetap sebelumnya (malah kadang sedikit
    lebih presisi) -- jadi tidak mengubah perilaku utk mayoritas pair yang
    selama ini sudah benar. Untuk harga ultra-kecil, presisi tetap
    terjaga (10 angka penting) sehingga SL/TP tidak pernah collapse jadi
    nilai yang sama.
    """
    if price is None:
        return price
    price = float(price)
    if price == 0:
        return 0.0
    sig_figs = 10
    import math
    magnitude = math.floor(math.log10(abs(price)))
    decimals = sig_figs - 1 - magnitude
    # Jaga-jaga: jangan sampai decimals negatif ekstrem/positif ekstrem
    # menyebabkan round() error pada harga di luar rentang wajar crypto.
    decimals = max(-10, min(decimals, 18))
    return round(price, decimals)


def calculate_position_size(balance_quote: float, entry_price: float, stop_loss_pct: float = None,
                             market_type: str = "SPOT") -> float:
    """
    Hitung jumlah aset (base asset, misal BTC) yang dibeli/dijual
    berdasarkan % risiko per trade dan batas maksimum posisi.

    stop_loss_pct: kalau None, pakai config.STOP_LOSS_PCT (mode persentase
    tetap). Diisi eksplisit oleh caller saat USE_ATR_RISK=True, berisi jarak
    SL SEBENARNYA (dalam %) dari entry -- supaya sizing tetap konsisten
    dengan risiko nyata, bukan asumsi statis yang sudah tidak berlaku.

    market_type: "SPOT" atau "FUTURES" -- menentukan plafon darurat mana
    yang dipakai (config.MAX_POSITION_PCT_SPOT vs MAX_POSITION_PCT_FUTURES),
    karena keduanya sekarang beda jauh (Spot lebih konservatif, Futures
    jauh lebih longgar) alih-alih satu MAX_POSITION_PCT yang sama untuk
    keduanya seperti sebelumnya. RISK_PER_TRADE_PCT (dasar sizing berbasis
    jarak SL) TETAP SAMA untuk kedua market, tidak dipisah.
    """
    if stop_loss_pct is None:
        stop_loss_pct = config.STOP_LOSS_PCT
    if stop_loss_pct <= 0:
        stop_loss_pct = config.STOP_LOSS_PCT  # jaga-jaga kalau ATR menghasilkan jarak 0

    max_position_pct = (
        config.MAX_POSITION_PCT_FUTURES if market_type == "FUTURES" else config.MAX_POSITION_PCT_SPOT
    )
    risk_amount = balance_quote * (config.RISK_PER_TRADE_PCT / 100)
    max_amount = balance_quote * (max_position_pct / 100)
    quote_to_use = min(risk_amount / (stop_loss_pct / 100), max_amount)
    quantity = quote_to_use / entry_price
    return quantity


def calculate_stop_loss(entry_price: float, side: str = "LONG",
                         candle_low: float = None, candle_high: float = None,
                         atr: float = None) -> float:
    """LONG: SL di bawah entry. SHORT: SL di atas entry (rugi kalau harga naik).

    Kalau USE_ATR_RISK=True dan atr diberikan, SL diletakkan di low/high
    CANDLE SINYAL (bukan entry_price) dikurangi/ditambah ATR*ATR_MULT_SL --
    ini yang membuat jarak SL mengikuti volatilitas & struktur candle,
    bukan persentase tetap dari harga.
    """
    if config.USE_ATR_RISK and atr is not None and atr > 0:
        if side == "SHORT":
            base = candle_high if candle_high is not None else entry_price
            return _round_price(base + atr * config.ATR_MULT_SL)
        base = candle_low if candle_low is not None else entry_price
        return _round_price(base - atr * config.ATR_MULT_SL)

    if side == "SHORT":
        return _round_price(entry_price * (1 + config.STOP_LOSS_PCT / 100))
    return _round_price(entry_price * (1 - config.STOP_LOSS_PCT / 100))


def calculate_take_profit(entry_price: float, side: str = "LONG",
                           stop_loss: float = None) -> float:
    """LONG: TP di atas entry. SHORT: TP di bawah entry (untung kalau harga turun).

    Kalau USE_ATR_RISK=True, TP = entry +/- (jarak_SL x RISK_REWARD) --
    butuh stop_loss yang SUDAH dihitung (lewat calculate_stop_loss) untuk
    tahu jarak risikonya.
    """
    if config.USE_ATR_RISK and stop_loss is not None:
        risk_distance = abs(entry_price - stop_loss)
        if side == "SHORT":
            return _round_price(entry_price - risk_distance * config.RISK_REWARD)
        return _round_price(entry_price + risk_distance * config.RISK_REWARD)

    if side == "SHORT":
        return _round_price(entry_price * (1 - config.TAKE_PROFIT_PCT / 100))
    return _round_price(entry_price * (1 + config.TAKE_PROFIT_PCT / 100))


def should_exit(current_price: float, entry_price: float, stop_loss: float,
                 take_profit: float, side: str = "LONG") -> str:
    """Cek apakah posisi saat ini harus ditutup karena SL/TP kena."""
    if side == "SHORT":
        if current_price >= stop_loss:
            return "STOP_LOSS"
        if current_price <= take_profit:
            return "TAKE_PROFIT"
    else:
        if current_price <= stop_loss:
            return "STOP_LOSS"
        if current_price >= take_profit:
            return "TAKE_PROFIT"
    return "HOLD"


def calculate_tp_levels(entry_price: float, stop_loss: float, side: str = "LONG") -> list:
    """Hitung level-level take-profit bertingkat, dipakai kalau USE_MULTI_TP=True.

    Pakai config.TP_LEVELS (kelipatan R, misal [1.0, 2.0, 3.0]) dan
    config.TP_PERCENTS (persen porsi ditutup di level itu, misal [30,30,40],
    HARUS total 100) -- porsi tiap level BISA BEDA-BEDA sesuai keinginan
    (bukan otomatis dibagi rata seperti versi sebelumnya).

    Level TERAKHIR di list SELALU dieksekusi dengan menutup SISA posisi
    sepenuhnya (bukan cuma persentase yang tertulis di TP_PERCENTS untuk
    level itu) -- supaya tidak ada dust tersisa akibat pembulatan floating
    point, walau totalnya sudah 100%. Lihat pemakaiannya di
    TradingBot.check_partial_exits() di bot.py.

    Return: list of dict {"price": float, "fraction": float, "hit": False, "final": bool}
    """
    if len(config.TP_LEVELS) != len(config.TP_PERCENTS):
        raise ValueError(
            f"config.TP_LEVELS ({len(config.TP_LEVELS)} item) dan config.TP_PERCENTS "
            f"({len(config.TP_PERCENTS)} item) harus SAMA PANJANG."
        )
    if sum(config.TP_PERCENTS) != 100:
        raise ValueError(f"config.TP_PERCENTS harus total 100, sekarang total {sum(config.TP_PERCENTS)}.")

    risk_distance = abs(entry_price - stop_loss)
    levels = []
    for i, (r_mult, pct) in enumerate(zip(config.TP_LEVELS, config.TP_PERCENTS)):
        price = (entry_price + risk_distance * r_mult) if side != "SHORT" else (entry_price - risk_distance * r_mult)
        is_final = (i == len(config.TP_LEVELS) - 1)
        levels.append({
            "price": _round_price(price),
            "fraction": pct / 100.0,
            "hit": False,
            "final": is_final,
        })
    return levels
