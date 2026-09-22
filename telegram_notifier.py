"""
telegram_notifier.py
Kirim notifikasi event penting bot (buka/tutup posisi, equity guard,
start/shutdown) ke Telegram lewat Bot API.

=== CARA SETUP ===
1. Chat ke @BotFather di Telegram, kirim /newbot, ikuti instruksinya --
   di akhir kamu dapat TOKEN (format: "123456789:AAxxxxxxxxxxxxxxxxxxxxx").
2. Cari bot BARU kamu di Telegram (nama yang kamu kasih tadi), kirim pesan
   apa saja ke bot itu (misal "halo") -- WAJIB langkah ini dulu, supaya
   bot punya "izin" kirim balik ke kamu nanti.
3. Buka di browser: https://api.telegram.org/bot<TOKEN>/getUpdates
   (ganti <TOKEN> dengan token asli kamu). Cari angka setelah "chat":{"id":
   di hasil JSON-nya -- itu TELEGRAM_CHAT_ID kamu.
4. Isi di .env:
     TELEGRAM_TOKEN=isi_token_dari_botfather
     TELEGRAM_CHAT_ID=isi_angka_chat_id_tadi
5. Set USE_TELEGRAM_ALERTS = True di config.py.

Semua fungsi di sini AMAN dipanggil kapan saja, termasuk kalau Telegram
belum dikonfigurasi sama sekali -- otomatis no-op (diam saja, return
False), TIDAK PERNAH melempar exception ke caller. Ini penting supaya
Telegram down/internet putus/token salah TIDAK PERNAH bikin bot trading
ikut berhenti -- notifikasi cuma "bonus", bukan bagian kritis dari logika
trading.
"""

import logging

import requests

logger = logging.getLogger("trading_bot")

TELEGRAM_API_BASE = "https://api.telegram.org"
_TIMEOUT = 8


def send_telegram(config, message: str, reply_markup: dict = None, chat_id_override=None) -> bool:
    """Kirim SATU pesan teks ke Telegram (format HTML sederhana didukung,
    misal <b>tebal</b>). Return True kalau berhasil terkirim, False kalau
    gagal ATAU memang belum dikonfigurasi/dimatikan -- caller TIDAK PERLU
    cek return value ini kalau cuma mau "fire and forget" notifikasi.

    reply_markup: opsional, dict hasil build_inline_keyboard() -- kalau
    diisi, pesan tampil DENGAN tombol inline keyboard di bawahnya. Semua
    pemanggilan lama (tanpa argumen ini) tetap jalan persis seperti
    sebelumnya, tidak ada perubahan perilaku.

    chat_id_override: opsional -- kalau diisi, pesan dikirim ke chat_id
    INI, BUKAN config.TELEGRAM_CHAT_ID default. WAJIB dipakai di konteks
    multi-user (registration_flow.py, central_dispatcher.py) supaya
    pesan ke SATU user tidak nyasar ke config.TELEGRAM_CHAT_ID global
    (yang di proses multi-user biasanya bahkan tidak relevan/tidak
    diisi sama sekali)."""
    if not getattr(config, "USE_TELEGRAM_ALERTS", False):
        return False

    token = getattr(config, "TELEGRAM_TOKEN", "")
    chat_id = chat_id_override if chat_id_override is not None else getattr(config, "TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.debug("Telegram alert dilewati: TELEGRAM_TOKEN/CHAT_ID belum diisi di .env.")
        return False

    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        resp = requests.post(
            f"{TELEGRAM_API_BASE}/bot{token}/sendMessage",
            json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(f"Telegram alert gagal terkirim (HTTP {resp.status_code}): {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        # SENGAJA tidak di-raise ulang -- kegagalan kirim notifikasi TIDAK
        # BOLEH pernah mengganggu jalannya bot trading yang sesungguhnya.
        logger.warning(f"Telegram alert gagal terkirim: {e}")
        return False


def build_inline_keyboard(rows) -> dict:
    """Bangun struktur inline keyboard Telegram dari list-of-list tuple
    (label_tombol, callback_data). Tiap sub-list = satu BARIS tombol.

    Contoh:
        build_inline_keyboard([
            [("✅ Ya", "confirm_yes"), ("❌ Tidak", "confirm_no")],
            [("⬅️ Kembali", "back")],
        ])
    -> baris pertama 2 tombol bersebelahan, baris kedua 1 tombol sendiri.

    PENTING: callback_data dibatasi Telegram maksimal 64 byte -- jangan
    ditaruh teks panjang/symbol gabungan di sini, cukup kode pendek yang
    nanti di-parse balik di handler callback_query."""
    return {
        "inline_keyboard": [
            [{"text": label, "callback_data": callback_data} for label, callback_data in row]
            for row in rows
        ]
    }


def send_telegram_keyboard(config, message: str, keyboard: dict) -> bool:
    """Kirim pesan DENGAN tombol inline keyboard (hasil build_inline_keyboard()).
    Cuma pemanis tipis di atas send_telegram() supaya caller tidak perlu
    inget nama parameter reply_markup tiap kali mau kirim tombol."""
    return send_telegram(config, message, reply_markup=keyboard)


def edit_telegram_message(config, chat_id, message_id: int, text: str, reply_markup: dict = None) -> bool:
    """Edit pesan Telegram yang SUDAH TERKIRIM -- dipakai buat navigasi menu
    inline keyboard supaya menu ke-UPDATE DI TEMPAT (nggak numpuk pesan baru
    tiap kali user tap tombol). Aman dipanggil kapan saja, TIDAK PERNAH
    raise exception, sama seperti fungsi lain di file ini."""
    if not getattr(config, "USE_TELEGRAM_ALERTS", False):
        return False
    token = getattr(config, "TELEGRAM_TOKEN", "")
    if not token:
        return False

    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        resp = requests.post(
            f"{TELEGRAM_API_BASE}/bot{token}/editMessageText",
            json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.debug(f"Telegram editMessageText gagal (HTTP {resp.status_code}): {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.debug(f"Telegram editMessageText gagal: {e}")
        return False


def answer_callback_query(config, callback_query_id: str, text: str = None) -> bool:
    """WAJIB dipanggil tiap kali ada klik tombol inline keyboard -- kalau
    tidak, tombol yang diklik nampilin loading spinner TANPA HENTI di HP
    user sampai Telegram timeout sendiri (UX jelek). Sama seperti fungsi
    lain di file ini, aman dipanggil kapan saja dan TIDAK PERNAH raise
    exception -- gagal jawab callback bukan hal fatal.

    text: opsional -- kalau diisi, muncul sebagai popup kecil di HP user
    (biasa dipakai buat pesan konfirmasi singkat, bukan notifikasi biasa)."""
    if not getattr(config, "USE_TELEGRAM_ALERTS", False):
        return False
    token = getattr(config, "TELEGRAM_TOKEN", "")
    if not token:
        return False

    try:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        resp = requests.post(
            f"{TELEGRAM_API_BASE}/bot{token}/answerCallbackQuery",
            json=payload,
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as e:
        logger.debug(f"Telegram answerCallbackQuery gagal: {e}")
        return False


def get_telegram_updates(config, offset=None, timeout: int = 25) -> list:
    """Ambil pesan/perintah MASUK dari Telegram pakai long polling (getUpdates).
    Return list kosong kalau gagal/belum dikonfigurasi -- TIDAK PERNAH raise
    exception, sama seperti send_telegram().

    offset: update_id terakhir yang SUDAH diproses + 1 -- Telegram cuma
    kirim update yang BELUM pernah diambil dengan offset ini, mencegah
    perintah yang sama diproses dua kali."""
    if not getattr(config, "USE_TELEGRAM_ALERTS", False):
        return []
    token = getattr(config, "TELEGRAM_TOKEN", "")
    if not token:
        return []

    try:
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        resp = requests.get(
            f"{TELEGRAM_API_BASE}/bot{token}/getUpdates",
            params=params,
            timeout=timeout + 5,
        )
        if resp.status_code != 200:
            logger.debug(f"Telegram getUpdates gagal (HTTP {resp.status_code})")
            return []
        data = resp.json()
        if not data.get("ok"):
            return []
        return data.get("result", [])
    except Exception as e:
        logger.debug(f"Telegram getUpdates gagal: {e}")
        return []


# ---------- Template pesan siap pakai untuk event-event umum ----------

def notify_bot_started(config, mode: str, network: str, total_pairs: int):
    send_telegram(config, f"🤖 <b>Bot dimulai</b>\nMode: {mode} | Jaringan: {network}\nTotal pair aktif: {total_pairs}")


def notify_bot_shutdown(config, realized_pnl_total: float, quote_asset: str):
    send_telegram(config, f"⏻ <b>Bot dimatikan</b>\nRealized PnL sesi ini: {realized_pnl_total:+.2f} {quote_asset}")


def notify_position_opened(config, symbol: str, side: str, entry: float, sl: float, tp: float, dry_run: bool):
    tag = "🧪 DRY_RUN" if dry_run else "💰 LIVE"
    emoji = "🟢" if side == "LONG" else "🔴"
    sl_tp_line = f"SL: {sl:.6g} | TP: {tp:.6g}" if sl is not None and tp is not None else "Tanpa SL/TP -- ditahan sampai ditutup manual"
    send_telegram(
        config,
        f"{emoji} <b>{side} {symbol}</b> ({tag})\nEntry: {entry:.6g}\n{sl_tp_line}",
    )


def notify_position_closed(config, symbol: str, side: str, reason: str, entry: float, exit_price: float,
                            pnl_quote: float, pnl_pct: float, quote_asset: str, dry_run: bool):
    tag = "🧪 DRY_RUN" if dry_run else "💰 LIVE"
    result_emoji = "✅" if pnl_quote >= 0 else "❌"
    pct_text = f" ({pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    send_telegram(
        config,
        f"{result_emoji} <b>{side} {symbol} ditutup</b> ({tag})\n"
        f"Alasan: {reason}\nEntry: {entry:.6g} -> Exit: {exit_price:.6g}\n"
        f"PnL: {pnl_quote:+.2f} {quote_asset}{pct_text}",
    )


def notify_equity_guard(config, drawdown_pct: float, max_drawdown_pct: float, current_equity: float, quote_asset: str):
    send_telegram(
        config,
        f"🚨 <b>EQUITY GUARD AKTIF -- BOT DIJEDA OTOMATIS</b>\n"
        f"Drawdown: {drawdown_pct:.2f}% (batas: {max_drawdown_pct}%)\n"
        f"Equity sekarang: {current_equity:.2f} {quote_asset}",
    )
