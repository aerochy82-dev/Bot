"""
discord_notifier.py
Kirim notifikasi event penting bot (buka/tutup posisi, equity guard,
start/shutdown) ke Discord lewat WEBHOOK -- BUKAN bot Discord asli, jadi
TIDAK BISA menerima command masuk (beda dari telegram_notifier.py yang
punya listener command lewat getUpdates). Ini SATU ARAH: bot -> Discord.

=== CARA SETUP (5 menit, tanpa Discord Developer Portal) ===
1. Buka server Discord kamu -> pilih channel tempat notifikasi mau
   muncul -> klik ikon gear (Edit Channel) -> Integrations -> Webhooks.
2. Klik "New Webhook", kasih nama (misal "Trading Bot"), copy URL
   webhook-nya (format: https://discord.com/api/webhooks/123.../abcDEF...).
3. Isi di .env:
     DISCORD_WEBHOOK_URL=isi_url_webhook_tadi
4. Set USE_DISCORD_ALERTS = True di config.py.

Semua fungsi di sini AMAN dipanggil kapan saja, termasuk kalau Discord
belum dikonfigurasi sama sekali -- otomatis no-op (diam saja, return
False), TIDAK PERNAH melempar exception ke caller. Sama filosofinya
dengan telegram_notifier.py -- notifikasi cuma "bonus", tidak boleh
pernah mengganggu jalannya bot trading yang sesungguhnya.

=== KALAU MAU DUA-ARAH (terima command dari Discord juga) ===
Webhook TIDAK BISA menerima pesan masuk. Untuk itu butuh Discord Bot asli
(lewat Discord Developer Portal + library discord.py), yang jalan sebagai
listener terpisah -- pola yang sama dengan telegram_listener_loop() di
bot.py, membaca command lalu menulis ke control.json. Modul ini TIDAK
mencakup itu; ini cuma jalur keluar (notifikasi).
"""

import logging

import requests

logger = logging.getLogger("trading_bot")

_TIMEOUT = 8

# Warna embed Discord (integer desimal dari hex) -- dipakai buat kasih
# aksen warna di sisi kiri kartu notifikasi, mirip fungsi emoji di Telegram.
COLOR_GREEN = 0x2ECC71
COLOR_RED = 0xE74C3C
COLOR_BLUE = 0x3498DB
COLOR_ORANGE = 0xE67E22
COLOR_GREY = 0x95A5A6


def _post(config, payload: dict) -> bool:
    """Kirim SATU payload ke webhook URL. Return True kalau berhasil,
    False kalau gagal ATAU memang belum dikonfigurasi/dimatikan --
    caller TIDAK PERLU cek return value ini kalau cuma mau "fire and
    forget" notifikasi (sama seperti send_telegram())."""
    if not getattr(config, "USE_DISCORD_ALERTS", False):
        return False

    webhook_url = getattr(config, "DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        logger.debug("Discord alert dilewati: DISCORD_WEBHOOK_URL belum diisi di .env.")
        return False

    try:
        resp = requests.post(webhook_url, json=payload, timeout=_TIMEOUT)
        # Discord balas 204 No Content kalau sukses (BEDA dari Telegram
        # yang balas 200 + body JSON) -- jangan disamakan.
        if resp.status_code not in (200, 204):
            logger.warning(f"Discord alert gagal terkirim (HTTP {resp.status_code}): {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        # SENGAJA tidak di-raise ulang -- sama alasannya dengan
        # telegram_notifier.py: gagal kirim notifikasi TIDAK BOLEH
        # pernah mengganggu jalannya bot trading yang sesungguhnya.
        logger.warning(f"Discord alert gagal terkirim: {e}")
        return False


def send_discord(config, message: str) -> bool:
    """Kirim SATU pesan teks polos (tanpa embed) ke Discord. Discord
    mendukung markdown dasar (**tebal**, *italic*, `code`, dll) di
    'content' biasa -- beda dari Telegram yang butuh parse_mode=HTML."""
    return _post(config, {"content": message[:2000]})  # Discord batasi 'content' maks 2000 karakter


def send_discord_embed(config, title: str, description: str = "", color: int = COLOR_BLUE, fields: list = None) -> bool:
    """Kirim pesan sebagai EMBED (kartu berwarna dengan judul, deskripsi,
    dan field key-value opsional) -- tampilan lebih rapi dari pesan teks
    polos, cocok untuk notifikasi event trading.

    fields: list of dict {"name": ..., "value": ..., "inline": bool},
    opsional -- dipakai untuk detail terstruktur (misal Entry/SL/TP)."""
    embed = {"title": title[:256], "color": color}
    if description:
        embed["description"] = description[:4096]
    if fields:
        embed["fields"] = [
            {"name": str(f.get("name", ""))[:256], "value": str(f.get("value", ""))[:1024], "inline": bool(f.get("inline", True))}
            for f in fields[:25]  # Discord batasi maks 25 field per embed
        ]
    return _post(config, {"embeds": [embed]})


# ---------- Template pesan siap pakai untuk event-event umum ----------
# SAMA PERSIS strukturnya dengan telegram_notifier.py -- panggil fungsi
# ini BERDAMPINGAN dengan notify_*() versi Telegram di bot.py kalau mau
# notifikasi ke DUA platform sekaligus (lihat catatan integrasi di akhir).

def notify_bot_started(config, mode: str, network: str, total_pairs: int):
    send_discord_embed(
        config, "🤖 Bot dimulai", color=COLOR_BLUE,
        fields=[
            {"name": "Mode", "value": mode, "inline": True},
            {"name": "Jaringan", "value": network, "inline": True},
            {"name": "Total pair aktif", "value": str(total_pairs), "inline": True},
        ],
    )


def notify_bot_shutdown(config, realized_pnl_total: float, quote_asset: str):
    send_discord_embed(
        config, "⏻ Bot dimatikan", color=COLOR_GREY,
        description=f"Realized PnL sesi ini: **{realized_pnl_total:+.2f} {quote_asset}**",
    )


def notify_position_opened(config, symbol: str, side: str, entry: float, sl: float, tp: float, dry_run: bool):
    tag = "🧪 DRY_RUN" if dry_run else "💰 LIVE"
    color = COLOR_GREEN if side == "LONG" else COLOR_RED
    sl_tp_value = f"SL: {sl:.6g} | TP: {tp:.6g}" if sl is not None and tp is not None else "Tanpa SL/TP -- ditahan sampai ditutup manual"
    send_discord_embed(
        config, f"{'🟢' if side == 'LONG' else '🔴'} {side} {symbol}", color=color,
        description=tag,
        fields=[
            {"name": "Entry", "value": f"{entry:.6g}", "inline": True},
            {"name": "SL / TP", "value": sl_tp_value, "inline": False},
        ],
    )


def notify_position_closed(config, symbol: str, side: str, reason: str, entry: float, exit_price: float,
                            pnl_quote: float, pnl_pct: float, quote_asset: str, dry_run: bool):
    tag = "🧪 DRY_RUN" if dry_run else "💰 LIVE"
    color = COLOR_GREEN if pnl_quote >= 0 else COLOR_RED
    pct_text = f" ({pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    send_discord_embed(
        config, f"{'✅' if pnl_quote >= 0 else '❌'} {side} {symbol} ditutup", color=color,
        description=tag,
        fields=[
            {"name": "Alasan", "value": reason, "inline": True},
            {"name": "Entry → Exit", "value": f"{entry:.6g} → {exit_price:.6g}", "inline": True},
            {"name": "PnL", "value": f"{pnl_quote:+.2f} {quote_asset}{pct_text}", "inline": False},
        ],
    )


def notify_equity_guard(config, drawdown_pct: float, max_drawdown_pct: float, current_equity: float, quote_asset: str):
    send_discord_embed(
        config, "🚨 EQUITY GUARD AKTIF -- BOT DIJEDA OTOMATIS", color=COLOR_ORANGE,
        fields=[
            {"name": "Drawdown", "value": f"{drawdown_pct:.2f}% (batas: {max_drawdown_pct}%)", "inline": True},
            {"name": "Equity sekarang", "value": f"{current_equity:.2f} {quote_asset}", "inline": True},
        ],
    )


# ---------- LISTENER PERINTAH MASUK (Discord Bot token) ----------
# Membutuhkan: pip install discord.py
# Message Content Intent harus ON di Discord Developer Portal.


def run_command_listener(config, on_command, stop_event=None):
    """Jalankan Discord bot yang mendengarkan pesan dengan prefix
    (default '!'). on_command(text, reply_fn) dipanggil tiap perintah
    valid. reply_fn(msg) kirim balasan ke channel yang sama.

    Blocking -- panggil dari thread daemon. stop_event: threading.Event
    opsional; kalau di-set, bot di-close.
    """
    if not getattr(config, "USE_DISCORD_COMMANDS", True):
        logger.info("USE_DISCORD_COMMANDS=False -- listener Discord tidak dijalankan.")
        return
    token = getattr(config, "DISCORD_BOT_TOKEN", "") or ""
    if not token:
        logger.info(
            "Discord command listener dilewati: DISCORD_BOT_TOKEN belum diisi di .env."
        )
        return

    try:
        import discord
        from discord.ext import tasks
    except ImportError:
        logger.warning(
            "discord.py belum terpasang -- perintah Discord tidak aktif. "
            "Install: pip install discord.py"
        )
        return

    prefix = getattr(config, "DISCORD_COMMAND_PREFIX", "!") or "!"
    allowed = set(str(x) for x in (getattr(config, "DISCORD_ALLOWED_USER_IDS", None) or []))

    intents = discord.Intents.default()
    intents.message_content = True  # WAJIB ON di Developer Portal juga
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        logger.info(
            f"Discord command listener siap sebagai {client.user} "
            f"(prefix '{prefix}', allowed_users={len(allowed) or 'SEMUA'})."
        )

    @client.event
    async def on_message(message):
        if message.author.bot:
            return
        content = (message.content or "").strip()
        if not content.startswith(prefix):
            return
        # potong prefix
        text = content[len(prefix):].strip()
        if not text:
            return

        if allowed and str(message.author.id) not in allowed:
            logger.warning(
                f"Perintah Discord dari user TIDAK DIKENAL "
                f"({message.author} id={message.author.id}) diabaikan."
            )
            try:
                await message.channel.send(
                    "⛔ Anda tidak diizinkan mengontrol bot ini."
                )
            except Exception:
                pass
            return

        # Normalisasi: terima "status" maupun "/status" setelah prefix
        # supaya user bisa ketik !status atau !/status
        if not text.startswith("/"):
            # map perintah teks ke bentuk slash internal
            first = text.split()[0].lower()
            rest = text[len(text.split()[0]):].strip()
            cmd_map = {
                "help": "/help",
                "status": "/status",
                "pause": "/pause",
                "resume": "/resume",
                "pairs": "/pairs",
                "settings": "/settings",
                "menu": "/menu",
                "start": "/menu",
                "shutdown": "/shutdown" + (" " + rest if rest else ""),
                "open": "/open " + rest if rest else "/open",
                "close": "/close " + rest if rest else "/close",
                "set": "/set " + rest if rest else "/set",
            }
            if first in cmd_map:
                text = cmd_map[first]
            else:
                text = "/" + text  # biar handler bisa balas "tidak dikenali"

        replies = []

        def reply_fn(msg: str):
            replies.append(msg)

        try:
            # author_id (Discord user ID, string) BARU ditambahkan 20 Sept
            # 2026 supaya discord_dispatcher.py (listener terpusat multi-
            # user) bisa memetakan pesan Discord ke user yang benar --
            # caller lama (bot.py, single-instance) cukup terima & abaikan
            # parameter ini kalau tidak butuh.
            on_command(text, reply_fn, str(message.author.id))
        except Exception as e:
            logger.warning(f"Discord command handler error: {e}")
            replies.append(f"Error memproses perintah: {e}")

        for msg in replies:
            try:
                import re
                chunk = msg[:1900]
                chunk = re.sub(r"</?b>", "**", chunk)
                chunk = re.sub(r"</?i>", "*", chunk)
                chunk = re.sub(r"</?code>", "`", chunk)
                await message.channel.send(chunk)
            except Exception as e:
                logger.warning(f"Gagal balas Discord: {e}")

    # Watch stop_event dari thread lain
    if stop_event is not None:
        @tasks.loop(seconds=2)
        async def _watch_stop():
            if stop_event.is_set():
                await client.close()

        @_watch_stop.before_loop
        async def _before():
            await client.wait_until_ready()

        @client.event
        async def on_connect():
            if not _watch_stop.is_running():
                _watch_stop.start()

    try:
        client.run(token, log_handler=None)
    except Exception as e:
        logger.warning(f"Discord command listener berhenti: {e}")
