"""
Client untuk OKX API v5 -- dipakai berdampingan dengan python-binance supaya
bot bisa trading di Binance DAN OKX sekaligus dalam satu proses.

DESAIN PENTING -- REST POLLING, BUKAN WEBSOCKET:
Binance pakai WebSocket (lewat ThreadedWebsocketManager dari python-binance)
untuk data real-time. OKX punya WebSocket sendiri juga, TAPI arsitekturnya
beda total (asyncio terpisah, format pesan beda) -- menggabungkan dua
WebSocket manager yang berbeda dalam satu proses menambah kompleksitas dan
risiko bug threading yang signifikan.

Sebagai gantinya, modul ini pakai REST API OKX dengan POLLING berkala
(lihat config.OKX_POLL_INTERVAL_SECONDS). Konsekuensinya: data OKX update
tiap beberapa detik (bukan push instan seperti WebSocket Binance), sedikit
kurang "real-time" dibanding pair Binance -- tapi jauh lebih sederhana dan
robust untuk mulai. Kalau nanti perlu WebSocket OKX yang benar-benar
real-time, itu pengembangan terpisah.

Autentikasi OKX BEDA dari Binance -- butuh TIGA kredensial:
- OKX_API_KEY, OKX_API_SECRET (mirip Binance)
- OKX_API_PASSPHRASE (TIDAK ADA di Binance -- wajib diisi saat generate
  API key di OKX, dan dipakai di header tiap request)

Referensi resmi: https://www.okx.com/docs-v5/en/
"""

import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger("trading_bot")

BASE_URL = "https://www.okx.com"


def get_okx_market_client() -> "OKXClient":
    """Factory untuk client market-data OKX (dipakai scanner & polling).

    Candle/ticker publik tidak wajib auth, tapi kita tetap isi kredensial
    dari config supaya instance yang sama bisa dipakai untuk endpoint
    privat kalau diperlukan nanti. Menggunakan config.OKX_PASSWORD
    (nama di config.py) sebagai passphrase.

    import config dilakukan DI DALAM fungsi (lazy) supaya tidak gagal
    dengan NameError kalau modul di-load sebelum path/package config siap,
    dan supaya tidak bergantung pada import di level modul.
    """
    import config as _cfg
    return OKXClient(
        api_key=getattr(_cfg, "OKX_API_KEY", "") or "",
        api_secret=getattr(_cfg, "OKX_API_SECRET", "") or "",
        passphrase=getattr(_cfg, "OKX_PASSWORD", "") or "",
        demo_trading=bool(getattr(_cfg, "OKX_DEMO_TRADING", False)),
    )


class OKXAPIError(Exception):
    """Dilempar kalau OKX API balas dengan code error (bukan '0')."""
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"OKX API Error {code}: {message}")


class OKXClient:
    def __init__(self, api_key: str, api_secret: str, passphrase: str, demo_trading: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.demo_trading = demo_trading
        self.session = requests.Session()
        # Cache spesifikasi instrument (ctVal/lotSz/minSz) per instId --
        # praktis tidak pernah berubah selama proses berjalan, jadi query
        # publik ini cukup sekali per symbol (lihat get_instrument_spec()).
        self._instrument_spec_cache = {}

    # ---------- SIGNING ----------
    @staticmethod
    def _timestamp() -> str:
        """Format ISO8601 dengan milidetik + 'Z', PERSIS seperti yang
        diwajibkan OKX (beda dari format epoch milidetik ala Binance).

        PENTING: datetime.now() dipanggil SEKALI, ditampung ke variabel --
        versi sebelumnya manggil datetime.now() DUA KALI terpisah (untuk
        bagian detik dan milidetik), yang secara teori bisa menghasilkan
        timestamp yang tidak konsisten kalau kebetulan pas lewat batas detik
        di antara dua panggilan itu (jarang, tapi nyata)."""
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    def _sign(self, timestamp: str, method: str, request_path: str, body: str) -> str:
        """base64(HMAC-SHA256(secret, timestamp + method + requestPath + body))"""
        message = f"{timestamp}{method}{request_path}{body}"
        mac = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()

    def _headers(self, method: str, request_path: str, body: str) -> dict:
        timestamp = self._timestamp()
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.demo_trading:
            # Header ini yang membedakan demo trading (dana virtual) dari
            # akun live -- BUKAN base URL yang beda seperti testnet Binance.
            headers["x-simulated-trading"] = "1"
        return headers

    def _request(self, method: str, path: str, params: dict = None, body: dict = None, auth: bool = True):
        query = ""
        if params:
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        request_path = path + query
        body_str = json.dumps(body) if body else ""

        headers = self._headers(method, request_path, body_str) if auth else {"Content-Type": "application/json"}
        url = BASE_URL + request_path

        resp = self.session.request(method, url, headers=headers, data=body_str if body else None, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") not in ("0", 0):
            # PENTING (ditemukan 21 Sept 2026): kode error TOP-LEVEL OKX
            # (misal "1" / "All operations failed") SERING kali cuma
            # pembungkus generik -- alasan SEBENARNYA kenapa satu order
            # gagal ada di dalam data.data[i].sCode/sMsg (per-order detail,
            # relevan terutama untuk endpoint batch seperti place_order).
            # Sebelum fix ini, detail itu SELALU DIBUANG (cuma msg
            # top-level yang dipakai) -- bikin SEMUA log error order OKX
            # selama ini cuma bilang "All operations failed" tanpa pernah
            # bilang alasan aslinya (saldo kurang? posSide salah? ukuran
            # order salah? dll) -- menyulitkan diagnosis nyata.
            top_msg = data.get("msg", "Unknown error")
            inner = data.get("data")
            sub_details = []
            if isinstance(inner, list):
                for item in inner:
                    if isinstance(item, dict) and item.get("sMsg"):
                        sub_details.append(f"{item.get('sCode', '?')}: {item.get('sMsg')}")
            detail = top_msg or "Unknown error"
            if sub_details:
                detail = f"{detail} | " + "; ".join(sub_details) if detail else "; ".join(sub_details)
            raise OKXAPIError(data.get("code"), detail)
        return data.get("data", [])

    # ---------- MARKET DATA (publik, tidak butuh auth) ----------
    def get_candles(self, inst_id: str, bar: str = "1m", limit: int = 300) -> list:
        """Return list candle TERBARU DULU (sesuai urutan asli OKX), tiap
        elemen: [ts_ms, open, high, low, close, vol, ...]. Caller yang
        perlu urutan waktu naik harus reverse sendiri."""
        return self._request(
            "GET", "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": limit},
            auth=False,
        )

    def get_ticker_price(self, inst_id: str) -> float:
        data = self._request("GET", "/api/v5/market/ticker", params={"instId": inst_id}, auth=False)
        if not data:
            raise OKXAPIError("NO_DATA", f"Ticker kosong untuk {inst_id}")
        return float(data[0]["last"])

    # ---------- AKUN (privat, butuh auth) ----------
    def get_balance(self, ccy: str) -> float:
        """Saldo trading account (unified/USDT-margined) untuk satu currency."""
        data = self._request("GET", "/api/v5/account/balance", params={"ccy": ccy})
        if not data:
            return 0.0
        details = data[0].get("details", [])
        for d in details:
            if d.get("ccy") == ccy:
                return float(d.get("availBal", 0) or 0)
        return 0.0

    def set_leverage(self, inst_id: str, leverage: int, mgn_mode: str = "cross"):
        return self._request("POST", "/api/v5/account/set-leverage", body={
            "instId": inst_id, "lever": str(leverage), "mgnMode": mgn_mode,
        })

    def get_positions(self, inst_type: str = "SWAP", inst_id: str = None) -> list:
        """Posisi FUTURES/SWAP yang BENERAN terbuka di exchange sekarang --
        ditambahkan 21 Sept 2026 untuk rekonsiliasi state internal bot vs
        kenyataan di exchange (lihat TradingBot.sync_okx_positions() di
        bot.py). TIDAK dipakai untuk keputusan entry/exit real-time (itu
        tetap lewat data candle REST polling yang sudah ada) -- murni buat
        deteksi "posisi hantu" (bot kira ada posisi tapi exchange bilang
        tidak ada, misal sisa simulasi DRY RUN) dan "posisi asing" (ada
        posisi di exchange yang bot tidak tahu, misal dibuka manual).

        Balikannya list of dict, tiap elemen (field yang relevan):
        instId, posSide ("long"/"short"/"net"), pos (ukuran, STRING,
        bertanda di mode net: positif=long, negatif=short), avgPx (harga
        entry rata-rata). SPOT tidak punya endpoint "posisi" -- cuma saldo
        wallet biasa (lihat get_balance()), jadi tidak bisa direkonsiliasi
        dengan cara yang sama."""
        params = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        return self._request("GET", "/api/v5/account/positions", params=params)

    # ---------- SPESIFIKASI INSTRUMENT (publik, tidak butuh auth) ----------
    def get_instrument_spec(self, inst_id: str, inst_type: str = "SWAP") -> dict:
        """Ambil spesifikasi kontrak SATU instrument -- ctVal (nilai 1
        contract dalam base asset), lotSz (kelipatan ukuran order yang
        valid), minSz (ukuran order minimum, dalam contracts).

        PENTING (ditemukan 21 Sept 2026): untuk SWAP/FUTURES, field "sz"
        di place_order()/"pos" di get_positions() SELALU dalam CONTRACTS,
        BUKAN jumlah koin langsung -- 1 contract beda-beda tiap instrument
        (contoh nyata: XRP-USDT-SWAP ctVal=100 [1 contract = 100 XRP],
        SATS-USDT-SWAP ctVal=10.000.000, BTC-USDT-SWAP ctVal=0.01). Kalau
        jumlah koin dikirim mentah-mentah sebagai contracts tanpa dibagi
        ctVal dulu, order jadi salah ukuran total (bisa jadi jauh lebih
        besar dari maksimum yang diizinkan exchange untuk koin bersuplai
        besar seperti SATS/PEPE -- inilah AKAR PENYEBAB order live yang
        berulang kali ditolak OKX dengan "All operations failed" sepanjang
        sesi ini -- atau jauh lebih kecil dari yang dimaksud untuk koin
        mahal seperti BTC). SPOT tidak pakai konsep contracts sama sekali
        (qty = jumlah koin langsung), jadi method ini cuma relevan untuk
        SWAP/FUTURES.

        Hasil di-cache per instId supaya tidak query API publik berulang
        kali -- spesifikasi kontrak praktis tidak pernah berubah selama
        proses bot berjalan.
        """
        if inst_id in self._instrument_spec_cache:
            return self._instrument_spec_cache[inst_id]
        data = self._request(
            "GET", "/api/v5/public/instruments",
            params={"instType": inst_type, "instId": inst_id}, auth=False,
        )
        if not data:
            raise OKXAPIError("NO_DATA", f"Spesifikasi instrument kosong untuk {inst_id}")
        info = data[0]
        spec = {
            "ct_val": float(info.get("ctVal") or 1),
            "ct_mult": float(info.get("ctMult") or 1),
            "lot_sz": float(info.get("lotSz") or 1),
            "min_sz": float(info.get("minSz") or 1),
        }
        self._instrument_spec_cache[inst_id] = spec
        return spec

    # ---------- TRADING (privat, butuh auth) ----------
    def place_order(self, inst_id: str, side: str, sz: str, td_mode: str = "cash", ord_type: str = "market",
                     reduce_only: bool = False):
        """
        side: 'buy' atau 'sell' (huruf kecil, beda dari Binance yang 'BUY'/'SELL')
        td_mode: 'cash' untuk spot, 'cross'/'isolated' untuk margin/futures (SWAP)
        sz: ukuran order dalam STRING (OKX mewajibkan string, bukan angka mentah)

        reduce_only (ditambahkan 21 Sept 2026): True kalau order ini CUMA
        boleh MENGURANGI posisi yang sudah ada -- dipakai KHUSUS untuk
        tutup/partial-close posisi (close_position()/partial_close_position()
        di bot.py), TIDAK PERNAH untuk order BUKA posisi baru. Tanpa flag
        ini, OKX bisa mengevaluasi order seolah berpotensi MEMBUKA/
        MEMPERBESAR posisi (butuh margin penuh sesuai ukuran order),
        padahal maksudnya cuma MENGURANGI eksposur yang sudah ada (yang
        seharusnya TIDAK butuh margin tambahan, bahkan membebaskan
        margin) -- diduga kuat salah satu penyebab order tutup posisi
        berulang kali gagal dengan "Insufficient USDT margin" (error
        51008) walau posisi yang mau ditutup itu VALID dan cukup besar.
        Cuma berlaku untuk MARGIN order dan FUTURES/SWAP di net mode
        (OKX API v5 docs) -- TIDAK berlaku untuk Spot ('cash'), jadi
        caller (execute_order() di bot.py) cuma mengirim flag ini kalau
        market_type == "FUTURES"."""
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": str(sz),
        }
        if reduce_only:
            body["reduceOnly"] = "true"
        return self._request("POST", "/api/v5/trade/order", body=body)
