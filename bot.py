#!/usr/bin/env python3
"""
Binance Real-Time Signal Bot — Railway ready
Uses WebSocket kline streams + Telegram alerts.
Credentials come from environment variables (never hardcode secrets).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Set

import requests
import websockets

# ---------------------------------------------------------------------------
# CONFIG FROM ENVIRONMENT (Railway Variables)
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

TOP_PAIRS_REFRESH = int(os.environ.get("TOP_PAIRS_REFRESH", "600"))  # 10 min
TOP_N_SPOT        = int(os.environ.get("TOP_N_SPOT", "25"))
TOP_N_FUTURES     = int(os.environ.get("TOP_N_FUTURES", "25"))
KLINE_INTERVAL    = os.environ.get("KLINE_INTERVAL", "1h")
KLINE_LIMIT       = 120
SIGNAL_COOLDOWN   = 4 * 3600

RSI_BULL = (45, 65)
RSI_BEAR = (35, 55)
SL_PCT   = 0.015
TP1_PCT  = 0.030
TP2_PCT  = 0.045

EXCLUDE_KEYWORDS = ("UP", "DOWN", "BEAR", "BULL", "3L", "3S", "5L", "5S")

SPOT_REST      = "https://api.binance.com"
FUTURES_REST   = "https://fapi.binance.com"
SPOT_WS_BASE   = "wss://stream.binance.com:9443"
FUTURES_WS_BASE = "wss://fstream.binance.com"

seen_signals: Dict[str, float] = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ws-scanner")

# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------
def telegram_send(text: str, parse_mode: str = "HTML") -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=12,
        )
        if r.status_code != 200:
            log.error("Telegram API: %s", r.text[:300])
            return False
        return True
    except Exception as e:
        log.error("Telegram send error: %s", e)
        return False

# ---------------------------------------------------------------------------
# INDICATORS
# ---------------------------------------------------------------------------
def ema(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period:
        return [None] * len(values)
    k = 2.0 / (period + 1)
    out: List[Optional[float]] = [None] * (period - 1)
    seed = sum(values[:period]) / period
    out.append(seed)
    for i in range(period, len(values)):
        seed = values[i] * k + seed * (1 - k)
        out.append(seed)
    return out

def rsi(closes: List[float], period: int = 14) -> List[Optional[float]]:
    if len(closes) < period + 1:
        return [None] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out: List[Optional[float]] = [None] * period
    out.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    return out

def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9):
    ema_f = ema(closes, fast)
    ema_s = ema(closes, slow)
    macd_line = [
        (f - s) if f is not None and s is not None else None
        for f, s in zip(ema_f, ema_s)
    ]
    valid = [v for v in macd_line if v is not None]
    if len(valid) < signal:
        n = len(closes)
        return macd_line, [None] * n, [None] * n
    sig = ema(valid, signal)
    pad = len(macd_line) - len(valid)
    signal_line = [None] * pad + sig
    hist = [
        (m - s) if m is not None and s is not None else None
        for m, s in zip(macd_line, signal_line)
    ]
    return macd_line, signal_line, hist

# ---------------------------------------------------------------------------
# REST HELPERS
# ---------------------------------------------------------------------------
def http_get(url: str, timeout: int = 12) -> Any:
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "WS-Scanner/3.1"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("GET %s failed (%d): %s", url, attempt + 1, e)
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"Failed {url}")

def get_top_pairs(market: str, n: int) -> List[Dict]:
    url = (
        f"{SPOT_REST}/api/v3/ticker/24hr"
        if market == "spot"
        else f"{FUTURES_REST}/fapi/v1/ticker/24hr"
    )
    tickers = http_get(url)
    pairs = []
    for t in tickers:
        sym = t["symbol"]
        if not sym.endswith("USDT"):
            continue
        if any(k in sym for k in EXCLUDE_KEYWORDS):
            continue
        try:
            price = float(t["lastPrice"])
            vol   = float(t["quoteVolume"])
            chg   = float(t.get("priceChangePercent", 0))
        except (KeyError, ValueError):
            continue
        if price <= 0 or vol <= 0:
            continue
        pairs.append({
            "symbol": sym,
            "price": price,
            "quoteVolume": vol,
            "change24h": chg,
            "market": market,
        })
    pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
    return pairs[:n]

def bootstrap_klines(symbol: str, market: str) -> List[float]:
    if market == "spot":
        url = f"{SPOT_REST}/api/v3/klines?symbol={symbol}&interval={KLINE_INTERVAL}&limit={KLINE_LIMIT}"
    else:
        url = f"{FUTURES_REST}/fapi/v1/klines?symbol={symbol}&interval={KLINE_INTERVAL}&limit={KLINE_LIMIT}"
    raw = http_get(url)
    return [float(k[4]) for k in raw]

# ---------------------------------------------------------------------------
# SIGNAL LOGIC
# ---------------------------------------------------------------------------
def signal_hash(symbol: str, side: str, market: str) -> str:
    raw = f"{symbol}|{side}|{market}|{KLINE_INTERVAL}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

def is_duplicate(h: str) -> bool:
    now = time.time()
    expired = [k for k, ts in seen_signals.items() if now - ts > SIGNAL_COOLDOWN]
    for k in expired:
        del seen_signals[k]
    return h in seen_signals

def mark_seen(h: str) -> None:
    seen_signals[h] = time.time()

def score_confidence(side: str, rsi_v: float, hist: float, spread_pct: float) -> float:
    rsi_score = max(0.0, 100 - abs(rsi_v - (55 if side == "LONG" else 45)) * 3)
    hist_score = min(100.0, abs(hist) * 500)
    ema_score  = min(100.0, abs(spread_pct) * 200)
    return round(0.40 * rsi_score + 0.35 * hist_score + 0.25 * ema_score, 1)

def check_signal(
    symbol: str,
    market: str,
    closes: List[float],
    last_price: float,
    change24h: float,
    volume: float,
) -> Optional[Dict]:
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r14 = rsi(closes, 14)
    _, _, hist = macd(closes)
    e20v, e50v, rv, hv = e20[-1], e50[-1], r14[-1], hist[-1]
    if None in (e20v, e50v, rv, hv):
        return None
    spread = (e20v - e50v) / e50v * 100
    side = None
    if e20v > e50v and RSI_BULL[0] <= rv <= RSI_BULL[1] and hv > 0:
        side = "LONG"
    elif e20v < e50v and RSI_BEAR[0] <= rv <= RSI_BEAR[1] and hv < 0:
        side = "SHORT"
    if side is None:
        return None
    h = signal_hash(symbol, side, market)
    if is_duplicate(h):
        return None
    entry = last_price
    if side == "LONG":
        sl  = round(entry * (1 - SL_PCT), 8)
        tp1 = round(entry * (1 + TP1_PCT), 8)
        tp2 = round(entry * (1 + TP2_PCT), 8)
    else:
        sl  = round(entry * (1 + SL_PCT), 8)
        tp1 = round(entry * (1 - TP1_PCT), 8)
        tp2 = round(entry * (1 - TP2_PCT), 8)
    conf = score_confidence(side, rv, hv, spread)
    mark_seen(h)
    return {
        "symbol": symbol,
        "market": market,
        "side": side,
        "entry": entry,
        "stop_loss": sl,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "rsi": round(rv, 2),
        "ema20": round(e20v, 8),
        "ema50": round(e50v, 8),
        "macd_hist": round(hv, 8),
        "ema_spread_pct": round(spread, 3),
        "confidence": conf,
        "change24h": change24h,
        "volume": volume,
        "hash": h,
        "time": datetime.now(timezone.utc).isoformat(),
    }

def format_telegram(sig: Dict) -> str:
    is_long = sig["side"] == "LONG"
    emoji = "🚀" if is_long else "🔻"
    market_tag = "SPOT" if sig["market"] == "spot" else "FUTURES"
    return (
        f"{emoji} <b>#{sig['symbol']}</b> — <b>{sig['side']}</b> Signal\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 Market: <code>{market_tag}</code>\n"
        f"⏱ Timeframe: <code>{KLINE_INTERVAL}</code>  •  <i>WebSocket live</i>\n"
        f"\n"
        f"📥 Entry: <code>{sig['entry']}</code>\n"
        f"🛑 Stop Loss: <code>{sig['stop_loss']}</code> (−1.5%)\n"
        f"🎯 TP1: <code>{sig['take_profit_1']}</code> (+3.0% · 1:2)\n"
        f"🎯 TP2: <code>{sig['take_profit_2']}</code> (+4.5% · 1:3)\n"
        f"\n"
        f"📊 RSI(14): <code>{sig['rsi']}</code>\n"
        f"📈 EMA20 / EMA50: <code>{sig['ema20']}</code> / <code>{sig['ema50']}</code>\n"
        f"📉 MACD Hist: <code>{sig['macd_hist']}</code>\n"
        f"💪 Confidence: <code>{sig['confidence']}/100</code>\n"
        f"\n"
        f"⚡ 24h: <code>{sig['change24h']:+.2f}%</code>  •  "
        f"Vol: <code>${sig['volume']/1e6:.1f}M</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Risk-to-Reward 1:2 / 1:3 · Not financial advice</i>"
    )

# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------
candle_buffers: Dict[str, Deque[float]] = {}
ticker_info: Dict[str, Dict] = {}
active_streams: Set[str] = set()

def key(symbol: str, market: str) -> str:
    return f"{symbol}|{market}"

# ---------------------------------------------------------------------------
# WEBSOCKET MANAGER
# ---------------------------------------------------------------------------
class StreamManager:
    def __init__(self):
        self._stop = False

    async def start(self):
        asyncio.create_task(self._maintain_spot())
        asyncio.create_task(self._maintain_futures())
        asyncio.create_task(self._refresh_top_pairs_loop())

    async def _refresh_top_pairs_loop(self):
        while not self._stop:
            try:
                await self._update_subscriptions()
            except Exception as e:
                log.exception("Top-pairs refresh error: %s", e)
            await asyncio.sleep(TOP_PAIRS_REFRESH)

    async def _update_subscriptions(self):
        log.info("Refreshing top-volume pairs…")
        spot = get_top_pairs("spot", TOP_N_SPOT)
        fut  = get_top_pairs("futures", TOP_N_FUTURES)
        new_streams: Set[str] = set()
        for p in spot + fut:
            k = key(p["symbol"], p["market"])
            ticker_info[k] = p
            stream_name = f"{p['symbol'].lower()}@kline_{KLINE_INTERVAL}"
            new_streams.add(f"{p['market']}:{stream_name}")
            if k not in candle_buffers:
                try:
                    closes = bootstrap_klines(p["symbol"], p["market"])
                    candle_buffers[k] = deque(closes, maxlen=KLINE_LIMIT)
                    log.info("Bootstrapped %s (%s) – %d candles", p["symbol"], p["market"], len(closes))
                except Exception as e:
                    log.warning("Bootstrap failed %s: %s", p["symbol"], e)
                    candle_buffers[k] = deque(maxlen=KLINE_LIMIT)
        active_streams.clear()
        active_streams.update(new_streams)
        log.info("Tracking %d streams", len(active_streams))

    def _build_combined_url(self, market: str) -> str:
        streams = [s.split(":", 1)[1] for s in active_streams if s.startswith(f"{market}:")]
        if not streams:
            return ""
        joined = "/".join(streams)
        base = SPOT_WS_BASE if market == "spot" else FUTURES_WS_BASE
        return f"{base}/stream?streams={joined}"

    async def _maintain_spot(self):
        while not self._stop:
            url = self._build_combined_url("spot")
            if not url:
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                    log.info("Spot WebSocket connected (%d streams)", url.count("@"))
                    async for raw in ws:
                        await self._on_message(raw, "spot")
            except Exception as e:
                log.warning("Spot WS error: %s – reconnect in 8s", e)
                await asyncio.sleep(8)

    async def _maintain_futures(self):
        while not self._stop:
            url = self._build_combined_url("futures")
            if not url:
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                    log.info("Futures WebSocket connected (%d streams)", url.count("@"))
                    async for raw in ws:
                        await self._on_message(raw, "futures")
            except Exception as e:
                log.warning("Futures WS error: %s – reconnect in 8s", e)
                await asyncio.sleep(8)

    async def _on_message(self, raw: str, market: str):
        try:
            msg = json.loads(raw)
            data = msg.get("data") or msg
            if data.get("e") != "kline":
                return
            k = data["k"]
            symbol = k["s"]
            is_closed = k["x"]
            close_price = float(k["c"])
            buf_key = key(symbol, market)
            if buf_key not in candle_buffers:
                return
            buf = candle_buffers[buf_key]
            if is_closed:
                if len(buf) == 0 or buf[-1] != close_price:
                    buf.append(close_price)
                await self._evaluate(symbol, market, list(buf), close_price)
            else:
                if buf:
                    buf[-1] = close_price
        except Exception as e:
            log.debug("Message parse error: %s", e)

    async def _evaluate(self, symbol: str, market: str, closes: List[float], price: float):
        info = ticker_info.get(key(symbol, market), {})
        sig = check_signal(
            symbol=symbol,
            market=market,
            closes=closes,
            last_price=price,
            change24h=info.get("change24h", 0.0),
            volume=info.get("quoteVolume", 0.0),
        )
        if sig:
            log.info(
                "LIVE SIGNAL %s %s (%s) conf=%.1f RSI=%.1f",
                sig["side"], symbol, market, sig["confidence"], sig["rsi"],
            )
            msg = format_telegram(sig)
            if telegram_send(msg):
                log.info("Telegram delivered for %s", symbol)
            else:
                log.error("Telegram delivery failed for %s", symbol)

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
        raise SystemExit(1)

    log.info("=" * 60)
    log.info("Binance WebSocket Scanner starting (Railway)")
    log.info("Interval : %s | Pairs: %d Spot + %d Futures", KLINE_INTERVAL, TOP_N_SPOT, TOP_N_FUTURES)
    log.info("Chat ID  : %s", TELEGRAM_CHAT_ID)
    log.info("=" * 60)

    telegram_send(
        "🤖 <b>WebSocket Signal Bot Online</b>\n"
        f"Live <code>{KLINE_INTERVAL}</code> candles · Top {TOP_N_SPOT + TOP_N_FUTURES} USDT pairs\n"
        "Running on Railway"
    )

    mgr = StreamManager()
    await mgr.start()

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown")
