#!/usr/bin/env python3
"""
Binance USDT-M Futures Signal + Trade Tracker Bot
=================================================
- Futures only, top 200 pairs by volume
- Indicators: RSI, EMA20/50, MACD, VWAP
- Opens virtual trades on signal
- Live monitors mark price → SL / TP1 / TP2
- Telegram notify on open + on close (hit)
- Removes closed trades from active list
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Set

import requests
import websockets

# ---------------------------------------------------------------------------
# ENV CONFIG
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

TOP_N_FUTURES     = int(os.environ.get("TOP_N_FUTURES", "200"))
TOP_PAIRS_REFRESH = int(os.environ.get("TOP_PAIRS_REFRESH", "300"))   # 5 min
KLINE_INTERVAL    = os.environ.get("KLINE_INTERVAL", "1h")
KLINE_LIMIT       = 120
SIGNAL_COOLDOWN   = int(os.environ.get("SIGNAL_COOLDOWN", "3600"))    # 1h same symbol/side
REST_SCAN_EVERY   = int(os.environ.get("REST_SCAN_EVERY", "2700"))    # 45 min
MIN_CONFIDENCE    = float(os.environ.get("MIN_CONFIDENCE", "50"))     # below this → no open / remove

RSI_BULL = (40, 70)
RSI_BEAR = (30, 60)
SL_PCT   = 0.015
TP1_PCT  = 0.030
TP2_PCT  = 0.045

EXCLUDE_KEYWORDS = ("UP", "DOWN", "BEAR", "BULL", "3L", "3S", "5L", "5S")

FUTURES_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]
FUTURES_WS = "wss://fstream.binance.com"

STATE_FILE = "/tmp/open_trades.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("futures-bot")

_working_base: Optional[str] = None
seen_signals: Dict[str, float] = {}

# ---------------------------------------------------------------------------
# TRADE MODEL
# ---------------------------------------------------------------------------
@dataclass
class OpenTrade:
    id: str
    symbol: str
    side: str                  # LONG / SHORT
    entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    opened_at: str
    rsi: float = 0.0
    vwap: float = 0.0
    confidence: float = 0.0
    tp1_hit: bool = False
    status: str = "OPEN"       # OPEN / CLOSED
    close_reason: str = ""
    close_price: float = 0.0
    closed_at: str = ""
    last_price: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "OpenTrade":
        return OpenTrade(**{k: d[k] for k in OpenTrade.__dataclass_fields__ if k in d})


open_trades: Dict[str, OpenTrade] = {}   # id -> trade
candle_data: Dict[str, Dict[str, Deque]] = {}
ticker_info: Dict[str, Dict] = {}
active_kline_streams: Set[str] = set()
price_watch_symbols: Set[str] = set()    # symbols with open trades

# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------
def telegram_send(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Telegram credentials missing")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=12,
        )
        if r.status_code != 200:
            log.error("Telegram: %s", r.text[:250])
            return False
        return True
    except Exception as e:
        log.error("Telegram error: %s", e)
        return False

# ---------------------------------------------------------------------------
# PERSISTENCE
# ---------------------------------------------------------------------------
def save_trades():
    try:
        data = {tid: t.to_dict() for tid, t in open_trades.items() if t.status == "OPEN"}
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning("Save trades failed: %s", e)

def load_trades():
    global open_trades
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE) as f:
            data = json.load(f)
        for tid, d in data.items():
            t = OpenTrade.from_dict(d)
            if t.status == "OPEN":
                open_trades[tid] = t
                price_watch_symbols.add(t.symbol)
        log.info("Loaded %d open trades from disk", len(open_trades))
    except Exception as e:
        log.warning("Load trades failed: %s", e)

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
    gains = [max(d, 0.0) for d in deltas]
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

def macd(closes: List[float], fast=12, slow=26, signal=9):
    ef, es = ema(closes, fast), ema(closes, slow)
    line = [(f - s) if f is not None and s is not None else None for f, s in zip(ef, es)]
    valid = [v for v in line if v is not None]
    if len(valid) < signal:
        n = len(closes)
        return line, [None] * n, [None] * n
    sig = ema(valid, signal)
    pad = len(line) - len(valid)
    signal_line = [None] * pad + sig
    hist = [(m - s) if m is not None and s is not None else None for m, s in zip(line, signal_line)]
    return line, signal_line, hist

def vwap_calc(highs, lows, closes, volumes) -> List[float]:
    cum_tp_vol = 0.0
    cum_vol = 0.0
    out = []
    for i in range(len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3.0
        vol = max(0.0, volumes[i])
        cum_tp_vol += tp * vol
        cum_vol += vol
        out.append(cum_tp_vol / cum_vol if cum_vol > 0 else closes[i])
    return out

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def http_get(url: str, timeout: int = 15) -> Any:
    last_err = None
    for attempt in range(3):
        try:
            r = requests.get(
                url, timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0 SignalBot/4.0", "Accept": "application/json"},
            )
            if r.status_code == 451:
                raise requests.HTTPError(f"451 {url}", response=r)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            log.warning("GET %s (%d): %s", url, attempt + 1, e)
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Failed {url}: {last_err}")

def http_get_futures(path: str) -> Any:
    global _working_base
    bases = list(FUTURES_BASES)
    if _working_base:
        bases = [_working_base] + [b for b in bases if b != _working_base]
    last_err = None
    for base in bases:
        try:
            data = http_get(f"{base}{path}")
            if _working_base != base:
                log.info("Futures API via: %s", base)
                _working_base = base
            return data
        except Exception as e:
            last_err = e
            log.warning("Base %s failed: %s", base, e)
    raise RuntimeError(f"All futures bases failed: {last_err}")

def get_top_futures(n: int) -> List[Dict]:
    tickers = http_get_futures("/fapi/v1/ticker/24hr")
    pairs = []
    for t in tickers:
        sym = t["symbol"]
        if not sym.endswith("USDT"):
            continue
        if any(k in sym for k in EXCLUDE_KEYWORDS):
            continue
        try:
            price = float(t["lastPrice"])
            vol = float(t["quoteVolume"])
            chg = float(t.get("priceChangePercent", 0))
        except (KeyError, ValueError):
            continue
        if price <= 0 or vol <= 0:
            continue
        pairs.append({"symbol": sym, "price": price, "quoteVolume": vol, "change24h": chg})
    pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
    return pairs[:n]

def bootstrap_ohlcv(symbol: str):
    raw = http_get_futures(
        f"/fapi/v1/klines?symbol={symbol}&interval={KLINE_INTERVAL}&limit={KLINE_LIMIT}"
    )
    highs = [float(k[2]) for k in raw]
    lows = [float(k[3]) for k in raw]
    closes = [float(k[4]) for k in raw]
    volumes = [float(k[5]) for k in raw]
    return highs, lows, closes, volumes

# ---------------------------------------------------------------------------
# SIGNAL → OPEN TRADE
# ---------------------------------------------------------------------------
def sig_hash(symbol: str, side: str) -> str:
    return hashlib.sha256(f"{symbol}|{side}|{KLINE_INTERVAL}".encode()).hexdigest()[:16]

def is_dup(h: str) -> bool:
    now = time.time()
    for k in [k for k, ts in seen_signals.items() if now - ts > SIGNAL_COOLDOWN]:
        del seen_signals[k]
    return h in seen_signals

def mark_seen(h: str):
    seen_signals[h] = time.time()

def score_conf(side, rsi_v, hist, spread, vwap_dist) -> float:
    mid = 55 if side == "LONG" else 45
    rsi_s = max(0.0, 100 - abs(rsi_v - mid) * 2.5)
    hist_s = min(100.0, abs(hist) * 400)
    ema_s = min(100.0, abs(spread) * 180)
    vwap_s = max(0.0, min(100.0, 50 + vwap_dist * 15))
    return round(0.30 * rsi_s + 0.25 * hist_s + 0.25 * ema_s + 0.20 * vwap_s, 1)

def analyse(symbol: str, highs, lows, closes, volumes, last_price: float, change24h: float, vol_q: float) -> Optional[OpenTrade]:
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r14 = rsi(closes, 14)
    _, _, hist = macd(closes)
    vw = vwap_calc(highs, lows, closes, volumes)

    e20v, e50v, rv, hv, vwv = e20[-1], e50[-1], r14[-1], hist[-1], vw[-1]
    if None in (e20v, e50v, rv, hv) or vwv is None:
        return None

    price = closes[-1]
    spread = (e20v - e50v) / e50v * 100
    vwap_dist = (price - vwv) / vwv * 100

    side = None
    if e20v > e50v and RSI_BULL[0] <= rv <= RSI_BULL[1] and hv > 0 and price > vwv:
        side = "LONG"
    elif e20v < e50v and RSI_BEAR[0] <= rv <= RSI_BEAR[1] and hv < 0 and price < vwv:
        side = "SHORT"
    if not side:
        return None

    h = sig_hash(symbol, side)
    if is_dup(h):
        return None

    # already have open trade on this symbol?
    for t in open_trades.values():
        if t.symbol == symbol and t.status == "OPEN":
            return None

    entry = last_price if last_price > 0 else price
    if side == "LONG":
        sl = round(entry * (1 - SL_PCT), 8)
        tp1 = round(entry * (1 + TP1_PCT), 8)
        tp2 = round(entry * (1 + TP2_PCT), 8)
    else:
        sl = round(entry * (1 + SL_PCT), 8)
        tp1 = round(entry * (1 - TP1_PCT), 8)
        tp2 = round(entry * (1 - TP2_PCT), 8)

    conf = score_conf(side, rv, hv, spread, vwap_dist if side == "LONG" else -vwap_dist)
    if conf < MIN_CONFIDENCE:
        log.info("Skip %s %s — confidence %.1f < min %.1f", side, symbol, conf, MIN_CONFIDENCE)
        return None

    mark_seen(h)
    tid = f"{symbol}-{side}-{int(time.time())}"

    return OpenTrade(
        id=tid,
        symbol=symbol,
        side=side,
        entry=entry,
        stop_loss=sl,
        take_profit_1=tp1,
        take_profit_2=tp2,
        opened_at=datetime.now(timezone.utc).isoformat(),
        rsi=round(rv, 2),
        vwap=round(vwv, 8),
        confidence=conf,
        last_price=entry,
    )

def format_open(t: OpenTrade) -> str:
    emoji = "🚀" if t.side == "LONG" else "🔻"
    return (
        f"{emoji} <b>#{t.symbol}</b> — <b>{t.side}</b> OPEN\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 Market: <code>FUTURES</code>\n"
        f"⏱ TF: <code>{KLINE_INTERVAL}</code> · VWAP confirmed\n\n"
        f"📥 Entry: <code>{t.entry}</code>\n"
        f"🛑 SL: <code>{t.stop_loss}</code> (−1.5%)\n"
        f"🎯 TP1: <code>{t.take_profit_1}</code> (+3% · 1:2)\n"
        f"🎯 TP2: <code>{t.take_profit_2}</code> (+4.5% · 1:3)\n\n"
        f"📊 RSI: <code>{t.rsi}</code> · Conf: <code>{t.confidence}</code>\n"
        f"📍 VWAP: <code>{t.vwap}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Live monitoring · Not financial advice</i>"
    )

def format_close(t: OpenTrade, reason: str, price: float) -> str:
    emoji = "✅" if "TP" in reason else "🛑"
    pnl_pct = 0.0
    if t.side == "LONG":
        pnl_pct = (price - t.entry) / t.entry * 100
    else:
        pnl_pct = (t.entry - price) / t.entry * 100
    sign = "+" if pnl_pct >= 0 else ""
    return (
        f"{emoji} <b>#{t.symbol}</b> — <b>{t.side}</b> CLOSED\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Reason: <b>{reason}</b>\n"
        f"📥 Entry: <code>{t.entry}</code>\n"
        f"📤 Exit: <code>{price}</code>\n"
        f"📈 PnL: <code>{sign}{pnl_pct:.2f}%</code>\n"
        f"⏱ Opened: <code>{t.opened_at[:19]}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

def format_tp1_partial(t: OpenTrade, price: float) -> str:
    return (
        f"🎯 <b>#{t.symbol}</b> — TP1 HIT ({t.side})\n"
        f"Entry <code>{t.entry}</code> → Price <code>{price}</code>\n"
        f"Still open · watching TP2 / SL"
    )

def format_low_confidence(t: OpenTrade, new_conf: float, price: float) -> str:
    return (
        f"⚠️ <b>#{t.symbol}</b> — <b>{t.side}</b> REMOVED\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Reason: <b>LOW CONFIDENCE</b>\n"
        f"📉 Confidence: <code>{t.confidence}</code> → <code>{new_conf:.1f}</code> "
        f"(min <code>{MIN_CONFIDENCE}</code>)\n"
        f"📥 Entry: <code>{t.entry}</code>\n"
        f"📤 Last price: <code>{price}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Setup weakened · trade removed from list</i>"
    )

def reevaluate_open_trade(t: OpenTrade, highs, lows, closes, volumes) -> Optional[float]:
    """
    Recompute confidence for an open trade.
    Returns new confidence, or None if data insufficient.
    """
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r14 = rsi(closes, 14)
    _, _, hist = macd(closes)
    vw = vwap_calc(highs, lows, closes, volumes)
    e20v, e50v, rv, hv, vwv = e20[-1], e50[-1], r14[-1], hist[-1], vw[-1]
    if None in (e20v, e50v, rv, hv) or vwv is None:
        return None
    price = closes[-1]
    spread = (e20v - e50v) / e50v * 100
    vwap_dist = (price - vwv) / vwv * 100

    # Setup still valid?
    valid = False
    if t.side == "LONG" and e20v > e50v and hv > 0 and price > vwv:
        valid = True
    elif t.side == "SHORT" and e20v < e50v and hv < 0 and price < vwv:
        valid = True

    if not valid:
        # treat as very low confidence when structure broken
        return 0.0

    return score_conf(
        t.side, rv, hv, spread,
        vwap_dist if t.side == "LONG" else -vwap_dist,
    )

# ---------------------------------------------------------------------------
# PRICE CHECK (SL / TP)
# ---------------------------------------------------------------------------
def check_trade_levels(t: OpenTrade, price: float) -> Optional[str]:
    """Returns close reason or None. May send TP1 partial notice."""
    t.last_price = price
    if t.side == "LONG":
        if price <= t.stop_loss:
            return "STOP LOSS"
        if not t.tp1_hit and price >= t.take_profit_1:
            t.tp1_hit = True
            telegram_send(format_tp1_partial(t, price))
            save_trades()
            # keep open for TP2
        if price >= t.take_profit_2:
            return "TAKE PROFIT 2"
        if t.tp1_hit and price >= t.take_profit_1:
            # already notified TP1; wait for TP2 or SL
            pass
    else:  # SHORT
        if price >= t.stop_loss:
            return "STOP LOSS"
        if not t.tp1_hit and price <= t.take_profit_1:
            t.tp1_hit = True
            telegram_send(format_tp1_partial(t, price))
            save_trades()
        if price <= t.take_profit_2:
            return "TAKE PROFIT 2"
    return None

def close_trade(t: OpenTrade, reason: str, price: float):
    t.status = "CLOSED"
    t.close_reason = reason
    t.close_price = price
    t.closed_at = datetime.now(timezone.utc).isoformat()
    telegram_send(format_close(t, reason, price))
    log.info("CLOSED %s %s @ %s (%s)", t.side, t.symbol, price, reason)
    if t.id in open_trades:
        del open_trades[t.id]
    # if no more open trades on this symbol, stop watching
    still = any(x.symbol == t.symbol and x.status == "OPEN" for x in open_trades.values())
    if not still:
        price_watch_symbols.discard(t.symbol)
    save_trades()

def open_new_trade(t: OpenTrade):
    open_trades[t.id] = t
    price_watch_symbols.add(t.symbol)
    save_trades()
    telegram_send(format_open(t))
    log.info("OPENED %s %s entry=%s conf=%.1f", t.side, t.symbol, t.entry, t.confidence)

# ---------------------------------------------------------------------------
# SCAN + WS
# ---------------------------------------------------------------------------
async def refresh_pairs_and_bootstrap():
    log.info("Refreshing top %d Futures pairs…", TOP_N_FUTURES)
    pairs = get_top_futures(TOP_N_FUTURES)
    log.info("Got %d pairs", len(pairs))
    new_streams: Set[str] = set()

    for i, p in enumerate(pairs):
        sym = p["symbol"]
        ticker_info[sym] = p
        new_streams.add(f"{sym.lower()}@kline_{KLINE_INTERVAL}")

        if sym not in candle_data:
            try:
                highs, lows, closes, volumes = bootstrap_ohlcv(sym)
                candle_data[sym] = {
                    "h": deque(highs, maxlen=KLINE_LIMIT),
                    "l": deque(lows, maxlen=KLINE_LIMIT),
                    "c": deque(closes, maxlen=KLINE_LIMIT),
                    "v": deque(volumes, maxlen=KLINE_LIMIT),
                }
                # evaluate on bootstrap
                trade = analyse(sym, highs, lows, closes, volumes, p["price"], p["change24h"], p["quoteVolume"])
                if trade:
                    open_new_trade(trade)
            except Exception as e:
                log.warning("Bootstrap %s: %s", sym, e)
                candle_data[sym] = {
                    "h": deque(maxlen=KLINE_LIMIT),
                    "l": deque(maxlen=KLINE_LIMIT),
                    "c": deque(maxlen=KLINE_LIMIT),
                    "v": deque(maxlen=KLINE_LIMIT),
                }
            if (i + 1) % 40 == 0:
                log.info("Bootstrapped %d/%d", i + 1, len(pairs))
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.08)

    active_kline_streams.clear()
    active_kline_streams.update(new_streams)
    log.info("Tracking %d kline streams · open trades: %d", len(active_kline_streams), len(open_trades))

async def pairs_loop():
    while True:
        try:
            await refresh_pairs_and_bootstrap()
        except Exception as e:
            log.exception("Pairs refresh error: %s", e)
            await asyncio.sleep(90)
            continue
        await asyncio.sleep(TOP_PAIRS_REFRESH)

async def rest_rescan_loop():
    await asyncio.sleep(120)
    while True:
        try:
            log.info("REST rescan of tracked symbols…")
            found = 0
            removed = 0

            # 1) Re-check OPEN trades → remove if confidence dropped
            for tid in list(open_trades.keys()):
                t = open_trades.get(tid)
                if not t or t.status != "OPEN":
                    continue
                try:
                    highs, lows, closes, volumes = bootstrap_ohlcv(t.symbol)
                    candle_data[t.symbol] = {
                        "h": deque(highs, maxlen=KLINE_LIMIT),
                        "l": deque(lows, maxlen=KLINE_LIMIT),
                        "c": deque(closes, maxlen=KLINE_LIMIT),
                        "v": deque(volumes, maxlen=KLINE_LIMIT),
                    }
                    new_conf = reevaluate_open_trade(t, highs, lows, closes, volumes)
                    price = closes[-1] if closes else t.last_price
                    if new_conf is not None and new_conf < MIN_CONFIDENCE:
                        log.info(
                            "LOW CONF %s %s: %.1f → %.1f (min %.1f) — removing",
                            t.side, t.symbol, t.confidence, new_conf, MIN_CONFIDENCE,
                        )
                        telegram_send(format_low_confidence(t, new_conf, price))
                        t.status = "CLOSED"
                        t.close_reason = "LOW CONFIDENCE"
                        t.close_price = price
                        t.closed_at = datetime.now(timezone.utc).isoformat()
                        if tid in open_trades:
                            del open_trades[tid]
                        still = any(
                            x.symbol == t.symbol and x.status == "OPEN"
                            for x in open_trades.values()
                        )
                        if not still:
                            price_watch_symbols.discard(t.symbol)
                        removed += 1
                        save_trades()
                    await asyncio.sleep(0.1)
                except Exception as e:
                    log.debug("Reeval %s: %s", t.symbol, e)

            # 2) Scan for new setups
            for sym in list(candle_data.keys()):
                try:
                    highs, lows, closes, volumes = bootstrap_ohlcv(sym)
                    candle_data[sym] = {
                        "h": deque(highs, maxlen=KLINE_LIMIT),
                        "l": deque(lows, maxlen=KLINE_LIMIT),
                        "c": deque(closes, maxlen=KLINE_LIMIT),
                        "v": deque(volumes, maxlen=KLINE_LIMIT),
                    }
                    info = ticker_info.get(sym, {})
                    trade = analyse(
                        sym, highs, lows, closes, volumes,
                        info.get("price", closes[-1]),
                        info.get("change24h", 0),
                        info.get("quoteVolume", 0),
                    )
                    if trade:
                        open_new_trade(trade)
                        found += 1
                    await asyncio.sleep(0.1)
                except Exception as e:
                    log.debug("Rescan %s: %s", sym, e)

            log.info(
                "REST rescan done · new: %d · removed(low conf): %d · open: %d",
                found, removed, len(open_trades),
            )
        except Exception as e:
            log.exception("REST rescan error: %s", e)
        await asyncio.sleep(REST_SCAN_EVERY)

def build_kline_url() -> str:
    streams = list(active_kline_streams)[:150]  # safety limit
    if not streams:
        return ""
    return f"{FUTURES_WS}/stream?streams=" + "/".join(streams)

def build_mark_price_url() -> str:
    """Subscribe to markPrice for symbols with open trades."""
    if not price_watch_symbols:
        return ""
    streams = [f"{s.lower()}@markPrice@1s" for s in list(price_watch_symbols)[:100]]
    return f"{FUTURES_WS}/stream?streams=" + "/".join(streams)

async def kline_ws_loop():
    while True:
        url = build_kline_url()
        if not url:
            await asyncio.sleep(5)
            continue
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                log.info("Kline WS connected (%d streams)", url.count("@"))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data") or msg
                        if data.get("e") != "kline":
                            continue
                        k = data["k"]
                        sym = k["s"]
                        if sym not in candle_data:
                            continue
                        high, low, close, vol = float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"])
                        buf = candle_data[sym]
                        if k["x"]:  # closed candle
                            buf["h"].append(high)
                            buf["l"].append(low)
                            buf["c"].append(close)
                            buf["v"].append(vol)
                            info = ticker_info.get(sym, {})
                            trade = analyse(
                                sym, list(buf["h"]), list(buf["l"]), list(buf["c"]), list(buf["v"]),
                                close, info.get("change24h", 0), info.get("quoteVolume", 0),
                            )
                            if trade:
                                open_new_trade(trade)
                        else:
                            if buf["c"]:
                                buf["h"][-1], buf["l"][-1], buf["c"][-1], buf["v"][-1] = high, low, close, vol
                    except Exception as e:
                        log.debug("Kline msg: %s", e)
        except Exception as e:
            log.warning("Kline WS error: %s — reconnect 8s", e)
            await asyncio.sleep(8)

async def mark_price_ws_loop():
    """Live SL/TP monitoring for open trades."""
    while True:
        url = build_mark_price_url()
        if not url:
            await asyncio.sleep(3)
            continue
        try:
            async with websockets.connect(url, ping_interval=15, ping_timeout=40) as ws:
                log.info("MarkPrice WS connected · watching %d symbols", len(price_watch_symbols))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data") or msg
                        # markPriceUpdate
                        sym = data.get("s")
                        if not sym:
                            continue
                        price = float(data.get("p") or data.get("markPrice") or 0)
                        if price <= 0:
                            continue
                        # check all open trades on this symbol
                        for tid in list(open_trades.keys()):
                            t = open_trades.get(tid)
                            if not t or t.symbol != sym or t.status != "OPEN":
                                continue
                            reason = check_trade_levels(t, price)
                            if reason:
                                close_trade(t, reason, price)
                    except Exception as e:
                        log.debug("MarkPrice msg: %s", e)
        except Exception as e:
            log.warning("MarkPrice WS error: %s — reconnect 5s", e)
            await asyncio.sleep(5)

async def status_loop():
    while True:
        await asyncio.sleep(600)
        n = len(open_trades)
        if n:
            lines = [f"· {t.side} {t.symbol} @ {t.entry} (last {t.last_price})" for t in list(open_trades.values())[:15]]
            telegram_send(
                f"📋 <b>Open trades: {n}</b>\n" + "\n".join(lines)
                + (f"\n… +{n-15} more" if n > 15 else "")
            )
        log.info("Status · open trades: %d · watched: %d · streams: %d",
                 n, len(price_watch_symbols), len(active_kline_streams))

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        raise SystemExit(1)

    load_trades()

    log.info("=" * 60)
    log.info("Futures Signal + Trade Tracker starting")
    log.info("Pairs: top %d | TF: %s | Cooldown: %ds", TOP_N_FUTURES, KLINE_INTERVAL, SIGNAL_COOLDOWN)
    log.info("Open trades loaded: %d", len(open_trades))
    log.info("=" * 60)

    telegram_send(
        "🤖 <b>Futures Trade Bot Online</b>\n"
        f"Scanning top <code>{TOP_N_FUTURES}</code> USDT-M pairs\n"
        f"TF: <code>{KLINE_INTERVAL}</code> · RSI · EMA · MACD · VWAP\n"
        f"Min confidence: <code>{MIN_CONFIDENCE}</code> (below → remove + notify)\n"
        f"Live SL/TP monitoring · Open trades: <code>{len(open_trades)}</code>"
    )

    await asyncio.gather(
        pairs_loop(),
        kline_ws_loop(),
        mark_price_ws_loop(),
        rest_rescan_loop(),
        status_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown")
        save_trades()
