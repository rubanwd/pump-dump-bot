#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit USDT-Perp Impulse Scanner — WebSocket v5 public (linear)
Soft-логика: 2 из 3 условий (цена/объём/RSI), TIER A/B, дельта/дисбаланс как подтверждение.
Каналы:
- kline.1.<SYMBOL>        — триггер по закрытию 1m
- publicTrade.<SYMBOL>    — поток сделок для дельты/кластера

Пороги дельты смягчаются, если сигнал TIER A.
"""

import os
import asyncio
import json
import math
import time
import logging
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import requests
import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv

BYBIT_BASE = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

logger = logging.getLogger("impulse")

# -------- utils --------

def ts_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0

def ema(x: np.ndarray, n: int) -> np.ndarray:
    return pd.Series(x).ewm(span=n, adjust=False).mean().values

def macd_hist(close: np.ndarray, fast=12, slow=26, signal=9) -> Tuple[float, float, float]:
    if len(close) < slow + signal + 1:
        return (np.nan, np.nan, np.nan)
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
    hist = macd_line - signal_line
    return macd_line[-1], signal_line[-1], hist[-1]

def rsi(series: np.ndarray, length: int = 14) -> float:
    if len(series) < length + 1:
        return np.nan
    deltas = np.diff(series)
    ups = deltas.clip(min=0)
    downs = -deltas.clip(max=0)
    ma_up = pd.Series(ups).rolling(length).mean().iloc[-1]
    ma_down = pd.Series(downs).rolling(length).mean().iloc[-1]
    if ma_down == 0 or np.isnan(ma_up) or np.isnan(ma_down):
        return 100.0 if (ma_down == 0 and ma_up > 0) else 0.0
    rs = ma_up / ma_down
    return 100.0 - (100.0 / (1.0 + rs))

def interval_to_bybit(interval_str: str) -> str:
    m = interval_str.lower().strip()
    if m.endswith("m"): return m[:-1]
    if m.endswith("h"): return str(int(m[:-1]) * 60)
    if m in ("1d","d"): return "D"
    return "1"

def tg_send(token: str, chat_id: str, text: str, disable_web_page_preview=True):
    if not token or not chat_id:
        logger.warning("Telegram not configured (skip)")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id": chat_id, "text": text, "parse_mode":"HTML",
                            "disable_web_page_preview": disable_web_page_preview}, timeout=10)
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")

# -------- universe via REST --------

def load_universe(universe_max: int, min_notional: float) -> List[str]:
    r = requests.get(f"{BYBIT_BASE}/v5/market/tickers", params={"category":"linear"}, timeout=10)
    data = r.json()
    items = data.get("result",{}).get("list",[]) if data.get("retCode")==0 else []
    rows = []
    for t in items:
        sym = t.get("symbol","")
        if not sym.endswith("USDT"):
            continue
        base = sym.replace("USDT","")
        if base in {"BTC","ETH","USDT"}:
            continue
        try:
            turnover24h = float(t.get("turnover24h", "0"))
        except:
            turnover24h = 0.0
        if turnover24h >= min_notional:
            rows.append((sym, turnover24h))
    rows.sort(key=lambda x:x[1], reverse=True)
    return [s for s,_ in rows[:universe_max]]

def fetch_ohlcv(symbol: str, interval_str: str, limit: int=350) -> Optional[pd.DataFrame]:
    """Через REST (разово) — чтобы считать RSI/MACD/Δ по закрытой 1m свече."""
    intr = interval_to_bybit(interval_str)
    r = requests.get(f"{BYBIT_BASE}/v5/market/kline",
                     params={"category":"linear","symbol":symbol,"interval":intr,"limit":str(limit)},
                     timeout=10)
    d = r.json()
    if d.get("retCode") != 0:
        return None
    kl = d.get("result",{}).get("list",[])
    if not kl or len(kl) < 60:
        return None
    rows=[]
    for rec in kl:
        ts=int(rec[0]); o=float(rec[1]); h=float(rec[2]); l=float(rec[3]); c=float(rec[4]); v=float(rec[5]); to=float(rec[6])
        rows.append((ts,o,h,l,c,v,to))
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","turnover"])
    df["dt"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
    return df

# -------- WS state (дельта по сделкам) --------

class DeltaWindow:
    """Хранит сделки за N секунд и даёт buy/sell USDT сумму + дельту."""
    def __init__(self, window_sec: int):
        self.window_sec = window_sec
        self.q: deque[Tuple[float,float,float]] = deque()  # (time_ts, buy_usdt, sell_usdt)

    def push_trade(self, ts: float, side: str, price: float, size: float):
        usdt = float(price) * float(size)
        self.q.append((ts, usdt if side=="Buy" else 0.0, usdt if side=="Sell" else 0.0))
        self._trim()

    def _trim(self):
        cut = time.time() - self.window_sec
        while self.q and self.q[0][0] < cut:
            self.q.popleft()

    def snapshot(self) -> Tuple[float,float,float]:
        self._trim()
        b = sum(x[1] for x in self.q)
        s = sum(x[2] for x in self.q)
        d = b - s
        return b, s, d

# -------- core signal check from df(1m) --------

def analyze_1m_soft(df_1m: pd.DataFrame, lookback_min:int, vol_sma:int,
                    rsi_len:int, rsi_high:float, rsi_low:float,
                    use_macd:bool, min_pct_move:float, min_vol_mult:float) -> Tuple[bool, Dict, str]:
    """2 из 3 условий + TIER A/B, MACD усиливает, но не блокирует."""
    if len(df_1m) < vol_sma + lookback_min + 3:
        return False, {}, "not_enough_history"
    close = df_1m["close"].values
    high  = df_1m["high"].values
    low   = df_1m["low"].values
    turn  = df_1m["turnover"].values

    last_close = close[-2]; last_high=high[-2]; last_low=low[-2]; last_turn=turn[-2]
    vol_sma_v = pd.Series(turn[:-1]).rolling(vol_sma).mean().iloc[-1]
    if vol_sma_v==0 or math.isnan(vol_sma_v):
        return False, {}, "vol_sma_nan"
    vol_mult = last_turn / vol_sma_v

    ref_close = close[-(lookback_min+2)]
    move_pct = pct(last_close, ref_close)
    rsi_v = rsi(close[:-1], rsi_len)
    _m, _s, hist = macd_hist(close[:-1])

    cond_price_up   = move_pct >= min_pct_move
    cond_price_down = -move_pct >= min_pct_move
    cond_vol        = vol_mult >= min_vol_mult
    cond_rsi_up     = rsi_v >= rsi_high
    cond_rsi_down   = rsi_v <= rsi_low

    score_up   = int(cond_price_up)   + int(cond_vol) + int(cond_rsi_up)
    score_down = int(cond_price_down) + int(cond_vol) + int(cond_rsi_down)

    direction=None; score=0
    if score_up >= 2 and score_up >= score_down:
        direction="PUMP"; score=score_up
    elif score_down >= 2 and score_down > score_up:
        direction="DUMP"; score=score_down
    else:
        return False, {}, "need_2_of_3"

    macd_boost = 0
    if not math.isnan(hist):
        if direction=="PUMP" and hist>0: macd_boost=1
        if direction=="DUMP" and hist<0: macd_boost=1
    tier = "A" if score==3 else "B"

    body = last_high - last_low
    if body<=0:
        return False, {}, "zero_body"

    if direction=="PUMP":
        fib236= last_high - 0.236*body
        fib382= last_high - 0.382*body
        fib500= last_high - 0.500*body
        entry_hint=f"{fib382:.6f}–{fib236:.6f} (аггр.) или {fib500:.6f}–{fib382:.6f} (конс.)"
    else:
        fib236= last_low + 0.236*body
        fib382= last_low + 0.382*body
        fib500= last_low + 0.500*body
        entry_hint=f"{fib236:.6f}–{fib382:.6f} (аггр.) или {fib382:.6f}–{fib500:.6f} (конс.)"

    details = {
        "direction": direction,
        "tier": tier,
        "score": score,
        "macd_boost": macd_boost,
        "move_pct": move_pct,
        "vol_mult": vol_mult,
        "rsi": rsi_v,
        "macd_hist": hist,
        "last_close": float(last_close),
        "last_high": float(last_high),
        "last_low": float(last_low),
        "entry_hint": entry_hint,
    }
    return True, details, ""

# -------- runner --------

async def ws_loop(symbols: List[str], cfg: Dict[str, float], tg: Dict[str,str],
                  tech_params: Dict, dedup_minutes:int, sanity_drift: float):
    """
    symbols: список "ALTUSDT"
    cfg: параметры delta/imbalance окна
    tg: telegram tokens
    tech_params: тех. индикаторы (RSI/MACD/thresholds)
    sanity_drift: допустимый контрдрейф на старшем ТФ (в %) — неригористично, проверяем 3m on demand
    """
    deltas: Dict[str, DeltaWindow] = {s: DeltaWindow(cfg["WS_DELTA_WINDOW_SEC"]) for s in symbols}
    last_signal_at: Dict[str, datetime] = {}

    subs = []
    subs.append({"op":"subscribe", "args":[f"kline.1.{s}" for s in symbols]})
    subs.append({"op":"subscribe", "args":[f"publicTrade.{s}" for s in symbols]})

    reconnect_sec = int(os.getenv("WS_RECONNECT_SEC","5"))
    heartbeat_min = int(os.getenv("WS_HEARTBEAT_MIN","5"))
    last_hb = time.time()

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                logger.info(f"WS connected. Subscribing to {len(symbols)} symbols...")
                for m in subs:
                    await ws.send(json.dumps(m))
                    await asyncio.sleep(0.05)

                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    msg = json.loads(raw)

                    if time.time() - last_hb >= heartbeat_min*60:
                        logger.info(f"[WS] alive: tracked={len(symbols)}; window={cfg['WS_DELTA_WINDOW_SEC']}s")
                        last_hb = time.time()

                    # trades
                    if msg.get("topic","").startswith("publicTrade."):
                        sym = msg["topic"].split(".")[1]
                        data = msg.get("data",[])
                        for tr in data:
                            ts = float(tr.get("T", int(time.time()*1000)))/1000.0
                            side = tr.get("S") or tr.get("side")
                            price = float(tr.get("p"))
                            size  = float(tr.get("v"))
                            deltas[sym].push_trade(ts, side, price, size)
                        continue

                    # 1m kline close
                    if msg.get("topic","").startswith("kline.1."):
                        sym = msg["topic"].split(".")[2]
                        arr = msg.get("data",[])
                        if not arr:
                            continue
                        bar = arr[-1]
                        if not bar.get("confirm", False):
                            continue

                        # закрылась 1m — подгружаем df для индикаторов
                        df_1m = fetch_ohlcv(sym, "1m", limit=max(350, tech_params["VOL_SMA"]+60))
                        if df_1m is None:
                            logger.debug(f"[{sym}] skip: no df_1m")
                            continue

                        ok, info, reason = analyze_1m_soft(
                            df_1m,
                            lookback_min=tech_params["LOOKBACK_MIN"],
                            vol_sma=tech_params["VOL_SMA"],
                            rsi_len=tech_params["RSI_LEN"],
                            rsi_high=tech_params["RSI_HIGH"],
                            rsi_low=tech_params["RSI_LOW"],
                            use_macd=tech_params["USE_MACD"],
                            min_pct_move=tech_params["MIN_PCT_MOVE"],
                            min_vol_mult=tech_params["MIN_VOL_MULT"],
                        )
                        if not ok:
                            logger.debug(f"[{sym}] no-impulse (kline close): {reason}")
                            continue

                        # лёгкий sanity на 3m: допускаем контрдвиг до sanity_drift (например, 1.0%)
                        df_3m = fetch_ohlcv(sym, "3m", limit=120)
                        if df_3m is not None and len(df_3m) >= 5:
                            closes = df_3m["close"].values
                            drift = pct(closes[-2], closes[-5])
                            if info["direction"] == "PUMP" and drift < -sanity_drift:
                                logger.debug(f"[{sym}] fail sanity 3m: drift={drift:.3f}% < -{sanity_drift}")
                                continue
                            if info["direction"] == "DUMP" and drift > sanity_drift:
                                logger.debug(f"[{sym}] fail sanity 3m: drift={drift:.3f}% > {sanity_drift}")
                                continue

                        # подтверждение по дельте за окно
                        buy_usdt, sell_usdt, delta_usdt = deltas[sym].snapshot()
                        abs_delta = abs(delta_usdt)
                        imbalance = (abs(buy_usdt) / max(1.0, abs(sell_usdt))) if sell_usdt>0 else float('inf')

                        # смягчаем пороги, если TIER A
                        tier = info.get("tier","B")
                        delta_min = cfg["WS_DELTA_MIN_USDT"] * (0.8 if tier=="A" else 1.0)
                        imb_min   = cfg["WS_DELTA_IMBALANCE"] * (0.9 if tier=="A" else 1.0)

                        if abs_delta < delta_min or imbalance < imb_min:
                            logger.debug(f"[{sym}] delta weak: |Δ|={abs_delta:.0f} < {delta_min} or imb={imbalance:.2f} < {imb_min}")
                            continue

                        key = f"{sym}|{info['direction']}"
                        last = last_signal_at.get(key)
                        now = datetime.now(timezone.utc)
                        if last and (now - last) < timedelta(minutes=dedup_minutes):
                            logger.debug(f"[{sym}] dedup: recent signal exists")
                            continue
                        last_signal_at[key] = now

                        logger.info(
                            f"[{sym}] SIGNAL {info['direction']} | TIER={info['tier']} (score={info['score']}{' +MACD' if info.get('macd_boost') else ''}) "
                            f"| Δ{tech_params['LOOKBACK_MIN']}m={info['move_pct']:.2f}% | turn×SMA={info['vol_mult']:.2f} | RSI={info['rsi']:.1f} | "
                            f"Δ_usdt={delta_usdt:,.0f} (buy={buy_usdt:,.0f} / sell={sell_usdt:,.0f}) | close={info['last_close']:.6f}"
                        )

                        msg_text = (
                            f"⚡️ <b>{info['direction']}</b> (WS, TIER {info['tier']}) на <b>{sym}</b>\n"
                            f"⏱ ТФ: 1m (закрытая свеча)\n"
                            f"📈 Δ за {tech_params['LOOKBACK_MIN']}m: <b>{info['move_pct']:.2f}%</b>\n"
                            f"🔊 Turnover xSMA({tech_params['VOL_SMA']}): <b>{info['vol_mult']:.2f}×</b>\n"
                            f"💪 RSI(1m): <b>{info['rsi']:.1f}</b>\n"
                            f"🧮 Сила: <b>{info['score']}/3</b>{' + MACD' if info.get('macd_boost') else ''}\n"
                            f"📊 Δ {cfg['WS_DELTA_WINDOW_SEC']}s: "
                            f"<b>buy={buy_usdt:,.0f}</b> / <b>sell={sell_usdt:,.0f}</b> / "
                            f"<b>|Δ|={abs_delta:,.0f}</b> (imb={imbalance:.2f})"
                        )
                        if not math.isnan(info["macd_hist"]):
                            msg_text += f"\n📉 MACD hist: <b>{info['macd_hist']:+.4f}</b>"
                        msg_text += (
                            f"\n\n💵 Цена(закр. 1m): <b>{info['last_close']:.6f}</b>"
                            f"\n🎯 Зона ретеста: <b>{info['entry_hint']}</b>"
                            f"\n\n🕒 {ts_now_iso()}"
                        )
                        tg_send(tg["token"], tg["chat_id"], msg_text)

        except (asyncio.TimeoutError, websockets.ConnectionClosedError, websockets.InvalidStatusCode) as e:
            reconnect_sec = int(os.getenv("WS_RECONNECT_SEC","5"))
            logger.warning(f"WS connection issue: {e}. Reconnecting in {reconnect_sec}s ...")
            await asyncio.sleep(reconnect_sec)
        except Exception as e:
            reconnect_sec = int(os.getenv("WS_RECONNECT_SEC","5"))
            logger.exception(f"WS fatal: {e}")
            await asyncio.sleep(reconnect_sec)

def run_ws_scanner():
    load_dotenv()
    # общие параметры (как в REST)
    universe_max = int(os.getenv("UNIVERSE_MAX","150"))
    min_notional = float(os.getenv("MIN_NOTIONAL_USDT","150000"))
    lookback_min = int(os.getenv("LOOKBACK_MIN","2"))
    vol_sma = int(os.getenv("VOL_SMA","20"))
    min_pct_move = float(os.getenv("MIN_PCT_MOVE","2.0"))
    min_vol_mult = float(os.getenv("MIN_VOL_MULT","3.0"))
    rsi_len = int(os.getenv("RSI_LEN","14"))
    rsi_high = float(os.getenv("RSI_HIGH","70"))
    rsi_low = float(os.getenv("RSI_LOW","30"))
    use_macd = os.getenv("USE_MACD","0")=="1"
    dedup_minutes = int(os.getenv("DEDUP_MINUTES","5"))
    sanity_drift = float(os.getenv("SANITY_DRIFT_PCT","1.0"))  # допуск контрдвижения на 3m

    # ws-специфика с более мягкими дефолтами
    cfg = {
        "WS_DELTA_WINDOW_SEC": int(os.getenv("WS_DELTA_WINDOW_SEC","8")),
        "WS_DELTA_MIN_USDT": float(os.getenv("WS_DELTA_MIN_USDT","15000")),
        "WS_DELTA_IMBALANCE": float(os.getenv("WS_DELTA_IMBALANCE","1.4")),
    }
    tg = {"token": os.getenv("TELEGRAM_BOT_TOKEN",""), "chat_id": os.getenv("TELEGRAM_CHAT_ID","")}

    logger.info("WS mode starting (soft thresholds)...")
    symbols = load_universe(universe_max, min_notional)
    logger.info(f"WS universe: {len(symbols)} symbols")
    tg_send(tg["token"], tg["chat_id"], f"🚀 <b>Impulse Scanner</b> (WS) запущен {ts_now_iso()}\nВсего в юниверсе: <b>{len(symbols)}</b> пар")

    tech_params = {
        "LOOKBACK_MIN": lookback_min,
        "VOL_SMA": vol_sma,
        "RSI_LEN": rsi_len,
        "RSI_HIGH": rsi_high,
        "RSI_LOW": rsi_low,
        "USE_MACD": use_macd,
        "MIN_PCT_MOVE": min_pct_move,
        "MIN_VOL_MULT": min_vol_mult,
    }

    asyncio.run(ws_loop(symbols, cfg, tg, tech_params, dedup_minutes, sanity_drift))
