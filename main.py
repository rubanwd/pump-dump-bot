#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit USDT-Perp Impulse Scanner — REST v5
Версия: soft (2/3) + Tier C (объёмный микро-брейкаут), расширенные логи и переключатель режимов.

Tiers:
- A: 3/3 условий (Δ% + объём + RSI)
- B: 2/3 условий
- C: объёмный микро-брейкаут (vol_mult >= vol_mult_c и пробой high/low на BREAKOUT_PCT)

Переключатель:
- SCAN_MODE=rest  — этот файл
- SCAN_MODE=ws    — см. ws_scanner.py
"""

import os
import time
import math
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

BYBIT_BASE = "https://api.bybit.com"

# ========================== logging setup ==========================

def setup_logging():
    load_dotenv()
    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    fmt = "[%(asctime)s] %(levelname)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    logger = logging.getLogger("impulse")
    logger.setLevel(level)
    if os.getenv("LOG_FILE", "0") == "1":
        os.makedirs("logs", exist_ok=True)
        fh = RotatingFileHandler("logs/bot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
        fh.setLevel(level); fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        logger.addHandler(fh)
    logger.info(f"Logging initialized: level={level_str}, file={'ON' if os.getenv('LOG_FILE','0')=='1' else 'OFF'}")
    return logger

logger = setup_logging()

# ========================== helpers ==========================

def env_list(key: str, default: str) -> List[str]:
    v = os.getenv(key, default); return [x.strip() for x in v.split(",") if x.strip()]

def pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a - b) / b * 100.0

def rsi(series: np.ndarray, length: int = 14) -> float:
    if len(series) < length + 1: return np.nan
    deltas = np.diff(series)
    ups = deltas.clip(min=0); downs = -deltas.clip(max=0)
    ma_up = pd.Series(ups).rolling(length).mean().iloc[-1]
    ma_down = pd.Series(downs).rolling(length).mean().iloc[-1]
    if ma_down == 0 or np.isnan(ma_up) or np.isnan(ma_down):
        return 100.0 if (ma_down == 0 and ma_up > 0) else 0.0
    rs = ma_up / ma_down; return 100.0 - (100.0 / (1.0 + rs))

def ema(x: np.ndarray, n: int) -> np.ndarray:
    return pd.Series(x).ewm(span=n, adjust=False).mean().values

def macd_hist(close: np.ndarray, fast=12, slow=26, signal=9) -> Tuple[float, float, float]:
    if len(close) < slow + signal + 1: return (np.nan, np.nan, np.nan)
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
    hist = macd_line - signal_line; return macd_line[-1], signal_line[-1], hist[-1]

def ts_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

# ========================== Telegram ==========================

def tg_send(token: str, chat_id: str, text: str, disable_web_page_preview=True):
    if not token or not chat_id:
        logger.warning("Telegram not configured: skip send"); return
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          data={"chat_id": chat_id, "text": text, "parse_mode":"HTML",
                                "disable_web_page_preview": disable_web_page_preview}, timeout=10)
        if r.status_code != 200: logger.error(f"Telegram error: {r.text}")
    except Exception as e:
        logger.exception(f"Telegram exception: {e}")

# ========================== Bybit REST mini-client ==========================

class BybitREST:
    def __init__(self, base=BYBIT_BASE): self.base = base; self.sess = requests.Session()
    def _get(self, path: str, params: Dict) -> Dict:
        url = self.base + path
        for attempt in range(3):
            try:
                r = self.sess.get(url, params=params, timeout=10)
                if r.status_code != 200:
                    logger.warning(f"HTTP {r.status_code} on {path} attempt={attempt+1}, retrying..."); time.sleep(0.2); continue
                return r.json()
            except Exception as e:
                logger.warning(f"GET exception {path} attempt={attempt+1}: {e}"); time.sleep(0.2)
        raise RuntimeError(f"GET failed {path} {params}")

    def get_linear_tickers(self) -> List[Dict]:
        data = self._get("/v5/market/tickers", {"category": "linear"})
        if data.get("retCode") != 0: raise RuntimeError(f"tickers error: {data}")
        return data["result"].get("list", [])

    def get_kline(self, symbol: str, interval: str, limit: int = 300) -> List[List[str]]:
        data = self._get("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":interval,"limit":str(limit)})
        if data.get("retCode") != 0: raise RuntimeError(f"kline error: {symbol} {interval} -> {data}")
        return data["result"].get("list", [])

# ========================== Scanner (REST) ==========================

class ImpulseScanner:
    def __init__(self):
        load_dotenv()
        # Telegram
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN","")
        self.chat_id   = os.getenv("TELEGRAM_CHAT_ID","")
        # Параметры
        self.universe_max = int(os.getenv("UNIVERSE_MAX","300"))
        self.cycle_sleep  = int(os.getenv("CYCLE_SLEEP_SEC","12"))
        self.timeframes   = env_list("TIMEFRAMES","1m,3m,5m")
        self.lookback_min = int(os.getenv("LOOKBACK_MIN","1"))
        self.vol_sma      = int(os.getenv("VOL_SMA","20"))
        self.min_pct_move = float(os.getenv("MIN_PCT_MOVE","1.0"))
        self.min_vol_mult = float(os.getenv("MIN_VOL_MULT","2.0"))
        self.rsi_len      = int(os.getenv("RSI_LEN","14"))
        self.rsi_high     = float(os.getenv("RSI_HIGH","68"))
        self.rsi_low      = float(os.getenv("RSI_LOW","32"))
        self.use_macd     = os.getenv("USE_MACD","0")=="1"
        self.anomaly_24h  = float(os.getenv("ANOMALY_24H_PCT_MAX","300"))
        self.min_notional = float(os.getenv("MIN_NOTIONAL_USDT","100000"))
        self.dedup_minutes= int(os.getenv("DEDUP_MINUTES","3"))
        self.sanity_drift = float(os.getenv("SANITY_DRIFT_PCT","3.0"))
        # Aggressive / Tier C
        self.aggressive   = os.getenv("AGGRESSIVE","1")=="1"
        self.br_lookback  = int(os.getenv("BREAKOUT_LOOKBACK","10"))
        self.br_pct       = float(os.getenv("BREAKOUT_PCT","0.20"))  # в %
        # Logs
        self.symbol_filter= set([s.strip().upper() for s in os.getenv("LOG_SYMBOL_FILTER","").split(",") if s.strip()])

        self.api = BybitREST()
        self.last_signal_at: Dict[str, datetime] = {}
        self.counters = { "checked":0, "sig":0, "reasons":{} }

    # ---------- Universe ----------
    def _load_universe(self) -> List[str]:
        tickers = self.api.get_linear_tickers(); universe=[]
        def f2(x): 
            try: return float(x)
            except: return 0.0
        for t in tickers:
            sym=t.get("symbol","")
            if not sym.endswith("USDT"): continue
            base = sym.replace("USDT","")
            if base in {"BTC","ETH","USDT"}: continue
            if f2(t.get("turnover24h")) >= self.min_notional:
                universe.append((sym, f2(t.get("turnover24h"))))
        universe.sort(key=lambda x:x[1], reverse=True)
        symbols = [s for s,_ in universe[:self.universe_max]]
        logger.info(f"Universe built: {len(symbols)} (min_notional={self.min_notional})")
        return symbols

    def _ticker_pct24(self, tmap: Dict[str,Dict], symbol:str)->float:
        t=tmap.get(symbol); 
        if not t: return 0.0
        p=t.get("price24hPcnt")
        if p is not None:
            try: return float(p)*100.0
            except: pass
        try:
            last=float(t.get("lastPrice",0) or 0); openp=float(t.get("openPrice24h",0) or 0)
            if last and openp: return pct(last, openp)
        except: pass
        return 0.0

    # ---------- OHLCV ----------
    @staticmethod
    def _interval_to_bybit(interval_str:str)->str:
        m=interval_str.lower().strip()
        if m.endswith("m"): return m[:-1]
        if m.endswith("h"): return str(int(m[:-1])*60)
        if m in ("1d","d"): return "D"
        return "1"

    def _fetch_ohlcv_df(self, symbol:str, tf_str:str, limit:int=300)->Optional[pd.DataFrame]:
        intr=self._interval_to_bybit(tf_str)
        try:
            kl=self.api.get_kline(symbol, intr, limit)
            if not kl or len(kl)<max(self.vol_sma+self.lookback_min+3,50):
                if logger.level<=logging.DEBUG or (self.symbol_filter and symbol in self.symbol_filter):
                    logger.debug(f"[{symbol} {tf_str}] not enough candles: {len(kl) if kl else 0}")
                return None
            rows=[]; 
            for rec in kl:
                ts=int(rec[0]); o=float(rec[1]); h=float(rec[2]); l=float(rec[3]); c=float(rec[4]); v=float(rec[5]); to=float(rec[6])
                rows.append((ts,o,h,l,c,v,to))
            df=pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","turnover"])
            df["dt"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
            return df
        except Exception as e:
            logger.warning(f"[{symbol} {tf_str}] kline error: {e}"); return None

    # ---------- Dedup ----------
    def _dedup_ok(self, key:str)->bool:
        now=datetime.now(timezone.utc); last=self.last_signal_at.get(key)
        if not last or (now-last)>=timedelta(minutes=self.dedup_minutes):
            self.last_signal_at[key]=now; return True
        return False

    # ---------- Core (2/3 + Tier C) ----------
    def _analyze_1m_impulse(self, symbol:str, df_1m:pd.DataFrame)->Tuple[bool, Dict, str]:
        if len(df_1m) < self.vol_sma + self.lookback_min + 3: return False, {}, "not_enough_history"
        close=df_1m["close"].values; high=df_1m["high"].values; low=df_1m["low"].values; turn=df_1m["turnover"].values
        last_close=close[-2]; last_high=high[-2]; last_low=low[-2]; last_turn=turn[-2]
        vol_sma = pd.Series(turn[:-1]).rolling(self.vol_sma).mean().iloc[-1]
        if vol_sma==0 or np.isnan(vol_sma): return False, {}, "vol_sma_nan"
        vol_mult= last_turn/vol_sma
        k=self.lookback_min; ref_close=close[-(k+2)]; move_pct=pct(last_close, ref_close)
        rsi_val=rsi(close[:-1], self.rsi_len)
        _m,_s,hist=macd_hist(close[:-1])

        cond_price_up   = move_pct >= self.min_pct_move
        cond_price_down = -move_pct >= self.min_pct_move
        cond_vol        = vol_mult >= self.min_vol_mult
        cond_rsi_up     = rsi_val >= self.rsi_high
        cond_rsi_down   = rsi_val <= self.rsi_low

        score_up   = int(cond_price_up) + int(cond_vol) + int(cond_rsi_up)
        score_down = int(cond_price_down)+ int(cond_vol) + int(cond_rsi_down)

        direction=None; score=0; tier=None; macd_boost=0

        # Base A/B
        if score_up >= 2 and score_up >= score_down:
            direction="PUMP"; score=score_up
        elif score_down >= 2 and score_down > score_up:
            direction="DUMP"; score=score_down

        # Tier C (объёмный микро-брейкаут) — если aggressive включён
        if not direction and self.aggressive:
            lb = self.br_lookback
            if len(high) > lb+2 and len(low) > lb+2:
                prev_high = np.max(high[-(lb+2):-2])
                prev_low  = np.min(low [-(lb+2):-2])
                breakout_up   = (last_close >= prev_high * (1.0 + self.br_pct/100.0)) and (vol_mult >= max(self.min_vol_mult, 3.0))
                breakout_down = (last_close <= prev_low  * (1.0 - self.br_pct/100.0))  and (vol_mult >= max(self.min_vol_mult, 3.0))
                if breakout_up:   direction="PUMP"; tier="C"; score=1
                elif breakout_down: direction="DUMP"; tier="C"; score=1

        if not direction: return False, {}, "no_conditions"

        if self.use_macd and not math.isnan(hist):
            if (direction=="PUMP" and hist>0) or (direction=="DUMP" and hist<0): macd_boost=1

        if tier is None:
            tier = "A" if score==3 else "B"

        body= last_high - last_low
        if body<=0: return False, {}, "zero_body"

        if direction=="PUMP":
            fib236= last_high - 0.236*body; fib382= last_high - 0.382*body; fib500= last_high - 0.500*body
            entry_hint=f"{fib382:.6f}–{fib236:.6f} (аггр.) или {fib500:.6f}–{fib382:.6f} (конс.)"
        else:
            fib236= last_low + 0.236*body; fib382= last_low + 0.382*body; fib500= last_low + 0.500*body
            entry_hint=f"{fib236:.6f}–{fib382:.6f} (аггр.) или {fib382:.6f}–{fib500:.6f} (конс.)"

        details = {
            "direction": direction, "tier": tier, "score": score, "macd_boost": macd_boost,
            "move_pct": move_pct, "vol_mult": vol_mult, "rsi": rsi_val, "macd_hist": hist,
            "last_close": float(last_close), "last_high": float(last_high), "last_low": float(last_low),
            "entry_hint": entry_hint
        }
        return True, details, ""

    # ---------- Main loop ----------
    def run(self):
        logger.info("Initializing REST scanner...")
        tg_send(self.bot_token, self.chat_id, f"🚀 <b>Impulse Scanner</b> (REST) запущен {ts_now_iso()}")

        tickers = self.api.get_linear_tickers(); tmap = {t["symbol"]: t for t in tickers}
        universe = self._load_universe()
        logger.info(f"Universe ready: {len(universe)}"); tg_send(self.bot_token, self.chat_id, f"Всего в юниверсе: <b>{len(universe)}</b> пар")

        tickers_refresh_every = 30; cycle_idx=0

        while True:
            cycle_idx += 1; t0=time.perf_counter()
            logger.info(f"=== New cycle #{cycle_idx} | universe={len(universe)} ===")
            checked_in_cycle=0; sig_in_cycle=0

            try:
                if cycle_idx % tickers_refresh_every == 0:
                    tickers = self.api.get_linear_tickers(); tmap = {t["symbol"]: t for t in tickers}
                    universe = self._load_universe()

                for i, symbol in enumerate(universe, start=1):
                    checked_in_cycle += 1
                    short=f"[{i}/{len(universe)}] {symbol}"
                    ch24=self._ticker_pct24(tmap, symbol)
                    if abs(ch24)>self.anomaly_24h:
                        if logger.level<=logging.DEBUG: logger.debug(f"{short} skip: 24h_anomaly={ch24:.2f}% > {self.anomaly_24h}%")
                        self.counters["reasons"]["24h_anomaly"]=self.counters["reasons"].get("24h_anomaly",0)+1
                        continue

                    df_1m=self._fetch_ohlcv_df(symbol, "1m", limit=max(300, self.vol_sma+50))
                    if df_1m is None:
                        self.counters["reasons"]["no_df"]=self.counters["reasons"].get("no_df",0)+1
                        continue

                    ok, info, reason = self._analyze_1m_impulse(symbol, df_1m)
                    if not ok:
                        self.counters["reasons"][reason]=self.counters["reasons"].get(reason,0)+1
                        if logger.level<=logging.DEBUG: logger.debug(f"{short} no-impulse: reason={reason}")
                        continue

                    # мягкий sanity на старших ТФ: допустим встречный дрейф до ±self.sanity_drift
                    senior_ok=True
                    for tf in [tf for tf in self.timeframes if tf!="1m"]:
                        df=self._fetch_ohlcv_df(symbol, tf, limit=200)
                        if df is None or len(df)<5: continue
                        closes=df["close"].values; drift=pct(closes[-2], closes[-5])
                        if info["direction"]=="PUMP" and drift < -self.sanity_drift:
                            senior_ok=False; 
                            if logger.level<=logging.DEBUG: logger.debug(f"{short} fail sanity {tf}: drift={drift:.2f}% < -{self.sanity_drift}")
                            break
                        if info["direction"]=="DUMP" and drift > self.sanity_drift:
                            senior_ok=False; 
                            if logger.level<=logging.DEBUG: logger.debug(f"{short} fail sanity {tf}: drift={drift:.2f}% > {self.sanity_drift}")
                            break
                        time.sleep(0.02)
                    if not senior_ok:
                        self.counters["reasons"]["sanity"]=self.counters["reasons"].get("sanity",0)+1
                        continue

                    key=f"{symbol}|{info['direction']}"
                    if not self._dedup_ok(key):
                        self.counters["reasons"]["dedup"]=self.counters["reasons"].get("dedup",0)+1
                        if logger.level<=logging.DEBUG: logger.debug(f"{short} dedup: skip")
                        continue

                    sig_in_cycle += 1; self.counters["sig"]+=1
                    logger.info(
                        f"{short} SIGNAL {info['direction']} | TIER={info['tier']} (score={info['score']}{' +MACD' if info.get('macd_boost') else ''}) | "
                        f"Δ{self.lookback_min}m={info['move_pct']:.2f}% | turn×SMA={info['vol_mult']:.2f} | RSI={info['rsi']:.1f} | "
                        f"close={info['last_close']:.6f}"
                    )
                    msg = (
                        f"⚡️ <b>{info['direction']}</b> (TIER {info['tier']}) на <b>{symbol}</b>\n"
                        f"⏱ ТФ: 1m (подтв: {', '.join(self.timeframes)})\n"
                        f"📈 Δ за {self.lookback_min}m: <b>{info['move_pct']:.2f}%</b>\n"
                        f"🔊 Turnover xSMA({self.vol_sma}): <b>{info['vol_mult']:.2f}×</b>\n"
                        f"💪 RSI(1m): <b>{info['rsi']:.1f}</b>\n"
                        f"🧮 Сила: <b>{info['score']}/3</b>{' + MACD' if info.get('macd_boost') else ''}\n"
                        f"💵 Цена(закр. 1m): <b>{info['last_close']:.6f}</b>\n"
                        f"🎯 Зона ретеста: <b>{info['entry_hint']}</b>\n"
                        f"🕒 {ts_now_iso()}"
                    )
                    tg_send(self.bot_token, self.chat_id, msg)
                    time.sleep(0.02)

            except Exception as e:
                logger.exception(f"Cycle error: {e}")

            self.counters["checked"] += checked_in_cycle
            took = time.perf_counter()-t0
            logger.info(f"=== Cycle #{cycle_idx} done in {took:.2f}s; checked={checked_in_cycle}, signals={sig_in_cycle}; sleep {self.cycle_sleep}s ===")
            logger.info(f"Stats total: checked={self.counters['checked']} sig={self.counters['sig']} reasons={self.counters['reasons']}")
            time.sleep(self.cycle_sleep)

# ========================== Entry point ==========================

if __name__ == "__main__":
    mode = os.getenv("SCAN_MODE","rest").lower()
    try:
        if mode=="ws":
            from ws_scanner import run_ws_scanner
            logger.info("Starting in WS mode..."); run_ws_scanner()
        else:
            logger.info("Starting in REST mode...")
            scanner=ImpulseScanner(); scanner.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Bye!")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
