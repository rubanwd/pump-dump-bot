#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit USDT-Perp Impulse Scanner — WebSocket v5 public (linear)
Soft: 2/3 + Tier C (объёмный микро-брейкаут) + Tier D (SCOUT fallback, чтобы не молчать).
Каналы: kline.1.<SYMBOL> + publicTrade.<SYMBOL>
"""

import os, asyncio, json, math, time, logging
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import requests, numpy as np, pandas as pd, websockets
from dotenv import load_dotenv

BYBIT_BASE = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

logger = logging.getLogger("impulse")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# ---------- utils ----------
def ts_now_iso(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def pct(a,b): return 0.0 if b==0 else (a-b)/b*100.0
def ema(x,n): return pd.Series(x).ewm(span=n, adjust=False).mean().values
def macd_hist(close, fast=12, slow=26, signal=9):
    if len(close)<slow+signal+1: return (np.nan,np.nan,np.nan)
    macd = ema(close,fast)-ema(close,slow); sig = pd.Series(macd).ewm(span=signal,adjust=False).mean().values
    return macd[-1], sig[-1], (macd - sig)[-1]
def rsi(series, length=14):
    if len(series)<length+1: return np.nan
    d=np.diff(series); up=d.clip(min=0); dn=-d.clip(max=0)
    mu=pd.Series(up).rolling(length).mean().iloc[-1]; md=pd.Series(dn).rolling(length).mean().iloc[-1]
    if md==0 or np.isnan(mu) or np.isnan(md): return 100.0 if (md==0 and mu>0) else 0.0
    rs=mu/md; return 100.0 - (100.0/(1.0+rs))
def interval_to_bybit(s):
    s=s.lower().strip()
    if s.endswith("m"): return s[:-1]
    if s.endswith("h"): return str(int(s[:-1])*60)
    return "D" if s in ("1d","d") else "1"

def tg_send(token, chat_id, text, disable_web_page_preview=True):
    if not token or not chat_id: 
        logger.debug("Telegram not configured (skip)"); return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":disable_web_page_preview}, timeout=10)
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")

# ---------- universe via REST ----------
def load_universe(universe_max:int, min_notional:float)->List[str]:
    r=requests.get(f"{BYBIT_BASE}/v5/market/tickers", params={"category":"linear"}, timeout=10)
    data=r.json(); items=data.get("result",{}).get("list",[]) if data.get("retCode")==0 else []
    rows=[]
    for t in items:
        sym=t.get("symbol","")
        if not sym.endswith("USDT"): continue
        base=sym.replace("USDT","")
        if base in {"BTC","ETH","USDT"}: continue
        try: turn=float(t.get("turnover24h","0"))
        except: turn=0.0
        if turn>=min_notional: rows.append((sym,turn))
    rows.sort(key=lambda x:x[1], reverse=True)
    return [s for s,_ in rows[:universe_max]]

def fetch_ohlcv(symbol:str, interval_str:str, limit:int=350)->Optional[pd.DataFrame]:
    intr=interval_to_bybit(interval_str)
    d=requests.get(f"{BYBIT_BASE}/v5/market/kline", params={"category":"linear","symbol":symbol,"interval":intr,"limit":str(limit)}, timeout=10).json()
    if d.get("retCode")!=0: return None
    kl=d.get("result",{}).get("list",[])
    if not kl or len(kl)<60: return None
    rows=[]
    for rec in kl:
        ts=int(rec[0]); o=float(rec[1]); h=float(rec[2]); l=float(rec[3]); c=float(rec[4]); v=float(rec[5]); to=float(rec[6])
        rows.append((ts,o,h,l,c,v,to))
    df=pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","turnover"])
    df["dt"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
    return df

# ---------- Delta window ----------
class DeltaWindow:
    def __init__(self, window_sec:int):
        self.window_sec=window_sec; self.q: deque[Tuple[float,float,float]] = deque()
    def push_trade(self, ts:float, side:str, price:float, size:float):
        usdt=float(price)*float(size)
        self.q.append((ts, usdt if side=="Buy" else 0.0, usdt if side=="Sell" else 0.0)); self._trim()
    def _trim(self):
        cut=time.time()-self.window_sec
        while self.q and self.q[0][0] < cut: self.q.popleft()
    def snapshot(self)->Tuple[float,float,float]:
        self._trim(); b=sum(x[1] for x in self.q); s=sum(x[2] for x in self.q); return b,s,(b-s)

# ---------- analyze (2/3 + Tier C) ----------
def analyze_1m_soft(df_1m:pd.DataFrame, p)->Tuple[bool,Dict,str]:
    if len(df_1m) < p["VOL_SMA"] + p["LOOKBACK_MIN"] + 3: return False, {}, "not_enough_history"
    c=df_1m["close"].values; h=df_1m["high"].values; l=df_1m["low"].values; t=df_1m["turnover"].values
    lc=c[-2]; lh=h[-2]; ll=l[-2]; lt=t[-2]
    vol_sma = pd.Series(t[:-1]).rolling(p["VOL_SMA"]).mean().iloc[-1]
    if vol_sma==0 or np.isnan(vol_sma): return False, {}, "vol_sma_nan"
    vol_mult=lt/vol_sma
    ref=c[-(p["LOOKBACK_MIN"]+2)]; mv=pct(lc, ref)
    r=rsi(c[:-1], p["RSI_LEN"])
    _m,_s,hi=macd_hist(c[:-1])

    cu = mv>=p["MIN_PCT_MOVE"]; cd = -mv>=p["MIN_PCT_MOVE"]; cv = vol_mult>=p["MIN_VOL_MULT"]; ru = r>=p["RSI_HIGH"]; rd = r<=p["RSI_LOW"]
    su = int(cu)+int(cv)+int(ru); sd = int(cd)+int(cv)+int(rd)

    direction=None; score=0; tier=None; macd_boost=0
    if su>=2 and su>=sd: direction="PUMP"; score=su
    elif sd>=2 and sd>su: direction="DUMP"; score=sd

    # Tier C (объёмный микро-брейкаут)
    if not direction and p["AGGRESSIVE"]:
        lb=p["BREAKOUT_LOOKBACK"]
        if len(h)>lb+2 and len(l)>lb+2:
            ph=np.max(h[-(lb+2):-2]); pl=np.min(l[-(lb+2):-2])
            br_up   = (lc >= ph*(1.0 + p["BREAKOUT_PCT"]/100.0)) and (vol_mult >= max(p["MIN_VOL_MULT"], 3.0))
            br_down = (lc <= pl*(1.0 - p["BREAKOUT_PCT"]/100.0))  and (vol_mult >= max(p["MIN_VOL_MULT"], 3.0))
            if br_up: direction="PUMP"; tier="C"; score=1
            elif br_down: direction="DUMP"; tier="C"; score=1

    if not direction: return False, {}, "no_conditions"

    if p["USE_MACD"]:
        if not math.isnan(hi) and ((direction=="PUMP" and hi>0) or (direction=="DUMP" and hi<0)):
            macd_boost=1
    if tier is None: tier="A" if score==3 else "B"

    body=lh-ll
    if body<=0: return False, {}, "zero_body"
    if direction=="PUMP":
        f236= lh - 0.236*body; f382= lh - 0.382*body; f500= lh - 0.500*body
        hint=f"{f382:.6f}–{f236:.6f} (аггр.) или {f500:.6f}–{f382:.6f} (конс.)"
    else:
        f236= ll + 0.236*body; f382= ll + 0.382*body; f500= ll + 0.500*body
        hint=f"{f236:.6f}–{f382:.6f} (аггр.) или {f382:.6f}–{f500:.6f} (конс.)"

    return True, {
        "direction":direction, "tier":tier, "score":score, "macd_boost":macd_boost,
        "move_pct":mv, "vol_mult":vol_mult, "rsi":r, "macd_hist":hi,
        "last_close":float(lc), "last_high":float(lh), "last_low":float(ll), "entry_hint":hint
    }, ""

# ---------- runner ----------
async def ws_loop(symbols:List[str], cfg:Dict[str,float], tg:Dict[str,str], p:Dict, dedup_min:int, sanity_drift:float, scout_cfg:Dict[str,float]):
    deltas={s:DeltaWindow(cfg["WS_DELTA_WINDOW_SEC"]) for s in symbols}
    last_signal_at_global: Optional[datetime] = None
    last_signal_at: Dict[str, datetime] = {}

    subs=[{"op":"subscribe","args":[f"kline.1.{s}" for s in symbols]},
          {"op":"subscribe","args":[f"publicTrade.{s}" for s in symbols]}]
    reconnect=int(os.getenv("WS_RECONNECT_SEC","5")); hb_min=int(os.getenv("WS_HEARTBEAT_MIN","3")); last_hb=time.time()

    # для SCOUT — хранить «лучшего кандидата» за короткий период
    best_candidate = {"sym": None, "score": -1e9, "info": None, "b":0.0, "s":0.0, "d":0.0}
    last_scout_eval = 0.0

    def consider_candidate(sym:str, info:Dict, b:float, s:float, dlt:float):
        """формируем «оценку» силы для SCOUT (простая линейка)"""
        # базовый скор: весим Δ, vol_mult и |дельту|
        score = (info.get("move_pct",0.0))*1.0 + (info.get("vol_mult",0.0))*2.0 + (abs(dlt)/10000.0)*1.0
        nonlocal best_candidate
        if score > best_candidate["score"]:
            best_candidate = {"sym": sym, "score": score, "info": info, "b": b, "s": s, "d": dlt}

    async def maybe_send_scout(now_dt: datetime):
        nonlocal last_signal_at_global, best_candidate
        if (now_dt is None) or (best_candidate["sym"] is None):
            return
        # если давно не было сигналов — отправляем SCOUT (Tier D)
        if (last_signal_at_global is None) or ((now_dt - last_signal_at_global) >= timedelta(minutes=scout_cfg["MIN_SIGNAL_INTERVAL_MIN"])):
            sym = best_candidate["sym"]
            info = best_candidate["info"]
            b, s, dlt = best_candidate["b"], best_candidate["s"], best_candidate["d"]
            # минимальный антиспам на символ/направление
            key = f"{sym}|{info['direction']}"
            last = last_signal_at.get(key)
            if last and (now_dt - last) < timedelta(minutes=dedup_min):
                return
            last_signal_at[key] = now_dt
            last_signal_at_global = now_dt

            logger.info(f"[{sym}] SCOUT (Tier D) sending due to silence: Δ{p['LOOKBACK_MIN']}m={info['move_pct']:.2f}% "
                        f"| turn×SMA={info['vol_mult']:.2f} | RSI={info['rsi']:.1f} | "
                        f"Δ_usdt={dlt:,.0f} (buy={b:,.0f}/sell={s:,.0f}) | close={info['last_close']:.6f}")

            msg = (
                f"🟡 <b>SCOUT</b> (Tier D, fallback) на <b>{sym}</b>\n"
                f"⏱ ТФ: 1m (мягкий сигнал — чтобы не молчать)\n"
                f"📈 Δ за {p['LOOKBACK_MIN']}m: <b>{info['move_pct']:.2f}%</b>\n"
                f"🔊 Turnover xSMA({p['VOL_SMA']}): <b>{info['vol_mult']:.2f}×</b>\n"
                f"💪 RSI(1m): <b>{info['rsi']:.1f}</b>\n"
                f"📊 Δ {cfg['WS_DELTA_WINDOW_SEC']}s: <b>buy={b:,.0f}</b> / <b>sell={s:,.0f}</b> / <b>|Δ|={abs(dlt):,.0f}</b>\n"
                f"💵 Цена(закр. 1m): <b>{info['last_close']:.6f}</b>\n"
                f"ℹ️ Это «мягкий» сигнал (Tier D). Условия облегчены, цель — дать хотя бы 1 сигнал/час.\n"
                f"🕒 {ts_now_iso()}"
            )
            tg_send(tg["token"], tg["chat_id"], msg)
            # сбросить кандидата
            best_candidate = {"sym": None, "score": -1e9, "info": None, "b":0.0, "s":0.0, "d":0.0}

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                logger.info(f"WS connected. Subscribing to {len(symbols)} symbols...")
                for m in subs: await ws.send(json.dumps(m)); await asyncio.sleep(0.05)

                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60); msg=json.loads(raw)

                    # heartbeat + периодическая проверка SCOUT
                    now_ts = time.time()
                    if now_ts - last_hb >= hb_min*60:
                        logger.info(f"[WS] alive: tracked={len(symbols)}; window={cfg['WS_DELTA_WINDOW_SEC']}s"); last_hb=now_ts
                    if now_ts - last_scout_eval >= scout_cfg["SCOUT_CHECK_EVERY_SEC"]:
                        await maybe_send_scout(datetime.now(timezone.utc))
                        last_scout_eval = now_ts

                    # trades
                    if msg.get("topic","").startswith("publicTrade."):
                        sym=msg["topic"].split(".")[1]; data=msg.get("data",[])
                        for tr in data:
                            ts=float(tr.get("T", int(time.time()*1000)))/1000.0
                            side=tr.get("S") or tr.get("side"); price=float(tr.get("p")); size=float(tr.get("v"))
                            deltas[sym].push_trade(ts, side, price, size)
                        continue

                    # kline 1m (по confirm закрытию)
                    if msg.get("topic","").startswith("kline.1."):
                        sym=msg["topic"].split(".")[2]; arr=msg.get("data",[])
                        if not arr: continue
                        bar=arr[-1]
                        if not bar.get("confirm", False): continue

                        # закрылась 1m — подгружаем df
                        df_1m=fetch_ohlcv(sym,"1m",limit=max(350, p["VOL_SMA"]+60))
                        if df_1m is None: continue

                        ok, info, reason = analyze_1m_soft(df_1m, p)

                        # лёгкий sanity 3m (можно почти выключить — сильно смягчён)
                        df_3m=fetch_ohlcv(sym,"3m",limit=120)
                        if df_3m is not None and len(df_3m)>=5:
                            cl=df_3m["close"].values; dr=pct(cl[-2], cl[-5])
                            if ok:
                                if info["direction"]=="PUMP" and dr < -sanity_drift: ok=False
                                if info["direction"]=="DUMP" and dr >  sanity_drift: ok=False

                        # текущая дельта
                        b,s,d = deltas[sym].snapshot(); ad=abs(d)
                        imb = (abs(b)/max(1.0, abs(s))) if s>0 else float('inf')

                        # основная проверка дельты (смягчаем для A/C)
                        if ok:
                            tier=info.get("tier","B")
                            delta_min = cfg["WS_DELTA_MIN_USDT"] * (0.75 if tier in ("A","C") else 1.0)
                            imb_min   = cfg["WS_DELTA_IMBALANCE"] * (0.9  if tier in ("A","C") else 1.0)
                            if ad < delta_min or imb < imb_min:
                                ok = False

                        # если нормальный сигнал прошёл — отправляем
                        if ok:
                            key=f"{sym}|{info['direction']}"; now_dt=datetime.now(timezone.utc)
                            last=last_signal_at.get(key)
                            if not last or (now_dt - last) >= timedelta(minutes=dedup_min):
                                last_signal_at[key]=now_dt
                                # глобальный таймер тишины сбрасываем
                                nonlocal_last = last_signal_at_global
                                last_signal_at_global = now_dt

                                logger.info(
                                    f"[{sym}] SIGNAL {info['direction']} | TIER={info['tier']} (score={info['score']}{' +MACD' if info.get('macd_boost') else ''}) "
                                    f"| Δ{p['LOOKBACK_MIN']}m={info['move_pct']:.2f}% | turn×SMA={info['vol_mult']:.2f} | RSI={info['rsi']:.1f} | "
                                    f"Δ_usdt={d:,.0f} (buy={b:,.0f}/sell={s:,.0f}) | close={info['last_close']:.6f}"
                                )
                                msgt = (
                                    f"⚡️ <b>{info['direction']}</b> (WS, TIER {info['tier']}) на <b>{sym}</b>\n"
                                    f"⏱ ТФ: 1m (закрытая свеча)\n"
                                    f"📈 Δ за {p['LOOKBACK_MIN']}m: <b>{info['move_pct']:.2f}%</b>\n"
                                    f"🔊 Turnover xSMA({p['VOL_SMA']}): <b>{info['vol_mult']:.2f}×</b>\n"
                                    f"💪 RSI(1m): <b>{info['rsi']:.1f}</b>\n"
                                    f"🧮 Сила: <b>{info['score']}/3</b>{' + MACD' if info.get('macd_boost') else ''}\n"
                                    f"📊 Δ {cfg['WS_DELTA_WINDOW_SEC']}s: <b>buy={b:,.0f}</b> / <b>sell={s:,.0f}</b> / <b>|Δ|={ad:,.0f}</b> (imb={imb:.2f})\n"
                                    f"💵 Цена(закр. 1m): <b>{info['last_close']:.6f}</b>\n"
                                    f"🎯 Зона ретеста: <b>{info['entry_hint']}</b>\n"
                                    f"🕒 {ts_now_iso()}"
                                )
                                tg_send(tg["token"], tg["chat_id"], msgt)
                            # обновим лучшего кандидата (на случай тишины)
                            consider_candidate(sym, info, b, s, d)
                            continue

                        # если «нормального» сигнала нет — рассмотрим как кандидата на SCOUT
                        # очень мягкие условия: хотя бы ОДНО из трёх
                        scout_ok = (
                            (info.get("move_pct",0.0) >= scout_cfg["SCOUT_MOVE_PCT"]) or
                            (info.get("vol_mult",0.0)  >= scout_cfg["SCOUT_VOL_MULT"]) or
                            ((ad >= scout_cfg["SCOUT_DELTA_MIN_USDT"]) and (imb >= scout_cfg["SCOUT_IMBALANCE"]))
                        )
                        if scout_ok:
                            consider_candidate(sym, info, b, s, d)

        except (asyncio.TimeoutError, websockets.ConnectionClosedError, websockets.InvalidStatusCode) as e:
            logger.warning(f"WS connection issue: {e}. Reconnecting in {reconnect}s ..."); await asyncio.sleep(reconnect)
        except Exception as e:
            logger.exception(f"WS fatal: {e}"); await asyncio.sleep(reconnect)

def run_ws_scanner():
    load_dotenv()
    # очень мягкие дефолты (можешь править через .env)
    universe_max=int(os.getenv("UNIVERSE_MAX","400"))
    min_notional=float(os.getenv("MIN_NOTIONAL_USDT","80000"))
    lookback_min=int(os.getenv("LOOKBACK_MIN","1"))
    vol_sma=int(os.getenv("VOL_SMA","20"))
    min_pct_move=float(os.getenv("MIN_PCT_MOVE","0.8"))
    min_vol_mult=float(os.getenv("MIN_VOL_MULT","1.8"))
    rsi_len=int(os.getenv("RSI_LEN","14")); rsi_high=float(os.getenv("RSI_HIGH","66")); rsi_low=float(os.getenv("RSI_LOW","34"))
    use_macd=os.getenv("USE_MACD","0")=="1"; dedup=int(os.getenv("DEDUP_MINUTES","3"))
    sanity_drift=float(os.getenv("SANITY_DRIFT_PCT","4.0"))

    p = {
        "LOOKBACK_MIN": lookback_min, "VOL_SMA": vol_sma,
        "MIN_PCT_MOVE": min_pct_move, "MIN_VOL_MULT": min_vol_mult,
        "RSI_LEN": rsi_len, "RSI_HIGH": rsi_high, "RSI_LOW": rsi_low,
        "USE_MACD": use_macd,
        "AGGRESSIVE": os.getenv("AGGRESSIVE","1")=="1",
        "BREAKOUT_LOOKBACK": int(os.getenv("BREAKOUT_LOOKBACK","10")),
        "BREAKOUT_PCT": float(os.getenv("BREAKOUT_PCT","0.20")),
    }
    cfg = {
        "WS_DELTA_WINDOW_SEC": int(os.getenv("WS_DELTA_WINDOW_SEC","5")),
        "WS_DELTA_MIN_USDT": float(os.getenv("WS_DELTA_MIN_USDT","4000")),
        "WS_DELTA_IMBALANCE": float(os.getenv("WS_DELTA_IMBALANCE","1.15")),
    }
    scout_cfg = {
        "MIN_SIGNAL_INTERVAL_MIN": int(os.getenv("MIN_SIGNAL_INTERVAL_MIN","60")),
        "SCOUT_CHECK_EVERY_SEC": int(os.getenv("SCOUT_CHECK_EVERY_SEC","60")),
        "SCOUT_MOVE_PCT": float(os.getenv("SCOUT_MOVE_PCT","0.25")),
        "SCOUT_VOL_MULT": float(os.getenv("SCOUT_VOL_MULT","1.15")),
        "SCOUT_DELTA_MIN_USDT": float(os.getenv("SCOUT_DELTA_MIN_USDT","2000")),
        "SCOUT_IMBALANCE": float(os.getenv("SCOUT_IMBALANCE","1.05")),
    }
    tg={"token":os.getenv("TELEGRAM_BOT_TOKEN",""), "chat_id":os.getenv("TELEGRAM_CHAT_ID","")}
    logger.info("WS mode starting (soft + Tier C + SCOUT Tier D)...")
    symbols=load_universe(universe_max, min_notional)
    logger.info(f"WS universe: {len(symbols)} symbols")
    tg_send(tg["token"], tg["chat_id"], f"🚀 <b>Impulse Scanner</b> (WS) запущен {ts_now_iso()}\nВсего в юниверсе: <b>{len(symbols)}</b> пар")
    asyncio.run(ws_loop(symbols, cfg, tg, p, dedup, sanity_drift, scout_cfg))
