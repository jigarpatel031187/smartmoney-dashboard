#!/usr/bin/env python3
"""
Smart Money Midcap/Smallcap Dashboard - daily pipeline.

Deterministic, rule-based. No AI-sourced numbers anywhere.

Data sources (all official/public archives):
  - Universe:    niftyindices.com constituent CSVs (Midcap 150 + Smallcap 250)
  - EOD prices,
    delivery %:  NSE sec_bhavdata_full archive CSV
  - Bulk/block:  NSE daily bulk.csv / block.csv archives
  - History:     Yahoo Finance via yfinance (.NS tickers) for RVol / U-D / returns
  - Fundamentals: yfinance quarterly income statements (weekly refresh)

Every value carries provenance. Missing data is flagged, never assumed.

Modes:
  python daily_update.py            # daily run (prices, smart money, re-rank)
  python daily_update.py --weekly   # additionally refresh fundamentals cache

Scoring (composite 0-10):
  Fundamental (trailing growth)  50%   -> rev YoY, PAT YoY, acceleration, margin trend
  Smart money                    40%   -> bulk/block 15, delivery spike 10,
                                          RVol+price-confirm 10, U/D ratio 5
  Technical context              10%   -> vs 200DMA, distance from 52w high

Vetoes:
  V1: 6-month return >= 100%  -> momentum confirmation, excluded from entry top-10
  V2: fundamentals unavailable -> excluded from top-10, flagged
  V3: net bulk/block SELLING   -> bulk component zeroed (activity != accumulation)
"""

import argparse
import io
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------- paths / config

ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = ROOT / "data"                 # committed state (rolling histories, caches)
OUT_DIR = ROOT / "docs" / "data"          # dashboard reads from here
HIST_DIR = OUT_DIR / "history"

IST = timezone(timedelta(hours=5, minutes=30))

WEIGHTS = {"fundamental": 0.20, "smart": 0.50, "technical": 0.30}
SMART_SUB = {"bulk": 15.0, "delivery": 10.0, "rvol": 10.0, "ud": 5.0}  # sums to 40

RVOL_HIGH, RVOL_MOD = 0.50, 0.25          # +50% / +25% vs 20d avg volume
UD_STRONG, UD_MOD = 4.0, 2.0              # Jigar's standing thresholds
DELIV_SPIKE_STRONG, DELIV_SPIKE_MOD = 1.5, 1.2   # x own 20d avg delivery %
V1_RUN_THRESHOLD = 1.00                   # 100% six-month run
LIQ_FLOOR = 3e7                           # Rs 3 crore avg 20d turnover (hard filter)
ATR_PERIOD = 14
ENTRY_ATR, EXT_ATR, SL_BUF_ATR = 0.5, 2.0, 0.25
T1_R, T2_R = 1.5, 2.5                     # R-multiple targets
MIN_RR = 1.5                              # min reward:risk at CMP for a valid setup
WIDE_STOP_PCT = 0.08                      # >8% risk -> size-down flag
HIGH_ATR_PCT = 0.05                       # ATR >5% of price -> volatility flag
VOL_CONFIRM_X = 1.5                       # up day on >=1.5x avg volume = CONFIRMED
NEAR_HIGH_PCT = 0.97                      # within 3% of 52w high = breakout context
FUND_STALE_DAYS = 8
BULK_LOOKBACK_SESSIONS = 5
MIN_DELIV_SESSIONS = 5                    # min sessions before delivery spike scored

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

UNIVERSE_SOURCES = {
    "midcap150": [
        "https://niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
        "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    ],
    "smallcap250": [
        "https://niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
        "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    ],
}

BHAV_URLS = [
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv",
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv",
]
BULK_URLS = [
    "https://nsearchives.nseindia.com/content/equities/bulk.csv",
    "https://archives.nseindia.com/content/equities/bulk.csv",
]
BLOCK_URLS = [
    "https://nsearchives.nseindia.com/content/equities/block.csv",
    "https://archives.nseindia.com/content/equities/block.csv",
]


# ---------------------------------------------------------------- small utilities

def log(msg: str) -> None:
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] {msg}", flush=True)


def http_get(urls, retries=3, timeout=30) -> requests.Response | None:
    """Try each URL with retries. Returns first 200 response or None."""
    if isinstance(urls, str):
        urls = [urls]
    sess = requests.Session()
    sess.headers.update(HEADERS)
    for url in urls:
        for attempt in range(retries):
            try:
                r = sess.get(url, timeout=timeout)
                if r.status_code == 200 and len(r.content) > 100:
                    return r
                log(f"  {url} -> HTTP {r.status_code}, attempt {attempt + 1}")
            except requests.RequestException as e:
                log(f"  {url} -> {type(e).__name__}, attempt {attempt + 1}")
            time.sleep(2 * (attempt + 1))
    return None


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            log(f"  WARN corrupt json {path.name}; starting fresh")
    return default


def _json_default(o):
    if hasattr(o, "item"):          # numpy scalar -> native python number
        return o.item()
    return str(o)


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, default=_json_default))


def clamp(x, lo=0.0, hi=10.0):
    return max(lo, min(hi, x))


# ---------------------------------------------------------------- 1. universe

def get_universe() -> tuple[dict, str]:
    """symbol -> {name, index}. Cached; refreshed if cache > 7 days old."""
    cache = STATE_DIR / "universe.json"
    cached = load_json(cache, None)
    if cached and (datetime.now(IST) - datetime.fromisoformat(cached["asof"]).replace(
            tzinfo=IST)).days < 7:
        return cached["symbols"], "cache"

    symbols = {}
    for idx, urls in UNIVERSE_SOURCES.items():
        r = http_get(urls)
        if r is None:
            log(f"  universe fetch FAILED for {idx}")
            continue
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip().lower() for c in df.columns]
        sym_col = "symbol" if "symbol" in df.columns else df.columns[2]
        name_col = "company name" if "company name" in df.columns else df.columns[0]
        for _, row in df.iterrows():
            s = str(row[sym_col]).strip()
            if s and s.upper() != "NAN":
                symbols[s] = {"name": str(row[name_col]).strip(), "index": idx}

    if len(symbols) >= 300:  # sanity: expect ~400
        save_json(cache, {"asof": datetime.now(IST).isoformat(), "symbols": symbols})
        return symbols, "fresh"
    if cached:
        log("  universe fetch incomplete; using stale cache")
        return cached["symbols"], "stale_cache"
    raise RuntimeError("No universe available (fetch failed, no cache).")


# ---------------------------------------------------------------- 2. bhavcopy (EOD + delivery %)

def fetch_bhavcopy_for(day: datetime) -> pd.DataFrame | None:
    """Bhavcopy for one specific date (None on holidays/weekends)."""
    dstr = day.strftime("%d%m%Y")
    r = http_get([u.format(d=dstr) for u in BHAV_URLS], retries=1)
    if r is None:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().upper() for c in df.columns]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df[df["SERIES"] == "EQ"].copy()
    for col in ("CLOSE_PRICE", "PREV_CLOSE", "TTL_TRD_QNTY", "DELIV_PER"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def backfill_delivery(universe: dict, days_back: int = 35) -> None:
    """One-time: fill the 20-session delivery window from NSE's dated archives."""
    log(f"Backfilling delivery history from last {days_back} calendar days...")
    start = datetime.now(IST) - timedelta(days=days_back)
    got = 0
    for i in range(days_back):
        day = start + timedelta(days=i)
        if day.weekday() >= 5:
            continue
        df = fetch_bhavcopy_for(day)
        if df is None:
            continue  # holiday or archive miss - skipped, never invented
        update_delivery_history(df, universe, day.strftime("%Y-%m-%d"))
        got += 1
        log(f"  {day.strftime('%Y-%m-%d')} ok ({got} sessions)")
        time.sleep(1)
    log(f"Backfill complete: {got} sessions loaded.")


def fetch_bhavcopy(max_back=7) -> tuple[pd.DataFrame | None, str | None]:
    """Latest available sec_bhavdata_full, walking back up to max_back days."""
    day = datetime.now(IST)
    for _ in range(max_back):
        df = fetch_bhavcopy_for(day)
        if df is not None:
            return df, day.strftime("%Y-%m-%d")
        day -= timedelta(days=1)
    return None, None


# ---------------------------------------------------------------- 3. bulk / block deals

def fetch_deals() -> list[dict]:
    """Today's bulk + block deals, normalised."""
    deals = []
    for kind, urls in (("bulk", BULK_URLS), ("block", BLOCK_URLS)):
        r = http_get(urls, retries=2)
        if r is None:
            log(f"  {kind} deals unavailable today")
            continue
        try:
            df = pd.read_csv(io.StringIO(r.text))
        except Exception as e:
            log(f"  {kind} parse failed: {e}")
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        sym_c = next((c for c in df.columns if "symbol" in c), None)
        bs_c = next((c for c in df.columns if "buy" in c and "sell" in c), None)
        qty_c = next((c for c in df.columns if "quantity" in c or "qty" in c), None)
        date_c = next((c for c in df.columns if "date" in c), None)
        if not all((sym_c, bs_c, qty_c)):
            log(f"  {kind} schema unrecognised: {list(df.columns)}")
            continue
        for _, row in df.iterrows():
            qty = pd.to_numeric(row[qty_c], errors="coerce")
            if pd.isna(qty):
                continue
            deals.append({
                "symbol": str(row[sym_c]).strip(),
                "side": "BUY" if "buy" in str(row[bs_c]).lower() else "SELL",
                "qty": float(qty),
                "kind": kind,
                "date": str(row[date_c]).strip() if date_c else "",
            })
    return deals


def update_deal_history(deals: list, trade_date: str) -> dict:
    """Rolling store of last N sessions of deals. Returns per-symbol net qty + counts."""
    path = STATE_DIR / "deals_history.json"
    hist = load_json(path, {"sessions": []})
    sessions = [s for s in hist["sessions"] if s["date"] != trade_date]
    sessions.append({"date": trade_date, "deals": deals})
    sessions = sorted(sessions, key=lambda s: s["date"])[-BULK_LOOKBACK_SESSIONS:]
    save_json(path, {"sessions": sessions})

    agg = {}
    for s in sessions:
        for d in s["deals"]:
            a = agg.setdefault(d["symbol"], {"net_qty": 0.0, "buys": 0, "sells": 0})
            if d["side"] == "BUY":
                a["net_qty"] += d["qty"]; a["buys"] += 1
            else:
                a["net_qty"] -= d["qty"]; a["sells"] += 1
    return agg


# ---------------------------------------------------------------- 4. delivery history

def update_delivery_history(bhav: pd.DataFrame, universe: dict, trade_date: str) -> dict:
    """Rolling 20-session delivery % per symbol. Returns {sym: {today, avg20, n}}."""
    path = STATE_DIR / "delivery_history.json"
    hist = load_json(path, {})
    today_map = {}
    sub = bhav[bhav["SYMBOL"].isin(universe.keys())]
    for _, row in sub.iterrows():
        dp = row["DELIV_PER"]
        if pd.isna(dp):
            continue
        sym = row["SYMBOL"]
        recs = [r for r in hist.get(sym, []) if r["d"] != trade_date]
        recs.append({"d": trade_date, "p": float(dp)})
        hist[sym] = sorted(recs, key=lambda r: r["d"])[-21:]
        prior = [r["p"] for r in hist[sym] if r["d"] != trade_date][-20:]
        today_map[sym] = {
            "today": float(dp),
            "avg20": (sum(prior) / len(prior)) if prior else None,
            "n": len(prior),
        }
    save_json(path, hist)
    return today_map


# ---------------------------------------------------------------- 5. yfinance history

def fetch_price_history(symbols: list[str]) -> dict:
    """1y daily OHLCV per symbol via yfinance. Returns {sym: DataFrame}."""
    import yfinance as yf
    out, chunk = {}, 50
    tickers = {s: f"{s}.NS" for s in symbols}
    tick_list = list(tickers.values())
    for i in range(0, len(tick_list), chunk):
        batch = tick_list[i:i + chunk]
        try:
            data = yf.download(batch, period="1y", interval="1d", group_by="ticker",
                               auto_adjust=True, progress=False, threads=True)
        except Exception as e:
            log(f"  yfinance batch {i // chunk} failed: {e}")
            continue
        for sym, tkr in tickers.items():
            if sym in out:
                continue
            try:
                df = data[tkr].dropna(how="all") if len(batch) > 1 else data.dropna(how="all")
                if len(df) >= 30:
                    out[sym] = df
            except (KeyError, TypeError):
                pass
        time.sleep(1)
    return out


# ---------------------------------------------------------------- 6. fundamentals (weekly)

def refresh_fundamentals(symbols: list[str]) -> dict:
    """Quarterly revenue / net income per symbol from yfinance. Slow: weekly only."""
    import yfinance as yf
    cache = {}
    for n, sym in enumerate(symbols, 1):
        if n % 50 == 0:
            log(f"  fundamentals {n}/{len(symbols)}")
        try:
            t = yf.Ticker(f"{sym}.NS")
            q = t.quarterly_income_stmt
            if q is None or q.empty:
                cache[sym] = {"available": False, "reason": "no quarterly data"}
                continue
            q = q.T.sort_index()  # rows = quarter-end dates ascending
            rev_col = next((c for c in ("Total Revenue", "Operating Revenue") if c in q.columns), None)
            ni_col = "Net Income" if "Net Income" in q.columns else None
            if rev_col is None or ni_col is None:
                cache[sym] = {"available": False, "reason": "missing revenue/PAT rows"}
                continue
            rows = []
            for dt, r in q.iterrows():
                rev, ni = r.get(rev_col), r.get(ni_col)
                if pd.notna(rev):
                    rows.append({"q": str(pd.Timestamp(dt).date()),
                                 "rev": float(rev),
                                 "pat": float(ni) if pd.notna(ni) else None})
            if len(rows) < 2:
                cache[sym] = {"available": False, "reason": "insufficient quarters"}
            else:
                cache[sym] = {"available": True, "quarters": rows[-6:]}
        except Exception as e:
            cache[sym] = {"available": False, "reason": f"fetch error: {type(e).__name__}"}
        time.sleep(0.4)  # be polite
    save_json(STATE_DIR / "fundamentals.json",
              {"asof": datetime.now(IST).isoformat(), "data": cache})
    return cache


def load_fundamentals(symbols: list[str], weekly: bool) -> tuple[dict, str]:
    path = STATE_DIR / "fundamentals.json"
    cached = load_json(path, None)
    stale = True
    if cached:
        age = (datetime.now(IST)
               - datetime.fromisoformat(cached["asof"]).replace(tzinfo=IST)).days
        stale = age > FUND_STALE_DAYS
    if weekly or cached is None or stale:
        log("Refreshing fundamentals cache (weekly pass)...")
        return refresh_fundamentals(symbols), "fresh"
    return cached["data"], f"cache ({cached['asof'][:10]})"


# ---------------------------------------------------------------- scoring

def yoy(curr, prev):
    if curr is None or prev is None or prev == 0:
        return None
    if prev < 0:  # sign-flip quarters: growth % is not meaningful
        return None
    return (curr - prev) / abs(prev)


def aligned_yoy_quarter(qs: list, ref: dict) -> dict | None:
    """The quarter 330-400 days older than ref (true same-quarter-last-year).
    Prevents broken YoY when yfinance has gaps in quarterly history."""
    ref_d = datetime.fromisoformat(ref["q"]).date()
    best = None
    for q in qs:
        dd = (ref_d - datetime.fromisoformat(q["q"]).date()).days
        if 330 <= dd <= 400:
            best = q
    return best


def score_fundamental(f: dict) -> tuple[float | None, dict]:
    """0-10 from trailing growth. None => unavailable (V2)."""
    if not f or not f.get("available"):
        return None, {"reason": (f or {}).get("reason", "not in cache")}
    qs = f["quarters"]
    if len(qs) < 5:
        return None, {"reason": f"only {len(qs)} quarters (need 5 for YoY)"}
    latest, prior_q = qs[-1], qs[-2]
    yoy_q = aligned_yoy_quarter(qs, latest)
    if yoy_q is None:
        return None, {"reason": "no date-aligned YoY quarter (gap in quarterly history)"}

    rev_yoy = yoy(latest["rev"], yoy_q["rev"])
    pat_yoy = yoy(latest["pat"], yoy_q["pat"])
    prior_yoy_q = aligned_yoy_quarter(qs, prior_q)
    prev_rev_yoy = yoy(prior_q["rev"], prior_yoy_q["rev"]) if prior_yoy_q else None

    if rev_yoy is None:
        return None, {"reason": "revenue YoY not computable"}

    # revenue growth: 0% -> 2, 15% -> 6, 30%+ -> 10   (0-10, weight .35)
    s_rev = clamp(2 + rev_yoy * 26.7)
    # PAT growth: same curve; sign-flip/loss quarters -> 0 (weight .35)
    s_pat = clamp(2 + pat_yoy * 26.7) if pat_yoy is not None else 0.0
    # acceleration: latest rev YoY vs previous quarter's rev YoY (weight .15)
    if prev_rev_yoy is not None:
        s_acc = clamp(5 + (rev_yoy - prev_rev_yoy) * 25)
    else:
        s_acc = 5.0
    # margin trend: PAT margin latest vs 4 quarters ago (weight .15)
    m_now = latest["pat"] / latest["rev"] if latest["pat"] is not None and latest["rev"] else None
    m_then = yoy_q["pat"] / yoy_q["rev"] if yoy_q["pat"] is not None and yoy_q["rev"] else None
    s_mgn = clamp(5 + (m_now - m_then) * 100) if (m_now is not None and m_then is not None) else 5.0

    score = 0.35 * s_rev + 0.35 * s_pat + 0.15 * s_acc + 0.15 * s_mgn
    return round(score, 2), {
        "rev_yoy": round(rev_yoy * 100, 1),
        "pat_yoy": round(pat_yoy * 100, 1) if pat_yoy is not None else None,
        "latest_quarter": latest["q"],
    }


def score_smart(sym: str, deals_agg: dict, deliv: dict, hist: pd.DataFrame | None,
                day_change_pct: float | None) -> tuple[float, dict, list]:
    """0-10 smart-money score (weighted per SMART_SUB), plus badges + notes."""
    pts, maxpts = 0.0, sum(SMART_SUB.values())
    badges, detail = [], {}

    # bulk/block (15)
    d = deals_agg.get(sym)
    if d:
        if d["net_qty"] > 0:
            pts += SMART_SUB["bulk"]
            badges.append("BULK+")
            detail["bulk"] = f"net buy {int(d['net_qty']):,} sh / {BULK_LOOKBACK_SESSIONS}s"
        else:  # V3: net selling -> zero, and note it
            detail["bulk"] = f"V3 net SELL {int(abs(d['net_qty'])):,} sh - component zeroed"
            badges.append("BULK-SELL")
    else:
        detail["bulk"] = "no deals in window"

    # delivery spike (10)
    dv = deliv.get(sym)
    if dv and dv["avg20"] and dv["n"] >= MIN_DELIV_SESSIONS:
        ratio = dv["today"] / dv["avg20"] if dv["avg20"] > 0 else 0
        if ratio >= DELIV_SPIKE_STRONG:
            pts += SMART_SUB["delivery"]; badges.append("DELIV++")
        elif ratio >= DELIV_SPIKE_MOD:
            pts += SMART_SUB["delivery"] * 0.5; badges.append("DELIV+")
        detail["delivery"] = f"{dv['today']:.1f}% vs avg {dv['avg20']:.1f}% ({dv['n']}s)"
        detail["deliv_ratio"] = round(ratio, 2)
    else:
        detail["delivery"] = f"warming up ({dv['n'] if dv else 0}/{MIN_DELIV_SESSIONS}s history)"

    # RVol with same-day price confirmation (10)
    if hist is not None and len(hist) >= 21:
        vols = hist["Volume"].dropna()
        if len(vols) >= 21:
            rvol = (vols.iloc[-1] / vols.iloc[-21:-1].mean()) - 1
            confirmed = day_change_pct is not None and day_change_pct > 0
            detail["rvol"] = f"{rvol * 100:+.0f}% vs 20d avg" + ("" if confirmed else " (no price confirm)")
            if confirmed:
                if rvol >= RVOL_HIGH:
                    pts += SMART_SUB["rvol"]; badges.append("RVOL-HI")
                elif rvol >= RVOL_MOD:
                    pts += SMART_SUB["rvol"] * 0.5; badges.append("RVOL")

    # U/D volume ratio, 20 sessions (5)
    if hist is not None and len(hist) >= 21:
        tail = hist.iloc[-20:]
        chg = tail["Close"].diff()
        up = tail["Volume"][chg > 0].sum()
        dn = tail["Volume"][chg < 0].sum()
        if dn > 0:
            ud = up / dn
            detail["ud"] = f"{ud:.2f}"
            detail["ud_val"] = round(float(ud), 2)
            if ud >= UD_STRONG:
                pts += SMART_SUB["ud"]; badges.append("U/D>4")
            elif ud >= UD_MOD:
                pts += SMART_SUB["ud"] * 0.5

    return round(pts / maxpts * 10, 2), detail, badges


def score_technical(hist: pd.DataFrame | None,
                    nifty_ret3m: float | None) -> tuple[float, dict]:
    """Stage-2 alignment (40%) + distance from 52w high (30%) + RS vs Nifty (30%)."""
    if hist is None or len(hist) < 60:
        return 5.0, {"note": "insufficient history; neutral 5.0"}
    close = hist["Close"].dropna()
    last = close.iloc[-1]
    dma50 = close.iloc[-50:].mean()
    dma200 = close.iloc[-200:].mean() if len(close) >= 200 else close.mean()
    hi52 = close.max()

    if last > dma50 > dma200:
        s_stage, stage = 10.0, "Stage 2 (P>50>200 DMA)"
    elif last > dma200:
        s_stage, stage = 6.0, "above 200 DMA"
    elif last > dma50:
        s_stage, stage = 4.0, "above 50 DMA only"
    else:
        s_stage, stage = 2.0, "below both DMAs"

    s_hi = clamp(10 - ((hi52 - last) / hi52) * 25)

    rs = None
    if nifty_ret3m is not None and len(close) >= 63:
        rs = float(close.iloc[-1] / close.iloc[-63] - 1) - nifty_ret3m
        s_rs = clamp(5 + rs * 25)
    else:
        s_rs = 5.0

    return round(0.4 * s_stage + 0.3 * s_hi + 0.3 * s_rs, 2), {
        "stage": stage,
        "above_200dma": bool(last > dma200),
        "vs_200dma_pct": round(float(last / dma200 - 1) * 100, 1),
        "off_52w_high_pct": round(float((hi52 - last) / hi52) * 100, 1),
        "rs_vs_nifty_3m_pct": round(rs * 100, 1) if rs is not None else None,
    }


def atr14(hist: pd.DataFrame) -> float | None:
    h, l, c = hist["High"], hist["Low"], hist["Close"]
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    tr = tr.dropna()
    return float(tr.iloc[-ATR_PERIOD:].mean()) if len(tr) >= ATR_PERIOD else None


def avg_turnover_20d(hist: pd.DataFrame) -> float | None:
    t = (hist["Close"] * hist["Volume"]).dropna()
    return float(t.iloc[-20:].mean()) if len(t) >= 20 else None


def build_trade_plan(hist: pd.DataFrame | None, cmp_: float,
                     chg_pct: float | None) -> dict:
    """Deterministic entry zone / SL / targets / volume state. All from price structure."""
    if hist is None or len(hist) < 60:
        return {"valid": False, "reason": "insufficient price history"}
    atr = atr14(hist)
    if not atr or atr <= 0:
        return {"valid": False, "reason": "ATR unavailable"}
    close = hist["Close"].dropna()
    vols = hist["Volume"].dropna()
    dma20 = float(close.iloc[-20:].mean())
    hi52 = float(close.max())
    swing_low = float(hist["Low"].dropna().iloc[-20:].min())

    extended = cmp_ > dma20 + EXT_ATR * atr
    if extended:
        zone_lo, zone_hi = dma20, dma20 + 1.0 * atr
        zone_note = "extended - wait for pullback to zone"
    else:
        zone_lo, zone_hi = cmp_ - ENTRY_ATR * atr, cmp_
        zone_note = None
    entry_mid = (zone_lo + zone_hi) / 2

    sl = swing_low - SL_BUF_ATR * atr
    risk = entry_mid - sl
    if risk <= 0:
        return {"valid": False, "reason": "no valid setup (SL above entry zone)"}
    t1, t2 = entry_mid + T1_R * risk, entry_mid + T2_R * risk

    # R:R measured where you would actually buy: CMP normally, the zone if extended
    ref = entry_mid if extended else cmp_
    rr_at_cmp = (t2 - ref) / (ref - sl) if ref > sl else None
    if rr_at_cmp is None or rr_at_cmp < MIN_RR:
        return {"valid": False,
                "reason": f"no valid setup ({'at zone' if extended else 'at CMP'}: R:R "
                          f"{'n/a' if rr_at_cmp is None else round(rr_at_cmp, 2)} < {MIN_RR})"}

    flags = []
    if risk / entry_mid > WIDE_STOP_PCT:
        flags.append(f"wide stop ({risk / entry_mid * 100:.1f}% risk) - size down")
    if atr / cmp_ > HIGH_ATR_PCT:
        flags.append(f"high volatility (ATR {atr / cmp_ * 100:.1f}% of price) - reduce size")
    resistance = round(hi52, 2) if entry_mid < hi52 < t2 else None

    # ---- volume confirmation (entry-validity, distinct from smart-money score) ----
    status, vol_note = "WAIT", "no volume trigger yet"
    v_today = float(vols.iloc[-1]) if len(vols) else None
    v_avg20 = float(vols.iloc[-21:-1].mean()) if len(vols) >= 21 else None
    near_high = cmp_ >= NEAR_HIGH_PCT * hi52
    in_zone = zone_lo <= cmp_ <= zone_hi
    if v_today and v_avg20:
        up_day = chg_pct is not None and chg_pct > 0
        if up_day and v_today >= VOL_CONFIRM_X * v_avg20:
            status = "CONFIRMED"
            vol_note = f"up day on {v_today / v_avg20:.1f}x avg volume"
        elif (not up_day) and in_zone and (not extended) and v_today < v_avg20:
            status = "PULLBACK OK"
            vol_note = f"pullback into zone on dry volume ({v_today / v_avg20:.1f}x avg)"
        elif near_high:
            vol_note = "near 52w high - breakout needs >=1.5x volume; not present"
    v20, v50 = (float(vols.iloc[-20:].mean()) if len(vols) >= 20 else None,
                float(vols.iloc[-50:].mean()) if len(vols) >= 50 else None)
    vol_trend = None
    if v20 and v50:
        vol_trend = "rising interest (20d vol > 50d)" if v20 > v50 * 1.05 else \
                    "fading interest (20d vol < 50d)" if v20 < v50 * 0.95 else "steady volume"

    return {"valid": True, "dma20": round(dma20, 2),
            "buy_lo": round(zone_lo, 2), "buy_hi": round(zone_hi, 2),
            "zone_note": zone_note, "sl": round(sl, 2),
            "t1": round(t1, 2), "t2": round(t2, 2),
            "risk_pct": round(risk / entry_mid * 100, 1),
            "rr_at_cmp": round(rr_at_cmp, 2), "resistance_52w": resistance,
            "entry_status": status, "vol_note": vol_note, "vol_trend": vol_trend,
            "atr": round(atr, 2), "flags": flags}


# ------------------------------------------------------- stage lifecycle engine

STAGE_HYSTERESIS = 2          # sessions a new stage must hold before transition
TIME_STOP_DAYS = 42           # ~6 weeks: exit-flat rule for READY positions
READY_BASE_ATR = 1.5          # "in base" = within this many ATRs of the 20 DMA
READY_MAX_RUN = 40.0          # 6m return ceiling for accumulation stage
READY_MIN_OFF_HIGH = 5.0      # must be >5% below 52w high


def distribution_days(hist: pd.DataFrame | None, lookback=10) -> int:
    """Down days on above-50d-average volume within the lookback window."""
    if hist is None or len(hist) < 55:
        return 0
    vols = hist["Volume"].dropna()
    avg50 = vols.iloc[-50:].mean()
    tail = hist.iloc[-lookback:]
    chg = tail["Close"].diff()
    return int(((chg < 0) & (tail["Volume"] > avg50)).sum())


def classify_stage(r: dict, plan: dict, hist: pd.DataFrame | None,
                   deal: dict | None) -> tuple[str, dict]:
    """RED > CONFIRMED > READY > NEUTRAL. All thresholds deterministic."""
    ud = r["smart_detail"].get("ud_val")
    dist = distribution_days(hist)
    r6 = r.get("ret_6m_pct")

    evidence = []
    if deal and deal["net_qty"] < 0:
        evidence.append(f"net bulk/block SELLING {abs(int(deal['net_qty'])):,} sh over "
                        f"{BULK_LOOKBACK_SESSIONS} sessions")
    if ud is not None and ud < 1 and dist >= 3:
        evidence.append(f"U/D {ud} with {dist} distribution days in 10 sessions")
    if r6 is not None and r6 >= 100 and (ud is None or ud < 2):
        evidence.append(f"+{r6:.0f}% six-month run with fading U/D "
                        f"({'n/a' if ud is None else ud}) - exhaustion pattern")
    if evidence:
        return "RED", {"evidence": evidence, "dist_days": dist}

    v1 = any(v["code"] == "V1" for v in r["vetoes"])
    off_hi = r["tech_detail"].get("off_52w_high_pct")
    rs = r["tech_detail"].get("rs_vs_nifty_3m_pct")
    breakout_ctx = off_hi is not None and off_hi <= 3 and (ud or 0) >= 2 and (rs or 0) > 0
    if v1 or plan.get("entry_status") == "CONFIRMED" or breakout_ctx:
        return "CONFIRMED", {"late_stage_v1": v1}

    signals, flags = [], []
    if deal and deal["net_qty"] > 0:
        signals.append(f"bulk/block net buy {int(deal['net_qty']):,} sh")
        if deal.get("buys", 0) == 1 and deal.get("sells", 0) == 0:
            flags.append("single-deal driven - verify buyer name on NSE "
                         "(could be promoter/inter-se, not fresh institutional money)")
    if any(b.startswith("DELIV") for b in r["badges"]):
        signals.append(f"delivery spike ({r['smart_detail'].get('deliv_ratio')}x own avg)")
    if ud is not None and ud >= UD_MOD:
        signals.append(f"U/D {ud}")
    if any(b.startswith("RVOL") for b in r["badges"]):
        signals.append("volume surge w/ price confirm")

    in_base = (plan.get("valid")
               and plan.get("dma20") and plan.get("atr")
               and abs(r["cmp"] - plan["dma20"]) <= READY_BASE_ATR * plan["atr"]
               and (off_hi or 0) > READY_MIN_OFF_HIGH
               and (r6 is None or r6 < READY_MAX_RUN))
    if signals and in_base:
        return "READY", {"signals": signals, "flags": flags,
                         "tier": "strong" if len(signals) >= 2 else "single"}
    return "NEUTRAL", {}


def apply_stage_hysteresis(raw: dict, trade_date: str) -> dict:
    """A stock transitions only after qualifying for the new stage on
    STAGE_HYSTERESIS consecutive sessions. Idempotent per trade date."""
    path = STATE_DIR / "stage_history.json"
    hist = load_json(path, {})
    out = {}
    for sym, (stage, info) in raw.items():
        h = hist.get(sym)
        if h is None:
            h = {"stage": stage, "since": trade_date, "cand": None,
                 "streak": 0, "last": trade_date}
        elif h.get("last") != trade_date:          # advance streaks once per session
            if stage == h["stage"]:
                h["cand"], h["streak"] = None, 0
            elif stage == h.get("cand"):
                h["streak"] += 1
                if h["streak"] >= STAGE_HYSTERESIS:
                    h.update({"stage": stage, "since": trade_date,
                              "cand": None, "streak": 0})
            else:
                h["cand"], h["streak"] = stage, 1
            h["last"] = trade_date
        hist[sym] = h
        days = (datetime.fromisoformat(trade_date)
                - datetime.fromisoformat(h["since"])).days
        out[sym] = {"stage": h["stage"], "since": h["since"], "days_in_stage": days,
                    "pending": h.get("cand"),
                    "time_stop_left": (TIME_STOP_DAYS - days) if h["stage"] == "READY" else None,
                    "info": info if h["stage"] == raw[sym][0] else {}}
    save_json(path, hist)
    return out


def build_rationale(r: dict) -> str:
    """Assembled strictly from computed facts - no invented conviction."""
    p, fd, td, sd = [], r["fund_detail"], r["tech_detail"], r["smart_detail"]
    if fd.get("rev_yoy") is not None:
        p.append(f"Revenue {fd['rev_yoy']:+.1f}% / PAT "
                 f"{'n/a' if fd.get('pat_yoy') is None else format(fd['pat_yoy'], '+.1f') + '%'} "
                 f"YoY (qtr {fd.get('latest_quarter', '')})")
    sig = []
    if "BULK+" in r["badges"]:
        sig.append(sd.get("bulk", "bulk buying"))
    if any(b.startswith("DELIV") for b in r["badges"]):
        sig.append("delivery spike " + sd.get("delivery", ""))
    if any(b.startswith("RVOL") for b in r["badges"]):
        sig.append("volume surge " + sd.get("rvol", ""))
    if any(b.startswith("U/D") for b in r["badges"]):
        sig.append(f"U/D ratio {sd.get('ud', '')}")
    p.append("Accumulation: " + "; ".join(sig) if sig
             else "No smart-money signal fired yet (history warming up)")
    if td.get("stage"):
        t = td["stage"]
        if td.get("rs_vs_nifty_3m_pct") is not None:
            t += f", {td['rs_vs_nifty_3m_pct']:+.1f}% vs Nifty over 3m"
        if td.get("off_52w_high_pct") is not None:
            t += f", {td['off_52w_high_pct']:.1f}% off 52w high"
        p.append(t)
    return ". ".join(p) + "."


def six_month_return(hist: pd.DataFrame | None) -> float | None:
    if hist is None:
        return None
    close = hist["Close"].dropna()
    if len(close) < 120:
        return None
    return float(close.iloc[-1] / close.iloc[-125:].iloc[0] - 1)


# ---------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weekly", action="store_true", help="refresh fundamentals cache")
    ap.add_argument("--backfill", type=int, default=0,
                    help="one-time: backfill delivery history from N calendar days of NSE archives")
    args = ap.parse_args()

    run_flags = []

    log("1/6 Universe...")
    universe, usrc = get_universe()
    log(f"    {len(universe)} symbols ({usrc})")

    if args.backfill:
        backfill_delivery(universe, args.backfill)

    log("2/6 Bhavcopy...")
    bhav, trade_date = fetch_bhavcopy()
    price_source = "NSE bhavcopy"
    if bhav is None:
        run_flags.append("NSE bhavcopy unavailable - prices from Yahoo, delivery % unavailable today")
        price_source = "Yahoo Finance (fallback)"
        trade_date = datetime.now(IST).strftime("%Y-%m-%d")
        log("    FAILED -> yfinance fallback for prices; no delivery data today")
    else:
        log(f"    trade date {trade_date}, {len(bhav)} rows")

    log("3/6 Bulk/block deals...")
    deals = fetch_deals() if bhav is not None else []
    if bhav is not None and not deals:
        run_flags.append("bulk/block files unavailable today - component scored from prior sessions only")
    deals_agg = update_deal_history([d for d in deals if d["symbol"] in universe],
                                    trade_date)
    log(f"    {len(deals)} deals today; {len(deals_agg)} universe symbols in {BULK_LOOKBACK_SESSIONS}s window")

    log("4/6 Delivery history...")
    deliv = update_delivery_history(bhav, universe, trade_date) if bhav is not None else {}

    log("5/6 Price history (yfinance)...")
    hists = fetch_price_history(list(universe.keys()))
    log(f"    history for {len(hists)}/{len(universe)}")
    if len(hists) < len(universe) * 0.7:
        run_flags.append(f"price history coverage low ({len(hists)}/{len(universe)})")

    log("6/6 Fundamentals...")
    funds, fsrc = load_fundamentals(list(universe.keys()), args.weekly)
    log(f"    source: {fsrc}")

    # per-symbol EOD close / day change
    eod = {}
    if bhav is not None:
        sub = bhav[bhav["SYMBOL"].isin(universe.keys())]
        for _, r in sub.iterrows():
            chg = (r["CLOSE_PRICE"] / r["PREV_CLOSE"] - 1) * 100 if r["PREV_CLOSE"] else None
            eod[r["SYMBOL"]] = {"close": float(r["CLOSE_PRICE"]),
                                "chg_pct": round(chg, 2) if chg is not None else None}
    else:
        for sym, h in hists.items():
            c = h["Close"].dropna()
            if len(c) >= 2:
                eod[sym] = {"close": round(float(c.iloc[-1]), 2),
                            "chg_pct": round(float(c.iloc[-1] / c.iloc[-2] - 1) * 100, 2)}

    # Nifty 50 three-month return for relative strength
    nifty_ret3m = None
    try:
        import yfinance as yf
        ndf = yf.download("^NSEI", period="6mo", interval="1d",
                          auto_adjust=True, progress=False)
        nc = ndf["Close"].squeeze().dropna()   # single ticker may return MultiIndex cols
        if len(nc) >= 63:
            nifty_ret3m = float(nc.iloc[-1] / nc.iloc[-63] - 1)
    except Exception as e:
        log(f"  Nifty benchmark unavailable ({type(e).__name__}); RS neutral")
    if nifty_ret3m is None:
        run_flags.append("Nifty benchmark unavailable - relative strength scored neutral")

    log("Scoring...")
    rows, excluded_v2 = [], 0
    excluded_illiquid = excluded_downtrend = 0
    for sym, meta in universe.items():
        if sym not in eod:
            continue
        h = hists.get(sym)
        f_score, f_det = score_fundamental(funds.get(sym))
        s_score, s_det, badges = score_smart(sym, deals_agg, deliv, h,
                                             eod[sym]["chg_pct"])
        t_score, t_det = score_technical(h, nifty_ret3m)

        vetoes = []
        r6 = six_month_return(h)
        if r6 is not None and r6 >= V1_RUN_THRESHOLD:
            vetoes.append({"code": "V1",
                           "text": f"+{r6*100:.0f}% in 6m - momentum confirmation, not fresh entry"})
        if f_score is None:
            vetoes.append({"code": "V2", "text": f"fundamentals unavailable: {f_det['reason']}"})
            excluded_v2 += 1

        composite = None
        if f_score is not None:
            composite = round(WEIGHTS["fundamental"] * f_score
                              + WEIGHTS["smart"] * s_score
                              + WEIGHTS["technical"] * t_score, 2)

        # hard filters (tracked, not silently dropped)
        turnover = avg_turnover_20d(h) if h is not None else None
        liquid = turnover is not None and turnover >= LIQ_FLOOR
        in_uptrend = bool(t_det.get("above_200dma", False))
        if not liquid:
            excluded_illiquid += 1
        elif not in_uptrend:
            excluded_downtrend += 1

        row = {
            "symbol": sym, "name": meta["name"], "index": meta["index"],
            "cmp": eod[sym]["close"], "chg_pct": eod[sym]["chg_pct"],
            "composite": composite,
            "scores": {"fundamental": f_score, "smart": s_score, "technical": t_score},
            "fund_detail": f_det, "smart_detail": s_det, "tech_detail": t_det,
            "badges": badges, "vetoes": vetoes,
            "ret_6m_pct": round(r6 * 100, 1) if r6 is not None else None,
            "liquid": liquid, "in_uptrend": in_uptrend,
            "avg_turnover_cr": round(turnover / 1e7, 2) if turnover else None,
        }
        row["rationale"] = build_rationale(row)
        rows.append(row)

    scored_ok = [r for r in rows if r["composite"] is not None]
    eligible = sorted((r for r in scored_ok if not r["vetoes"]
                       and r["liquid"] and r["in_uptrend"]),
                      key=lambda r: r["composite"], reverse=True)
    momentum = sorted((r for r in scored_ok
                       if any(v["code"] == "V1" for v in r["vetoes"])),
                      key=lambda r: r["composite"], reverse=True)
    broken = sorted((r for r in scored_ok if not r["vetoes"]
                     and r["liquid"] and not r["in_uptrend"]),
                    key=lambda r: r["composite"], reverse=True)

    # trade plans for the entry candidates plus stage classification for all liquid names
    raw_stages, plans = {}, {}
    for r in scored_ok + [x for x in rows if x["composite"] is None]:
        if not r["liquid"]:
            continue
        p = build_trade_plan(hists.get(r["symbol"]), r["cmp"], r["chg_pct"])
        plans[r["symbol"]] = p
        raw_stages[r["symbol"]] = classify_stage(
            r, p, hists.get(r["symbol"]), deals_agg.get(r["symbol"]))
    staged = apply_stage_hysteresis(raw_stages, trade_date)

    def with_stage(r, want, include_plan=True):
        s = staged.get(r["symbol"])
        if not s or s["stage"] != want:
            return None
        r = dict(r)
        r["stage"] = s
        if include_plan and not (want == "CONFIRMED" and s["info"].get("late_stage_v1")):
            r["plan"] = plans.get(r["symbol"])
        return r

    def comp_key(r):
        return r["composite"] if r["composite"] is not None else -1

    ready_list = sorted(filter(None, (with_stage(r, "READY") for r in scored_ok
                                      if r["in_uptrend"])),
                        key=comp_key, reverse=True)[:10]
    confirmed_list = sorted(filter(None, (with_stage(r, "CONFIRMED") for r in scored_ok
                                          if r["in_uptrend"])),
                            key=comp_key, reverse=True)[:10]
    red_list = sorted(filter(None, (with_stage(r, "RED", include_plan=False)
                                    for r in rows if r["liquid"])),
                      key=comp_key, reverse=True)[:10]
    for lst in (ready_list, confirmed_list, red_list):
        for i, r in enumerate(lst, 1):
            r["rank"] = i

    for r in eligible[:10]:
        r["plan"] = plans.get(r["symbol"], build_trade_plan(
            hists.get(r["symbol"]), r["cmp"], r["chg_pct"]))

    # rank movement vs last published output
    prev = load_json(OUT_DIR / "latest.json", None)
    prev_ranks = {r["symbol"]: i + 1 for i, r in
                  enumerate(prev.get("top10", []))} if prev else {}
    top10 = []
    for i, r in enumerate(eligible[:10], 1):
        r = dict(r)
        r["rank"] = i
        r["prev_rank"] = prev_ranks.get(r["symbol"])
        top10.append(r)

    out = {
        "generated_at": datetime.now(IST).isoformat(),
        "trade_date": trade_date,
        "price_source": price_source,
        "fundamentals_source": f"yfinance quarterly statements ({fsrc})",
        "universe_size": len(universe),
        "scored": len([r for r in rows if r["composite"] is not None]),
        "excluded_no_fundamentals": excluded_v2,
        "excluded_illiquid": excluded_illiquid,
        "excluded_downtrend": excluded_downtrend,
        "liquidity_floor_cr": LIQ_FLOOR / 1e7,
        "run_flags": run_flags,
        "weights": WEIGHTS, "smart_subweights": SMART_SUB,
        "top10": top10,
        "stages": {
            "ready": ready_list,
            "confirmed": confirmed_list,
            "red_flags": red_list,
            "hysteresis_sessions": STAGE_HYSTERESIS,
            "time_stop_days": TIME_STOP_DAYS,
            "note": ("READY = accumulation visible, price still in base - not a "
                     "guarantee of movement; bases can fail. RED = evidence-based "
                     "inference from delivery/deals/volume, not per-stock FII/DII "
                     "data (which is not public). Stage changes require "
                     f"{STAGE_HYSTERESIS} consecutive qualifying sessions."),
        },
        "momentum_watch": momentum[:5],
        "broken_watch": broken[:5],
        "methodology_note": ("Fundamental score uses TRAILING reported growth "
                             "(latest quarter YoY, date-aligned), not forward "
                             "projections - forward analyst estimates are not "
                             "freely available for this universe and are never "
                             "fabricated. Hard filters: EQ series only, avg 20d "
                             "turnover >= Rs 3 cr, price above 200 DMA. Trade "
                             "plans are ATR/structure-derived levels with "
                             "volume-state entry validity, not advice."),
    }
    save_json(OUT_DIR / "latest.json", out)
    save_json(HIST_DIR / f"{trade_date}.json", out)
    log(f"Done. Top-10 written; leader: "
        f"{top10[0]['symbol'] if top10 else 'NONE'} "
        f"({top10[0]['composite'] if top10 else '-'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
