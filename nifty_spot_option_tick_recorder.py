#!/usr/bin/env python3
"""
NIFTY spot + option tick recorder (multi-expiry, LTP-only)
GitHub-ready version
"""

import os
import csv
import time
import re
from datetime import datetime, date, time as dtime

import pandas as pd
import pytz
from kiteconnect import KiteTicker, KiteConnect

# ---------------- USER CONFIGURATION ----------------
SYMBOLS = ["NIFTY 50"]
EXCHANGE = "NSE"

TICK_CSV_FILE = "nifty_spot_and_options_ticks_multi.csv"

STRIKE_STEP = 50

OPTION_EXPIRIES = [
    date(2026, 1, 20),
    date(2026, 1, 27),
    date(2026, 2, 3)
]

START_TIME = dtime(9, 15)
END_TIME = dtime(15, 30)

IST = pytz.timezone("Asia/Kolkata")

# ---------------- LOGIN (ENV BASED) ----------------
api_key = os.getenv("KITE_API_KEY")
access_token = os.getenv("KITE_ACCESS_TOKEN")

if not api_key or not access_token:
    raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN not set")

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# ---------------- STATE ----------------
token_to_sym = {}
sym_to_token = {}

_nfo_index_by_key = {}
_nfo_tradingsymbol_map = {}

option_meta_by_token = {}

current_atm_strike = None
current_option_tokens = set()
subscribed_tokens = set()
initial_option_tokens_to_subscribe = []

# ---------------- UTIL ----------------
def is_trading_hours(ts: pd.Timestamp) -> bool:
    return START_TIME <= ts.time() <= END_TIME

def round_to_strike(price):
    try:
        return int(round(price / STRIKE_STEP) * STRIKE_STEP)
    except Exception:
        return None

def _extract_tick_ts(t):
    for k in ("timestamp", "last_trade_time", "exchange_timestamp"):
        if t.get(k):
            try:
                return pd.to_datetime(t[k])
            except Exception:
                pass
    return pd.Timestamp.now()

# ---------------- CSV ----------------
tick_file = None
tick_writer = None

def init_csv():
    global tick_file, tick_writer
    tick_file = open(TICK_CSV_FILE, "a+", newline="")
    tick_writer = csv.writer(tick_file)

    if os.stat(TICK_CSV_FILE).st_size == 0:
        tick_writer.writerow([
            "timestamp", "instrument_token", "tradingsymbol", "kind",
            "last_price", "expiry", "strike", "option_type"
        ])
        tick_file.flush()

def record_tick(ts, instrument_token, tradingsymbol, kind, last_price):
    expiry = strike = opt_type = ""

    if kind == "OPT":
        meta = option_meta_by_token.get(instrument_token, {})
        expiry = meta.get("expiry", "")
        strike = meta.get("strike", "")
        opt_type = meta.get("opt_type", "")
        expiry = expiry.isoformat() if expiry else ""

    tick_writer.writerow([
        ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        instrument_token,
        tradingsymbol,
        kind,
        last_price,
        expiry,
        strike,
        opt_type,
    ])
    tick_file.flush()

# ---------------- NFO INDEX ----------------
_TS_RE = re.compile(r"^(?P<underlying>[A-Z]+)(?P<day>\d{1,2})(?P<month>[A-Z]{3})(?P<strike>\d+)(?P<opt>CE|PE)$")
_MONTH_MAP = {m: i + 1 for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}

def _parse_expiry_from_tradingsymbol(tsym):
    m = _TS_RE.match(tsym)
    if not m:
        return None
    day = int(m.group("day"))
    mon = _MONTH_MAP.get(m.group("month"))
    if not mon:
        return None
    today = date.today()
    year = today.year
    if mon < today.month or (mon == today.month and day < today.day):
        year += 1
    return date(year, mon, day)

def build_nfo_index_with_expiry():
    instruments = kite.instruments("NFO")
    index, ts_map, token_meta = {}, {}, {}

    for inst in instruments:
        ts = (inst.get("tradingsymbol") or "").upper()
        ts_map[ts] = inst

        strike = inst.get("strike")
        strike = int(strike) if strike else None

        opt_type = (inst.get("option_type") or "").upper()
        expiry = inst.get("expiry")
        if expiry:
            if hasattr(expiry, "date"):
                expiry = expiry.date()
            elif isinstance(expiry, date):
                expiry = expiry
            else:
                try:
                    expiry = datetime.strptime(str(expiry), "%Y-%m-%d").date()
                except Exception:
                    expiry = _parse_expiry_from_tradingsymbol(ts)
        else:
            expiry = _parse_expiry_from_tradingsymbol(ts)

        underlying = (inst.get("name") or "").upper()

        if not (underlying and strike and opt_type and expiry):
            continue

        key = (underlying, strike, opt_type, expiry)
        index[key] = inst

        tok = int(inst["instrument_token"])
        token_meta[tok] = {
            "expiry": expiry,
            "strike": strike,
            "opt_type": opt_type,
            "tradingsymbol": ts
        }

    return index, ts_map, token_meta

def resolve_option(index, underlying, strike, opt_type, expiry):
    inst = index.get((underlying, strike, opt_type, expiry))
    if inst:
        return int(inst["instrument_token"]), inst["tradingsymbol"]
    return None, None

# ---------------- SUBSCRIPTIONS ----------------
def subscribe_tokens(ws, tokens):
    t = [x for x in tokens if x not in subscribed_tokens]
    if t:
        ws.subscribe(t)
        ws.set_mode(ws.MODE_FULL, t)
        subscribed_tokens.update(t)

def unsubscribe_tokens(ws, tokens):
    t = [x for x in tokens if x in subscribed_tokens]
    if t:
        ws.unsubscribe(t)
        for x in t:
            subscribed_tokens.discard(x)

def update_option_band(ws, underlying, spot_ltp):
    global current_atm_strike, current_option_tokens
    atm = round_to_strike(spot_ltp)
    if atm == current_atm_strike:
        return

    strikes = {atm, atm-50, atm+50, atm-100, atm+100}
    new_tokens = set()

    for st in strikes:
        for opt in ("CE", "PE"):
            for exp in OPTION_EXPIRIES:
                tok, tsym = resolve_option(_nfo_index_by_key, underlying, st, opt, exp)
                if tok:
                    new_tokens.add(tok)
                    token_to_sym[tok] = tsym

    unsubscribe_tokens(ws, current_option_tokens - new_tokens)
    subscribe_tokens(ws, new_tokens - current_option_tokens)

    current_option_tokens = new_tokens
    current_atm_strike = atm

# ---------------- TICKS ----------------
def on_ticks(ws, ticks):
    for t in ticks:
        tok = int(t.get("instrument_token", 0))
        ltp = t.get("last_price")
        ts = _extract_tick_ts(t)

        tsym = token_to_sym.get(tok, "")
        kind = "SPOT" if tok in spot_tokens else "OPT"
        record_tick(ts, tok, tsym, kind, ltp)

        if tok in spot_tokens:
            update_option_band(ws, "NIFTY", ltp)

def on_connect(ws, resp):
    subscribe_tokens(ws, spot_tokens)
    subscribe_tokens(ws, initial_option_tokens_to_subscribe)
    print("[WS] Connected")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("[INFO] Building NFO index...")
    _nfo_index_by_key, _, option_meta_by_token = build_nfo_index_with_expiry()

    insts = kite.instruments("NSE")
    spot_tokens = [int(i["instrument_token"]) for i in insts if i["tradingsymbol"] == "NIFTY 50"]
    token_to_sym = {spot_tokens[0]: "NIFTY 50"}

    init_csv()

    ticker = KiteTicker(api_key, access_token)
    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect

    ticker.connect(threaded=True)

    try:
        while datetime.now(IST).time() <= END_TIME:
            time.sleep(1)
    finally:
        ticker.close()
        tick_file.close()
        print("Recorder stopped")
