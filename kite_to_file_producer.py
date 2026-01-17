#!/usr/bin/env python3
"""
kite_to_file_producer.py
GitHub-ready version:
- Uses env vars for Kite login
- Graceful exit at 15:15 IST
"""

import os
import time
import json
import threading
import collections
import traceback
from datetime import datetime, timezone, time as dtime
import pytz

from kiteconnect import KiteTicker, KiteConnect

OUT_FILE = os.getenv("TICKS_FILE", "ticks.jsonl")
QUEUE_MAXLEN = 200_000
FLUSH_INTERVAL = 0.05
DEBUG = True
SYMBOLS = ["NIFTY 50"]

IST = pytz.timezone("Asia/Kolkata")
MARKET_CLOSE = dtime(15, 15)

def dbg(*a):
    if DEBUG:
        print("[file-producer]", *a, flush=True)

_write_queue = collections.deque(maxlen=QUEUE_MAXLEN)
_queue_lock = threading.Lock()
_queue_event = threading.Event()

def enqueue_tick(t):
    with _queue_lock:
        _write_queue.append(t)
        _queue_event.set()

def _writer_loop():
    dbg("file writer thread started")
    while True:
        _queue_event.wait(timeout=FLUSH_INTERVAL)
        batch = []
        with _queue_lock:
            while _write_queue:
                batch.append(_write_queue.popleft())
            _queue_event.clear()

        if not batch:
            continue

        with open(OUT_FILE, "a", encoding="utf8") as f:
            for tick in batch:
                f.write(json.dumps(tick, default=str) + "\n")

threading.Thread(target=_writer_loop, daemon=True).start()

def _extract_ts_from_tick(t):
    for k in ("timestamp", "last_trade_time", "exchange_timestamp"):
        if t.get(k):
            return str(t.get(k))
    return datetime.now(timezone.utc).isoformat()

def on_ticks(ws, ticks):
    for t in ticks:
        try:
            enqueue_tick({
                "instrument_token": t.get("instrument_token"),
                "tradingsymbol": t.get("tradingsymbol"),
                "last_price": t.get("last_price"),
                "timestamp": _extract_ts_from_tick(t),
            })
        except Exception:
            dbg("tick error", traceback.format_exc())

def on_connect(ws, resp):
    dbg("Connected, subscribing…")
    instruments = kite.instruments("NSE")
    wanted = {s.upper() for s in SYMBOLS}
    tokens = [
        int(i["instrument_token"])
        for i in instruments
        if i["tradingsymbol"].upper() in wanted
    ]
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    dbg("Subscribed:", tokens)

def on_error(ws, code, reason):
    dbg("WS error:", code, reason)

def on_close(ws, code, reason):
    dbg("WS closed:", code, reason)

def should_exit():
    return datetime.now(IST).time() >= MARKET_CLOSE

def run_producer():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN not set")

    global kite
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    ticker = KiteTicker(api_key, access_token)
    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect
    ticker.on_error = on_error
    ticker.on_close = on_close
    ticker.connect(threaded=True)

    dbg("Ticker started")
    while True:
        if should_exit():
            dbg("15:15 reached, exiting")
            break
        time.sleep(1)

    ticker.close()

if __name__ == "__main__":
    dbg("Starting producer →", OUT_FILE)
    run_producer()
