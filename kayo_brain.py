"""
KAYO BRAIN - COMPLETE WEB3 INTELLIGENCE BOT
VERSION: 11.0 - FULL UPGRADE
- DEX charts open inline in Telegram (WebApp)
- Auto scan & report new tokens before they move
- Twitter/news scraper with CA detection, auto-reports to group
- Runners filter loosened (DexScreener defaults)
- Buttons/autoresponder fixed (proper global state)
- Auto unusual activity alerts
- Self-learning from Twitter narratives
- Smart replies like a Web3 pro
"""

import asyncio
import logging
import re
import time
import json
import random
import math
import hashlib
import os
import base64
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote_plus

import aiohttp
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, MenuButtonCommands, WebAppInfo,
)
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters,
)

from flask import Flask, request, jsonify
import threading

BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID", "0"))  # Set this on Render to auto-report

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "🦅 Kayo Brain v11 is alive!", 200

@flask_app.route('/health')
def health_check():
    return "OK", 200

def run_webserver():
    flask_app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)

start_time = time.time()
threading.Thread(target=run_webserver, daemon=True).start()
logger.info("🌐 Web server started on port 8080")

# ── Global state ─────────────────────────────────────────────
settings: Dict[int, Dict] = {}          # per-chat settings
active_calls: Dict[int, Dict] = {}      # per-user calls
user_xp: Dict[int, int] = {}
seen_tokens: set = set()                # tokens already reported
seen_news: set = set()                  # news already reported
last_token_report = 0
last_twitter_scan = 0
kayo_knowledge: List[str] = []          # learned narratives


# ── Helpers ──────────────────────────────────────────────────
def fmt_price(p):
    if p == 0: return "$0"
    if p < 0.000001: return f"${p:.10f}".rstrip('0').rstrip('.')
    if p < 0.0001:   return f"${p:.8f}".rstrip('0').rstrip('.')
    if p < 0.01:     return f"${p:.6f}".rstrip('0').rstrip('.')
    if p < 1:        return f"${p:.4f}".rstrip('0').rstrip('.')
    return f"${p:,.4f}"

def fmt_usd(v):
    if v >= 1_000_000_000: return f"${v/1_000_000_000:.2f}B"
    if v >= 1_000_000:     return f"${v/1_000_000:.2f}M"
    if v >= 1_000:         return f"${v/1_000:.1f}K"
    return f"${v:.2f}"

def fmt_pct(v, sign=True):
    try:
        return f"{'+' if sign and v>0 else ''}{v:.1f}%"
    except:
        return "N/A"

def safety_emoji(s):
    if s >= 80: return "🟢"
    if s >= 50: return "🟡"
    if s >= 20: return "🟠"
    return "🔴"

def add_xp(uid: int, amount: int = 5):
    user_xp[uid] = user_xp.get(uid, 0) + amount


# ── API calls ────────────────────────────────────────────────
async def dex_token(session: aiohttp.ClientSession, address: str) -> Optional[Dict]:
    try:
        async with session.get(f"https://api.dexscreener.com/latest/dex/tokens/{address}", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return None
            data = await r.json()
            pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
            if not pairs: return None
            pairs.sort(key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)
            return pairs[0]
    except:
        return None

async def dex_search(session: aiohttp.ClientSession, query: str = "solana") -> List[Dict]:
    try:
        async with session.get(f"https://api.dexscreener.com/latest/dex/search?q={quote_plus(query)}", timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200: return []
            data = await r.json()
            return [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
    except:
        return []

async def dex_new_pairs(session: aiohttp.ClientSession) -> List[Dict]:
    """Fetch brand new pairs from DexScreener."""
    try:
        async with session.get("https://api.dexscreener.com/token-profiles/latest/v1", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                return [p for p in (data if isinstance(data, list) else []) if p.get("chainId") == "solana"]
    except:
        pass
    # fallback: search boosted
    try:
        async with session.get("https://api.dexscreener.com/token-boosts/latest/v1", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                return data if isinstance(data, list) else []
    except:
        pass
    return []

async def goplus_sec(session: aiohttp.ClientSession, address: str) -> Dict:
    try:
        async with session.get(f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={address}", timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return {}
            data = await r.json()
            result = data.get("result", {})
            return result.get(address.lower(), result.get(address, {}))
    except:
        return {}

async def coingecko_coin(session: aiohttp.ClientSession, coin_id: str) -> Optional[Dict]:
    try:
        async with session.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return None
            return await r.json()
    except:
        return None

async def coingecko_global(session: aiohttp.ClientSession) -> Dict:
    try:
        async with session.get("https://api.coingecko.com/api/v3/global", timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return {}
            return (await r.json()).get("data", {})
    except:
        return {}

async def coingecko_trending(session: aiohttp.ClientSession) -> List:
    try:
        async with session.get("https://api.coingecko.com/api/v3/search/trending", timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return []
            return (await r.json()).get("coins", [])
    except:
        return []

async def scrape_nitter(session: aiohttp.ClientSession, query: str, limit=10) -> List[Dict]:
    instances = [
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
        "https://nitter.1d4.us",
    ]
    for base in instances:
        try:
            url = f"{base}/search?q={quote_plus(query)}&f=tweets"
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: continue
                html = await r.text()
                tweets = re.findall(r'<div class="tweet-content[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
                users  = re.findall(r'<a class="username"[^>]*href="/([^"]+)"', html)
                results = []
                for i, t in enumerate(tweets[:limit]):
                    clean = re.sub(r'<[^>]+>', '', t).strip()
                    if clean and len(clean) > 20:
                        results.append({"text": clean[:400], "user": users[i] if i < len(users) else "unknown"})
                if results:
                    return results
        except:
            continue
    return []


# ── Smart scan ───────────────────────────────────────────────
async def smart_scan(address: str) -> Dict:
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
    if not pair:
        return {"error": "Token not found on Solana"}
    base    = pair.get("baseToken", {})
    symbol  = base.get("symbol", "???")
    name    = base.get("name", "Unknown")
    price   = float(pair.get("priceUsd", 0) or 0)
    fdv     = float(pair.get("fdv", 0) or 0)
    liq     = float(pair.get("liquidity", {}).get("usd", 0) or 0)
    ch_1h   = float(pair.get("priceChange", {}).get("h1", 0) or 0)
    ch_5m   = float(pair.get("priceChange", {}).get("m5", 0) or 0)
    ch_24h  = float(pair.get("priceChange", {}).get("h24", 0) or 0)
    buys_1h = int(pair.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells_1h= int(pair.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    vol_1h  = float(pair.get("volume", {}).get("h1", 0) or 0)
    vol_5m  = float(pair.get("volume", {}).get("m5", 0) or 0)
    vol_24h = float(pair.get("volume", {}).get("h24", 0) or 0)
    # narrative
    narrative, narrative_score = "Meme", 5
    text = f"{name} {symbol}".lower()
    if any(w in text for w in ['ai', 'agent', 'gpt', 'intelligence']): narrative, narrative_score = "AI", 9
    elif any(w in text for w in ['game', 'play', 'gaming', 'nft']):    narrative, narrative_score = "Gaming", 8
    elif any(w in text for w in ['defi', 'swap', 'yield', 'lend']):    narrative, narrative_score = "DeFi", 8
    elif any(w in text for w in ['rwa', 'real', 'asset', 'estate']):   narrative, narrative_score = "RWA", 9
    # momentum
    vol_ratio = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 1
    momentum_score = min(100, max(0,
        (min(50, ch_1h * 2) if ch_1h > 0 else 0) +
        (min(30, vol_ratio * 10) if vol_ratio > 1 else 0) +
        (min(20, buys_1h / 2) if buys_1h > 20 else 0)
    ))
    # safety
    rug_score = 100
    if sec.get("is_honeypot") == "1":       rug_score -= 60
    if sec.get("cannot_sell_all") == "1":   rug_score -= 40
    if float(sec.get("sell_tax", 0) or 0) > 10: rug_score -= 20
    if sec.get("lp_locked") == "1":         rug_score += 10
    rug_score = max(0, min(100, rug_score))
    liq_ratio = liq / fdv if fdv > 0 else 0
    # kayo AI opinion
    if momentum_score > 70 and rug_score > 70:
        opinion = "🟢 KAYO SAYS: APE — Strong momentum + clean safety."
    elif momentum_score > 50 and rug_score > 50:
        opinion = "🟡 KAYO SAYS: WATCH — Decent setup, wait for confirmation."
    elif rug_score < 40:
        opinion = "🔴 KAYO SAYS: AVOID — Too many safety red flags."
    else:
        opinion = "🟠 KAYO SAYS: CAUTION — Mixed signals, manage risk."
    return {
        "address": address, "symbol": symbol, "name": name,
        "price": price, "fdv": fdv, "liq": liq,
        "ch_1h": ch_1h, "ch_5m": ch_5m, "ch_24h": ch_24h,
        "buys_1h": buys_1h, "sells_1h": sells_1h,
        "vol_ratio": vol_ratio, "vol_24h": vol_24h,
        "momentum_score": momentum_score,
        "narrative": narrative, "narrative_score": narrative_score,
        "rug_score": rug_score, "liq_ratio": liq_ratio,
        "opinion": opinion, "pair": pair, "sec": sec,
    }


def build_scan_card(a: Dict) -> str:
    buy_pressure = "🔥 BUY PRESSURE" if a['buys_1h'] > a['sells_1h'] * 1.5 else ("⚖️ BALANCED" if abs(a['buys_1h'] - a['sells_1h']) < a['buys_1h'] * 0.3 else "🔻 SELL PRESSURE")
    return (
        f"🦅 **KAYO SCAN — ${a['symbol']}** ({a['name']})\n"
        f"{'═'*42}\n\n"
        f"💰 **Price:** {fmt_price(a['price'])}\n"
        f"📊 **MCap:** {fmt_usd(a['fdv'])}  |  **Liq:** {fmt_usd(a['liq'])}\n"
        f"📈 **5m:** {fmt_pct(a['ch_5m'])}  |  **1h:** {fmt_pct(a['ch_1h'])}  |  **24h:** {fmt_pct(a['ch_24h'])}\n\n"
        f"⚡ **Momentum:** {a['momentum_score']}/100  |  Vol spike: {a['vol_ratio']:.1f}x\n"
        f"🅱 Buys: {a['buys_1h']}  🆂 Sells: {a['sells_1h']}  →  {buy_pressure}\n\n"
        f"🔮 **Narrative:** {a['narrative']} ({a['narrative_score']}/10)\n"
        f"🛡️ **Safety:** {safety_emoji(a['rug_score'])} {a['rug_score']}/100\n"
        f"💧 **Liq/MCap:** {a['liq_ratio']*100:.1f}%\n\n"
        f"🧠 **{a['opinion']}**\n\n"
        f"`{a['address']}`"
    )


# ── DEX Chart buttons (opens inside Telegram) ────────────────
def get_chart_buttons(address: str, symbol: str) -> InlineKeyboardMarkup:
    dex_url    = f"https://dexscreener.com/solana/{address}"
    birdeye    = f"https://birdeye.so/token/{address}?chain=solana"
    pumpfun    = f"https://pump.fun/{address}"
    photon     = f"https://photon-sol.tinyastro.io/en/lp/{address}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 DEX Chart", web_app=WebAppInfo(url=dex_url)),
            InlineKeyboardButton("🦅 Birdeye",   web_app=WebAppInfo(url=birdeye)),
        ],
        [
            InlineKeyboardButton("⚡ Photon",    url=photon),
            InlineKeyboardButton("🎰 Pump.fun",  url=pumpfun),
        ],
        [
            InlineKeyboardButton("🔍 Full Scan", callback_data=f"scan:{address}"),
            InlineKeyboardButton("🛡️ Rug Check", callback_data=f"rug:{address}"),
        ],
    ])


# ── Commands ─────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_xp(update.effective_user.id, 2)
    await update.message.reply_text(
        "🦅 **KAYO BRAIN v11 — WEB3 INTELLIGENCE**\n\n"
        "**🔥 Analysis:**\n"
        "• `/scan <ca>` — Full scan + Kayo opinion\n"
        "• `/smartscan` — Best coins right now\n"
        "• `/runners` — Today's top runners\n"
        "• `/momentum` — Volume spike coins\n"
        "• `/verify <ca>` — Quick rug check\n\n"
        "**📊 Charts (open inside Telegram):**\n"
        "• `/chart <ca>` — DEX chart in chat\n"
        "• `/dex <ca>` — DexScreener inline\n\n"
        "**📰 Intel:**\n"
        "• `/news` — Twitter alpha + CA drops\n"
        "• `/trending` — Hot narratives\n"
        "• `/dt` — Trending DEX tokens\n\n"
        "**💰 Trading:**\n"
        "• `/call <ca>` — Register entry\n"
        "• `/mycalls` — Your calls + P&L\n"
        "• `/stop <ca>` — Lock profits\n\n"
        "**👛 Wallet:**\n"
        "• `/w <address>` — Wallet overview\n\n"
        "**🌍 Market:**\n"
        "• `/macro` — Global market overview\n"
        "• `/a <coin>` — CoinGecko price\n\n"
        "**🧠 Auto Features (24/7):**\n"
        "• Auto-reports new tokens to group\n"
        "• Twitter CA drops detected instantly\n"
        "• Unusual activity alerts\n\n"
        "Drop a CA in chat for instant scan 🦅",
        parse_mode="Markdown"
    )


async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/scan <token_address>`", parse_mode="Markdown")
        return
    address = context.args[0].strip()
    wait = await update.message.reply_text("🔍 Analyzing...")
    analysis = await smart_scan(address)
    if analysis.get("error"):
        await wait.edit_text(f"❌ {analysis['error']}")
        return
    add_xp(update.effective_user.id, 5)
    await wait.edit_text(
        build_scan_card(analysis),
        reply_markup=get_chart_buttons(address, analysis['symbol']),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )


async def chart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Open DEX chart directly inside Telegram."""
    if not context.args:
        await update.message.reply_text("Usage: `/chart <token_address>`\nExample: `/chart EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`", parse_mode="Markdown")
        return
    address = context.args[0].strip()
    dex_url = f"https://dexscreener.com/solana/{address}"
    birdeye = f"https://birdeye.so/token/{address}?chain=solana"
    await update.message.reply_text(
        f"📊 **DEX Chart**\n\nTap below to open chart inside Telegram:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Open DEX Chart", web_app=WebAppInfo(url=dex_url))],
            [InlineKeyboardButton("🦅 Birdeye Chart",  web_app=WebAppInfo(url=birdeye))],
        ])
    )


async def dex_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Alias for chart - opens DexScreener inline."""
    await chart_cmd(update, context)


async def smartscan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔍 Smart scanning...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    candidates = []
    for p in pairs[:100]:
        base = p.get("baseToken", {})
        fdv  = float(p.get("fdv", 0) or 0)
        liq  = float(p.get("liquidity", {}).get("usd", 0) or 0)
        ch_1h = float(p.get("priceChange", {}).get("h1", 0) or 0)
        buys  = int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        # relaxed filters
        if fdv < 1000 or liq < 1000: continue
        if ch_1h < -50: continue  # skip massive dumps only
        candidates.append({
            "address": base.get("address", ""),
            "symbol": base.get("symbol", "???"),
            "fdv": fdv, "liq": liq,
            "ch_1h": ch_1h,
            "score": ch_1h * 2 + buys / 5 + (liq / 10000)
        })
    candidates.sort(key=lambda x: x["score"], reverse=True)
    if not candidates:
        await wait.edit_text("❌ No coins found. Try again in a moment.")
        return
    lines = ["🦅 **SMART SCAN**\n" + "═"*32 + "\n"]
    for i, c in enumerate(candidates[:10], 1):
        e = "🚀" if c["ch_1h"] > 20 else "📈" if c["ch_1h"] > 5 else "📊"
        lines.append(f"{e} **{i}. ${c['symbol']}**\n   MCap: {fmt_usd(c['fdv'])} | Liq: {fmt_usd(c['liq'])} | 1h: {fmt_pct(c['ch_1h'])}\n   `/scan {c['address']}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def runners_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Runners with loosened DexScreener-style filters. Supports /runners <min_mcap> <min_gain>"""
    # default filters
    min_ch  = 5.0
    min_vol = 5000
    min_fdv = 0
    max_fdv = 50_000_000
    if context.args:
        try: min_ch = float(context.args[0])
        except: pass
    if len(context.args) > 1:
        try: min_vol = float(context.args[1])
        except: pass

    wait = await update.message.reply_text(f"🏃 Finding runners (1h >{min_ch}%, vol >${fmt_usd(min_vol)})...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    runners = []
    for p in pairs:
        base  = p.get("baseToken", {})
        ch_1h = float(p.get("priceChange", {}).get("h1", 0) or 0)
        vol   = float(p.get("volume", {}).get("h24", 0) or 0)
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float(p.get("liquidity", {}).get("usd", 0) or 0)
        if ch_1h >= min_ch and vol >= min_vol and liq >= 500:
            runners.append({"symbol": base.get("symbol", "???"), "address": base.get("address", ""), "ch_1h": ch_1h, "fdv": fdv, "vol": vol, "liq": liq})
    runners.sort(key=lambda x: x["ch_1h"], reverse=True)
    if not runners:
        await wait.edit_text(
            f"No runners found with current filters.\n\n"
            f"💡 Try looser filters:\n`/runners 2` (1h >2%)\n`/runners 1 1000` (1h >1%, vol >$1K)",
            parse_mode="Markdown"
        )
        return
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = [f"🚀 **TODAY'S RUNNERS** (1h >{min_ch}%)\n" + "═"*32 + "\n"]
    for i, r in enumerate(runners[:10]):
        lines.append(f"{medals[i]} **${r['symbol']}** — {fmt_pct(r['ch_1h'])}\n   MCap: {fmt_usd(r['fdv'])} | Vol: {fmt_usd(r['vol'])}\n   `/scan {r['address']}`\n")
    lines.append(f"\n💡 Change filters: `/runners <min_gain%> <min_vol>`")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def momentum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("⚡ Scanning momentum...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    spikes = []
    for p in pairs[:100]:
        base   = p.get("baseToken", {})
        ch_5m  = float(p.get("priceChange", {}).get("m5", 0) or 0)
        ch_1h  = float(p.get("priceChange", {}).get("h1", 0) or 0)
        vol_5m = float(p.get("volume", {}).get("m5", 0) or 0)
        vol_1h = float(p.get("volume", {}).get("h1", 0) or 0)
        fdv    = float(p.get("fdv", 0) or 0)
        liq    = float(p.get("liquidity", {}).get("usd", 0) or 0)
        if liq < 500: continue
        vol_ratio = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 0
        if (ch_5m > 3 and vol_ratio > 1.5) or ch_1h > 10:
            spikes.append({"address": base.get("address",""), "symbol": base.get("symbol","???"), "ch_5m": ch_5m, "ch_1h": ch_1h, "fdv": fdv, "vol_ratio": vol_ratio})
    spikes.sort(key=lambda x: x["ch_5m"], reverse=True)
    if not spikes:
        await wait.edit_text("No momentum spikes right now. Markets quiet.")
        return
    lines = ["⚡ **MOMENTUM SPIKES**\n" + "═"*30 + "\n"]
    for s in spikes[:10]:
        lines.append(f"🔥 **${s['symbol']}**\n   5m: {fmt_pct(s['ch_5m'])} | 1h: {fmt_pct(s['ch_1h'])} | Vol: {s['vol_ratio']:.1f}x\n   `/scan {s['address']}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def verify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/verify <token_address>`", parse_mode="Markdown")
        return
    address = context.args[0].strip()
    wait = await update.message.reply_text("🔍 Running rug check...")
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
    if not pair:
        await wait.edit_text("❌ Token not found on Solana")
        return
    base  = pair.get("baseToken", {})
    rug   = 0
    red   = []
    green = []
    if sec.get("is_honeypot") == "1":
        rug += 60; red.append("🚨 HONEYPOT — Cannot sell!")
    sell_tax = float(sec.get("sell_tax", 0) or 0)
    if sell_tax > 20:  rug += 40; red.append(f"💸 Extreme sell tax: {sell_tax}%")
    elif sell_tax > 10: rug += 20; red.append(f"⚠️ High sell tax: {sell_tax}%")
    if sec.get("lp_locked") == "1": green.append("🔒 Liquidity locked")
    else: rug += 35; red.append("⚠️ Liquidity NOT locked")
    if sec.get("owner_change_balance") == "1": rug += 30; red.append("👑 Owner can change balances")
    if sec.get("is_blacklisted") == "1": rug += 40; red.append("🚫 Contract blacklisted")
    liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
    fdv = float(pair.get("fdv", 0) or 0)
    if fdv > 0 and liq > 0 and liq/fdv < 0.02:
        rug += 25; red.append(f"💧 Shallow liq ({liq/fdv*100:.1f}% of MCap)")
    rug = min(100, rug)
    verdict = ("🔴 CONFIRMED RUG" if rug >= 70 else "🟠 HIGH RISK" if rug >= 50 else "🟡 SUSPICIOUS" if rug >= 30 else "🟢 CLEAN")
    red_text   = "\n".join([f"  • {f}" for f in red[:4]]) if red else "  None found"
    green_text = "\n".join([f"  • {f}" for f in green]) if green else "  None found"
    await wait.edit_text(
        f"🔍 **RUG CHECK — ${base.get('symbol','???')}**\n{'═'*35}\n\n"
        f"**Verdict:** {verdict}\n**Score:** {rug}/100\n\n"
        f"🚩 **Red Flags:**\n{red_text}\n\n"
        f"✅ **Green Flags:**\n{green_text}",
        parse_mode="Markdown"
    )


async def news_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("📰 Scanning Twitter for alpha...")
    async with aiohttp.ClientSession() as session:
        tweets = []
        for q in ["solana new token ca", "alpha alert solana", "new gem solana"]:
            batch = await scrape_nitter(session, q, limit=6)
            tweets.extend(batch)
            await asyncio.sleep(0.5)
    found_cas = []
    news_lines = ["📰 **TWITTER ALPHA**\n" + "═"*30 + "\n"]
    seen = set()
    for tw in tweets:
        text = tw.get("text", "")
        tid  = hashlib.md5(text.encode()).hexdigest()
        if tid in seen: continue
        seen.add(tid)
        cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
        eth = re.findall(r'0x[a-fA-F0-9]{40}', text)
        all_cas = cas + eth
        user = tw.get("user","unknown")
        snippet = text[:120].replace('\n', ' ')
        if all_cas:
            found_cas.extend(all_cas)
            news_lines.append(f"🚨 **@{user}**\n{snippet}\n📌 CA: `{all_cas[0]}`\n")
        elif any(kw in text.lower() for kw in ['launch', 'gem', 'alpha', 'solana', 'pump']):
            news_lines.append(f"📢 **@{user}**\n{snippet}\n")
    if len(news_lines) == 1:
        await wait.edit_text("No fresh alpha found right now. Try again shortly.")
        return
    result = "\n".join(news_lines[:8])
    if found_cas:
        result += f"\n\n💡 Paste any CA above with `/scan <ca>` for full analysis"
    await wait.edit_text(result, parse_mode="Markdown")


async def trending_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔥 Fetching trending...")
    async with aiohttp.ClientSession() as session:
        cg_trending, pairs = await asyncio.gather(
            coingecko_trending(session),
            dex_search(session, "solana")
        )
    lines = ["🔥 **HOT NARRATIVES & TRENDING**\n" + "═"*35 + "\n"]
    if cg_trending:
        lines.append("**📈 CoinGecko Trending:**")
        for c in cg_trending[:5]:
            item = c.get("item", {})
            lines.append(f"  • **${item.get('symbol','?').upper()}** — {item.get('name','')} (Rank #{item.get('market_cap_rank','?')})")
        lines.append("")
    # narrative counter from dex
    narrative_count = Counter()
    for p in pairs[:100]:
        base = p.get("baseToken", {})
        text = f"{base.get('name','')} {base.get('symbol','')}".lower()
        if any(w in text for w in ['ai','agent','gpt']): narrative_count['🤖 AI'] += 1
        elif any(w in text for w in ['game','play','nft']): narrative_count['🎮 Gaming'] += 1
        elif any(w in text for w in ['meme','doge','pepe','cat','dog']): narrative_count['🐸 Meme'] += 1
        elif any(w in text for w in ['defi','swap','yield']): narrative_count['💰 DeFi'] += 1
        else: narrative_count['🎲 Other'] += 1
    lines.append("**🔮 Active Narratives on Solana:**")
    for narrative, count in narrative_count.most_common(5):
        bar = "█" * min(10, count // 2)
        lines.append(f"  {narrative}: {bar} ({count} tokens)")
    if kayo_knowledge:
        lines.append(f"\n**🧠 Kayo Learned:**")
        for k in kayo_knowledge[-3:]:
            lines.append(f"  • {k}")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def dt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔥 Fetching trending DEX...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    trending = sorted(pairs, key=lambda x: float(x.get("volume",{}).get("h24",0) or 0), reverse=True)[:10]
    lines = ["🔥 **TRENDING DEX (by volume)**\n" + "═"*35 + "\n"]
    for i, p in enumerate(trending, 1):
        base = p.get("baseToken", {})
        ch   = float(p.get("priceChange",{}).get("h24",0) or 0)
        vol  = float(p.get("volume",{}).get("h24",0) or 0)
        lines.append(f"{i}. **${base.get('symbol','???')}** {fmt_pct(ch)} | Vol: {fmt_usd(vol)}\n   `/scan {base.get('address','')}` \n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def macro_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🌍 Fetching macro data...")
    async with aiohttp.ClientSession() as session:
        g = await coingecko_global(session)
    if g:
        mcap = g.get("total_market_cap", {}).get("usd", 0)
        vol  = g.get("total_volume", {}).get("usd", 0)
        btc_dom = g.get("market_cap_percentage", {}).get("btc", 0)
        eth_dom = g.get("market_cap_percentage", {}).get("eth", 0)
        ch_24h  = g.get("market_cap_change_percentage_24h_usd", 0)
        await wait.edit_text(
            f"🌍 **MACRO OVERVIEW**\n{'═'*35}\n\n"
            f"**Total MCap:** {fmt_usd(mcap)} ({fmt_pct(ch_24h)})\n"
            f"**24h Volume:** {fmt_usd(vol)}\n"
            f"**BTC Dominance:** {btc_dom:.1f}%\n"
            f"**ETH Dominance:** {eth_dom:.1f}%\n\n"
            f"**Individual Coins:**\n"
            f"• `/a bitcoin` — BTC price\n"
            f"• `/a ethereum` — ETH price\n"
            f"• `/a solana` — SOL price\n\n"
            f"🦅 Kayo's read: {'🟢 Risk ON — deploy capital' if ch_24h > 2 else '🔴 Risk OFF — stay cautious' if ch_24h < -3 else '🟡 Neutral — selective plays'}",
            parse_mode="Markdown"
        )
    else:
        await wait.edit_text("🌍 **MACRO**\n\n• `/a bitcoin` — BTC\n• `/a ethereum` — ETH\n• `/a solana` — SOL\n\n_CoinGecko data unavailable right now._", parse_mode="Markdown")


async def a_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/a <coin>`\nExample: `/a solana`", parse_mode="Markdown")
        return
    query = " ".join(context.args).lower()
    wait  = await update.message.reply_text("🔍 Checking CoinGecko...")
    async with aiohttp.ClientSession() as session:
        data = await coingecko_coin(session, query)
    if not data:
        await wait.edit_text(f"❌ Coin '{query}' not found on CoinGecko.\n\nTry the exact slug e.g. `/a solana`, `/a bonk`", parse_mode="Markdown")
        return
    m     = data.get("market_data", {})
    price = m.get("current_price", {}).get("usd", 0)
    ch24  = m.get("price_change_percentage_24h", 0)
    ch7   = m.get("price_change_percentage_7d", 0)
    mcap  = m.get("market_cap", {}).get("usd", 0)
    vol   = m.get("total_volume", {}).get("usd", 0)
    await wait.edit_text(
        f"🪙 **{data.get('name','')} (${data.get('symbol','').upper()})**\n{'═'*35}\n\n"
        f"💰 Price: {fmt_price(price)}\n"
        f"📈 24h: {fmt_pct(ch24)} | 7d: {fmt_pct(ch7)}\n"
        f"📊 MCap: {fmt_usd(mcap)}\n"
        f"🔄 Vol 24h: {fmt_usd(vol)}\n\n"
        f"🔗 [CoinGecko](https://coingecko.com/en/coins/{query})",
        parse_mode="Markdown",
        disable_web_page_preview=True
    )


async def call_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/call <token_address>`", parse_mode="Markdown")
        return
    address = context.args[0].strip()
    wait    = await update.message.reply_text("📞 Locking entry...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if not pair:
        await wait.edit_text("❌ Token not found")
        return
    price  = float(pair.get("priceUsd", 0) or 0)
    symbol = pair.get("baseToken", {}).get("symbol", "???")
    uid    = update.effective_user.id
    active_calls.setdefault(uid, {})[address] = {
        "symbol": symbol, "entry": price, "at": datetime.utcnow().isoformat()
    }
    add_xp(uid, 10)
    await wait.edit_text(
        f"📞 **CALL LOCKED — ${symbol}**\n\nEntry: {fmt_price(price)}\nTime: {datetime.utcnow().strftime('%H:%M UTC')}\n\nUse `/stop {address}` to close.",
        parse_mode="Markdown"
    )


async def mycalls_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    calls = active_calls.get(uid, {})
    if not calls:
        await update.message.reply_text("You have no active calls. Use `/call <ca>` to register one.", parse_mode="Markdown")
        return
    wait = await update.message.reply_text("📊 Fetching live P&L...")
    lines = ["📊 **YOUR ACTIVE CALLS**\n" + "═"*30 + "\n"]
    async with aiohttp.ClientSession() as session:
        for addr, c in list(calls.items()):
            pair = await dex_token(session, addr)
            if pair:
                curr  = float(pair.get("priceUsd", 0) or 0)
                entry = c["entry"]
                pnl   = ((curr - entry) / entry * 100) if entry > 0 else 0
                e     = "🟢" if pnl > 0 else "🔴"
                lines.append(f"{e} **${c['symbol']}**\n   Entry: {fmt_price(entry)} → Now: {fmt_price(curr)}\n   P&L: {fmt_pct(pnl)}\n")
            else:
                lines.append(f"❓ **${c['symbol']}** — price unavailable\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/stop <token_address>`", parse_mode="Markdown")
        return
    address = context.args[0].strip()
    uid     = update.effective_user.id
    calls   = active_calls.get(uid, {})
    if address not in calls:
        await update.message.reply_text("❌ No active call for this address.", parse_mode="Markdown")
        return
    c    = calls.pop(address)
    wait = await update.message.reply_text("🔒 Closing call...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if pair:
        curr = float(pair.get("priceUsd", 0) or 0)
        pnl  = ((curr - c["entry"]) / c["entry"] * 100) if c["entry"] > 0 else 0
        add_xp(uid, 20)
        await wait.edit_text(
            f"🔒 **CALL CLOSED — ${c['symbol']}**\n\nEntry: {fmt_price(c['entry'])}\nExit:  {fmt_price(curr)}\nP&L:   {fmt_pct(pnl)} {'🎉' if pnl > 0 else '💀'}",
            parse_mode="Markdown"
        )
    else:
        await wait.edit_text(f"✅ Call for ${c['symbol']} closed.")


async def w_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/w <wallet_address>`", parse_mode="Markdown")
        return
    wallet = context.args[0].strip()
    await update.message.reply_text(
        f"👛 **Wallet**\n`{wallet[:12]}...{wallet[-6:]}`\n\n"
        f"[Solscan](https://solscan.io/account/{wallet}) | "
        f"[Step Finance](https://app.step.finance/en/dashboard?watching={wallet})",
        parse_mode="Markdown",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👛 View on Solscan", url=f"https://solscan.io/account/{wallet}"),
        ]])
    )


async def rank_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    xp  = user_xp.get(uid, 0)
    if xp < 100:   rank, nxt = "🥉 Rookie", 100
    elif xp < 500: rank, nxt = "🥈 Degen",  500
    elif xp < 2000:rank, nxt = "🥇 Alpha",  2000
    else:          rank, nxt = "💎 Chad",    0
    await update.message.reply_text(
        f"⭐ **YOUR RANK**\n\nXP: {xp}\nRank: {rank}\n"
        f"{f'Next rank: {nxt - xp} XP away' if nxt else '🏆 Max rank!'}\n\n"
        f"Earn XP: `/scan` +5 | `/call` +10 | `/stop` +20",
        parse_mode="Markdown"
    )


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = settings.get(chat_id, {})
    btn = "✅ ON" if s.get("buttons", True)      else "❌ OFF"
    aut = "✅ ON" if s.get("autoresponder", True) else "❌ OFF"
    await update.message.reply_text(
        f"⚙️ **KAYO SETTINGS**\n\nButtons: {btn}\nAuto-scan: {aut}\n\n"
        f"Toggle with `/buttons` and `/autoresponder`",
        parse_mode="Markdown"
    )


async def buttons_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    cur = settings[chat_id].get("buttons", True)
    settings[chat_id]["buttons"] = not cur
    await update.message.reply_text(f"🔘 Buttons: {'✅ ON' if not cur else '❌ OFF'}")


async def autoresponder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    cur = settings[chat_id].get("autoresponder", True)
    settings[chat_id]["autoresponder"] = not cur
    await update.message.reply_text(f"🤖 Auto address scan: {'✅ ON' if not cur else '❌ OFF'}")


async def gp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏆 **GROUP POINTS**\n\n"
        "• `/scan` → +5 XP\n• `/call` → +10 XP\n• `/stop` → +20 XP\n\n"
        "Use `/rank` to see your XP!",
        parse_mode="Markdown"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ── Callback handler ─────────────────────────────────────────
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if data.startswith("scan:"):
        address  = data[5:]
        analysis = await smart_scan(address)
        if analysis.get("error"):
            await q.message.reply_text(f"❌ {analysis['error']}")
        else:
            await q.message.reply_text(
                build_scan_card(analysis),
                reply_markup=get_chart_buttons(address, analysis['symbol']),
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
    elif data.startswith("rug:"):
        address = data[4:]
        await context.bot.send_message(
            chat_id=q.message.chat_id,
            text=f"🔍 Running rug check on `{address[:12]}...`\nUse: `/verify {address}`",
            parse_mode="Markdown"
        )


# ── Auto message handler (CA detection) ──────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text    = update.message.text
    chat_id = update.effective_chat.id
    chat_settings = settings.get(chat_id, {})
    if not chat_settings.get("autoresponder", True):
        return
    addresses = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
    if not addresses:
        return
    address = addresses[0]
    wait    = await update.message.reply_text("🔍 CA detected — scanning...")
    analysis = await smart_scan(address)
    if not analysis.get("error"):
        show_buttons = chat_settings.get("buttons", True)
        markup = get_chart_buttons(address, analysis['symbol']) if show_buttons else None
        await wait.edit_text(
            build_scan_card(analysis),
            reply_markup=markup,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
    else:
        await wait.edit_text(f"❌ {analysis['error']}")


# ── Background tasks ─────────────────────────────────────────

async def bg_twitter_scanner(app: Application):
    """Scan Twitter/Nitter every 45 seconds for new CAs and narratives."""
    global last_twitter_scan, seen_news, kayo_knowledge
    await asyncio.sleep(60)  # wait for bot to fully start
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                tweets = []
                for q in ["solana new token ca drop", "alpha call solana", "new gem solana pump"]:
                    batch = await scrape_nitter(session, q, limit=5)
                    tweets.extend(batch)
                    await asyncio.sleep(1)
            for tw in tweets:
                text = tw.get("text", "")
                tid  = hashlib.md5(text.encode()).hexdigest()
                if tid in seen_news:
                    continue
                seen_news.add(tid)
                # learn narrative
                for kw in ['ai agent', 'rwa', 'defi', 'gaming', 'meme season', 'pump incoming', 'bullish solana']:
                    if kw in text.lower():
                        entry = f"{kw.title()} trending ({datetime.utcnow().strftime('%H:%M')})"
                        if entry not in kayo_knowledge:
                            kayo_knowledge.append(entry)
                            kayo_knowledge = kayo_knowledge[-50:]
                # detect CA
                cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
                if cas and GROUP_CHAT_ID != 0:
                    ca   = cas[0]
                    user = tw.get("user", "unknown")
                    snippet = text[:150].replace('\n',' ')
                    msg = (
                        f"🚨 **TWITTER CA DROP**\n"
                        f"👤 @{user}\n"
                        f"📝 {snippet}\n\n"
                        f"📌 CA: `{ca}`\n\n"
                        f"Use `/scan {ca}` for full analysis"
                    )
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=msg,
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.warning(f"Could not send twitter alert: {e}")
        except Exception as e:
            logger.error(f"Twitter scanner error: {e}")
        await asyncio.sleep(45)


async def bg_new_token_scanner(app: Application):
    """Scan DexScreener for new tokens every 30 seconds and report to group."""
    global seen_tokens, last_token_report
    await asyncio.sleep(90)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # search for fresh pairs with low age
                pairs = await dex_search(session, "solana")
                # also try boosted/new
                new_pairs = await dex_new_pairs(session)

            all_pairs = pairs[:50] + new_pairs[:20]
            for p in all_pairs:
                base    = p.get("baseToken", {}) if "baseToken" in p else {}
                address = base.get("address", p.get("tokenAddress", ""))
                if not address or address in seen_tokens:
                    continue
                fdv  = float(p.get("fdv", 0) or 0)
                liq  = float(p.get("liquidity", {}).get("usd", 0) or 0)
                ch_5m= float(p.get("priceChange", {}).get("m5", 0) or 0)
                ch_1h= float(p.get("priceChange", {}).get("h1", 0) or 0)
                vol  = float(p.get("volume", {}).get("h24", 0) or 0)
                buys = int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
                symbol = base.get("symbol", p.get("symbol", "???"))
                # quality filter: catch early before they move
                if liq < 500 or fdv > 10_000_000:
                    seen_tokens.add(address)
                    continue
                if vol < 500 and buys < 5:
                    seen_tokens.add(address)
                    continue
                seen_tokens.add(address)
                if GROUP_CHAT_ID == 0:
                    continue
                # rug check
                async with aiohttp.ClientSession() as session:
                    sec = await goplus_sec(session, address)
                if sec.get("is_honeypot") == "1":
                    continue  # skip confirmed honeypots
                rug_ok = "🟢 CLEAN" if sec.get("lp_locked") == "1" else "🟡 CHECK LP"
                msg = (
                    f"🆕 **NEW TOKEN ALERT**\n{'═'*30}\n\n"
                    f"**${symbol}**\n"
                    f"💧 Liq: {fmt_usd(liq)} | MCap: {fmt_usd(fdv)}\n"
                    f"📈 5m: {fmt_pct(ch_5m)} | 1h: {fmt_pct(ch_1h)}\n"
                    f"🔄 Vol: {fmt_usd(vol)} | Buys: {buys}\n"
                    f"🛡️ Safety: {rug_ok}\n\n"
                    f"`{address}`"
                )
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=msg,
                        parse_mode="Markdown",
                        reply_markup=get_chart_buttons(address, symbol)
                    )
                    await asyncio.sleep(3)  # rate limit between messages
                except Exception as e:
                    logger.warning(f"Token alert send error: {e}")
        except Exception as e:
            logger.error(f"New token scanner error: {e}")
        await asyncio.sleep(30)


async def bg_unusual_activity(app: Application):
    """Detect unusual volume/price spikes on existing tokens every 2 minutes."""
    baseline: Dict[str, Dict] = {}
    await asyncio.sleep(120)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                pairs = await dex_search(session, "solana")
            for p in pairs[:80]:
                base    = p.get("baseToken", {})
                address = base.get("address", "")
                symbol  = base.get("symbol", "???")
                ch_5m   = float(p.get("priceChange", {}).get("m5", 0) or 0)
                vol_5m  = float(p.get("volume", {}).get("m5", 0) or 0)
                vol_1h  = float(p.get("volume", {}).get("h1", 0) or 0)
                liq     = float(p.get("liquidity", {}).get("usd", 0) or 0)
                if liq < 2000 or not address:
                    continue
                vol_ratio = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 0
                prev = baseline.get(address, {})
                baseline[address] = {"vol_5m": vol_5m, "vol_1h": vol_1h, "ch_5m": ch_5m}
                if not prev:
                    continue
                # alert conditions
                alert = None
                if ch_5m > 15 and vol_ratio > 3:
                    alert = f"🚀 **PUMP ALERT** — ${symbol} +{ch_5m:.1f}% in 5m with {vol_ratio:.1f}x volume!"
                elif ch_5m < -15 and vol_ratio > 3:
                    alert = f"💀 **DUMP ALERT** — ${symbol} {ch_5m:.1f}% in 5m with {vol_ratio:.1f}x volume!"
                elif vol_ratio > 5 and abs(ch_5m) < 5:
                    alert = f"🐳 **WHALE ACTIVITY** — ${symbol} massive volume ({vol_ratio:.1f}x) with no big price move yet!"
                if alert and GROUP_CHAT_ID != 0:
                    fdv = float(p.get("fdv", 0) or 0)
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=f"{alert}\n\nMCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}\n`{address}`",
                            parse_mode="Markdown",
                            reply_markup=get_chart_buttons(address, symbol)
                        )
                    except Exception as e:
                        logger.warning(f"Activity alert error: {e}")
        except Exception as e:
            logger.error(f"Unusual activity scanner error: {e}")
        await asyncio.sleep(120)


async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start",         "🏠 Welcome"),
        BotCommand("scan",          "🔍 Full scan + opinion"),
        BotCommand("smartscan",     "🎯 Best coins NOW"),
        BotCommand("runners",       "🏃 Today's runners"),
        BotCommand("momentum",      "⚡ Momentum spikes"),
        BotCommand("verify",        "🛡️ Rug check"),
        BotCommand("chart",         "📊 DEX chart in Telegram"),
        BotCommand("dex",           "📊 DexScreener inline"),
        BotCommand("news",          "📰 Twitter alpha + CA drops"),
        BotCommand("trending",      "🔥 Hot narratives"),
        BotCommand("dt",            "🔥 Trending DEX tokens"),
        BotCommand("call",          "📞 Register a call"),
        BotCommand("mycalls",       "📊 Your calls + P&L"),
        BotCommand("stop",          "🔒 Close a call"),
        BotCommand("w",             "👛 Wallet overview"),
        BotCommand("a",             "🪙 CoinGecko price"),
        BotCommand("macro",         "🌍 Global market"),
        BotCommand("rank",          "⭐ Your XP & rank"),
        BotCommand("settings",      "⚙️ Settings"),
        BotCommand("buttons",       "🔘 Toggle chart buttons"),
        BotCommand("autoresponder", "🤖 Toggle auto-scan"),
        BotCommand("gp",            "🏆 Group points"),
        BotCommand("help",          "❓ All commands"),
    ])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    logger.info("=" * 60)
    logger.info("🦅 KAYO BRAIN v11.0 - FULL UPGRADE")
    logger.info("=" * 60)
    logger.info("✅ ALL FEATURES ACTIVE")
    logger.info(f"📢 Group reports: {'ENABLED (chat ' + str(GROUP_CHAT_ID) + ')' if GROUP_CHAT_ID != 0 else 'DISABLED (set GROUP_CHAT_ID)'}")
    logger.info("=" * 60)


def main():
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        print("⚠️  Set BOT_TOKEN environment variable!")
        return

    import requests as req
    try:
        req.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=5)
    except:
        pass

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    handlers = [
        ("start", start), ("help", help_cmd),
        ("scan", scan_cmd), ("smartscan", smartscan_cmd),
        ("runners", runners_cmd), ("momentum", momentum_cmd),
        ("verify", verify_cmd),
        ("chart", chart_cmd), ("dex", dex_cmd),
        ("news", news_cmd), ("trending", trending_cmd),
        ("dt", dt_cmd), ("macro", macro_cmd), ("a", a_cmd),
        ("call", call_cmd), ("mycalls", mycalls_cmd), ("stop", stop_cmd),
        ("w", w_cmd),
        ("rank", rank_cmd), ("settings", settings_cmd),
        ("buttons", buttons_cmd), ("autoresponder", autoresponder_cmd),
        ("gp", gp_cmd),
    ]
    for name, handler in handlers:
        app.add_handler(CommandHandler(name, handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # background tasks
    loop = asyncio.get_event_loop()
    app.job_queue  # ensure job queue is initialized

    async def run():
        async with app:
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            logger.info("🚀 Kayo Brain v11 polling started")
            # launch background tasks
            asyncio.create_task(bg_twitter_scanner(app))
            asyncio.create_task(bg_new_token_scanner(app))
            asyncio.create_task(bg_unusual_activity(app))
            # keep alive
            while True:
                await asyncio.sleep(3600)

    loop.run_until_complete(run())


if __name__ == "__main__":
    main()
