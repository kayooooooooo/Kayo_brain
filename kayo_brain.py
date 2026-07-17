"""
╔══════════════════════════════════════════════════════════════════════╗
║                    KAYO BRAIN v40 — PRO REBUILD                     ║
║  AI:      Groq REST (primary) → Gemini REST (fallback) — NO SDK     ║
║           AI always injected with LIVE price data before answering  ║
║  Data:    DexScreener ALL endpoints + CoinGecko + GoPlus            ║
║  News:    5 RSS feeds + keyword→CA narrative matching               ║
║  Alerts:  Pump / Gem / Whale / New Launch / Narrative               ║
║  State:   Redis async (persistent) → local JSON (fallback)          ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio, logging, re, time, json, os, threading, hashlib
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Set

import aiohttp
import redis.asyncio as aioredis
import xml.etree.ElementTree as ET
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, BotCommand
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters,
)
from flask import Flask

# ═══════════════════════════════════════════════════════════════
# ENV + CONFIG
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "")
GROUP_CHAT_ID      = int(os.environ.get("GROUP_CHAT_ID", "0"))
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
GROQ_API_KEY       = os.environ.get("GROQ_API_KEY", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
REDIS_URL          = os.environ.get("REDIS_URL", "")
TWITTER_AUTH_TOKEN = os.environ.get("TWITTER_AUTH_TOKEN", "")
STATE_FILE         = "kayo_state.json"
REDIS_KEY          = "kayo_v15_state"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# HEALTH SERVER
# ═══════════════════════════════════════════════════════════════
flask_app = Flask(__name__)

@flask_app.route("/")
def _root(): return "🦅 Kayo Brain v40", 200

@flask_app.route("/health")
def _health(): return "OK", 200

threading.Thread(
    target=lambda: flask_app.run(
        host="0.0.0.0", port=int(os.environ.get("PORT", 8080)),
        debug=False, use_reloader=False
    ),
    daemon=True
).start()

# ═══════════════════════════════════════════════════════════════
# REDIS  (async client — does not block the event loop)
# ═══════════════════════════════════════════════════════════════
_redis: Optional[aioredis.Redis] = None   # set in post_init after loop starts

def _make_redis() -> Optional[aioredis.Redis]:
    """Create async Redis client — no sync ping, lazy async connect."""
    if not REDIS_URL:
        return None
    try:
        return aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
    except Exception as e:
        logger.warning(f"Redis client creation error: {e}")
        return None
    try:
        client = aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        logger.info("Redis client created (lazy connect)")
        return client
    except Exception as e:
        logger.warning(f"Redis client creation failed: {e}")
        return None

# ═══════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════
watchlist:       Dict[str, dict] = {}
user_alerts:     List[dict]      = []
portfolios:      Dict[str, list] = {}
active_calls:    List[dict]      = []
blacklist:       Set[str]        = set()
xp_db:           Dict[str, int]  = {}
user_settings:   Dict[str, dict] = {}
user_wallets:    Dict[str, str]  = {}
tracked_wallets: Dict[str, dict] = {}
knowledge_base:  List[str]       = []
reminders:       List[dict]      = []
group_messages:  list            = []
_ai_reply_cooldown: dict           = {}  # uid → last AI reply ts (group rate-limit)
# BUG FIX: Use OrderedDict as a bounded ordered set so we can evict
# the OLDEST entries (not random ones like plain set).
seen_alert_ids:  "OrderedDict[str, int]" = OrderedDict()  # key=id, value=timestamp
dropped_calls:   Dict[str, dict]         = {}  # addr -> {sym,entry_price,time,alert_type,...} for follow-ups
pattern_memory: Dict[str, dict]         = {}  # alert_type+nar -> {wins,losses,total,avg_mult} for self-learning
watchlist_seen:  "OrderedDict[str, int]" = OrderedDict()
seen_news_ids:   Set[str]                = set()
_MAX_SEEN = 3000   # max entries before oldest are trimmed

def _seen_add(od: "OrderedDict[str, int]", key: str):
    """Add key to bounded ordered dedup dict, evicting oldest if over limit."""
    od[key] = int(time.time())
    while len(od) > _MAX_SEEN:
        od.popitem(last=False)   # remove oldest

def _seen_check(od: "OrderedDict[str, int]", key: str) -> bool:
    return key in od

_save_lock = asyncio.Lock()

async def _save():
    """Async-safe state save — runs without blocking the event loop."""
    async with _save_lock:
        data = {
            "watchlist": watchlist,
            "user_alerts": user_alerts,
            "portfolios": portfolios,
            "active_calls": active_calls,
            "blacklist": list(blacklist),
            "xp_db": xp_db,
            "user_settings": user_settings,
            "user_wallets": user_wallets,
            "tracked_wallets": tracked_wallets,
            "knowledge_base": knowledge_base,
            "reminders": reminders,
            "seen_alert_ids": list(seen_alert_ids.keys())[-3000:],
            "dropped_calls":  dropped_calls,
            "pattern_memory": pattern_memory,
        }
        raw = json.dumps(data)
        try:
            if _redis:
                try:
                    await _redis.set(REDIS_KEY, raw)
                except Exception as redis_err:
                    logger.warning(f"Redis save failed ({redis_err}) — falling back to file")
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, lambda: open(STATE_FILE, "w").write(raw))
                    except Exception as fe:
                        logger.warning(f"File save also failed: {fe}")
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: open(STATE_FILE, "w").write(raw))
        except Exception as e:
            logger.warning(f"save_state: {e}")

async def _load():
    global watchlist, user_alerts, portfolios, active_calls, blacklist
    global xp_db, user_settings, user_wallets, tracked_wallets
    global knowledge_base, reminders, seen_alert_ids
    try:
        raw = None
        if _redis:
            try:
                raw = await _redis.get(REDIS_KEY)
                if raw:
                    logger.info("State loaded from Redis")
            except Exception as redis_err:
                logger.warning(f"Redis load failed ({redis_err}) — trying file")
        if not raw and os.path.exists(STATE_FILE):
            try:
                raw = open(STATE_FILE).read()
                if raw:
                    logger.info("State loaded from local file")
            except Exception as fe:
                logger.warning(f"File load failed: {fe}")
        if not raw: return
        d = json.loads(raw)
        watchlist       = d.get("watchlist", {})
        user_alerts     = d.get("user_alerts", [])
        portfolios      = d.get("portfolios", {})
        active_calls    = d.get("active_calls", [])
        blacklist       = set(d.get("blacklist", []))
        xp_db           = d.get("xp_db", {})
        user_settings   = d.get("user_settings", {})
        user_wallets    = d.get("user_wallets", {})
        tracked_wallets = d.get("tracked_wallets", {})
        knowledge_base  = d.get("knowledge_base", [])
        reminders       = d.get("reminders", [])
        seen_alert_ids  = OrderedDict()  # intentionally NOT restored — fresh dedup each session
        global dropped_calls, pattern_memory
        dropped_calls   = d.get("dropped_calls", {})
        pattern_memory  = d.get("pattern_memory", {})
        logger.info(f"✅ State loaded — {len(watchlist)} watched, {len(active_calls)} calls, {len(dropped_calls)} tracked drops (seen_alert_ids cleared for fresh session)")
        # Prune dropped_calls older than 7 days so it doesn't block forever
        cutoff = time.time() - 604800
        dropped_calls = {k: v for k, v in dropped_calls.items() if v.get("time", 0) > cutoff}
        logger.info(f"[STARTUP] {len(dropped_calls)} active tracked drops after 7d prune")
    except Exception as e:
        logger.warning(f"load_state: {e}")

def add_xp(uid, pts: int):
    k = str(uid)
    xp_db[k] = xp_db.get(k, 0) + pts

def get_setting(uid, key, default=None):
    return user_settings.get(str(uid), {}).get(key, default)

def set_setting(uid, key, val):
    uid = str(uid)
    if uid not in user_settings: user_settings[uid] = {}
    user_settings[uid][key] = val

# ═══════════════════════════════════════════════════════════════
# LIVE MARKET CONTEXT  — injected into every AI call
# Fetches BTC/SOL/ETH real-time prices so AI never hallucinates
# ═══════════════════════════════════════════════════════════════
_market_ctx_cache: Dict = {"data": None, "ts": 0}
_MARKET_CTX_TTL = 120   # seconds between refreshes

async def get_live_market_context() -> str:
    """
    Returns a compact market-context string with LIVE prices.
    Used to ground every AI prompt — no more outdated price hallucinations.
    Refreshes at most once per minute (cached).
    """
    global _market_ctx_cache
    now = time.time()
    if _market_ctx_cache["data"] and (now - _market_ctx_cache["ts"]) < _MARKET_CTX_TTL:
        return _market_ctx_cache["data"]

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin,solana,ethereum,binancecoin,ripple,cardano,avalanche-2,"
                "dogecoin,polkadot,chainlink,uniswap,litecoin,near,aptos,sui,"
                "pepe,shiba-inu,bonk,dogwifcoin,jupiter,raydium,jito-governance-token,"
                "trump-official,popcat,book-of-meme"
                "&vs_currencies=usd"
                "&include_24hr_change=true"
                "&include_market_cap=true",
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0"}
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    btc  = d.get("bitcoin", {})
                    sol  = d.get("solana", {})
                    eth  = d.get("ethereum", {})
                    bnb  = d.get("binancecoin", {})
                    fg   = await cg_fear_greed()
                    fg_v = fg.get("value", "?")
                    fg_c = fg.get("value_classification", "?")

                    # Try to grab trending Solana tokens for context
                    trending_line = ""
                    try:
                        tr_data = await cg_trending()
                        tr_names = [
                            f"${c.get('item',{}).get('symbol','?')}"
                            for c in (tr_data.get("coins") or [])[:5]
                        ]
                        if tr_names:
                            trending_line = f"Trending: {', '.join(tr_names)}\n"
                    except Exception:
                        pass

                    # (Solana gainers removed — was adding 5-10s latency to every AI call)
                    sol_gainers = ""

                    # Format all coins dynamically
                    def _fmt_coin(name_id, data, d):
                        cd = d.get(name_id, {})
                        p   = cd.get("usd", 0)
                        chg = cd.get("usd_24h_change", 0)
                        if p == 0: return ""
                        sym_map = {
                            "bitcoin":"BTC","solana":"SOL","ethereum":"ETH","binancecoin":"BNB",
                            "ripple":"XRP","cardano":"ADA","avalanche-2":"AVAX","dogecoin":"DOGE",
                            "polkadot":"DOT","chainlink":"LINK","uniswap":"UNI","litecoin":"LTC",
                            "near":"NEAR","aptos":"APT","sui":"SUI","pepe":"PEPE",
                            "shiba-inu":"SHIB","bonk":"BONK","dogwifcoin":"WIF",
                            "jupiter":"JUP","raydium":"RAY","jito-governance-token":"JTO",
                            "trump-official":"TRUMP","popcat":"POPCAT","book-of-meme":"BOME",
                        }
                        sym = sym_map.get(name_id, name_id.upper()[:6])
                        icon = "📈" if chg >= 0 else "📉"
                        if p >= 1000:   p_str = f"${p:,.0f}"
                        elif p >= 1:    p_str = f"${p:,.2f}"
                        elif p >= 0.001: p_str = f"${p:.4f}"
                        else:           p_str = f"${p:.8f}"
                        return f"{icon} {sym}: {p_str} ({chg:+.1f}%)"

                    coin_ids = [
                        "bitcoin","ethereum","solana","binancecoin","ripple",
                        "cardano","avalanche-2","dogecoin","polkadot","chainlink",
                        "near","aptos","sui","pepe","shiba-inu","bonk","dogwifcoin",
                        "jupiter","raydium","trump-official","popcat","book-of-meme"
                    ]
                    coin_lines = [_fmt_coin(cid, d, d) for cid in coin_ids]
                    coin_lines = [x for x in coin_lines if x]
                    # Group: majors | alts | sol memes
                    majors = coin_lines[:5]
                    alts   = coin_lines[5:11]
                    memes  = coin_lines[11:]

                    ctx = (
                        f"[LIVE MARKET — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}]\n"
                        f"MAJORS: {'  '.join(majors)}\n"
                        f"ALTS:   {'  '.join(alts)}\n"
                        f"MEMES:  {'  '.join(memes)}\n"
                        f"Fear & Greed: {fg_v}/100 ({fg_c})\n"
                        f"{trending_line}"
                        f"---\n"
                        f"You are Kayo — a sharp, witty Solana alpha intelligence. "
                        f"ALWAYS use the LIVE prices above when asked. Never hallucinate prices. "
                        f"You know EVERY coin above — price, 24h change, narratives. "
                        f"Answer any question: crypto, Web3, life, sports, culture. "
                        f"Talk like a degen pro who also knows everything. No disclaimers."
                    )
                    _market_ctx_cache["data"] = ctx
                    _market_ctx_cache["ts"]   = now
                    return ctx
    except Exception as e:
        logger.debug(f"market_ctx: {e}")

    # fallback minimal context
    ctx = (
        f"[LIVE MARKET DATA - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}]\n"
        f"(Price data temporarily unavailable - use DexScreener/CoinGecko as source.)\n"
        f"You are Kayo, a sharp Solana alpha intelligence bot. Be honest if data is missing."
    )
    return ctx

# ═══════════════════════════════════════════════════════════════
# AI — PURE HTTP, NO SDK
# Groq primary → Gemini fallback
# Every call is injected with LIVE market context so prices are always real
# ═══════════════════════════════════════════════════════════════
# Live Groq models as of June 2026 — dead models removed
# llama3-8b-8192 + llama-3.1-70b-versatile are DECOMMISSIONED (HTTP 400)
GROQ_MODELS = [
    "llama-3.3-70b-versatile",      # Primary — best quality
    "llama-3.1-8b-instant",         # Fast fallback — good for rate limits
    "llama3-70b-8192",              # Fallback — still live
    "gemma2-9b-it",                 # Final Groq fallback
]

async def ai_ask(prompt: str, fallback: str = "", max_tokens: int = 380,
                 inject_market: bool = True) -> str:
    """
    Primary: Groq — fast, free tier 500k tokens/day
    Fallback: Gemini 2.0 Flash
    inject_market=True prepends live BTC/SOL/ETH prices so AI never
    gives stale price answers.
    """
    # Build system context with live prices
    system_ctx = await get_live_market_context() if inject_market else (
        "You are Kayo, a sharp Solana alpha intelligence bot. Be direct, professional, "
        "and data-driven. No fluff, no disclaimers."
    )
    # Build a context-aware system message
    # When inject_market=True: full degen alpha mode with live prices
    # When inject_market=False: casual/general mode — no price rules to avoid confusion
    # Single unified system prompt regardless of inject_market
    # inject_market just controls whether live price data is prepended
    market_block = f"{system_ctx}\n\n" if inject_market else ""
    system_content = (
        f"{market_block}"
        "You are Kayo. You live in a Solana degen Telegram group. Sharp, real, funny. "
        "You think like a Web3 power user who also knows everything — sports, culture, science, life. "
        "RULES: "
        "1. Answer EVERYTHING. Never say you cant. Never say you are a bot. Never refuse. "
        "2. Crypto questions: use the LIVE prices from the data above. Be specific with numbers. "
        "3. Casual messages (gm, lol, vibes): short, warm, human. Max 2 sentences. "
        "4. Web3 alpha: talk like a degen pro — sharp, direct, call the play. "
        "5. General knowledge: smart friend who gives real answers, no disclaimers. "
        "6. Match energy. Hyped = hyped reply. Chill = chill. "
        "7. No filler. No Great question. No As an AI. Just answer. "
        "FORMAT: Plain text mostly. *bold* only for key crypto numbers."
    )
    system_msg = {"role": "system", "content": system_content}

    if GROQ_API_KEY:
        async with aiohttp.ClientSession() as s:
            for model in GROQ_MODELS:
                try:
                    async with s.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {GROQ_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": model,
                            "messages": [system_msg, {"role": "user", "content": prompt}],
                            "max_tokens": max_tokens,
                            "temperature": 0.7,
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            text_out = d["choices"][0]["message"]["content"].strip()
                            if text_out:
                                logger.debug(f"ai_ask: Groq {model} OK")
                                return text_out
                        elif r.status == 429:
                            # Rate limited — immediately try next model, no wait
                            logger.warning(f"Groq 429 on {model} — trying next")
                            continue
                        elif r.status == 400:
                            # Bad request — model likely decommissioned, skip
                            err = await r.text()
                            if "decommissioned" in err or "no longer supported" in err:
                                logger.warning(f"Groq {model} DECOMMISSIONED — skip")
                            else:
                                logger.error(f"Groq {model} 400: {err[:100]}")
                            continue
                        else:
                            err_body = await r.text()
                            logger.error(f"Groq {model} HTTP {r.status}: {err_body[:100]}")
                            continue
                except asyncio.TimeoutError:
                    logger.warning(f"Groq {model} timeout — next model")
                    continue
                except Exception as e:
                    logger.error(f"Groq {model}: {e}")
                    continue

    # Gemini fallback — try multiple models in case one hits quota
    if GEMINI_API_KEY:
        gemini_models = [
            "gemini-2.5-flash",        # Newest — best quality + speed
            "gemini-2.0-flash",        # Fast and capable
            "gemini-2.0-flash-lite",   # Lightweight, lower quota usage
            "gemini-1.5-flash",        # Different quota bucket
            "gemini-1.5-flash-8b",     # Smallest, highest rate limits
        ]
        full_prompt = f"{system_ctx}\n\n{prompt}"
        async with aiohttp.ClientSession() as s:
            for gem_model in gemini_models:
                try:
                    async with s.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/{gem_model}:generateContent?key={GEMINI_API_KEY}",
                        json={
                            "contents": [{"parts": [{"text": full_prompt}]}],
                            "generationConfig": {"maxOutputTokens": max_tokens}
                        },
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            text_out = d["candidates"][0]["content"]["parts"][0]["text"].strip()
                            if text_out:
                                logger.debug(f"ai_ask: Gemini {gem_model} OK")
                                return text_out
                        elif r.status == 429:
                            logger.warning(f"Gemini {gem_model} quota exhausted — trying next")
                            continue
                        else:
                            err_body = await r.text()
                            logger.error(f"Gemini {gem_model} HTTP {r.status}: {err_body[:100]}")
                            continue
                except asyncio.TimeoutError:
                    logger.warning(f"Gemini {gem_model} timeout")
                    continue
                except Exception as e:
                    logger.error(f"Gemini {gem_model}: {e}")
                    continue


    # OpenRouter fallback — free models available
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
    if OPENROUTER_API_KEY:
        or_models = [
            "meta-llama/llama-3.3-70b-instruct:free",
            "google/gemini-2.0-flash-exp:free",
            "meta-llama/llama-3.1-8b-instruct:free",
        ]
        full_prompt = f"{system_ctx}\n\n{prompt}"
        async with aiohttp.ClientSession() as s:
            for or_model in or_models:
                try:
                    async with s.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": or_model,
                            "messages": [system_msg, {"role": "user", "content": prompt}],
                            "max_tokens": max_tokens,
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            text_out = d["choices"][0]["message"]["content"].strip()
                            if text_out:
                                logger.debug(f"ai_ask: OpenRouter {or_model} OK")
                                return text_out
                        elif r.status == 429:
                            logger.warning(f"OpenRouter {or_model} rate limited")
                            continue
                        else:
                            err_body = await r.text()
                            logger.error(f"OpenRouter {or_model} HTTP {r.status}: {err_body[:100]}")
                            continue
                except Exception as e:
                    logger.error(f"OpenRouter {or_model}: {e}")
                    continue

    logger.error(f"ai_ask: ALL backends failed. Groq={bool(GROQ_API_KEY)} Gemini={bool(GEMINI_API_KEY)}")
    return fallback

# ═══════════════════════════════════════════════════════════════
# FORMATTERS
# ═══════════════════════════════════════════════════════════════
def _usd(n: float) -> str:
    if n >= 1_000_000_000: return f"${n/1_000_000_000:.2f}B"
    if n >= 1_000_000:     return f"${n/1_000_000:.2f}M"
    if n >= 1_000:         return f"${n/1_000:.1f}K"
    return f"${n:.2f}"

def _pct(n: float) -> str:
    return f"{'🟢' if n >= 0 else '🔴'} {n:+.2f}%"

def _price(n: float) -> str:
    if n == 0:      return "$0"
    if n < 0.000001: return f"${n:.10f}"
    if n < 0.001:   return f"${n:.8f}"
    if n < 1:       return f"${n:.6f}"
    return f"${n:,.4f}"

def _age(ms: int) -> str:
    if ms <= 0: return "?"
    s = (time.time() * 1000 - ms) / 1000
    if s < 3600:  return f"{int(s/60)}m"
    if s < 86400: return f"{int(s/3600)}h"
    return f"{int(s/86400)}d"

def _bar(val: int, mx: int = 100, width: int = 10) -> str:
    filled = round(val / max(mx, 1) * width)
    return "█" * filled + "░" * (width - filled)

def _risk(score: int) -> str:
    if score < 20: return "🟢 LOW RISK"
    if score < 50: return "🟡 MODERATE"
    if score < 75: return "🟠 HIGH RISK"
    return "🔴 DANGER"

def _safe_md(text: str) -> str:
    """Escape special chars that break Telegram MarkdownV1."""
    return text

# ═══════════════════════════════════════════════════════════════
# DEXSCREENER — ALL ENDPOINTS (no API key)
# ═══════════════════════════════════════════════════════════════
_DSX = "https://api.dexscreener.com"

async def _get(url: str, timeout: int = 10):
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                             headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            logger.debug(f"GET {url}: {e}")
    return None

async def dex_pairs_by_token(addr: str) -> List[Dict]:
    d = await _get(f"{_DSX}/token-pairs/v1/solana/{addr}")
    return d if isinstance(d, list) else []

async def dex_search_pairs(query: str) -> List[Dict]:
    d = await _get(f"{_DSX}/latest/dex/search?q={query.replace(' ','+')}")
    if d and "pairs" in d:
        return [p for p in d["pairs"] if p.get("chainId") == "solana"]
    return []

async def dex_token_profiles_latest() -> List[Dict]:
    d = await _get(f"{_DSX}/token-profiles/latest/v1")
    return d if isinstance(d, list) else []

async def dex_token_profiles_recent() -> List[Dict]:
    d = await _get(f"{_DSX}/token-profiles/recent-updates/v1")
    return d if isinstance(d, list) else []

async def dex_boosts_latest() -> List[Dict]:
    d = await _get(f"{_DSX}/token-boosts/latest/v1")
    return d if isinstance(d, list) else []

async def dex_boosts_top() -> List[Dict]:
    d = await _get(f"{_DSX}/token-boosts/top/v1")
    return d if isinstance(d, list) else []

async def dex_trending_metas() -> List[Dict]:
    d = await _get(f"{_DSX}/metas/trending/v1")
    return d if isinstance(d, list) else []

async def dex_meta_tokens(slug: str) -> List[Dict]:
    d = await _get(f"{_DSX}/metas/meta/v1/{slug}")
    if d and "pairs" in d: return d["pairs"]
    return []

async def dex_community_takeovers() -> List[Dict]:
    d = await _get(f"{_DSX}/community-takeovers/latest/v1")
    return d if isinstance(d, list) else []

async def dex_token_orders(addr: str) -> List[Dict]:
    d = await _get(f"{_DSX}/orders/v1/solana/{addr}")
    return d if isinstance(d, list) else []

async def dex_batch(addresses: List[str]) -> List[Dict]:
    if not addresses: return []
    chunk = ",".join(addresses[:30])
    d = await _get(f"{_DSX}/tokens/v1/solana/{chunk}")
    return d if isinstance(d, list) else []

async def dex_new_pairs(limit: int = 100) -> List[Dict]:
    """Fetch newest Solana pairs — catches coins in first 5 minutes of trading."""
    d = await _get(f"{_DSX}/token-pairs/v1/solana/new?limit={limit}")
    if isinstance(d, list): return [p for p in d if p.get("chainId") == "solana"]
    # Fallback: search for brand new pairs via trending endpoint
    d2 = await _get(f"{_DSX}/latest/dex/tokens/solana")
    if d2 and isinstance(d2, list): return d2[:limit]
    return []

async def dex_gainers_solana() -> List[Dict]:
    """Top gaining Solana pairs — sorted by 5m price change."""
    results = []
    for q in ["solana pump", "solana moon", "solana 100x", "solana gem", "solana meme coin", "solana dog coin"]:
        pairs = await dex_search_pairs(q)
        results.extend(pairs)
    # Dedup
    seen = {}
    for p in results:
        a = (p.get("baseToken") or {}).get("address", "")
        if a: seen[a] = p
    # Sort by 5m gain
    return sorted(seen.values(), key=lambda p: float((p.get("priceChange") or {}).get("m5", 0) or 0), reverse=True)

# ═══════════════════════════════════════════════════════════════
# GECKOTERMINAL — free, no key, real new Solana pools
# ═══════════════════════════════════════════════════════════════
_GT = "https://api.geckoterminal.com/api/v2"

async def gt_new_pools(page: int = 1) -> List[Dict]:
    """Newest Solana pools — catches coins in first few minutes. 20 per page."""
    d = await _get(f"{_GT}/networks/solana/new_pools?page={page}", timeout=12)
    return d.get("data", []) if isinstance(d, dict) else []

async def gt_trending_pools(page: int = 1) -> List[Dict]:
    """Trending Solana pools right now. 20 per page."""
    d = await _get(f"{_GT}/networks/solana/trending_pools?page={page}", timeout=12)
    return d.get("data", []) if isinstance(d, dict) else []

def gt_parse_pool(pool: Dict) -> Optional[Dict]:
    """Convert GeckoTerminal pool object → our standard token dict."""
    try:
        a   = pool.get("attributes", {})
        rel = pool.get("relationships", {})
        addr_raw = rel.get("base_token", {}).get("data", {}).get("id", "")
        addr = addr_raw.replace("solana_", "") if addr_raw.startswith("solana_") else ""
        if not addr: return None
        name_full = a.get("name", "")
        # name_full is like "SYMBOL / SOL" — extract symbol
        sym = name_full.split(" / ")[0].strip() if " / " in name_full else name_full
        fdv  = float(a.get("fdv_usd") or 0)
        mcap = float(a.get("market_cap_usd") or fdv)
        liq  = float(a.get("reserve_in_usd") or 0)
        price = float(a.get("base_token_price_usd") or 0)
        chg  = a.get("price_change_percentage", {})
        ch5m  = float(chg.get("m5") or 0)
        ch1h  = float(chg.get("h1") or 0)
        ch6h  = float(chg.get("h6") or 0)
        ch24h = float(chg.get("h24") or 0)
        txns_h1 = a.get("transactions", {}).get("h1", {})
        txns_m5 = a.get("transactions", {}).get("m5", {})
        b1h = int(txns_h1.get("buys", 0) or 0)
        s1h = int(txns_h1.get("sells", 0) or 0)
        b5m = int(txns_m5.get("buys", 0) or 0)
        s5m = int(txns_m5.get("sells", 0) or 0)
        vol = a.get("volume_usd", {})
        v5m  = float(vol.get("m5") or 0)
        v1h  = float(vol.get("h1") or 0)
        v24h = float(vol.get("h24") or 0)
        created_str = a.get("pool_created_at", "")
        buy_pct = b1h / max(b1h + s1h, 1) * 100
        avg_5m_vol = v1h / 12 if v1h > 0 else 1
        vol_spike = v5m / max(avg_5m_vol, 1)
        return {
            "address": addr, "sym": sym, "name": sym,
            "price": price, "fdv": fdv, "mcap": mcap, "liq": liq,
            "liq_ratio": liq / max(fdv, 1) * 100,
            "ch5m": ch5m, "ch1h": ch1h, "ch6h": ch6h, "ch24h": ch24h,
            "v5m": v5m, "v1h": v1h, "v24h": v24h,
            "b5m": b5m, "s5m": s5m, "b1h": b1h, "s1h": s1h,
            "b24h": 0, "s24h": 0,
            "buy_pct": buy_pct, "vol_spike": vol_spike,
            "created_str": created_str,
            "pair_addr": a.get("address", ""),
            "_source": "gecko",
        }
    except Exception:
        return None

async def dex_multi_search(queries: List[str]) -> Dict[str, Dict]:
    """Run multiple queries in parallel, dedup by address."""
    tasks = [dex_search_pairs(q) for q in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    seen: Dict[str, Dict] = {}
    for batch in results:
        if isinstance(batch, list):
            for p in batch:
                a = p.get("baseToken", {}).get("address", "")
                if a: seen[a] = p
    return seen

# ═══════════════════════════════════════════════════════════════
# COINGECKO (free, no key)
# ═══════════════════════════════════════════════════════════════
async def cg_trending() -> List[Dict]:
    d = await _get("https://api.coingecko.com/api/v3/search/trending")
    return d.get("coins", []) if d else []

async def cg_global() -> Dict:
    d = await _get("https://api.coingecko.com/api/v3/global")
    return d.get("data", {}) if d else {}

async def cg_fear_greed() -> Dict:
    d = await _get("https://api.alternative.me/fng/?limit=1")
    return d.get("data", [{}])[0] if d else {}

async def cg_coin(coin_id: str) -> Optional[Dict]:
    return await _get(f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=false&developer_data=false")

# ═══════════════════════════════════════════════════════════════
# GOPLUS SECURITY (free)
# ═══════════════════════════════════════════════════════════════
async def goplus_check(addr: str) -> Dict:
    d = await _get(f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={addr}")
    if d and "result" in d:
        return d["result"].get(addr.lower(), d["result"].get(addr, {}))
    return {}

def parse_security(sec: Dict) -> tuple:
    """Returns (risk_score 0-100, red_flags[], green_flags[])"""
    if not sec:
        return 30, [], ["⚠️ Security data unavailable"]
    risk, red, green = 0, [], []
    if sec.get("is_honeypot") == "1":
        risk += 80; red.append("🚨 HONEYPOT — You CANNOT sell this token")
    st = float(sec.get("sell_tax", 0) or 0)
    bt = float(sec.get("buy_tax",  0) or 0)
    if st > 20:  risk += 40; red.append(f"💸 Sell tax: {st}% (very high)")
    elif st > 10: risk += 20; red.append(f"⚠️ Sell tax: {st}%")
    elif st > 0:  red.append(f"ℹ️ Sell tax: {st}%")
    if bt > 10:  risk += 15; red.append(f"⚠️ Buy tax: {bt}%")
    if sec.get("owner_change_balance") == "1": risk += 35; red.append("👑 Owner can change balances")
    if sec.get("can_take_back_ownership") == "1": risk += 30; red.append("🔑 Ownership can be reclaimed")
    if sec.get("is_mintable") == "1": risk += 25; red.append("🖨️ Supply is mintable (infinite)")
    if sec.get("is_blacklisted") == "1": risk += 40; red.append("🚫 Token is blacklisted")
    if sec.get("is_proxy") == "1": risk += 15; red.append("🔀 Proxy contract (upgradeable)")
    if sec.get("lp_locked") == "1": green.append("🔒 Liquidity locked")
    else: risk += 30; red.append("⚠️ Liquidity NOT locked")
    if sec.get("is_renounced") == "1": green.append("✅ Contract renounced")
    if sec.get("is_open_source") == "1": green.append("📖 Open source contract")
    return min(risk, 100), red, green

# ═══════════════════════════════════════════════════════════════
# NEWS — MULTI-SOURCE RSS
# ═══════════════════════════════════════════════════════════════
RSS_FEEDS = [
    ("CoinDesk",      "https://feeds.feedburner.com/CoinDesk"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt",       "https://decrypt.co/feed"),
    ("The Block",     "https://www.theblock.co/rss.xml"),
    ("DLNews",        "https://www.dlnews.com/arc/outboundfeeds/rss/"),
]

async def fetch_news(limit: int = 10) -> List[Dict]:
    items = []
    async with aiohttp.ClientSession() as s:
        for source, url in RSS_FEEDS:
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=8),
                                 headers={"User-Agent": "Mozilla/5.0"}) as r:
                    if r.status == 200:
                        text = await r.text()
                        root = ET.fromstring(text)
                        for item in root.iter("item"):
                            title = item.findtext("title", "")
                            link  = item.findtext("link", "")
                            pub   = item.findtext("pubDate", "")
                            desc  = item.findtext("description", "")
                            if title and link:
                                items.append({
                                    "source": source,
                                    "title": title,
                                    "link": link,
                                    "pub": pub,
                                    "desc": (desc or "")[:200],
                                    "id": hashlib.md5(link.encode()).hexdigest()[:12],
                                })
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.debug(f"RSS {source}: {e}")
    items.sort(key=lambda x: x.get("pub", ""), reverse=True)
    return items[:limit]

# ═══════════════════════════════════════════════════════════════
# TWITTER (requires TWITTER_AUTH_TOKEN cookie)
# ═══════════════════════════════════════════════════════════════
def _tw_headers() -> Optional[Dict]:
    """Twitter auth headers — kept for TWITTER_AUTH_TOKEN cookie auth."""
    if not TWITTER_AUTH_TOKEN: return None
    return {
        "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
        "Cookie": f"auth_token={TWITTER_AUTH_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "x-twitter-active-user": "yes",
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-client-language": "en",
    }

# ─── RSS helpers ────────────────────────────────────────────────
async def _fetch_rss(url: str) -> List[str]:
    """Fetch RSS feed and return list of article titles."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8),
                             headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status == 200:
                    body = await r.text()
                    titles = re.findall(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', body)
                    return [t.strip() for t in titles if len(t.strip()) > 15 and '<' not in t][:20]
    except Exception as e:
        logger.debug(f"RSS {url}: {e}")
    return []

async def fetch_crypto_news() -> List[str]:
    """Fetch latest crypto headlines from 4 reliable free RSS feeds."""
    sources = [
        "https://decrypt.co/feed",
        "https://cointelegraph.com/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://blockworks.co/feed",
        "https://beincrypto.com/feed/",
        "https://cryptoslate.com/feed/",
    ]
    results = await asyncio.gather(*[_fetch_rss(s) for s in sources], return_exceptions=True)
    seen, unique = set(), []
    for r in results:
        if isinstance(r, list):
            for t in r:
                key = t[:40].lower()
                if key not in seen:
                    seen.add(key)
                    unique.append(t)
    return unique[:30]

# ─── Pump.fun v3 API (working, no key needed) ───────────────────
_PUMPFUN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://pump.fun/",
    "Origin": "https://pump.fun",
}

async def pumpfun_latest(limit: int = 20) -> List[Dict]:
    """Get latest Pump.fun launches — real-time CT social signal."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"offset": "0", "limit": str(limit), "sort": "created_timestamp", "order": "DESC"},
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        return data
    except Exception as e:
        logger.debug(f"pumpfun_latest: {e}")
    return []

async def pumpfun_trending(limit: int = 10) -> List[Dict]:
    """Get trending Pump.fun coins by market cap."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"offset": "0", "limit": str(limit), "sort": "market_cap", "order": "DESC"},
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        return data
    except Exception as e:
        logger.debug(f"pumpfun_trending: {e}")
    return []


def pumpfun_to_token(coin: Dict) -> Optional[Dict]:
    """Convert Pump.fun coin API response → our standard token dict.
    Extracts narrative from description + name + symbol.
    """
    try:
        mint = coin.get("mint", "")
        if not mint: return None
        sym  = coin.get("symbol", coin.get("name", "???"))
        name = coin.get("name", sym)
        desc = coin.get("description", "") or ""

        # Market cap from pump.fun (in USD)
        mcap = float(coin.get("usd_market_cap", 0) or coin.get("market_cap", 0) or 0)
        # Pump.fun doesn't give us price/liquidity directly in the coin listing
        # but we can estimate liquidity from bonding curve reserves
        virtual_sol = float(coin.get("virtual_sol_reserves", 0) or 0) / 1e9  # lamports to SOL
        # Estimate: liq ≈ 30% of market cap for pump.fun bonding curve tokens
        liq = max(mcap * 0.3, virtual_sol * 150) if mcap > 0 else 0

        # Narratives from pump.fun description
        nar = ""
        nar_text = f"{name} {sym} {desc}".lower()
        if any(kw in nar_text for kw in ["ai","agent","llm","gpt","robot","autonomous"]):  nar = "ai"
        elif any(kw in nar_text for kw in ["dog","doge","pup","puppy","shib","inu"]):      nar = "dog"
        elif any(kw in nar_text for kw in ["cat","kitty","kitten","feline","meow"]):       nar = "cat"
        elif any(kw in nar_text for kw in ["frog","pepe","ribbit"]):                        nar = "frog"
        elif any(kw in nar_text for kw in ["trump","maga","donald","politician"]):          nar = "politics"
        elif any(kw in nar_text for kw in ["meme","funny","lol","viral"]):                  nar = "meme"
        elif any(kw in nar_text for kw in ["game","gaming","play","arcade"]):              nar = "gaming"
        elif any(kw in nar_text for kw in ["degen","degen","ape","casino","gamble"]):      nar = "degen"
        elif any(kw in nar_text for kw in ["food","drink","coffee","pizza","burger"]):     nar = "food"
        elif any(kw in nar_text for kw in ["frog","pepe","ribbit"]):                        nar = "meme"
        else:
            # Use detect_narrative as fallback
            nar = detect_narrative(f"{name} {sym}")

        # Social links
        tw_link = coin.get("twitter", "") or ""
        tg_link = coin.get("telegram", "") or ""
        web_link = coin.get("website", "") or ""

        # Created timestamp
        created_ts = int(coin.get("created_timestamp", 0) or 0) / 1000 if coin.get("created_timestamp") else 0

        # Reply count = social engagement signal
        reply_count = int(coin.get("reply_count", 0) or 0)
        is_live = bool(coin.get("is_currently_live", False))
        is_banned = bool(coin.get("is_banned", False))
        creator = coin.get("creator", "")
        is_graduated = bool(coin.get("raydium_pool"))

        return {
            "address": mint, "sym": sym, "name": name,
            "price": 0,  # pump.fun doesn't give price in listing
            "fdv": mcap, "mcap": mcap, "liq": liq,
            "liq_ratio": 30 if mcap > 0 else 0,
            "ch5m": 0, "ch1h": 0, "ch6h": 0, "ch24h": 0,
            "v5m": 0, "v1h": 0, "v24h": 0,
            "b5m": 0, "s5m": 0, "b1h": 0, "s1h": 0,
            "b24h": 0, "s24h": 0,
            "buy_pct": 55,  # default bullish for fresh pump.fun tokens
            "vol_spike": 1.0,
            "created_str": coin.get("created_timestamp", ""),
            "pair_addr": coin.get("bonding_curve", ""),
            "narrative": nar,
            "description": desc,
            "tw_link": tw_link, "tg_link": tg_link, "web_link": web_link,
            "creator": creator,
            "reply_count": reply_count,
            "is_pumpfun": True,
            "is_pumpfun_live": is_live,
            "is_graduated": is_graduated,
            "is_banned": is_banned,
            "created": created_ts,
            "_source": "pumpfun",
        }
    except Exception:
        return None

async def fetch_social_signals() -> Dict:
    """
    Combined social signal from multiple real-time sources.
    Returns: {
        "pump_latest": [...],    # Pump.fun new launches
        "pump_trending": [...],  # Pump.fun trending
        "news": [...],           # RSS headlines
        "cg_trending": [...],    # CoinGecko trending coins
    }
    """
    pump_lat, pump_trend, news, cg_tr = await asyncio.gather(
        pumpfun_latest(20),
        pumpfun_trending(10),
        fetch_crypto_news(),
        cg_trending(),
        return_exceptions=True
    )
    return {
        "pump_latest":  pump_lat  if isinstance(pump_lat, list)  else [],
        "pump_trending":pump_trend if isinstance(pump_trend, list) else [],
        "news":         news       if isinstance(news, list)       else [],
        "cg_trending":  (cg_tr.get("coins",[]) if isinstance(cg_tr, dict) else []),
    }

# ─── Twitter search — best-effort, graceful fallback ────────────
async def tw_search(query: str, limit: int = 15) -> List[Dict]:
    """
    Try Twitter cookie auth first. Falls back to Pump.fun keyword search.
    NOTE: All public Twitter scrapers (Nitter, RSSHub) are dead as of 2026.
    """
    # Try cookie auth if TWITTER_AUTH_TOKEN is set
    if TWITTER_AUTH_TOKEN:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.twitter.com/1.1/guest/activate.json",
                    headers={"Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as rg:
                    guest = (await rg.json()).get("guest_token", "") if rg.status == 200 else ""
                if guest:
                    async with s.get(
                        "https://twitter.com/i/api/2/search/adaptive.json",
                        headers={**_tw_headers(), "x-guest-token": guest},
                        params={"q": query, "count": str(min(limit, 20)), "tweet_mode": "extended", "result_type": "recent"},
                        timeout=aiohttp.ClientTimeout(total=12)
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            tweets_raw = d.get("globalObjects", {}).get("tweets", {})
                            result = []
                            for tid, t in tweets_raw.items():
                                result.append({"id": tid, "text": t.get("full_text", t.get("text", ""))})
                            if result:
                                logger.info(f"tw_search cookie: {len(result)} tweets for '{query}'")
                                return result[:limit]
        except Exception as e:
            logger.debug(f"tw_search cookie: {e}")

    # Fallback: search Pump.fun for keyword
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"offset": "0", "limit": "20", "sort": "created_timestamp", "order": "DESC"},
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        q_low = query.lower()
                        matches = [
                            {"id": str(i), "text": f"${c.get('symbol','?')} — {c.get('name','')} — {c.get('description','')[:100]}"}
                            for i, c in enumerate(data)
                            if any(w in (c.get('name','')+c.get('symbol','')+c.get('description','')).lower()
                                   for w in q_low.split())
                        ]
                        return matches[:limit]
    except Exception as e:
        logger.debug(f"tw_search pumpfun fallback: {e}")
    return []

async def tw_user_tweets(username: str, limit: int = 10) -> List[Dict]:
    """
    Try Twitter cookie auth. If no auth token, returns empty (no working public scraper exists).
    """
    if not TWITTER_AUTH_TOKEN:
        return []
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.twitter.com/1.1/guest/activate.json",
                headers={"Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as rg:
                guest = (await rg.json()).get("guest_token", "") if rg.status == 200 else ""
            if guest:
                async with s.get(
                    "https://api.twitter.com/1.1/statuses/user_timeline.json",
                    headers={**_tw_headers(), "x-guest-token": guest},
                    params={"screen_name": username, "count": str(min(limit,20)), "tweet_mode": "extended", "include_rts": "false"},
                    timeout=aiohttp.ClientTimeout(total=12)
                ) as r:
                    if r.status == 200:
                        tweets = await r.json()
                        return [{"id": t.get("id_str",""), "text": t.get("full_text", t.get("text",""))} for t in tweets]
    except Exception as e:
        logger.debug(f"tw_user_tweets: {e}")
    return []

def extract_cas(text: str) -> List[str]:
    """
    Extract Solana contract addresses from plain text AND URLs.
    Supports: DexScreener, GMGN, Pump.fun, Birdeye, Solscan, Photon, BullX, Raydium, Jupiter, and bare CAs.
    """
    results = set()
    _SKIP = {'solana','ethereum','bitcoin','token','tokens','address','search','trending',
             'dexscreener','birdeye','gmgn','photon','raydium','orca','jupiter',
             'pumpfun','coinbase','binance','metamask','phantom','serum',
             'coindesk','cointelegraph','decrypt','blockworks'}
    # URL-embedded addresses (most common case people send)
    _URL_PATS = [
        r'dexscreener\.com/(?:solana|ethereum|bsc)/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'gmgn\.ai/(?:sol|eth)/token/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'pump\.fun/(?:coin|token)/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'birdeye\.so/token/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'solscan\.io/(?:token|account)/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'photon-sol\.tinyastro\.io/[^?]+/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'bullx\.io[^?]*[?&]address=([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'neo\.bullx\.io[^?]*[?&]address=([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'dextools\.io/app/[^/]+/pair-explorer/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'jup\.ag/swap/([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'raydium\.io/[^?]+\?(?:inputMint|outputMint)=([1-9A-HJ-NP-Za-km-z]{32,44})',
        r'magiceden\.io/item-details/([1-9A-HJ-NP-Za-km-z]{32,44})',
        # Generic: URL path ending in a base58 address
        r'https?://[^\s]+/([1-9A-HJ-NP-Za-km-z]{32,44})(?:[/?#\s]|$)',
    ]
    for pat in _URL_PATS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            addr = m.group(1)
            if addr.lower() not in _SKIP and len(addr) >= 32:
                results.add(addr)
    # Bare addresses (not inside URLs)
    for m in re.finditer(r'(?<![/A-Za-z0-9])([1-9A-HJ-NP-Za-km-z]{32,44})(?![A-Za-z0-9])', text):
        addr = m.group(1)
        if addr.lower() not in _SKIP and len(addr) >= 32:
            results.add(addr)
    return list(results)

# ═══════════════════════════════════════════════════════════════
# NARRATIVE ENGINE
# ═══════════════════════════════════════════════════════════════

# Static keyword library — expand over time
NARRATIVES = {
    "ai":         ["ai", "agent", "gpt", "intelligence", "neural", "llm", "openai", "deepseek", "groq",
                   "agi", "robot", "claude", "gemini", "skynet", "matrix", "hal", "jarvis"],
    "gaming":     ["game", "gaming", "play", "nft", "quest", "rpg", "metaverse", "gamer",
                   "chess", "poker", "casino", "arcade", "pixel", "minecraft", "fortnite", "roblox"],
    "defi":       ["defi", "swap", "yield", "lend", "farm", "liquidity", "amm", "dex", "vault",
                   "stake", "restake", "lst", "jito", "raydium", "orca", "jupiter"],
    "meme":       ["dog", "cat", "pepe", "frog", "doge", "shib", "bonk", "wif", "wen", "gm",
                   "inu", "elon", "moon", "rocket", "ape", "monkey", "gorilla", "bear", "bull",
                   "chad", "based", "degen", "wagmi", "ngmi", "lol", "lmao", "gigachad",
                   "wojak", "cope", "seethe", "rekt", "pump", "gg", "ez", "npc",
                   "shiba", "akita", "floki", "samo", "cheems", "dood", "popcat",
                   "myro", "bome", "mother", "slerf", "giga", "smol", "retard", "autism",
                   "bird", "duck", "goat", "hamster", "turtle", "fish", "penguin", "panda",
                   "jotchua", "michi", "ponke", "bloke", "pnut", "goatseus", "max"],
    "sports":     ["football", "soccer", "fifa", "worldcup", "nba", "sport", "athlete", "fan",
                   "f1", "racing", "boxing", "ufc", "mma", "tennis", "golf", "cricket"],
    "rwa":        ["rwa", "real", "estate", "bond", "treasury", "commodity", "gold", "asset",
                   "silver", "platinum", "oil", "grain", "carbon", "credit"],
    "infra":      ["infra", "layer", "bridge", "zk", "rollup", "validator", "oracle", "chain",
                   "node", "rpc", "sequencer", "prover", "data", "avail", "eigen"],
    "payments":   ["payment", "pay", "visa", "card", "bank", "fiat", "transfer", "remit",
                   "cash", "money", "dollar", "peso", "naira", "gbp", "euro"],
    "social":     ["social", "friend", "community", "dao", "vote", "creator", "tiktok", "twitter",
                   "instagram", "youtube", "viral", "influencer", "collab", "lens", "farcaster"],
    "health":     ["health", "medical", "bio", "pharma", "longevity", "fitness", "wellness",
                   "gym", "protein", "supplement", "brain", "nootropic"],
    "politics":   ["trump", "election", "president", "government", "fed", "reserve", "macro",
                   "maga", "democrat", "republican", "senate", "white house", "tariff"],
    "celebrity":  ["elon", "musk", "kanye", "trump", "maga", "celebrity", "viral", "hype",
                   "kylie", "kardashian", "drake", "taylor", "swift", "rihanna", "lebron",
                   "ronaldo", "messi", "obama", "bezos", "zuck"],
    "animal":     ["cat", "dog", "inu", "shib", "doge", "hamster", "bird", "duck", "frog",
                   "penguin", "panda", "lion", "tiger", "wolf", "fox", "rabbit", "turtle",
                   "fish", "shark", "whale", "bear", "bull", "ape", "monkey", "gorilla"],
}

def detect_narrative(text: str) -> str:
    t = text.lower()
    scores = {n: sum(1 for kw in kws if kw in t) for n, kws in NARRATIVES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"

def narrative_from_news(headlines: List[str]) -> List[str]:
    """Extract dominant crypto narratives from a batch of news headlines."""
    combined = " ".join(headlines).lower()
    active = []
    for name, kws in NARRATIVES.items():
        hits = sum(1 for kw in kws if kw in combined)
        if hits >= 2:
            active.append(name)
    return active

# ═══════════════════════════════════════════════════════════════
# FULL TOKEN SCAN
# ═══════════════════════════════════════════════════════════════
async def full_token_scan(address: str) -> Dict:
    # Use gather with individual timeouts so GoPlus/orders can't block the whole scan
    async def _safe(coro, default, name=""):
        try:
            return await asyncio.wait_for(coro, timeout=12)
        except asyncio.TimeoutError:
            logger.warning(f"full_token_scan: {name} timed out — using default")
            return default
        except Exception as e:
            logger.warning(f"full_token_scan: {name} failed: {e}")
            return default

    pairs, sec, orders = await asyncio.gather(
        _safe(dex_pairs_by_token(address), [],  "dex_pairs"),
        _safe(goplus_check(address),        {},  "goplus"),
        _safe(dex_token_orders(address),    [],  "dex_orders"),
    )
    if not pairs:
        result = await dex_search_pairs(address)
        pairs = result
    if not pairs:
        return {"error": "Token not found on DexScreener"}

    p      = pairs[0]
    base   = p.get("baseToken", {})
    sym    = base.get("symbol", "???")
    name   = base.get("name", "Unknown")
    price  = float(p.get("priceUsd", 0) or 0)
    fdv    = float(p.get("fdv", 0) or 0)
    mcap   = float(p.get("marketCap", 0) or p.get("fdv", 0) or 0)
    liq    = float((p.get("liquidity") or {}).get("usd", 0) or 0)
    pc     = p.get("priceChange") or {}
    ch5m   = float(pc.get("m5", 0) or 0)
    ch1h   = float(pc.get("h1", 0) or 0)
    ch6h   = float(pc.get("h6", 0) or 0)
    ch24h  = float(pc.get("h24", 0) or 0)
    vol    = p.get("volume") or {}
    v5m    = float(vol.get("m5", 0) or 0)
    v1h    = float(vol.get("h1", 0) or 0)
    v24h   = float(vol.get("h24", 0) or 0)
    txns   = p.get("txns") or {}
    b5m    = int((txns.get("m5") or {}).get("buys", 0) or 0)
    s5m    = int((txns.get("m5") or {}).get("sells", 0) or 0)
    b1h    = int((txns.get("h1") or {}).get("buys", 0) or 0)
    s1h    = int((txns.get("h1") or {}).get("sells", 0) or 0)
    b24h   = int((txns.get("h24") or {}).get("buys", 0) or 0)
    s24h   = int((txns.get("h24") or {}).get("sells", 0) or 0)
    created = int(p.get("pairCreatedAt", 0) or 0)

    # Social links
    info    = p.get("info") or {}
    socials = info.get("socials") or []
    sites   = info.get("websites") or []
    tw_link = next((s.get("url", "") for s in socials if s.get("type") == "twitter"), "")
    tg_link = next((s.get("url", "") for s in socials if s.get("type") == "telegram"), "")
    web_link = sites[0].get("url", "") if sites else ""

    # Boosts & paid orders
    boosts = p.get("boosts") or {}
    boost_active = int(boosts.get("active", 0) or 0)
    has_profile = any(o.get("type") == "tokenProfile" and o.get("status") == "approved" for o in orders)
    has_ad      = any(o.get("type") == "tokenAd"      and o.get("status") == "approved" for o in orders)

    # Security
    risk_score, red_flags, green_flags = parse_security(sec)
    sell_tax = float(sec.get("sell_tax", 0) or 0)
    buy_tax  = float(sec.get("buy_tax",  0) or 0)
    is_honeypot  = sec.get("is_honeypot", "0") == "1"
    lp_locked    = sec.get("lp_locked",  "0") == "1"
    is_renounced = sec.get("is_renounced","0") == "1"

    # Derived metrics
    liq_ratio  = liq / max(fdv, 1) * 100
    buy_pct    = b1h / max(b1h + s1h, 1) * 100
    avg_5m_vol = v1h / 12 if v1h > 0 else 1
    vol_spike  = v5m / max(avg_5m_vol, 1)
    narrative  = detect_narrative(f"{name} {sym}")

    # Momentum score
    mscore = 0
    if ch1h > 100: mscore += 35
    elif ch1h > 50: mscore += 28
    elif ch1h > 20: mscore += 20
    elif ch1h > 5:  mscore += 10
    if buy_pct > 70: mscore += 25
    elif buy_pct > 55: mscore += 15
    if vol_spike > 4: mscore += 20
    elif vol_spike > 2: mscore += 12
    if liq_ratio > 15: mscore += 15
    if risk_score < 20: mscore += 10
    if boost_active > 0: mscore += 5
    mscore = min(mscore, 100)

    # ATH estimate: 24h high = current_price / (1 - |ch24h|/100) if ch24h < 0
    # If price is up 24h, ATH could be now or higher — we show 24h high as a proxy
    # For a real ATH we'd need historical OHLCV which DexScreener doesn't expose freely.
    # Instead fetch all pairs and find highest price across timeframes.
    price_24h_ago = price / (1 + ch24h / 100) if ch24h != -100 else 0
    ath_24h = max(price, price_24h_ago)   # 24h high proxy (conservative)
    # ch24h from DexScreener is % change from 24h ago to NOW
    # so 24h high ≈ max(price_now, price_24h_ago)
    # We label it "24h High" honestly rather than "ATH" to avoid misleading

    return {
        "address": address, "sym": sym, "name": name, "price": price,
        "fdv": fdv, "mcap": mcap, "liq": liq, "liq_ratio": liq_ratio,
        "ch5m": ch5m, "ch1h": ch1h, "ch6h": ch6h, "ch24h": ch24h,
        "v5m": v5m, "v1h": v1h, "v24h": v24h,
        "b5m": b5m, "s5m": s5m, "b1h": b1h, "s1h": s1h, "b24h": b24h, "s24h": s24h,
        "buy_pct": buy_pct, "vol_spike": vol_spike, "mscore": mscore,
        "risk_score": risk_score, "red_flags": red_flags, "green_flags": green_flags,
        "sell_tax": sell_tax, "buy_tax": buy_tax,
        "is_honeypot": is_honeypot, "lp_locked": lp_locked, "is_renounced": is_renounced,
        "created": created, "narrative": narrative,
        "tw_link": tw_link, "tg_link": tg_link, "web_link": web_link,
        "boost_active": boost_active, "has_profile": has_profile, "has_ad": has_ad,
        "pair_addr": p.get("pairAddress", ""),
        "dex_url": p.get("url", f"https://dexscreener.com/solana/{address}"),
        "ath_24h": ath_24h,  # 24h high proxy
        "price_24h_ago": price_24h_ago,
    }

# ═══════════════════════════════════════════════════════════════
# MESSAGE CARDS
# ═══════════════════════════════════════════════════════════════

def _social_line(t: Dict) -> str:
    parts = []
    if t.get("tw_link"):  parts.append(f"[Twitter]({t['tw_link']})")
    if t.get("tg_link"):  parts.append(f"[Telegram]({t['tg_link']})")
    if t.get("web_link"): parts.append(f"[Website]({t['web_link']})")
    return " · ".join(parts) if parts else "None"

def build_scan_card(t: Dict, ai: str = "") -> str:
    """Pro-style deep scan card — clean layout matching GMGN/BonkBot standards."""
    sym  = _md(t.get("sym", "???"))
    name = _md(t.get("name", sym))
    age  = _age(t.get("created", 0))
    nar  = f" #{t.get('narrative','').upper()}" if t.get("narrative") else ""
    pf_desc     = t.get("pf_description", "")
    pf_replies  = t.get("pf_reply_count", 0)
    is_pf       = t.get("is_pumpfun", False)
    is_grad     = t.get("is_graduated", False)
    bp   = float(t.get("buy_pct", 50))
    sp   = 100 - bp


    fill = int(bp / 10)
    bar  = "🟩" * fill + "🟥" * (10 - fill)
    press = "🔥 BUY PRESSURE" if bp > 60 else ("❄️ SELL PRESSURE" if bp < 40 else "⚖️ BALANCED")

    badges = []
    if t.get("is_renounced"):     badges.append("✅ Renounced")
    if t.get("lp_locked"):        badges.append("🔒 LP Locked")
    if t.get("boost_active",0)>0: badges.append("💰 Boosted")
    if t.get("has_profile"):      badges.append("📋 Verified")
    if t.get("is_honeypot"):      badges.append("🚨 Honeypot")
    badge_str = "  ".join(badges) if badges else "⚠️ Unverified"

    slinks = []
    if t.get("tw_link"):  slinks.append(f"[🐦 Twitter]({t['tw_link']})")
    if t.get("tg_link"):  slinks.append(f"[💬 TG]({t['tg_link']})")
    if t.get("web_link"): slinks.append(f"[🌐 Web]({t['web_link']})")
    social_str = "  ".join(slinks) if slinks else "_(no socials)_"

    ms = int(t.get("mscore", 0))
    ms_emoji = "🔥" if ms >= 70 else ("⚡" if ms >= 40 else "💤")
    liq_ratio = t.get("liq_ratio", 0)

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🦅 *KAYO DEEP SCAN*{nar}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{sym}* — _{name}_\n"
        f"📋 `{t.get('address', '')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: `{_price(t.get('price', 0))}`  ·  Age: {age}\n"
        f"📊 MCap: `{_usd(t.get('mcap', 0))}`  ·  FDV: `{_usd(t.get('fdv', 0))}`\n"
        f"🌊 Liquidity: `{_usd(t.get('liq', 0))}` ({liq_ratio:.1f}% of MCap)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Price Change*\n"
        f"  5m: {_chg(t.get('ch5m',0))}  ·  1h: {_chg(t.get('ch1h',0))}\n"
        f"  6h: {_chg(t.get('ch6h',0))}  ·  24h: {_chg(t.get('ch24h',0))}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Volume*\n"
        f"  5m: `{_usd(t.get('v5m',0))}`  1h: `{_usd(t.get('v1h',0))}`  24h: `{_usd(t.get('v24h',0))}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 *Transactions (1h)*\n"
        f"  🟢 Buys: {t.get('b1h',0)}  ·  🔴 Sells: {t.get('s1h',0)}\n"
        f"  {bar}\n"
        f"  {bp:.0f}% Buy / {sp:.0f}% Sell — {press}\n"
        f"  Vol Spike: {t.get('vol_spike', 0):.1f}x\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Momentum: {ms_emoji} {ms}/100  ·  {_risk(t.get('risk_score', 30))}\n"
        f"🛡️ {badge_str}\n"
    )
    bt, st = float(t.get("buy_tax", 0)), float(t.get("sell_tax", 0))
    if bt > 0 or st > 0:
        card += f"🧾 Tax: Buy {bt:.1f}% / Sell {st:.1f}%\n"
    if t.get("red_flags"):
        card += "\n🚩 *Risk Flags:*\n" + "\n".join(f"  • {_md(f)}" for f in t["red_flags"][:3]) + "\n"
    if t.get("green_flags"):
        card += "\n✅ *Green Flags:*\n" + "\n".join(f"  • {_md(f)}" for f in t["green_flags"][:2]) + "\n"
    card += f"\n🌐 Socials: {social_str}\n"
    card += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if ai:
        card += f"\n\n🧠 *Kayo AI Verdict:*\n_{ai}_"
    return card
def _md(s: str) -> str:
    """Escape Markdown special chars in dynamic text for Telegram V1."""
    if not s: return ""
    return re.sub(r'([*_`\[\]()~>#+=|{}.!\\])', r'\\\1', str(s))

def build_alert_card(t: Dict, alert_type: str, ai: str = "") -> str:
    """
    Pro-style alert card — GMGN/BonkBot inspired.
    Clean sections, visual bars, color-coded metrics, security badges.
    """
    headers = {
        "pump":      "🚀 PUMP ALERT",
        "dump":      "💀 DUMP ALERT",
        "whale":     "🐋 WHALE ACCUMULATION",
        "gem":       "💎 HIDDEN GEM",
        "new":       "🆕 NEW LAUNCH",
        "narrative": "📖 NARRATIVE PLAY",
        "rug":       "⚠️ RUG ALERT",
        "unusual":   "⚡ UNUSUAL ACTIVITY",
        "migration": "🔄 GRADUATION ALERT",
        "rebrand":   "🏷️ REBRAND ALERT",
        "momentum":  "📈 MOMENTUM ALERT",
    }
    header = headers.get(alert_type, "⚡ KAYO ALERT")

    sym  = _md(t.get("sym", "???"))
    name = _md(t.get("name", sym))
    nar  = f" #{t.get('narrative','').upper()}" if t.get("narrative") else ""
    age  = _age(t.get("created", 0))

    def _chg(v):
        v = float(v or 0)
        if v > 0:  return f"🟢 +{v:.1f}%"
        if v < 0:  return f"🔴 {v:.1f}%"
        return f"⚪ {v:.1f}%"

    ch5m  = _chg(t.get("ch5m", 0))
    ch1h  = _chg(t.get("ch1h", 0))
    ch6h  = _chg(t.get("ch6h", 0))
    ch24h = _chg(t.get("ch24h", 0))

    bp   = float(t.get("buy_pct", 50))
    sp   = 100 - bp
    fill = int(bp / 10)
    bar  = "🟩" * fill + "🟥" * (10 - fill)
    press = "🔥 BUY PRESSURE" if bp > 60 else ("❄️ SELL PRESSURE" if bp < 40 else "⚖️ BALANCED")

    badges = []
    if is_pf:               badges.append("🟣 Pump.fun")
    if is_grad:             badges.append("✅ Graduated to Raydium")
    if t.get("is_renounced"): badges.append("✅ Renounced")
    if t.get("lp_locked"):    badges.append("🔒 LP Locked")
    if t.get("boost_active", 0) > 0: badges.append("💰 Boosted")
    if t.get("is_honeypot"):  badges.append("🚨 Honeypot")
    if pf_replies >= 10:     badges.append(f"💬 {pf_replies} replies")
    badge_str = "  ".join(badges) if badges else "⚠️ Unverified"

    tax_line = ""
    bt = float(t.get("buy_tax", 0))
    st = float(t.get("sell_tax", 0))
    if bt > 0 or st > 0:
        tax_line = f"\n🧾 Tax: Buy {bt:.1f}% / Sell {st:.1f}%"

    socials = []
    if t.get("tw_link"):  socials.append(f"[🐦]({t['tw_link']})")
    if t.get("tg_link"):  socials.append(f"[💬]({t['tg_link']})")
    if t.get("web_link"): socials.append(f"[🌐]({t['web_link']})")
    social_str = "  ".join(socials)

    ms = int(t.get("mscore", 0))
    ms_emoji = "🔥" if ms >= 70 else ("⚡" if ms >= 40 else "💤")

    pf_narrative_line = ""
    if pf_desc and len(pf_desc) > 5:
        # Show pump.fun narrative description (truncated)
        pf_narrative_line = f"📖 _{pf_desc[:150]}_\n"

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  {header}{nar}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{sym}* — _{name}_\n"
        f"📋 `{t.get('address', '')}`\n"
        f"{pf_narrative_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: `{_price(t.get('price', 0))}`  ·  Age: {age}\n"
        f"📊 MCap: `{_usd(t.get('mcap', 0))}`  ·  FDV: `{_usd(t.get('fdv', 0))}`\n"
        f"🌊 Liquidity: `{_usd(t.get('liq', 0))}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Price Change*\n"
        f"  5m: {ch5m}  ·  1h: {ch1h}\n"
        f"  6h: {ch6h}  ·  24h: {ch24h}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 *Transactions (1h)*\n"
        f"  🟢 Buys: {t.get('b1h', 0)}  ·  🔴 Sells: {t.get('s1h', 0)}\n"
        f"  {bar}\n"
        f"  {bp:.0f}% Buy / {sp:.0f}% Sell — {press}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Momentum: {ms_emoji} {ms}/100  ·  {_risk(t.get('risk_score', 30))}\n"
        f"🛡️ {badge_str}{tax_line}\n"
    )
    if social_str:
        card += f"🔗 Socials: {social_str}\n"
    card += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if ai:
        card += f"\n\n🧠 *Kayo AI:*\n_{ai}_"
    return card
def scan_buttons(addr: str, sym: str = "", pair_addr: str = "") -> InlineKeyboardMarkup:
    """
    All buttons open INSIDE Telegram using WebApp (web_app=WebAppInfo(url=...)).
    This keeps the user inside the app — no external browser, no leaving Telegram.
    Row 1 — Charts:  DexScreener WebApp  |  GMGN WebApp
    Row 2 — Trade:   BullX Neo WebApp    |  Photon WebApp
    Row 3 — Trade:   Banana Gun WebApp   |  Trojan WebApp
    """
    dex_pair = pair_addr or addr
    return InlineKeyboardMarkup([
        [
            # DexScreener — opens chart inside Telegram WebApp browser
            InlineKeyboardButton(
                "\U0001f4ca DexScreener",
                web_app=WebAppInfo(url=f"https://dexscreener.com/solana/{dex_pair}")
            ),
            # GMGN — opens inside Telegram WebApp browser
            InlineKeyboardButton(
                "\U0001f438 GMGN",
                web_app=WebAppInfo(url=f"https://gmgn.ai/sol/token/{addr}")
            ),
        ],
        [
            # BullX Neo — opens WebApp terminal inside Telegram
            InlineKeyboardButton(
                "\U0001f319 BullX",
                web_app=WebAppInfo(url=f"https://neo.bullx.io/terminal?chainId=1399811149&address={addr}")
            ),
            # Photon — opens WebApp inside Telegram
            InlineKeyboardButton(
                "\U0001f52b Photon",
                web_app=WebAppInfo(url=f"https://photon-sol.tinyastro.io/en/lp/{addr}")
            ),
        ],
        [
            InlineKeyboardButton(
                "\U0001f34c Banana",
                url=f"https://t.me/BananaGunSolana_bot?start=snipe_{addr}"
            ),
            InlineKeyboardButton(
                "\U0001f5e1 Trojan",
                url=f"https://t.me/hector_trojanbot?start=snipe-SOL-{addr}"
            ),
        ],
        [
            InlineKeyboardButton(
                "\U0001f504 Refresh",
                callback_data=f"refresh:{addr}"
            ),
        ],
    ])

# ═══════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    add_xp(u.effective_user.id, 10)
    name = u.effective_user.first_name or "degen"
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f52c Scan Token",   callback_data="menu:scan"),
            InlineKeyboardButton("\U0001f4ca Chart",        callback_data="menu:chart"),
        ],
        [
            InlineKeyboardButton("\U0001f3c3 Runners",      callback_data="menu:runners"),
            InlineKeyboardButton("\U0001f195 New Launches", callback_data="menu:new"),
        ],
        [
            InlineKeyboardButton("\U0001f680 Pumps",        callback_data="menu:pump"),
            InlineKeyboardButton("\U0001f48e Gems",         callback_data="menu:gems"),
        ],
        [
            InlineKeyboardButton("\U0001f525 Trending",     callback_data="menu:trending"),
            InlineKeyboardButton("\U0001f4f0 News",         callback_data="menu:news"),
        ],
        [
            InlineKeyboardButton("\U0001f916 Ask Kayo AI",  callback_data="menu:ask"),
            InlineKeyboardButton("\U0001f624 Sentiment",    callback_data="menu:sentiment"),
        ],
        [
            InlineKeyboardButton("\U0001f4bc Portfolio",    callback_data="menu:portfolio"),
            InlineKeyboardButton("\U0001f514 My Alerts",    callback_data="menu:myalerts"),
        ],
        [
            InlineKeyboardButton("\U0001f4e2 Leaderboard",  callback_data="menu:leaderboard"),
            InlineKeyboardButton("\U00002b50 My Rank",      callback_data="menu:rank"),
        ],
        [
            InlineKeyboardButton("\U0001f4b5 Price Check",  callback_data="menu:price"),
            InlineKeyboardButton("\U0001f6e1 Verify Token", callback_data="menu:verify"),
        ],
        [
            InlineKeyboardButton("\U0001f4cb Full Command List", callback_data="menu:help"),
        ],
    ])
    await (u.message or u.effective_message).reply_text(
        f"\U0001f985 *KAYO BRAIN v40*\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"_Yo {name}! Your Solana alpha intelligence bot is live._\n\n"
        f"Tap any button below or type `/` to browse all commands in the menu bar."
        f"\nEvery command is tap-to-use from the menu \u2b07\ufe0f",
        parse_mode="Markdown",
        reply_markup=markup,
    )

async def help_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Paginated help with tappable category buttons."""
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f52c Scan & Analyze",  callback_data="help:scan"),
            InlineKeyboardButton("\U0001f50d Discover",        callback_data="help:discover"),
        ],
        [
            InlineKeyboardButton("\U0001f4d6 Narratives",      callback_data="help:narrative"),
            InlineKeyboardButton("\U0001f4f0 News & AI",       callback_data="help:ai"),
        ],
        [
            InlineKeyboardButton("\U0001f426 Twitter/Social",  callback_data="help:twitter"),
            InlineKeyboardButton("\U0001f514 Alerts",          callback_data="help:alerts"),
        ],
        [
            InlineKeyboardButton("\U0001f4e2 Calls",           callback_data="help:calls"),
            InlineKeyboardButton("\U0001f4bc Portfolio",       callback_data="help:portfolio"),
        ],
        [
            InlineKeyboardButton("\U0001f45b Wallets",         callback_data="help:wallets"),
            InlineKeyboardButton("\U0001f3ae XP & Social",     callback_data="help:social"),
        ],
        [
            InlineKeyboardButton("\U00002699\ufe0f System",   callback_data="help:system"),
        ],
    ])
    await u.effective_message.reply_text(
        "\U0001f985 *KAYO BRAIN v40 — COMMANDS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "Tap a category \U0001f447 to see its commands.\n"
        "Or type `/` in the chat bar to tap any command directly.",
        parse_mode="Markdown",
        reply_markup=markup,
    )

async def scan_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/scan <contract_address>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("🔍 *Scanning...*", parse_mode="Markdown")
    t    = await full_token_scan(addr)
    if t.get("error"):
        await msg.edit_text(f"❌ {t['error']}"); return
    add_xp(u.effective_user.id, 5)
    _track_scan(t, u.effective_user.id)
    # Send card IMMEDIATELY — no AI wait
    buttons = scan_buttons(addr, t.get("sym", ""), t.get("pair_addr", ""))
    sent = await msg.edit_text(
        build_scan_card(t, ""),
        parse_mode="Markdown",
        reply_markup=buttons,
        disable_web_page_preview=True,
    )
    # AI verdict as background task — edits message when ready
    async def _scan_ai(msg_id, chat_id, token_data, btns):
        try:
            ai_v = await asyncio.wait_for(
                ai_ask(
                    f"Solana token ${token_data['sym']} — MCap {_usd(token_data['mcap'])}, "
                    f"liq {_usd(token_data['liq'])}, age {_age(token_data['created'])}, "
                    f"5m {_pct(token_data['ch5m'])}, 1h {_pct(token_data['ch1h'])}, "
                    f"24h {_pct(token_data['ch24h'])}, buy ratio {token_data['buy_pct']:.0f}%, "
                    f"vol spike {token_data['vol_spike']:.1f}x, momentum {token_data['mscore']}/100, "
                    f"risk {token_data['risk_score']}/100, narrative #{token_data['narrative']}, "
                    f"honeypot={token_data['is_honeypot']}, lp_locked={token_data['lp_locked']}. "
                    "Give a sharp alpha verdict: is this worth aping right now? "
                    "Consider the current market conditions from your live context. "
                    "Call out any red flags. 2-3 direct sentences.",
                    fallback="",
                    inject_market=True
                ),
                timeout=15
            )
            if ai_v and ai_v.strip():
                try:
                    await c.bot.edit_message_text(
                        chat_id=chat_id, message_id=msg_id,
                        text=build_scan_card(token_data, ai_v),
                        parse_mode="Markdown",
                        reply_markup=btns,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
        except Exception:
            pass

    asyncio.create_task(_scan_ai(
        sent.message_id if hasattr(sent, 'message_id') else msg.message_id,
        u.effective_chat.id, t, buttons
    ))

async def c_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/c <ca>`", parse_mode="Markdown"); return
    addr  = c.args[0].strip()
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await u.effective_message.reply_text("❌ Token not found."); return
    p     = pairs[0]
    base  = p.get("baseToken", {})
    sym   = base.get("symbol", "???")
    price = float(p.get("priceUsd", 0) or 0)
    fdv   = float(p.get("fdv", 0) or 0)
    liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
    ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
    ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
    b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
    s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
    await u.effective_message.reply_text(
        f"⚡ *${sym}*\n"
        f"Price: {_price(price)}\n"
        f"MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
        f"1h: {_pct(ch1h)}  24h: {_pct(ch24h)}\n"
        f"Buys/Sells (1h): {b1h} / {s1h}\n"
        f"`{addr}`",
        parse_mode="Markdown",
        reply_markup=scan_buttons(addr, sym),
    )

async def verify_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/verify <ca>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("🛡 *Running security check...*", parse_mode="Markdown")
    sec  = await goplus_check(addr)
    if not sec:
        await msg.edit_text("⚠️ Security data unavailable for this token."); return
    risk, red, green = parse_security(sec)
    add_xp(u.effective_user.id, 3)
    st = float(sec.get("sell_tax", 0) or 0)
    bt = float(sec.get("buy_tax",  0) or 0)
    text = (
        f"🛡 *SECURITY CHECK*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Risk Score: {risk}/100 — {_risk(risk)}\n"
        f"Buy Tax: {bt}%  ·  Sell Tax: {st}%\n"
    )
    if red:   text += "\n*🚩 Red Flags:*\n" + "\n".join(f"  {f}" for f in red) + "\n"
    if green: text += "\n*✅ Green Flags:*\n" + "\n".join(f"  {f}" for f in green) + "\n"
    text += f"\n`{addr}`"
    await msg.edit_text(text, parse_mode="Markdown")

async def runners_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.effective_message.reply_text("🏃 *Scanning for top runners...*", parse_mode="Markdown")
    # Use GeckoTerminal — returns real coins, not just popular searched ones
    pools_new, pools_trend = await asyncio.gather(
        gt_new_pools(page=1),
        gt_trending_pools(page=1),
    )
    all_toks: Dict[str, Dict] = {}
    for pool in (pools_new + pools_trend):
        tok = gt_parse_pool(pool)
        if tok and tok["address"] not in all_toks:
            all_toks[tok["address"]] = tok

    runners = [
        tok for tok in all_toks.values()
        if tok["address"] not in blacklist
        and 0 < tok["fdv"] <= 500_000
        and tok["liq"] >= 500
        and tok["ch1h"] > 5
        and tok["buy_pct"] >= 48
    ]
    runners.sort(key=lambda t: t["ch1h"], reverse=True)
    top = runners[:10]

    if not top:
        await msg.edit_text("😴 No sub-$500k runners right now. Market is quiet."); return
    add_xp(u.effective_user.id, 3)
    out_lines = [f"🏃 *TOP SOLANA RUNNERS — 1H*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_{len(all_toks)} coins scanned_"]
    for i, tok in enumerate(top, 1):
        sym  = tok["sym"]
        addr = tok["address"]
        nar  = detect_narrative(f"{sym} {tok['name']}")
        out_lines.append(
            f"\n*{i}. ${sym}* — #{nar.upper()}\n"
            f"  5m: {_pct(tok['ch5m'])}  1h: {_pct(tok['ch1h'])}\n"
            f"  MCap: `{_usd(tok['fdv'])}`  Liq: `{_usd(tok['liq'])}`\n"
            f"  Buys/Sells(1h): {tok['b1h']}/{tok['s1h']} — {tok['buy_pct']:.0f}% buys\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(out_lines), parse_mode="Markdown")

async def new_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg      = await u.effective_message.reply_text("🆕 *Fetching new launches...*", parse_mode="Markdown")
    profiles = await dex_token_profiles_latest()
    sol      = [p for p in profiles if p.get("chainId") == "solana"][:20]
    if not sol:
        await msg.edit_text("❌ No new profiles found."); return
    addrs     = [p.get("tokenAddress", "") for p in sol if p.get("tokenAddress")]
    pairs_data = await dex_batch(addrs[:15])
    pair_map  = {pd.get("baseToken", {}).get("address", ""): pd for pd in pairs_data}
    add_xp(u.effective_user.id, 2)
    lines = ["🆕 *BRAND NEW LAUNCHES*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    count = 0
    for prof in sol:
        addr = prof.get("tokenAddress", "")
        p    = pair_map.get(addr)
        if not p: continue
        base = p.get("baseToken", {})
        sym  = base.get("symbol", "???")
        fdv  = float(p.get("fdv", 0) or 0)
        liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        if liq < 300: continue
        ch1h    = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        age     = _age(p.get("pairCreatedAt", 0) or 0)
        links   = prof.get("links") or []
        soc_str = "".join(["🐦" if l.get("type") == "twitter" else "💬" if l.get("type") == "telegram" else "🌐" for l in links[:3]])
        nar     = detect_narrative(f"{sym} {base.get('name','')}")
        lines.append(
            f"\n*${sym}* {soc_str} — #{nar.upper()}\n"
            f"  Age: {age}  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
            f"  1h: {_pct(ch1h)}\n"
            f"  `{addr}`"
        )
        count += 1
        if count >= 8: break
    if count == 0:
        await msg.edit_text("😴 No new launches with sufficient liquidity.")
    else:
        await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def pump_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.effective_message.reply_text("\U0001f680 *Finding fresh pumps...*", parse_mode="Markdown")
    pools_new, pools_p2 = await asyncio.gather(gt_new_pools(page=1), gt_new_pools(page=2))
    all_toks: Dict[str, Dict] = {}
    for pool in (pools_new + pools_p2):
        tok = gt_parse_pool(pool)
        if tok and tok["address"] not in all_toks:
            all_toks[tok["address"]] = tok
    pumping = sorted(
        [t for t in all_toks.values()
         if t["address"] not in blacklist
         and 0 < t["fdv"] <= 500_000
         and t["liq"] >= 500
         and t["ch5m"] >= 3
         and t["b5m"] > t["s5m"]],
        key=lambda t: t["ch5m"], reverse=True
    )
    if not pumping:
        await msg.edit_text("Nothing pumping hard right now."); return
    add_xp(u.effective_user.id, 2)
    header = "\U0001f680 *FRESH PUMPS \u2014 5M*\n" + "\u2501" * 14
    out_lines = [header]
    for t in pumping[:8]:
        out_lines.append(
            "\n*$" + t["sym"] + "*\n"
            "  5m: " + _pct(t["ch5m"]) + "  1h: " + _pct(t["ch1h"]) + "\n"
            "  MCap: `" + _usd(t["fdv"]) + "` Liq: `" + _usd(t["liq"]) + "`\n"
            "  " + str(t["b5m"]) + "B / " + str(t["s5m"]) + "S (5m)\n"
            "  `" + t["address"] + "`"
        )
    await msg.edit_text("\n".join(out_lines), parse_mode="Markdown")

async def gems_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.effective_message.reply_text("💎 *Hunting hidden gems...*", parse_mode="Markdown")
    # GeckoTerminal new_pools — fresh coins, real data, no key needed
    pools_p1, pools_p2 = await asyncio.gather(
        gt_new_pools(page=1),
        gt_new_pools(page=2),
    )
    all_toks: Dict[str, Dict] = {}
    for pool in (pools_p1 + pools_p2):
        tok = gt_parse_pool(pool)
        if tok and tok["address"] not in all_toks:
            all_toks[tok["address"]] = tok

    gems = []
    for tok in all_toks.values():
        if tok["address"] in blacklist: continue
        if not (0 < tok["fdv"] <= 500_000): continue
        if tok["liq"] < 300: continue
        if tok["buy_pct"] < 48: continue
        # Gem scoring: prefer high buy%, low mcap, real activity
        score = 0
        if tok["fdv"] < 50_000:  score += 30
        elif tok["fdv"] < 150_000: score += 20
        elif tok["fdv"] < 300_000: score += 10
        if tok["buy_pct"] > 70:  score += 25
        elif tok["buy_pct"] > 60: score += 15
        elif tok["buy_pct"] > 55: score += 8
        if tok["b1h"] > 30: score += 20
        elif tok["b1h"] > 10: score += 10
        elif tok["b1h"] > 3:  score += 5
        if tok["ch1h"] > 50: score += 20
        elif tok["ch1h"] > 20: score += 12
        elif tok["ch1h"] > 5:  score += 6
        if tok["liq_ratio"] > 20: score += 10  # high liq relative to mcap = safer
        if tok["vol_spike"] > 2: score += 10
        if score >= 25:
            gems.append((score, tok))

    gems.sort(reverse=True)
    top = gems[:8]
    if not top:
        await msg.edit_text("💎 No hidden gems found right now. Try again in a few minutes."); return
    add_xp(u.effective_user.id, 3)
    out_lines = [f"💎 *HIDDEN GEMS — SOLANA SUB-$500K*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_{len(all_toks)} coins scanned_"]
    for i, (score, tok) in enumerate(top, 1):
        sym  = tok["sym"]
        addr = tok["address"]
        nar  = detect_narrative(f"{sym} {tok['name']}")
        out_lines.append(
            f"\n*{i}. ${sym}* — #{nar.upper()} | Score: {score}\n"
            f"  MCap: `{_usd(tok['fdv'])}`  Liq: `{_usd(tok['liq'])}`\n"
            f"  1h: {_pct(tok['ch1h'])}  Buys(1h): {tok['b1h']} — {tok['buy_pct']:.0f}% buys\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(out_lines), parse_mode="Markdown")


async def trending_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg   = await u.effective_message.reply_text("🔥 *Fetching trending metas...*", parse_mode="Markdown")
    metas = await dex_trending_metas()
    if not metas:
        await msg.edit_text("❌ Could not fetch trending metas."); return
    add_xp(u.effective_user.id, 2)
    lines = ["🔥 *TRENDING METAS* _(narrative categories, not coins)_\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Use /gems or /runners for sub-$500k degen plays_"]
    for m in metas[:8]:
        name   = m.get("name", "?")
        mcap   = float(m.get("marketCap", 0) or 0)
        vol    = float(m.get("volume", 0) or 0)
        count  = m.get("tokenCount", 0)
        chg    = m.get("marketCapChange") or {}
        c1h    = float(chg.get("h1", 0) or 0)
        c24h   = float(chg.get("h24", 0) or 0)
        lines.append(
            f"\n🏷 *{name}*\n"
            f"  MCap: `{_usd(mcap)}`  Vol: `{_usd(vol)}`  Tokens: {count}\n"
            f"  1h: {_pct(c1h)}  24h: {_pct(c24h)}"
        )
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def narrative_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/narrative <word>` e.g. `/narrative ai`", parse_mode="Markdown"); return
    slug = c.args[0].lower().strip()
    msg  = await u.effective_message.reply_text(f"📖 *Finding #{slug} tokens...*", parse_mode="Markdown")
    pairs = await dex_meta_tokens(slug)
    if not pairs:
        pairs = await dex_search_pairs(f"solana {slug}")
    pairs = [
        p for p in pairs
        if p.get("chainId") == "solana"
        and float((p.get("liquidity") or {}).get("usd", 0) or 0) > 2000
        and float(p.get("fdv", 0) or 0) <= 500_000   # hard $500k cap
        and float(p.get("fdv", 0) or 0) >= 3_000
    ]
    pairs.sort(key=lambda p: float((p.get("volume") or {}).get("h24", 0) or 0), reverse=True)
    if not pairs:
        await msg.edit_text(f"❌ No coins found for #{slug}."); return
    add_xp(u.effective_user.id, 2)
    lines = [f"📖 *#{slug.upper()} NARRATIVE*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for p in pairs[:7]:
        base  = p.get("baseToken", {})
        sym   = base.get("symbol", "???")
        addr  = base.get("address", "")
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        v24h  = float((p.get("volume") or {}).get("h24", 0) or 0)
        lines.append(
            f"\n*${sym}*\n"
            f"  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
            f"  1h: {_pct(ch1h)}  24h: {_pct(ch24h)}\n"
            f"  Vol 24h: `{_usd(v24h)}`\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def explain_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """AI explains a narrative in professional terms."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/explain <narrative>` e.g. `/explain RWA`", parse_mode="Markdown"); return
    topic = " ".join(c.args)
    msg   = await u.effective_message.reply_text(f"🧠 *Explaining #{topic}...*", parse_mode="Markdown")
    ai = await ai_ask(
        f"Explain the '{topic}' crypto narrative in professional terms for a Solana trader. "
        f"Cover: what it is, why it's relevant now, what kind of tokens fall under it, "
        f"and what drives price action in this narrative. "
        f"Use 4-5 bullet points. Be sharp and insightful, not generic.",
        fallback="AI unavailable right now.",
        max_tokens=400
    )
    await msg.edit_text(
        f"📖 *#{topic.upper()} — NARRATIVE BRIEFING*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}",
        parse_mode="Markdown"
    )

async def boosted_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg    = await u.effective_message.reply_text("💰 *Fetching boosted tokens...*", parse_mode="Markdown")
    boosts = await dex_boosts_top()
    sol    = [b for b in boosts if b.get("chainId") == "solana"][:15]
    if not sol:
        await msg.edit_text("❌ No boosted tokens right now."); return
    addrs  = [b.get("tokenAddress", "") for b in sol if b.get("tokenAddress")]
    pairs  = await dex_batch(addrs[:15])
    p_map  = {pd.get("baseToken", {}).get("address", ""): pd for pd in pairs}
    add_xp(u.effective_user.id, 2)
    lines = ["💰 *TOP BOOSTED TOKENS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Teams paid to boost these — shows intent_"]
    count = 0
    for b in sol:
        addr  = b.get("tokenAddress", "")
        bamt  = b.get("totalAmount", 0)
        p     = p_map.get(addr)
        if not p: continue
        base  = p.get("baseToken", {})
        sym   = base.get("symbol", "???")
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        lines.append(
            f"\n💰 *${sym}* — Boost: {bamt}\n"
            f"  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
            f"  1h: {_pct(ch1h)}  24h: {_pct(ch24h)}\n"
            f"  `{addr}`"
        )
        count += 1
        if count >= 7: break
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def takeover_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg  = await u.effective_message.reply_text("🫧 *Fetching community takeovers...*", parse_mode="Markdown")
    data = await dex_community_takeovers()
    sol  = [t for t in data if t.get("chainId") == "solana"][:8]
    if not sol:
        await msg.edit_text("No active community takeovers on Solana right now."); return
    add_xp(u.effective_user.id, 2)
    lines = ["🫧 *COMMUNITY TAKEOVERS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Dead projects revived by community_"]
    for t in sol[:6]:
        addr  = t.get("tokenAddress", "")
        date  = (t.get("claimDate") or "")[:10]
        links = t.get("links") or []
        soc   = "".join(["🐦" if l.get("type") == "twitter" else "💬" if l.get("type") == "telegram" else "🌐" for l in links[:2]])
        lines.append(f"\n🫧 Claimed: {date} {soc}\n  `{addr}`")
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def news_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.effective_message.reply_text("📰 *Fetching latest crypto news...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)
    headlines = await fetch_crypto_news()
    if not headlines:
        await msg.edit_text("❌ Could not fetch news right now. Try again in a minute."); return
    # Show top 6 headlines
    top = headlines[:6]
    headlines_text = "\n".join([f"• {h}" for h in top])
    # AI market intelligence briefing based on real headlines
    ai_brief = await ai_ask(
        f"These are today's top crypto news headlines:\n" + "\n".join(top) + "\n\n"
        "Give a sharp 3-sentence market intelligence briefing: "
        "What narratives are emerging? What should Solana degens watch? "
        "What's the biggest market-moving story?",
        max_tokens=200, inject_market=True,
        fallback="Multiple developments in the crypto space today. Check individual headlines for details."
    )
    card = (
        "📰 *CRYPTO INTELLIGENCE BRIEFING*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{headlines_text}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 *Kayo's Take:*\n_{ai_brief}_"
    )
    try:
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
        plain = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', card)
        await msg.edit_text(plain[:4000])
async def ask_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/ask <question>`\nExample: `/ask what's the price of sol?`", parse_mode="Markdown"); return
    q   = " ".join(c.args)
    msg = await u.effective_message.reply_text("\U0001f9e0 *Kayo thinking...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)

    prompt = (
        f"Question: \"{q}\"\n\n"
        "Answer as Kayo. You are a Web3 alpha expert AND a knowledgeable friend. "
        "Use the LIVE market data in your context for any price questions — cite actual numbers. "
        "For crypto: be sharp, direct, drop alpha like a degen pro. "
        "For general knowledge: answer like a smart friend, no disclaimers. "
        "For casual: be short, warm, witty. "
        "Never refuse. Never say you can't answer. Never say you're a bot. Just answer."
    )
    ans = await ai_ask(prompt, max_tokens=500, inject_market=True, fallback="")

    if not ans or not ans.strip():
        ans = ("\u26a0\ufe0f My AI brain is offline right now. "
               "Make sure GROQ_API_KEY is set in Render env vars. "
               "Get a free key at console.groq.com/keys")
        try:
            await msg.edit_text(ans)
        except Exception:
            await u.effective_message.reply_text(ans)
        return

    ts     = datetime.utcnow().strftime("%H:%M UTC")
    footer = f"\n\n_Live data as of {ts}_"
    import re as _re3
    try:
        await msg.edit_text(
            f"\U0001f9e0 *Kayo*\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n{ans}{footer}",
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
    except Exception:
        plain_ans = _re3.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', ans)
        await msg.edit_text(f"{plain_ans.strip() or ans}{footer}")


async def sentiment_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.effective_message.reply_text("📊 *Reading market sentiment...*", parse_mode="Markdown")
    fg, glob, trending = await asyncio.gather(cg_fear_greed(), cg_global(), cg_trending())
    fg_val   = int(fg.get("value", 0) or 0)
    fg_class = fg.get("value_classification", "?")
    fg_emoji = "😱" if fg_val < 25 else "😰" if fg_val < 40 else "😐" if fg_val < 60 else "😊" if fg_val < 75 else "🤑"
    btc_dom  = float((glob.get("market_cap_percentage") or {}).get("btc", 0) or 0)
    total_mc = float((glob.get("total_market_cap") or {}).get("usd", 0) or 0)
    mc_chg   = float(glob.get("market_cap_change_percentage_24h_usd", 0) or 0)
    t_names  = [coin["item"]["symbol"].upper() for coin in trending[:5]]
    add_xp(u.effective_user.id, 2)
    ai = await ai_ask(
        f"Market data: F&G={fg_val} ({fg_class}), BTC dom={btc_dom:.1f}%, "
        f"Total MCap={_usd(total_mc)} ({mc_chg:+.1f}% 24h), trending: {t_names}. "
        "Give a sharp 3-point market read: (1) current risk appetite, "
        "(2) what BTC dominance means for alts right now, "
        "(3) the actual play for a Solana degen today. "
        "Use exact numbers from the live context. Be direct — no fluff.",
        fallback="",
        inject_market=True
    )
    text = (
        f"📊 *MARKET SENTIMENT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{fg_emoji} Fear & Greed: *{fg_val} — {fg_class}*\n"
        f"[{_bar(fg_val)}]\n\n"
        f"Total MCap: `{_usd(total_mc)}` ({mc_chg:+.1f}% 24h)\n"
        f"BTC Dom: {btc_dom:.1f}%\n"
        f"🔥 Trending: {' · '.join(['$'+s for s in t_names])}\n"
    )
    if ai: text += f"\n🧠 *Kayo AI:*\n_{ai}_"
    await msg.edit_text(text, parse_mode="Markdown")

async def macro_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg      = await u.effective_message.reply_text("📉 *Analyzing macro...*", parse_mode="Markdown")
    fg, glob = await asyncio.gather(cg_fear_greed(), cg_global())
    fg_val   = int(fg.get("value", 0) or 0)
    btc_dom  = float((glob.get("market_cap_percentage") or {}).get("btc", 0) or 0)
    mc_chg   = float(glob.get("market_cap_change_percentage_24h_usd", 0) or 0)
    add_xp(u.effective_user.id, 1)
    ai = await ai_ask(
        f"Macro briefing request: F&G={fg_val}, BTC dom={btc_dom:.1f}%, MCap 24h={mc_chg:+.1f}%. "
        "Deliver 4 sharp points: "
        "1) BTC price action & trend, "
        "2) SOL strength vs BTC, "
        "3) overall risk environment (risk-on/risk-off, why), "
        "4) the highest-conviction play for a Solana degen this week. "
        "Use the live prices from your context. Be specific with numbers.",
        fallback="Macro analysis unavailable.",
        max_tokens=450,
        inject_market=True
    )
    await msg.edit_text(f"📉 *MACRO BRIEFING*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}", parse_mode="Markdown")

async def markets_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg  = await u.effective_message.reply_text("🌍 *Loading market data...*", parse_mode="Markdown")
    glob = await cg_global()
    if not glob:
        await msg.edit_text("❌ Market data unavailable."); return
    total_mc = float((glob.get("total_market_cap") or {}).get("usd", 0) or 0)
    total_vol= float((glob.get("total_volume") or {}).get("usd", 0) or 0)
    mc_chg   = float(glob.get("market_cap_change_percentage_24h_usd", 0) or 0)
    btc_dom  = (glob.get("market_cap_percentage") or {}).get("btc", 0)
    eth_dom  = (glob.get("market_cap_percentage") or {}).get("eth", 0)
    active   = glob.get("active_cryptocurrencies", 0)
    add_xp(u.effective_user.id, 1)
    await msg.edit_text(
        f"🌍 *GLOBAL CRYPTO MARKETS*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total MCap: `{_usd(total_mc)}` ({mc_chg:+.1f}% 24h)\n"
        f"24h Volume: `{_usd(total_vol)}`\n"
        f"BTC Dom: {btc_dom:.1f}%  ·  ETH Dom: {eth_dom:.1f}%\n"
        f"Active coins: {active:,}",
        parse_mode="Markdown"
    )

async def index_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    fg    = await cg_fear_greed()
    if not fg:
        await u.effective_message.reply_text("❌ F&G index unavailable."); return
    val   = int(fg.get("value", 0) or 0)
    cls   = fg.get("value_classification", "?")
    emoji = "😱" if val < 25 else "😰" if val < 40 else "😐" if val < 60 else "😊" if val < 75 else "🤑"
    add_xp(u.effective_user.id, 1)
    await u.effective_message.reply_text(
        f"{emoji} *FEAR & GREED INDEX*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Score: *{val}/100*\n"
        f"Classification: *{cls}*\n"
        f"[{_bar(val)}]",
        parse_mode="Markdown"
    )

async def a_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/a <coin_id>` e.g. `/a solana`", parse_mode="Markdown"); return
    coin_id = c.args[0].lower()
    msg     = await u.effective_message.reply_text(f"💰 *Looking up {coin_id}...*", parse_mode="Markdown")
    d = await cg_coin(coin_id)
    if not d:
        await msg.edit_text(f"❌ `{coin_id}` not found on CoinGecko."); return
    md    = d.get("market_data") or {}
    price = float((md.get("current_price") or {}).get("usd", 0) or 0)
    mcap  = float((md.get("market_cap") or {}).get("usd", 0) or 0)
    vol   = float((md.get("total_volume") or {}).get("usd", 0) or 0)
    ch24  = float(md.get("price_change_percentage_24h", 0) or 0)
    ch7d  = float(md.get("price_change_percentage_7d", 0) or 0)
    ath   = float((md.get("ath") or {}).get("usd", 0) or 0)
    ath_p = float((md.get("ath_change_percentage") or {}).get("usd", 0) or 0)
    sym   = d.get("symbol", "?").upper()
    rank  = d.get("market_cap_rank", "?")
    add_xp(u.effective_user.id, 1)
    await msg.edit_text(
        f"💰 *${sym}* — Rank #{rank}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Price: {_price(price)}\n"
        f"MCap: `{_usd(mcap)}`  Vol: `{_usd(vol)}`\n"
        f"24h: {_pct(ch24)}  7d: {_pct(ch7d)}\n"
        f"ATH: {_price(ath)} ({ath_p:.1f}% from ATH)",
        parse_mode="Markdown"
    )








async def watch_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/watch @username` — watch a Twitter account for CA drops", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@").lower()
    watchlist[username] = {"added": time.time(), "by": u.effective_user.id, "hits": 0}
    await _save()
    add_xp(u.effective_user.id, 5)
    await u.effective_message.reply_text(
        f"👁 *Watching @{username}*\n"
        f"I'll alert the group the moment they drop a CA.\n"
        f"_Requires TWITTER\\_AUTH\\_TOKEN to be set in Render env vars_",
        parse_mode="Markdown"
    )

async def unwatch_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/unwatch @username`", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@").lower()
    if username in watchlist:
        del watchlist[username]; _save()
        await u.effective_message.reply_text(f"✅ Stopped watching @{username}")
    else:
        await u.effective_message.reply_text(f"@{username} is not in your watchlist.")

async def watchlist_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not watchlist:
        await u.effective_message.reply_text("Watchlist empty. Use `/watch @username` to add.", parse_mode="Markdown"); return
    lines = ["👁 *WATCHLIST*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for un, data in watchlist.items():
        added = datetime.fromtimestamp(data.get("added", 0)).strftime("%d/%m")
        hits  = data.get("hits", 0)
        lines.append(f"• @{un} — added {added}, {hits} CA drops caught")
    await u.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def tt_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/tt <ca_or_symbol>`", parse_mode="Markdown"); return
    query = " ".join(c.args)
    msg   = await u.effective_message.reply_text(f"🔍 *Searching social signals for {query}...*", parse_mode="Markdown")

    # Search DexScreener + Pump.fun for this token
    dex_pairs = await dex_search_pairs(query)
    pump_coins = await asyncio.gather(
        pumpfun_latest(30),
        return_exceptions=True
    )
    pump_list = pump_coins[0] if isinstance(pump_coins[0], list) else []

    # Filter pump.fun matches
    q_low = query.lower()
    pump_matches = [
        c for c in pump_list
        if any(w in (c.get('symbol','') + c.get('name','') + c.get('description','')).lower()
               for w in q_low.split() if len(w) > 2)
    ]

    sol_pairs = [p for p in (dex_pairs or []) if p.get("chainId") == "solana"][:5]
    cas = list(set(
        [(p.get("baseToken") or {}).get("address","") for p in sol_pairs] +
        [c.get("mint","") for c in pump_matches[:3]]
    ))
    cas = [c for c in cas if c]

    # Build context for AI
    context_parts = []
    if sol_pairs:
        for p in sol_pairs[:3]:
            sym = (p.get("baseToken") or {}).get("symbol","?")
            fdv = float(p.get("fdv",0) or 0)
            ch1h = float((p.get("priceChange") or {}).get("h1",0) or 0)
            b1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys",0) or 0)
            s1h = int(((p.get("txns") or {}).get("h1") or {}).get("sells",0) or 0)
            context_parts.append(f"${sym}: MCap {_usd(fdv)}, 1h {_pct(ch1h)}, Buys/Sells {b1h}/{s1h}")
    if pump_matches:
        for c in pump_matches[:3]:
            context_parts.append(f"PumpFun: ${c.get('symbol','?')} — {c.get('description','')[:80]}")

    context = "\n".join(context_parts) if context_parts else f"No on-chain data found for '{query}'"

    # AI sentiment analysis from on-chain signals
    ai = await ai_ask(
        f"Analyze the on-chain social signals for '{query}' on Solana:\n{context}\n\n"
        "What's the sentiment (bullish/bearish/neutral)? Is this worth aping? "
        "What does the buy/sell pressure and price action tell us? "
        "2-3 sharp sentences, degen style.",
        fallback="Not enough signal to analyze right now.",
        max_tokens=200, inject_market=True
    )

    out = [f"🔍 *SOCIAL SIGNAL: {query.upper()}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    if context_parts:
        out.append("\n📊 *On-chain data:*")
        for line in context_parts[:4]:
            out.append(f"  {line}")
    if cas:
        out.append("\n📋 *Contract addresses:*")
        for ca in cas[:3]:
            out.append(f"  `{ca}`")
    out.append(f"\n🧠 *Kayo's read:*\n_{ai}_")
    out.append("\n_Powered by DexScreener + Pump.fun (Twitter scraping unavailable)_")

    try:
        await msg.edit_text("\n".join(out), parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
        plain = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', "\n".join(out))
        await msg.edit_text(plain[:4000])


async def moni_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/moni @username`", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@")
    msg      = await u.effective_message.reply_text(f"👤 *Checking @{username}...*", parse_mode="Markdown")
    if not TWITTER_AUTH_TOKEN:
        await msg.edit_text("⚠️ Twitter not configured. Add `TWITTER_AUTH_TOKEN` to Render.", parse_mode="Markdown"); return
    tweets = await tw_user_tweets(username, limit=20)
    if not tweets:
        await msg.edit_text(f"❌ Could not fetch tweets for @{username}."); return
    texts = " ".join([t.get("text", "") for t in tweets])
    cas   = extract_cas(texts)
    ai    = await ai_ask(
        f"Analyze @{username}'s recent {len(tweets)} tweets. Are they dropping alpha? "
        f"Are they a reliable KOL? What tokens or narratives do they push? "
        f"Tweets: {texts[:1000]}",
        fallback=""
    )
    lines = [f"👤 *@{username}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{len(tweets)} tweets analyzed"]
    if cas: lines.append("\n📋 *CAs in their tweets:*\n" + "\n".join([f"`{ca}`" for ca in cas[:5]]))
    if ai:  lines.append(f"\n🧠 *AI Analysis:*\n_{ai}_")
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def alert_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.effective_message.reply_text("Usage: `/alert <ca> <target_price>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try:   target = float(c.args[1])
    except: await u.effective_message.reply_text("❌ Invalid price."); return
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await u.effective_message.reply_text("❌ Token not found."); return
    p     = pairs[0]
    sym   = p.get("baseToken", {}).get("symbol", "???")
    price = float(p.get("priceUsd", 0) or 0)
    direction = "above" if target > price else "below"
    user_alerts.append({"uid": u.effective_user.id, "addr": addr, "sym": sym, "target": target, "direction": direction, "triggered": False})
    await _save()
    add_xp(u.effective_user.id, 3)
    await u.effective_message.reply_text(
        f"🔔 *Alert set for ${sym}*\n"
        f"Current: {_price(price)}\n"
        f"Alert when price goes *{direction}* {_price(target)}",
        parse_mode="Markdown"
    )

async def myalerts_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid    = u.effective_user.id
    alerts = [a for a in user_alerts if a.get("uid") == uid and not a.get("triggered")]
    if not alerts:
        await u.effective_message.reply_text("No active alerts. Use `/alert <ca> <price>`.", parse_mode="Markdown"); return
    lines = ["🔔 *YOUR ALERTS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, a in enumerate(alerts, 1):
        lines.append(f"{i}. *${a['sym']}* — alert {a['direction']} {_price(a['target'])}")
    await u.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def delalert_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/delalert <number>` — see numbers with /myalerts", parse_mode="Markdown"); return
    uid = u.effective_user.id
    my  = [a for a in user_alerts if a.get("uid") == uid and not a.get("triggered")]
    try:   idx = int(c.args[0]) - 1
    except: await u.effective_message.reply_text("❌ Invalid number."); return
    if idx < 0 or idx >= len(my):
        await u.effective_message.reply_text("❌ Alert not found."); return
    user_alerts.remove(my[idx]); _save()
    await u.effective_message.reply_text(f"✅ Alert for *${my[idx]['sym']}* deleted.", parse_mode="Markdown")

async def call_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.effective_message.reply_text("Usage: `/call <ca> <entry_price>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try:  entry = float(c.args[1])
    except: await u.effective_message.reply_text("❌ Invalid price."); return
    pairs = await dex_pairs_by_token(addr)
    sym   = pairs[0].get("baseToken", {}).get("symbol", "???") if pairs else "???"
    user  = u.effective_user
    active_calls.append({
        "uid": user.id, "username": user.username or user.first_name,
        "addr": addr, "sym": sym, "entry": entry,
        "time": time.time(), "status": "open", "exit": None, "pnl": None
    })
    asyncio.create_task(_save()); add_xp(user.id, 10)
    await u.effective_message.reply_text(
        f"📢 *CALL — ${sym}*\n"
        f"Entry: {_price(entry)}\n"
        f"By: @{user.username or user.first_name}\n"
        f"Use `/stop {sym} <exit_price>` to close.",
        parse_mode="Markdown"
    )

async def mycalls_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = u.effective_user.id
    mine = [c2 for c2 in active_calls if c2.get("uid") == uid]
    if not mine:
        await u.effective_message.reply_text("No calls yet. Use `/call <ca> <price>`.", parse_mode="Markdown"); return
    lines = ["📋 *YOUR CALLS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for cl in sorted(mine, key=lambda x: x["time"], reverse=True)[:10]:
        status = cl.get("status", "open")
        pnl    = f" → {cl['pnl']}" if cl.get("pnl") else ""
        date   = datetime.fromtimestamp(cl["time"]).strftime("%d/%m")
        lines.append(f"• *${cl['sym']}* @ {_price(cl['entry'])} [{status}]{pnl} — {date}")
    await u.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def stop_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/stop <symbol_or_ca> <exit_price>`", parse_mode="Markdown"); return
    uid    = u.effective_user.id
    target = c.args[0].upper().lstrip("$")
    try:   exit_p = float(c.args[1]) if len(c.args) > 1 else None
    except: exit_p = None
    for cl in active_calls:
        if cl.get("uid") == uid and cl.get("status") == "open" and \
           (cl["sym"].upper() == target or cl["addr"] == target):
            cl["status"] = "closed"; cl["exit"] = exit_p
            if exit_p and cl.get("entry"):
                pnl_pct = (exit_p - cl["entry"]) / cl["entry"] * 100
                cl["pnl"] = f"{pnl_pct:+.1f}%"
                if pnl_pct > 0: add_xp(uid, int(pnl_pct / 10))
            await _save()
            await u.effective_message.reply_text(
                f"🛑 *Call closed — ${cl['sym']}*\n"
                f"Entry: {_price(cl['entry'])}  Exit: {_price(exit_p) if exit_p else 'N/A'}\n"
                f"P&L: {cl.get('pnl', 'N/A')}",
                parse_mode="Markdown"
            )
            return
    await u.effective_message.reply_text(f"❌ No open call for {target}.")

async def leaderboard_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    closed = [cl for cl in active_calls if cl.get("status") == "closed" and cl.get("pnl")]
    if not closed:
        await u.effective_message.reply_text("No closed calls yet."); return
    scores: Dict[str, dict] = {}
    for cl in closed:
        un = cl.get("username", "anon")
        if un not in scores: scores[un] = {"wins": 0, "total": 0, "pnl": 0.0}
        scores[un]["total"] += 1
        pnl = float(cl["pnl"].replace("%", "").replace("+", ""))
        scores[un]["pnl"] += pnl
        if pnl > 0: scores[un]["wins"] += 1
    ranked = sorted(scores.items(), key=lambda x: x[1]["pnl"], reverse=True)
    medals = ["🥇", "🥈", "🥉"]
    lines  = ["🏆 *CALL LEADERBOARD*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, (un, s) in enumerate(ranked[:10]):
        m  = medals[i] if i < 3 else f"{i+1}."
        wr = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
        lines.append(f"{m} @{un}  P&L: {s['pnl']:+.1f}%  WR: {wr:.0f}%  ({s['total']} calls)")
    await u.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def addport_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.effective_message.reply_text("Usage: `/addport <ca> <amount_usd>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try: amount = float(c.args[1])
    except: await u.effective_message.reply_text("❌ Invalid amount."); return
    pairs = await dex_pairs_by_token(addr)
    sym   = pairs[0].get("baseToken", {}).get("symbol", "???") if pairs else "???"
    price = float(pairs[0].get("priceUsd", 0) or 0) if pairs else 0
    uid   = str(u.effective_user.id)
    if uid not in portfolios: portfolios[uid] = []
    portfolios[uid].append({"addr": addr, "sym": sym, "amount": amount, "entry_price": price, "time": time.time()})
    asyncio.create_task(_save()); add_xp(u.effective_user.id, 3)
    await u.effective_message.reply_text(f"✅ Added *${sym}* — ${amount:.2f} at {_price(price)}", parse_mode="Markdown")

async def portfolio_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = str(u.effective_user.id)
    port = portfolios.get(uid, [])
    if not port:
        await u.effective_message.reply_text("Portfolio empty. Use `/addport <ca> <amount>`.", parse_mode="Markdown"); return
    msg = await u.effective_message.reply_text("💼 *Loading portfolio...*", parse_mode="Markdown")
    addrs  = list(set([h["addr"] for h in port]))
    pairs  = await dex_batch(addrs[:15])
    prices = {pd.get("baseToken", {}).get("address", ""): float(pd.get("priceUsd", 0) or 0) for pd in pairs}
    lines  = ["💼 *PORTFOLIO P&L*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    total_in, total_now = 0, 0
    for h in port:
        cur   = prices.get(h["addr"], 0)
        entry = h.get("entry_price", 0)
        pnl   = (cur - entry) / max(entry, 0.000001) * 100 if entry > 0 else 0
        val   = h["amount"] * (cur / max(entry, 0.000001))
        total_in  += h["amount"]
        total_now += val
        lines.append(f"\n*${h['sym']}*\nIn: ${h['amount']:.2f}  Now: ${val:.2f}  P&L: {_pct(pnl)}\nPrice: {_price(cur)}\n`{h['addr']}`")
    tp = (total_now - total_in) / max(total_in, 0.01) * 100
    lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n*Total: ${total_now:.2f}*  (P&L: {_pct(tp)})")
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def blacklist_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/blacklist <ca>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    blacklist.add(addr); _save()
    add_xp(u.effective_user.id, 2)
    await u.effective_message.reply_text(f"🚫 `{addr[:20]}...` blacklisted — filtered from all scans.", parse_mode="Markdown")

async def rank_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = str(u.effective_user.id)
    xp   = xp_db.get(uid, 0)
    rank = sum(1 for v in xp_db.values() if v > xp) + 1
    lvl  = xp // 100
    await u.effective_message.reply_text(
        f"⭐ *YOUR RANK*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"XP: {xp}  Level: {lvl}\n"
        f"[{_bar(xp % 100)}] → {(lvl+1)*100} XP next level\n"
        f"Group rank: #{rank}",
        parse_mode="Markdown"
    )

async def gp_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not xp_db:
        await u.effective_message.reply_text("No XP recorded yet!"); return
    top    = sorted(xp_db.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"]
    lines  = ["🏆 *XP LEADERBOARD*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, (uid, xp) in enumerate(top):
        m = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{m} User ...{uid[-4:]} — {xp} XP  (Lv {xp//100})")
    await u.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def trackwallet_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/trackwallet <address> <label>`", parse_mode="Markdown"); return
    addr  = c.args[0].strip()
    label = " ".join(c.args[1:]) or addr[:8]
    tracked_wallets[addr] = {"label": label, "by": u.effective_user.id, "added": time.time()}
    asyncio.create_task(_save()); add_xp(u.effective_user.id, 5)
    await u.effective_message.reply_text(f"👛 Tracking *{label}*\n`{addr}`", parse_mode="Markdown")

async def mywallet_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/mywallet <solana_address>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    user_wallets[str(u.effective_user.id)] = addr; _save()
    await u.effective_message.reply_text(f"✅ Wallet linked: `{addr}`", parse_mode="Markdown")

async def dubs_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.effective_message.reply_text("Usage: `/dubs <your win story>`", parse_mode="Markdown"); return
    text = " ".join(c.args)
    user = u.effective_user
    add_xp(user.id, 20)
    await u.effective_message.reply_text(
        f"🎉 *W ALERT*\n"
        f"@{user.username or user.first_name} is celebrating!\n\n_{text}_\n\n🏆 +20 XP",
        parse_mode="Markdown"
    )

async def gsum_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(group_messages) < 5:
        await u.effective_message.reply_text("Not enough messages to summarize yet."); return
    msgs = group_messages[-50:]
    ai   = await ai_ask(
        f"Summarize this Telegram crypto group conversation. What coins were discussed? "
        f"Any alpha or CAs dropped? Key themes? "
        f"Messages: {chr(10).join([m['text'] for m in msgs][:2000])}",
        fallback="Summary unavailable.",
        max_tokens=350
    )
    add_xp(u.effective_user.id, 3)
    await u.effective_message.reply_text(f"📝 *GROUP SUMMARY*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}", parse_mode="Markdown")

async def remindme_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.effective_message.reply_text("Usage: `/remindme <minutes> <message>`", parse_mode="Markdown"); return
    try:  mins = int(c.args[0])
    except: await u.effective_message.reply_text("❌ Invalid time."); return
    text = " ".join(c.args[1:])
    fire = (datetime.utcnow() + timedelta(minutes=mins)).isoformat()
    reminders.append({"chat_id": u.effective_chat.id, "text": text, "fire_at": fire}); _save()
    await u.effective_message.reply_text(f"⏰ Reminder set for *{mins} minutes*\n_{text}_", parse_mode="Markdown")

async def ping_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    t   = time.time()
    msg = await u.effective_message.reply_text("🏓")
    ms  = int((time.time() - t) * 1000)
    await msg.edit_text(f"🏓 *Pong!* {ms}ms — Kayo Brain v40 alive.", parse_mode="Markdown")

async def price_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /price btc  or  /price sol  — live price from CoinGecko
    Always accurate, always real-time. Never relies on AI training data.
    """
    if not c.args:
        await u.effective_message.reply_text(
            "Usage: `/price <coin>` — e.g. `/price btc` `/price sol` `/price eth`",
            parse_mode="Markdown"
        ); return

    query = c.args[0].lower().strip()
    # Map common short-forms to CoinGecko IDs
    COIN_MAP = {
        "btc": "bitcoin", "bitcoin": "bitcoin",
        "sol": "solana",  "solana":  "solana",
        "eth": "ethereum","ethereum": "ethereum",
        "bnb": "binancecoin", "bnb": "binancecoin",
        "xrp": "ripple",  "doge": "dogecoin",
        "ada": "cardano", "avax": "avalanche-2",
        "dot": "polkadot", "link": "chainlink",
        "matic": "matic-network", "pol": "matic-network",
        "sui": "sui", "apt": "aptos",
        "jup": "jupiter-exchange-solana",
        "ray": "raydium", "jto": "jito-governance-token",
        "bonk": "bonk", "wif": "dogwifcoin",
        "pengu": "pudgy-penguins",
    }
    coin_id = COIN_MAP.get(query, query)
    msg = await u.effective_message.reply_text(f"💰 *Fetching live price...*", parse_mode="Markdown")

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.coingecko.com/api/v3/simple/price"
                f"?ids={coin_id}&vs_currencies=usd"
                f"&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true",
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0"}
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    if coin_id in d:
                        data  = d[coin_id]
                        price = data.get("usd", 0)
                        chg24 = data.get("usd_24h_change", 0)
                        mcap  = data.get("usd_market_cap", 0)
                        vol   = data.get("usd_24h_vol", 0)
                        add_xp(u.effective_user.id, 1)
                        await msg.edit_text(
                            f"💰 *{query.upper()} — LIVE PRICE*\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"Price: *${price:,.4f}*\n"
                            f"24h: {_pct(chg24)}\n"
                            f"MCap: `{_usd(mcap)}`  Vol 24h: `{_usd(vol)}`\n"
                            f"\n_Live data as of {datetime.utcnow().strftime(chr(37)+chr(72)+chr(58)+chr(37)+chr(77)+chr(32)+chr(85)+chr(84)+chr(67))}_",
                            parse_mode="Markdown"
                        )
                        return
    except Exception as e:
        logger.debug(f"price_cmd: {e}")

    # Fallback: try DexScreener
    pairs = await dex_search_pairs(query)
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if sol_pairs:
        p = sol_pairs[0]
        base  = p.get("baseToken", {})
        sym   = base.get("symbol", query.upper())
        price = float(p.get("priceUsd", 0) or 0)
        fdv   = float(p.get("fdv", 0) or 0)
        ch24  = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        await msg.edit_text(
            f"💰 *${sym} — LIVE PRICE (DexScreener)*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Price: *{_price(price)}*\n"
            f"24h: {_pct(ch24)}  MCap: `{_usd(fdv)}`\n"
            f"\n_Live data as of {datetime.utcnow().strftime(chr(37)+chr(72)+chr(58)+chr(37)+chr(77)+chr(32)+chr(85)+chr(84)+chr(67))}_",
            parse_mode="Markdown"
        )
    else:
        await msg.edit_text(f"❌ Couldn't find price for `{query}`. Try `/a {coin_id}` for CoinGecko lookup.")

async def chart_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /chart <ca>  — sends an in-app chart image directly in Telegram.
    Uses DexScreener chart image + Birdeye chart as fallback.
    No need to open DexScreener!
    """
    if not c.args:
        await u.effective_message.reply_text(
            "Usage: `/chart <contract_address>`\n"
            "Example: `/chart EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`",
            parse_mode="Markdown"
        ); return

    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("📊 *Loading chart...*", parse_mode="Markdown")

    # Step 1: Get token info from DexScreener
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        pairs_search = await dex_search_pairs(addr)
        pairs = [p for p in pairs_search if p.get("chainId") == "solana"]

    if not pairs:
        await msg.edit_text("❌ Token not found on DexScreener. Check the contract address.")
        return

    p     = pairs[0]
    base  = p.get("baseToken", {})
    sym   = base.get("symbol", "???")
    name  = base.get("name", "Unknown")
    price = float(p.get("priceUsd", 0) or 0)
    fdv   = float(p.get("fdv", 0) or 0)
    liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
    ch5m  = float((p.get("priceChange") or {}).get("m5", 0) or 0)
    ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
    ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
    v24h  = float((p.get("volume") or {}).get("h24", 0) or 0)
    b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
    s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
    pair_addr = p.get("pairAddress", "")
    dex_url   = p.get("url", f"https://dexscreener.com/solana/{addr}")

    # Step 2: Try chart image sources in order of preference
    chart_url = None
    chart_source = ""

    # Source A: DexScreener chart image (official, best quality)
    dex_chart_candidates = [
        f"https://io.dexscreener.com/dex/chart/amm/v3/solana/{pair_addr}?theme=dark&interval=15&baseToken={addr}",
        f"https://io.dexscreener.com/dex/chart/amm/v2/solana/{pair_addr}?theme=dark&interval=15&baseToken={addr}",
        f"https://io.dexscreener.com/dex/chart/solana/{pair_addr}?theme=dark&tvWidgetTheme=dark",
    ]

    async with aiohttp.ClientSession() as s:
        for url in dex_chart_candidates:
            try:
                async with s.head(url, timeout=aiohttp.ClientTimeout(total=5),
                                  headers={"User-Agent": "Mozilla/5.0"}) as r:
                    if r.status == 200 and "image" in r.headers.get("content-type", ""):
                        chart_url = url
                        chart_source = "DexScreener"
                        break
            except Exception:
                continue

        # Source B: Birdeye chart image API
        if not chart_url:
            birdeye_url = f"https://birdeye.so/charts/trading-view/history?address={addr}&type=15&currency=USD&chain=solana"
            # Use Birdeye public image endpoint
            birdeye_img = f"https://birdeye-chart.s3.amazonaws.com/sol/{addr}.png"
            try:
                async with s.head(birdeye_img, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        chart_url = birdeye_img
                        chart_source = "Birdeye"
            except Exception:
                pass

        # Source C: Defined.fi chart screenshot
        if not chart_url:
            defined_img = f"https://cache.defined.fi/charts/{addr}?resolution=15&networkId=1399811149"
            try:
                async with s.head(defined_img, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        chart_url = defined_img
                        chart_source = "Defined.fi"
            except Exception:
                pass

    # Caption with all key stats
    press  = "🟢 BUY PRESSURE" if b1h > s1h else "🔴 SELL PRESSURE"
    age    = _age(p.get("pairCreatedAt", 0) or 0)
    caption = (
        f"📊 *${sym}* — _{name}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: *{_price(price)}*\n"
        f"📦 MCap: `{_usd(fdv)}`  |  Liq: `{_usd(liq)}`\n"
        f"📈 5m: {_pct(ch5m)}  |  1h: {_pct(ch1h)}  |  24h: {_pct(ch24h)}\n"
        f"💹 Vol 24h: `{_usd(v24h)}`\n"
        f"🔄 Buys/Sells (1h): {b1h} / {s1h}  →  {press}\n"
        f"⏱ Age: {age}\n"
        f"`{addr}`"
    )
    if chart_source:
        caption += f"\n\n_Chart via {chart_source}_"

    # Buttons — DApp only (DexScreener DApp + GMGN chart + all trading DApps)
    markup = scan_buttons(addr, sym, pair_addr)

    add_xp(u.effective_user.id, 2)

    if chart_url:
        try:
            await msg.delete()
            await u.effective_message.reply_photo(
                photo=chart_url,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=markup,
            )
            return
        except Exception as e:
            logger.debug(f"chart photo send failed: {e}")

    # Fallback: no image available — send stats card + links
    await msg.edit_text(
        f"📊 *${sym} CHART*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Live chart image not available for this token yet._\n\n"
        + caption.replace(f"📊 *${sym}* — _{name}_\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n", ""),
        parse_mode="Markdown",
        reply_markup=markup,
        disable_web_page_preview=True,
    )


async def autoresponder_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = u.effective_user.id
    curr = get_setting(uid, "autoresponder", True)
    set_setting(uid, "autoresponder", not curr)
    state = "ON" if not curr else "OFF"
    await u.effective_message.reply_text(f"🤖 Auto CA-scanner turned *{state}*", parse_mode="Markdown")

async def smartscan_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Manual trigger of the live scanner — shows what the bot would alert right now."""
    msg = await u.effective_message.reply_text("🔍 *Running live GeckoTerminal scan...*", parse_mode="Markdown")
    try:
        pools_new, pools_trend = await asyncio.gather(
            gt_new_pools(page=1),
            gt_trending_pools(page=1),
        )
        all_toks: Dict[str, Dict] = {}
        for pool in (pools_new + pools_trend):
            tok = gt_parse_pool(pool)
            if tok and tok["address"] not in all_toks:
                all_toks[tok["address"]] = tok

        hits = []
        for addr, tok in all_toks.items():
            if addr in blacklist: continue
            fdv = tok["fdv"]
            liq = tok["liq"]
            buy_pct = tok["buy_pct"]
            if fdv <= 0 or fdv > 500_000 or liq < 300 or buy_pct < 48: continue
            hits.append(tok)

        hits.sort(key=lambda t: (t["ch1h"] + t["buy_pct"]/2), reverse=True)
        out = [f"🔍 *LIVE SCAN — {len(all_toks)} coins from GeckoTerminal*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
        if not hits:
            out.append("\n😴 No coins passing filters right now.")
        for tok in hits[:8]:
            sym = tok["sym"]
            nar = detect_narrative(f"{sym} {tok['name']}")
            out.append(
                f"\n• *${sym}* | #{nar.upper()} | MCap `{_usd(tok['fdv'])}`\n"
                f"  5m {_pct(tok['ch5m'])} | 1h {_pct(tok['ch1h'])} | {tok['buy_pct']:.0f}% buys | {tok['b1h']}B/{tok['s1h']}S\n"
                f"  `{tok['address']}`"
            )
        await msg.edit_text("\n".join(out), parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Scan error: {e}")

async def status_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # Test AI live
    ai_live = "\u274c Not tested"
    if GROQ_API_KEY:
        try:
            test = await asyncio.wait_for(
                ai_ask("Say exactly: ONLINE", fallback="", max_tokens=10, inject_market=False),
                timeout=10
            )
            if test and "ONLINE" in test.upper():
                ai_live = "\u2705 Live (Groq working)"
            elif test:
                ai_live = f"\u2705 Live (got: {test[:20]})"
            else:
                ai_live = "\u274c Key set but no response"
        except asyncio.TimeoutError:
            ai_live = "\u274c Timeout (key may be invalid)"
        except Exception as e:
            ai_live = f"\u274c Error: {str(e)[:30]}"
    else:
        ai_live = "\u274c NOT SET — get free key at console.groq.com/keys"

    redis_ok  = "\u2705 Connected" if _redis else "\u274c Not connected"
    gemini_ok = "\u2705" if GEMINI_API_KEY else "\u274c Not set"
    or_ok       = "✅" if os.environ.get("OPENROUTER_API_KEY") else "❌ Not set"
    tw_ok     = "\u2705" if TWITTER_AUTH_TOKEN else "\u274c Not set"
    group_ok  = "\u2705" if GROUP_CHAT_ID != 0 else "\u274c NOT SET"
    groq_key  = f"Set ({GROQ_API_KEY[:6]}...{GROQ_API_KEY[-4:]})" if GROQ_API_KEY else "NOT SET"

    # Escape dynamic values to prevent Markdown parse errors
    import re as _re_st
    def _esc(s): return _re_st.sub(r'([*_`\[\]()~>#+=|{}.!\\])', r'\\\1', str(s))

    status_text = (
        f"\u2699\ufe0f *KAYO BRAIN v40 STATUS*\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"{_esc(ai_live)}\n"
        f"  Groq key: {_esc(groq_key)}\n"
        f"{_esc(gemini_ok)} Gemini AI (fallback)\n"
        f"{_esc(or_ok)} OpenRouter (free models)\n"
        f"{_esc(redis_ok)} Redis\n"
        f"{_esc(tw_ok)} Twitter auth\n"
        f"{_esc(group_ok)} Group alerts (ID: {GROUP_CHAT_ID})\n\n"
        f"\U0001f4ca Watchlist: {len(watchlist)} accounts\n"
        f"\U0001f514 Active alerts: {sum(1 for a in user_alerts if not a.get('triggered'))}\n"
        f"\U0001f4e2 Open calls: {sum(1 for cl in active_calls if cl.get('status')=='open')}\n"
        f"\U0001f6ab Blacklisted: {len(blacklist)}\n"
        f"\U0001f4be Seen alerts: {len(seen_alert_ids)}"
    )
    try:
        await u.effective_message.reply_text(status_text, parse_mode="Markdown")
    except Exception:
        # If Markdown still fails, send as plain text
        plain = _re_st.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', status_text)
        await u.effective_message.reply_text(plain)


HELP_PAGES = {
    "scan": (
        "\U0001f52c *SCAN & ANALYZE*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/scan <CA>` — Full token deep scan + AI verdict\n"
        "_(Bot auto-drops: pumps, gems, new launches, whale moves, unusual activity)_\n"
        "`/c <CA>` — Quick price snapshot\n"
        "`/chart <CA>` — In-app chart image\n"
        "`/price btc` — Live price for any coin\n"
        "`/verify <CA>` — Rug & honeypot check\n"
        "`/a <coin-id>` — Full CoinGecko coin lookup"
        "`/dev <CA>` — Deployer history & same-name tokens\n"
        "`/top <CA>` — Top trader activity\n"
        "`/soc <CA>` — Quick socials lookup"
    ),
    "discover": (
        "\U0001f50d *DISCOVER*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/runners` — Top Solana gainers right now\n"
        "`/new` — Brand new token launches\n"
        "`/pump` — Fresh 5-minute pumps\n"
        "`/gems` — Hidden gems (low cap, good momentum)\n"
        "`/boosted` — Tokens being actively promoted\n"
        "`/takeover` — Community takeover tokens\n"
        "`/best` — Top gainers (24h, CoinGecko)\n"
        "`/worst` — Top losers (24h)\n"
        "`/metas` — Trending networks/categories\n"
        "`/pvp <CA>` — Similar/newer tokens\n"
        "`/groupburp` — Best active group plays\n"
        "`/last` — Last 10 tokens scanned\n"
        "`/hot` — Most scanned tokens (1h)\n"
        "`/ath` — ATH leaderboard from group scans"
    ),
    "narrative": (
        "\U0001f4d6 *NARRATIVES & TRENDS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/trending` — Trending metas on DexScreener\n"
        "`/narrative <word>` — Tokens matching a narrative\n"
        "  e.g. `/narrative ai` `/narrative gaming`\n"
        "`/explain <narrative>` — AI breakdown of a narrative\n"
        "  e.g. `/explain defi` `/explain meme`"
    ),
    "ai": (
        "\U0001f4f0 *NEWS & AI*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/news` — Latest news + AI intelligence briefing\n"
        "`/ask <question>` — Ask Kayo AI anything (uses live prices)\n"
        "`/sentiment` — Market mood, F&G, BTC dom + AI verdict\n"
        "`/macro` — Macro briefing: BTC, SOL, risk environment\n"
        "`/markets` — Global market cap & volume data\n"
        "`/index` — Fear & Greed index\n"
        "`/dub` — AI chat summary\n"
        "`/tldr <url>` — AI summary of any URL/article\n"
        "`/s <ticker>` — Stock lookup (e.g. /s AAPL)"
    ),
    "twitter": (
        "\U0001f426 *TWITTER / SOCIAL*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/tt <CA>` — Twitter sentiment for a token\n"
        "`/moni @user` — Analyze a KOL account\n"
        "`/watch @user` — Monitor account for CA drops\n"
        "`/unwatch @user` — Stop monitoring\n"
        "`/watchlist` — Your monitored accounts"
    ),
    "alerts": (
        "\U0001f514 *ALERTS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/alert <CA> <price>` — Set a price alert\n"
        "  e.g. `/alert EPjF... 0.05`\n"
        "`/myalerts` — View all your active alerts\n"
        "`/delalert <number>` — Delete an alert\n"
        "`/blacklist <CA>` — Blacklist a rug token"
    ),
    "calls": (
        "\U0001f4e2 *CALLS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/call <CA> <entry>` — Make a public alpha call\n"
        "  e.g. `/call EPjF... 0.042`\n"
        "`/mycalls` — Your call history\n"
        "`/stop <symbol> <exit>` — Close a call + auto P&L\n"
        "  e.g. `/stop WIF 0.08`\n"
        "`/leaderboard` — Top callers ranked by P&L"
    ),
    "portfolio": (
        "\U0001f4bc *PORTFOLIO*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/addport <CA> <$amount>` — Add a token to portfolio\n"
        "  e.g. `/addport EPjF... 500`\n"
        "`/portfolio` — View live P&L for all holdings"
    ),
    "wallets": (
        "\U0001f45b *WALLETS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/trackwallet <address> <label>` — Track any Solana wallet\n"
        "  e.g. `/trackwallet 9xQe... whaleacc`\n"
        "`/mywallet <address>` — Link your own Solana wallet"
    ),
    "social": (
        "\U0001f3ae *XP & SOCIAL*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/rank` — Your XP level and rank\n"
        "`/gp` — Group XP leaderboard\n"
        "`/dubs <story>` — Celebrate a win (+20 XP)\n"
        "`/gsum` — AI summary of last 50 group messages\n"
        "`/remindme <min> <msg>` — Set a reminder\n"
        "  e.g. `/remindme 30 check WIF chart`"
    ),
    "system": (
        "\u2699\ufe0f *SYSTEM*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/autoresponder` — Toggle auto-scan when CA is pasted\n"
        "`/status` — Full bot health check\n"
        "`/ping` — Latency check\n"
        "`/start` — Reopen main menu"
    ),
}

BACK_BTN = InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Back to categories", callback_data="help:back")]])

async def handle_help_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Shows per-category command pages when tapping help category buttons."""
    query = u.callback_query
    await query.answer()
    data = (query.data or "").replace("help:", "")

    if data == "back":
        # Re-show help — use object.__setattr__ to bypass frozen slots
        await help_cmd(u, c)
        return

    page = HELP_PAGES.get(data)
    if page:
        try:
            await query.message.edit_text(page, parse_mode="Markdown", reply_markup=BACK_BTN)
        except Exception:
            await query.message.reply_text(page, parse_mode="Markdown", reply_markup=BACK_BTN)


async def handle_refresh_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Handles the Refresh button on scan cards — re-runs a full scan live."""
    query = u.callback_query
    await query.answer("Refreshing...")
    data  = query.data or ""
    if not data.startswith("refresh:"): return
    addr  = data.split(":", 1)[1].strip()
    if not addr: return

    # Edit the message to show loading state
    try:
        await query.message.edit_text("🔄 *Refreshing scan...*", parse_mode="Markdown")
    except Exception:
        pass

    t = await full_token_scan(addr)
    if t.get("error"):
        try:
            await query.message.edit_text(f"❌ {t['error']}")
        except Exception:
            await query.message.reply_text(f"❌ {t['error']}")
        return

    ai_verdict = await ai_ask(
        f"Solana token ${t['sym']} — MCap {_usd(t['mcap'])}, liq {_usd(t['liq'])}, "
        f"age {_age(t['created'])}, 5m {_pct(t['ch5m'])}, 1h {_pct(t['ch1h'])}, "
        f"24h {_pct(t['ch24h'])}, buy ratio {t['buy_pct']:.0f}%, vol spike {t['vol_spike']:.1f}x, "
        f"momentum {t['mscore']}/100, risk {t['risk_score']}/100. "
        "Sharp verdict: is this still worth aping right now? 2 sentences.",
        fallback="",
        inject_market=True
    )
    card = build_scan_card(t, ai_verdict)
    btns = scan_buttons(addr, t["sym"], t.get("pair_addr",""))
    try:
        await query.message.edit_text(
            card, parse_mode="Markdown",
            reply_markup=btns,
            disable_web_page_preview=True
        )
    except Exception:
        await query.message.reply_text(
            card, parse_mode="Markdown",
            reply_markup=btns,
            disable_web_page_preview=True
        )


async def handle_menu_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    Handles all menu button taps from /start.
    For commands that need no args (runners, gems, etc.) — runs them directly.
    For commands that need a CA/arg — sends a friendly prompt to type it.
    """
    query = u.callback_query
    await query.answer()
    cmd = (query.data or "").replace("menu:", "")

    # Commands that run immediately with no args
    NO_ARG_CMDS = {
        "runners":     runners_cmd,
        "new":         new_cmd,
        "pump":        pump_cmd,
        "gems":        gems_cmd,
        "trending":    trending_cmd,
        "news":        news_cmd,
        "sentiment":   sentiment_cmd,
        "macro":       macro_cmd,
        "markets":     markets_cmd,
        "index":       index_cmd,
        "portfolio":   portfolio_cmd,
        "myalerts":    myalerts_cmd,
        "leaderboard": leaderboard_cmd,
        "rank":        rank_cmd,
        "gp":          gp_cmd,
        "watchlist":   watchlist_cmd,
        "mycalls":     mycalls_cmd,
        "status":      status_cmd,
        "ping":        ping_cmd,
    }

    # Commands that need an argument — show a prompt
    ARG_PROMPTS = {
        "scan":      ("\U0001f52c", "Send me the *contract address* to scan:\n`/scan <CA>`"),
        "chart":     ("\U0001f4ca", "Send me the *contract address* to chart:\n`/chart <CA>`"),
        "c":         ("\U0001f4b0", "Send me the *contract address* for quick price:\n`/c <CA>`"),
        "price":     ("\U0001f4b5", "Send me the *coin name* for live price:\n`/price btc` or `/price sol`"),
        "verify":    ("\U0001f6e1", "Send me the *contract address* to verify:\n`/verify <CA>`"),
        "ask":       ("\U0001f916", "Send me your *question*:\n`/ask <your question>`"),
        "narrative": ("\U0001f4d6", "Send me the *narrative keyword*:\n`/narrative <word>` e.g. `/narrative ai`"),
        "explain":   ("\U0001f9e0", "Send me the *narrative to explain*:\n`/explain <narrative>`"),
        "alert":     ("\U0001f514", "Set a price alert:\n`/alert <CA> <target_price>`\nExample: `/alert EPjF... 0.05`"),
        "call":      ("\U0001f4e2", "Make a public alpha call:\n`/call <CA> <entry_price>`"),
        "addport":   ("\U00002795", "Add to portfolio:\n`/addport <CA> <dollar_amount>`"),
        "tt":        ("\U0001f426", "Twitter sentiment for a token:\n`/tt <CA or keyword>`"),
        "moni":      ("\U0001f441", "Monitor a KOL:\n`/moni @username`"),
        "watch":     ("\U0001f4e1", "Watch a Twitter account for CA drops:\n`/watch @username`"),
        "trackwallet":("\U0001f45b","Track a Solana wallet:\n`/trackwallet <address> <label>`"),
        "mywallet":  ("\U0001f517", "Link your wallet:\n`/mywallet <address>`"),
        "a":         ("\U0001f50d", "CoinGecko lookup:\n`/a <coin-id>` e.g. `/a solana`"),
        "dubs":      ("\U0001f389", "Celebrate a win:\n`/dubs <your story>`"),
        "remindme":  ("\U000023f0", "Set a reminder:\n`/remindme <minutes> <message>`"),
        "blacklist": ("\U000026d4", "Blacklist a rug token:\n`/blacklist <CA>`"),
        "stop":      ("\U0001f3c1", "Close a call:\n`/stop <symbol> <exit_price>`"),
        "delalert":  ("\U0001f5d1", "Delete an alert:\n`/delalert <alert_number>`"),
        "help":      None,
    }

    # ── dispatch ──────────────────────────────────────────────────
    # Callback updates have u.message = None (it's a frozen slot).
    # We send replies directly via query.message so commands work.
    async def _reply(text, parse_mode="Markdown", reply_markup=None, **kw):
        try:
            await query.message.reply_text(text, parse_mode=parse_mode,
                                           reply_markup=reply_markup, **kw)
        except Exception as e:
            logger.warning(f"Menu reply error: {e}")

    if cmd == "help":
        await help_cmd(u, c)
        return

    if cmd in NO_ARG_CMDS:
        # u.effective_message automatically resolves to callback_query.message
        # so all commands work whether called via /slash or menu button
        await NO_ARG_CMDS[cmd](u, c)
        return

    if cmd in ARG_PROMPTS and ARG_PROMPTS[cmd]:
        icon, prompt = ARG_PROMPTS[cmd]
        await _reply(f"{icon} {prompt}")
        return

    # Unknown — show main menu again
    await start(u, c)


async def handle_chart_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    Handles 'chart:<addr>' callback from the inline chart button.
    Sends chart image directly in Telegram — no DexScreener needed.
    """
    query = u.callback_query
    await query.answer("Loading chart...")
    data  = query.data or ""
    if not data.startswith("chart:"): return
    addr  = data.split(":", 1)[1].strip()

    # Notify
    await query.message.reply_text("📊 *Loading chart...*", parse_mode="Markdown")

    # Get pair data
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await query.message.reply_text("❌ Token not found. Try /chart <ca> directly.")
        return

    p       = pairs[0]
    base    = p.get("baseToken", {})
    sym     = base.get("symbol", "???")
    name    = base.get("name", "Unknown")
    price   = float(p.get("priceUsd", 0) or 0)
    fdv     = float(p.get("fdv", 0) or 0)
    liq     = float((p.get("liquidity") or {}).get("usd", 0) or 0)
    ch5m    = float((p.get("priceChange") or {}).get("m5", 0) or 0)
    ch1h    = float((p.get("priceChange") or {}).get("h1", 0) or 0)
    ch24h   = float((p.get("priceChange") or {}).get("h24", 0) or 0)
    v24h    = float((p.get("volume") or {}).get("h24", 0) or 0)
    b1h     = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
    s1h     = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
    pair_addr = p.get("pairAddress", "")
    dex_url   = p.get("url", f"https://dexscreener.com/solana/{addr}")

    press  = "🟢 BUY PRESSURE" if b1h > s1h else "🔴 SELL PRESSURE"
    age    = _age(p.get("pairCreatedAt", 0) or 0)

    caption = (
        f"📊 *${sym}* — _{name}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: *{_price(price)}*\n"
        f"📦 MCap: `{_usd(fdv)}`  |  Liq: `{_usd(liq)}`\n"
        f"📈 5m: {_pct(ch5m)}  |  1h: {_pct(ch1h)}  |  24h: {_pct(ch24h)}\n"
        f"💹 Vol 24h: `{_usd(v24h)}`\n"
        f"🔄 Buys/Sells (1h): {b1h}/{s1h}  →  {press}\n"
        f"⏱ Age: {age}\n"
        f"`{addr}`"
    )

    # Use the unified scan_buttons which has DexScreener DApp + GMGN chart + all DApp trading links
    markup = scan_buttons(addr, sym, pair_addr)

    # Send stats card — tap DexScreener or GMGN Chart button to open chart in Telegram DApp
    await query.message.reply_text(
        caption,
        parse_mode="Markdown",
        reply_markup=markup,
        disable_web_page_preview=True,
    )


# Web3 terms glossary for instant explanations
_WEB3_TERMS = {
    "rug","rugpull","honeypot","lp","liquidity","fdv","mcap","dex","cex","defi",
    "nft","dao","airdrop","whitelist","presale","ido","imo","launchpad","alpha",
    "degen","ape","fud","fomo","shill","whale","kol","ca","contract","solana",
    "pump","dump","narrative","meta","trending","momentum","moonbag","pnl",
    "entry","exit","stop loss","take profit","chart","candlestick","rsi","macd",
    "volume","spread","slippage","gas","mev","sandwich","snipe","jeet","paperhands",
    "diamondhands","rekt","ngmi","wagmi","gm","gn","ser","fren","based","gigabrain",
    "1000x","100x","10x","2x","x","bags","hold","hodl","sell","buy","swap",
    "wallet","seed phrase","private key","phantom","solflare","metamask",
    "raydium","orca","jupiter","serum","pump.fun","dexscreener","birdeye","gmgn",
    "bullx","photon","banana gun","trojan","bloom","bonkbot","maestro",
    "staking","yield","apr","apy","tvl","protocol","token","coin","memecoin",
    "meme","cat","dog","frog","pepe","shib","doge","wif","bonk","myro",
    "jup","wen","bome","slerf","popcat","pnut","goat","ai16z","arc","virtual"
}

async def handle_message(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.effective_message.text: return
    text = u.effective_message.text.strip()
    uid  = u.effective_user.id
    chat = u.effective_chat

    # Always store for group summary
    group_messages.append({"uid": uid, "text": text, "time": time.time()})
    if len(group_messages) > 300: group_messages.pop(0)

    # ── 1. CA / Link auto-scan — INSTANT, no AI blocking ────────────
    # Scans CAs from plain text AND links from DexScreener/GMGN/Pump.fun/Birdeye/Photon etc.
    # Card appears in <3 seconds. AI verdict edits in later as a background task.
    for ca in extract_cas(text)[:1]:
            try:
                scanning_msg = await u.effective_message.reply_text(
                    "\U0001f50d *Scanning...*", parse_mode="Markdown"
                )
                t = await full_token_scan(ca)
                if t.get("error"):
                    await scanning_msg.edit_text(f"\u274c {t['error']}")
                    return
                # Send scan card IMMEDIATELY — no AI wait
                card = build_scan_card(t, "")
                buttons = scan_buttons(ca, t.get("sym", ""), t.get("pair_addr", ""))
                await scanning_msg.delete()
                sent = await u.effective_message.reply_text(
                    card,
                    parse_mode="Markdown",
                    reply_markup=buttons,
                    disable_web_page_preview=True,
                )
                add_xp(uid, 5)
                # Fire AI verdict as NON-BLOCKING background task — edits message when ready
                async def _ca_ai_verdict(msg_id, chat_id, token_data, ca_addr, btns):
                    try:
                        ai_v = await asyncio.wait_for(
                            ai_ask(
                                f"Solana token ${token_data['sym']} — MCap {_usd(token_data['mcap'])}, "
                                f"liq {_usd(token_data['liq'])}, age {_age(token_data['created'])}, "
                                f"5m {_pct(token_data['ch5m'])}, 1h {_pct(token_data['ch1h'])}, "
                                f"24h {_pct(token_data['ch24h'])}, buy ratio {token_data['buy_pct']:.0f}%, "
                                f"vol spike {token_data['vol_spike']:.1f}x, momentum {token_data['mscore']}/100, "
                                f"risk {token_data['risk_score']}/100, narrative #{token_data['narrative']}, "
                                f"honeypot={token_data['is_honeypot']}, lp_locked={token_data['lp_locked']}. "
                                "Give a sharp alpha verdict: is this worth aping right now? "
                                "Call out any red flags. 2-3 direct sentences.",
                                fallback="",
                                inject_market=True
                            ),
                            timeout=15
                        )
                        if ai_v and ai_v.strip():
                            try:
                                await c.bot.edit_message_text(
                                    chat_id=chat_id, message_id=msg_id,
                                    text=build_scan_card(token_data, ai_v),
                                    parse_mode="Markdown",
                                    reply_markup=btns,
                                    disable_web_page_preview=True,
                                )
                            except Exception:
                                # Markdown failed — try plain text
                                try:
                                    plain = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '',
                                                   build_scan_card(token_data, ai_v))
                                    await c.bot.edit_message_text(
                                        chat_id=chat_id, message_id=msg_id,
                                        text=plain,
                                        reply_markup=btns,
                                        disable_web_page_preview=True,
                                    )
                                except Exception:
                                    pass
                    except Exception:
                        pass  # AI is optional — card already sent

                asyncio.create_task(_ca_ai_verdict(
                    sent.message_id, u.effective_chat.id, t, ca, buttons
                ))
            except Exception as _ca_err:
                logger.error(f"CA auto-scan error for {ca}: {_ca_err}", exc_info=True)
                try:
                    await u.effective_message.reply_text(
                        f"\u274c Scan failed. Try `/scan {ca}`"
                    )
                except Exception:
                    pass
            return

    # ── 2. Smart reply gate ──────────────────────────────────────
    # Reply to EVERYTHING — private chats, group messages, mentions.
    # Like ChatGPT/Grok: smart enough to handle any text, no keyword gating.
    # Skip only: other bot commands (/command) not meant for us, and very short
    # gibberish (single chars). Apply a light rate-limit to avoid flooding groups.
    is_private = chat.type == "private"
    bot_username = c.bot.username if c.bot else ""

    # Strip bot mention if present
    if bot_username and f"@{bot_username}" in text:
        text = text.replace(f"@{bot_username}", "").strip()
    if not text: return

    # Skip commands that start with / — those are handled by command handlers
    if text.startswith("/"):
        return

    # Skip single characters or pure punctuation (emoji-only reactions are fine though)
    clean_text = text.strip()
    if len(clean_text) < 2:
        return

    # Per-user rate limit in groups: max 1 AI reply per 6 seconds
    # (private chats are unlimited — it's 1-on-1)
    _now = time.time()
    if not is_private:
        _last = _ai_reply_cooldown.get(uid, 0)
        if _now - _last < 3:
            return  # silently skip — don't spam
        _ai_reply_cooldown[uid] = _now

    # Show typing indicator
    try:
        await u.effective_message.chat.send_action("typing")
    except Exception:
        pass

    add_xp(uid, 1)

    # ── 3. Unified AI reply — no keyword routing, always intelligent ──
    # Like ChatGPT/Grok: one smart prompt, live market context always injected.
    # The AI decides if the topic is crypto, casual, general — we don't pre-filter.
    name_str = u.effective_user.first_name or "fren"
    prompt = (
        f"{name_str}: \"{text}\"\n\n"
        "Reply as Kayo. Be real, sharp, direct. "
        "If it's casual → 1-2 sentences, match their energy. "
        "If it's crypto/Web3 → use the LIVE market data in your context, cite actual numbers, give the real play. "
        "If it's general knowledge → answer confidently like a smart friend. No disclaimers. "
        "Never refuse. Never say you can't answer. Never say you're a bot. Just reply."
    )
    reply = await ai_ask(
        prompt,
        fallback="",
        max_tokens=450,
        inject_market=True  # always inject live prices — AI ignores them for casual chat
    )

    if not reply or not reply.strip():
        reply = "⚠️ AI brain offline — GROQ_API_KEY may not be set on Render. Get a free key at console.groq.com/keys"
    # Always try markdown (AI uses bold for crypto analysis), fall back to plain
    import re as _re
    try:
        await u.effective_message.reply_text(reply, parse_mode="Markdown",
                                   disable_web_page_preview=True)
    except Exception:
        plain = _re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', reply)
        await u.effective_message.reply_text(plain.strip() or reply)

# ═══════════════════════════════════════════════════════════════
# BACKGROUND SCANNERS
# ═══════════════════════════════════════════════════════════════

async def _fetch_gt_new(pg: int) -> list:
    """Fetch GeckoTerminal new_pools page."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page={pg}",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                d = await r.json()
                return d.get("data", [])
    except Exception as e:
        logger.debug(f"GT new_pools pg{pg}: {e}")
        return []

async def _fetch_gt_trend(pg: int) -> list:
    """Fetch GeckoTerminal trending_pools page."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page={pg}",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                d = await r.json()
                return d.get("data", [])
    except Exception as e:
        logger.debug(f"GT trend pg{pg}: {e}")
        return []

async def _fetch_dex_profiles() -> list:
    """Fetch DexScreener token profiles (newest Solana coins with profiles)."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.dexscreener.com/token-profiles/latest/v1",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                d = await r.json()
                return [x for x in (d if isinstance(d, list) else []) if x.get("chainId") == "solana"]
    except Exception as e:
        logger.debug(f"dex_profiles: {e}")
        return []

async def _fetch_dex_boosts() -> list:
    """Fetch DexScreener boosted Solana tokens."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.dexscreener.com/token-boosts/latest/v1",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                d = await r.json()
                return [x for x in (d if isinstance(d, list) else []) if x.get("chainId") == "solana"]
    except Exception as e:
        logger.debug(f"dex_boosts: {e}")
        return []

# ═══════════════════════════════════════════════════════════════════════
# KAYO v40 ELITE INJECTION — ALL NEW FEATURES
# Injected before bg_main_scanner
# ═══════════════════════════════════════════════════════════════════════

# ── FREE API HELPERS ─────────────────────────────────────────────────

async def solscan_wallet_txns(addr: str, limit: int = 10) -> List[Dict]:
    """SolanaFM / Solscan public API — no key needed."""
    urls = [
        f"https://api.solscan.io/v2/account/transactions?account={addr}&limit={limit}",
        f"https://public-api.solscan.io/account/transactions?account={addr}&limit={limit}",
    ]
    async with aiohttp.ClientSession() as s:
        for url in urls:
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if isinstance(data, list): return data
                        if isinstance(data, dict) and "data" in data: return data["data"]
            except Exception:
                continue
    return []

async def solanafm_wallet_txns(addr: str, limit: int = 10) -> List[Dict]:
    """SolanaFM API — free, no key."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.solana.fm/v0/accounts/{addr}/transactions?limit={limit}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    return d.get("result", {}).get("data", []) or []
    except Exception:
        pass
    return []

async def dex_wallet_pnl(addr: str) -> Dict:
    """Get wallet PnL from DexScreener portfolio endpoint."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/portfolio/solana/{addr}",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                if r.status == 200:
                    return await r.json()
    except Exception:
        pass
    return {}

async def fetch_token_holders(addr: str) -> Dict:
    """Get top holder concentration via Solscan."""
    result = {"top10_pct": 0, "holder_count": 0, "top_holders": []}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.solscan.io/v2/token/holders?token={addr}&offset=0&limit=10",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    holders = d.get("data", {}).get("result", []) or []
                    total_supply = float(d.get("data", {}).get("total", 1) or 1)
                    top10_amt = sum(float(h.get("amount", 0)) for h in holders[:10])
                    result["top10_pct"] = min(100, (top10_amt / max(total_supply, 1)) * 100)
                    result["holder_count"] = int(d.get("data", {}).get("total", 0) or 0)
                    result["top_holders"] = holders[:5]
    except Exception:
        pass
    return result

async def fetch_token_metadata(addr: str) -> Dict:
    """Get rich token metadata from multiple free sources."""
    meta = {}
    try:
        async with aiohttp.ClientSession() as s:
            # Try Pump.fun metadata
            async with s.get(
                f"https://frontend-api-v3.pump.fun/coins/{addr}",
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    meta["twitter"] = d.get("twitter", "")
                    meta["telegram"] = d.get("telegram", "")
                    meta["website"] = d.get("website", "")
                    meta["dev_wallet"] = d.get("creator", "")
                    meta["total_supply"] = d.get("total_supply", 0)
                    meta["raydium_pool"] = d.get("raydium_pool", "")
                    meta["is_currently_live"] = d.get("is_currently_live", False)
    except Exception:
        pass
    return meta

async def detect_bundled_launch(addr: str) -> Dict:
    """
    Detect if token was bundled at launch (coordinated multi-wallet buy).
    Uses DexScreener first-txn data — no key needed.
    """
    result = {"is_bundled": False, "bundle_wallets": 0, "bundle_pct": 0.0}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs", [])
                    if pairs:
                        p = pairs[0]
                        created = int(p.get("pairCreatedAt", 0) or 0) / 1000
                        b5m = int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
                        s5m = int(((p.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0)
                        age_min = (time.time() - created) / 60 if created else 999
                        # Heuristic: if token is <30min old and has many buys in first 5min
                        # relative to current txns, likely bundled
                        if age_min < 30 and b5m > 15:
                            result["is_bundled"] = True
                            result["bundle_wallets"] = b5m
    except Exception:
        pass
    return result

async def get_sol_price() -> float:
    """Get live SOL price — CoinGecko free."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    return float(d.get("solana", {}).get("usd", 0))
    except Exception:
        pass
    return 0.0

async def fetch_smart_money_tokens() -> List[Dict]:
    """
    Detect tokens being accumulated by smart/profitable wallets.
    Uses GeckoTerminal trending data cross-referenced with Pump.fun trending.
    """
    results = []
    try:
        gt_trending, pf_trending = await asyncio.gather(
            gt_trending_pools(page=1),
            _fetch_pumpfun_trending(),
            return_exceptions=True
        )
        gt_addrs = set()
        if not isinstance(gt_trending, Exception):
            for p in gt_trending:
                tok = gt_parse_pool(p)
                if tok: gt_addrs.add(tok["address"])

        pf_addrs = set()
        if not isinstance(pf_trending, Exception):
            for c in (pf_trending or [])[:20]:
                addr = c.get("mint", "")
                if addr: pf_addrs.add(addr)

        # Tokens appearing in BOTH sources = smart money convergence
        overlap = gt_addrs & pf_addrs
        for addr in list(overlap)[:10]:
            results.append({"address": addr, "signal": "smart_money_convergence"})
    except Exception:
        pass
    return results

async def _fetch_pumpfun_trending() -> List[Dict]:
    """Get trending Pump.fun coins — uses v3 API."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"offset": "0", "limit": "20", "sort": "market_cap", "order": "DESC"},
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data if isinstance(data, list) else []
    except Exception:
        pass
    return []

async def check_dev_activity(dev_wallet: str, token_addr: str) -> Dict:
    """Check if dev wallet has sold tokens or is still holding."""
    result = {"dev_sold": False, "dev_hold_pct": 100, "dev_txn_count": 0, "warning": ""}
    if not dev_wallet:
        return result
    try:
        txns = await solscan_wallet_txns(dev_wallet, limit=20)
        for txn in txns:
            # If dev wallet appears to have transferred the token
            if isinstance(txn, dict):
                result["dev_txn_count"] += 1
        if result["dev_txn_count"] > 10:
            result["warning"] = "High dev wallet activity"
        elif result["dev_txn_count"] == 0:
            result["warning"] = "Dev wallet inactive"
    except Exception:
        pass
    return result

# ── ENHANCED SCAN CARD WITH HOLDER + BUNDLE DATA ──────────────────────

async def full_enhanced_scan(addr: str) -> Dict:
    """
    Full token intelligence: base data + holder analysis + bundle detection + dev check.
    All free APIs.
    """
    base, holders, bundle, metadata = await asyncio.gather(
        full_token_scan(addr),
        fetch_token_holders(addr),
        detect_bundled_launch(addr),
        fetch_token_metadata(addr),
        return_exceptions=True
    )

    if isinstance(base, Exception) or not base:
        return {"error": "Token not found"}

    result = dict(base)

    # Holder data
    if not isinstance(holders, Exception):
        result["top10_pct"]    = holders.get("top10_pct", 0)
        result["holder_count"] = holders.get("holder_count", 0)
        result["top_holders"]  = holders.get("top_holders", [])

    # Bundle detection
    if not isinstance(bundle, Exception):
        result["is_bundled"]      = bundle.get("is_bundled", False)
        result["bundle_wallets"]  = bundle.get("bundle_wallets", 0)

    # Metadata
    if not isinstance(metadata, Exception) and metadata:
        if not result.get("tw_link") and metadata.get("twitter"):
            result["tw_link"] = metadata["twitter"]
        if not result.get("tg_link") and metadata.get("telegram"):
            result["tg_link"] = metadata["telegram"]
        if not result.get("web_link") and metadata.get("website"):
            result["web_link"] = metadata["website"]
        result["dev_wallet"]       = metadata.get("dev_wallet", "")
        result["pump_live"]        = metadata.get("is_currently_live", False)

    # Dev check
    if result.get("dev_wallet"):
        try:
            dev = await asyncio.wait_for(
                check_dev_activity(result["dev_wallet"], addr), timeout=8
            )
            result["dev_warning"]    = dev.get("warning", "")
            result["dev_txn_count"]  = dev.get("dev_txn_count", 0)
        except Exception:
            pass

    return result

def build_elite_scan_card(t: Dict, ai: str = "") -> str:
    """
    Elite scan card — includes holder concentration, bundle flag, dev wallet check.
    Rick-style information density.
    """
    sym   = _md(t.get("sym", "???"))
    name  = _md(t.get("name", sym))
    addr  = t.get("address", "")
    age   = _age(t.get("created", 0))
    nar   = f"#{t.get('narrative','').upper()}" if t.get("narrative") else ""

    def _chg(v):
        v = float(v or 0)
        if v > 0: return f"🟢 +{v:.1f}%"
        if v < 0: return f"🔴 {v:.1f}%"
        return f"⚪ {v:.1f}%"

    # Price bars
    bp   = float(t.get("buy_pct", 50))
    sp   = 100 - bp
    fill = int(bp / 10)
    bar  = "🟩" * fill + "🟥" * (10 - fill)
    press = "🔥 BUY PRESSURE" if bp > 60 else ("❄️ SELL PRESSURE" if bp < 40 else "⚖️ BALANCED")

    # Security flags
    flags = []
    if t.get("is_honeypot"):        flags.append("🚨 HONEYPOT")
    if t.get("is_bundled"):         flags.append(f"🔴 BUNDLED ({t.get('bundle_wallets',0)} wallets)")
    if t.get("top10_pct", 0) > 70:  flags.append(f"⚠️ TOP10 HOLD {t['top10_pct']:.0f}%")
    if t.get("dev_warning"):        flags.append(f"👨‍💻 DEV: {t['dev_warning']}")
    if t.get("lp_locked"):          flags.append("🔒 LP LOCKED")
    if t.get("is_renounced"):       flags.append("✅ RENOUNCED")
    if t.get("boost_active", 0) > 0: flags.append("💰 BOOSTED")
    if t.get("pump_live"):          flags.append("🟣 PUMP.FUN LIVE")
    flag_str = "  ".join(flags) if flags else "⚠️ UNVERIFIED"

    # Risk color
    rs = int(t.get("risk_score", 30))
    risk_icon = "🔴" if rs >= 70 else ("🟡" if rs >= 40 else "🟢")
    ms = int(t.get("mscore", 0))
    ms_icon = "🔥" if ms >= 70 else ("⚡" if ms >= 40 else "💤")

    holders_line = ""
    if t.get("holder_count"):
        h10 = t.get("top10_pct", 0)
        holders_line = f"👥 Holders: `{t['holder_count']:,}`  ·  Top10: `{h10:.1f}%`\n"

    socials = []
    if t.get("tw_link"):  socials.append(f"[🐦 Twitter]({t['tw_link']})")
    if t.get("tg_link"):  socials.append(f"[💬 TG]({t['tg_link']})")
    if t.get("web_link"): socials.append(f"[🌐 Web]({t['web_link']})")
    soc_str = "  ".join(socials)

    tax_line = ""
    bt = float(t.get("buy_tax", 0))
    st = float(t.get("sell_tax", 0))
    if bt > 0 or st > 0:
        tax_line = f"🧾 Tax: Buy `{bt:.1f}%` / Sell `{st:.1f}%`\n"

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍 *KAYO ELITE SCAN* {f'| {nar}' if nar else ''}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{sym}* — _{name}_\n"
        f"📋 `{addr}`\n"
        f"🕐 Age: {age}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 Price: `{_price(t.get('price',0))}`\n"
        f"📊 MCap: `{_usd(t.get('mcap',0))}`  ·  FDV: `{_usd(t.get('fdv',0))}`\n"
        f"🌊 Liquidity: `{_usd(t.get('liq',0))}`\n"
        f"{holders_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Price Action*\n"
        f"  5m: {_chg(t.get('ch5m',0))}  ·  1h: {_chg(t.get('ch1h',0))}\n"
        f"  6h: {_chg(t.get('ch6h',0))}  ·  24h: {_chg(t.get('ch24h',0))}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 *Txns (1h)*  🟢 {t.get('b1h',0)} buys · 🔴 {t.get('s1h',0)} sells\n"
        f"  {bar}\n"
        f"  {bp:.0f}% Buy / {sp:.0f}% Sell — {press}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Momentum: {ms_icon} `{ms}/100`  ·  Risk: {risk_icon} `{rs}/100`\n"
        f"{tax_line}"
        f"🛡️ {flag_str}\n"
    )
    if soc_str:
        card += f"🔗 {soc_str}\n"
    card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if ai:
        card += f"\n\n🧠 *Kayo AI:*\n_{ai}_"
    return card

# ── WALLET INTELLIGENCE COMMANDS ─────────────────────────────────────

async def wallet_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /wallet <address> — full wallet intelligence:
    last 10 txns, token holdings snapshot, PnL estimate.
    All free APIs (DexScreener, SolanaFM, Solscan).
    """
    if not c.args:
        await u.effective_message.reply_text(
            "Usage: `/wallet <solana_address>`\n"
            "Shows last trades, holdings, PnL estimate.",
            parse_mode="Markdown"
        ); return

    addr = c.args[0].strip()
    if len(addr) < 32:
        await u.effective_message.reply_text("❌ Invalid Solana address."); return

    msg = await u.effective_message.reply_text("👛 *Fetching wallet intelligence...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 3)

    # Fetch in parallel
    txns, pnl_data = await asyncio.gather(
        solanafm_wallet_txns(addr, limit=10),
        dex_wallet_pnl(addr),
        return_exceptions=True
    )

    short = f"`{addr[:6]}...{addr[-4:]}`"
    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 *WALLET INTEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 {short}\n"
        f"🔗 [Solscan](https://solscan.io/account/{addr})  ·  "
        f"[SolanaFM](https://solana.fm/address/{addr})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    # PnL section
    if not isinstance(pnl_data, Exception) and pnl_data:
        positions = pnl_data.get("positions", []) or []
        total_pnl = sum(float(p.get("unrealizedPnl", 0) or 0) for p in positions)
        realized  = sum(float(p.get("realizedPnl", 0) or 0) for p in positions)
        pnl_icon  = "🟢" if total_pnl >= 0 else "🔴"
        card += (
            f"💰 *Portfolio Snapshot*\n"
            f"  Unrealized PnL: {pnl_icon} `{_usd(total_pnl)}`\n"
            f"  Realized PnL:   `{_usd(realized)}`\n"
            f"  Positions: `{len(positions)}`\n"
        )
        # Top 3 positions
        if positions:
            card += "\n📊 *Top Positions*\n"
            for p in sorted(positions, key=lambda x: abs(float(x.get("unrealizedPnl", 0) or 0)), reverse=True)[:3]:
                sym = p.get("token", {}).get("symbol", "???")
                upnl = float(p.get("unrealizedPnl", 0) or 0)
                icon = "🟢" if upnl >= 0 else "🔴"
                card += f"  {icon} `${sym}` — {_usd(upnl)}\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # Recent txns
    if not isinstance(txns, Exception) and txns:
        card += f"🔄 *Recent Txns ({len(txns)} fetched)*\n"
        for txn in txns[:5]:
            if isinstance(txn, dict):
                sig   = str(txn.get("signature", txn.get("hash", "?")))[:10] + "..."
                t_ms  = int(txn.get("blockTime", txn.get("timestamp", 0)) or 0)
                t_str = datetime.fromtimestamp(t_ms).strftime("%m/%d %H:%M") if t_ms > 1e9 else "?"
                card += f"  ·  `{sig}` — {t_str}\n"
    else:
        card += "ℹ️ Transaction history unavailable (rate limit)\n"

    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 Track this wallet: `/trackwallet {addr[:12]}... <label>`"
    )

    await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)


async def holders_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/holders <CA> — show top holder concentration + bundle risk."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/holders <CA>`", parse_mode="Markdown"); return

    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("👥 *Fetching holder data...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 3)

    holders, bundle = await asyncio.gather(
        fetch_token_holders(addr),
        detect_bundled_launch(addr),
        return_exceptions=True
    )

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 *HOLDER ANALYSIS*\n"
        f"📋 `{addr[:20]}...`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    if not isinstance(holders, Exception) and holders:
        h10  = holders.get("top10_pct", 0)
        hcnt = holders.get("holder_count", 0)
        risk = "🔴 HIGH" if h10 > 70 else ("🟡 MEDIUM" if h10 > 40 else "🟢 HEALTHY")
        card += (
            f"📊 Total Holders: `{hcnt:,}`\n"
            f"🏆 Top 10 Control: `{h10:.1f}%` — {risk}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )
        top = holders.get("top_holders", [])
        if top:
            card += "🔑 *Top Wallets*\n"
            for i, h in enumerate(top[:5], 1):
                wa   = h.get("address", "?")
                pct  = float(h.get("amount", 0)) / max(float(h.get("total_supply", 1) or 1), 1) * 100
                short_wa = f"{wa[:6]}...{wa[-4:]}"
                card += f"  {i}. `{short_wa}` — `{pct:.1f}%`\n"
    else:
        card += "⚠️ Holder data unavailable\n"

    if not isinstance(bundle, Exception) and bundle.get("is_bundled"):
        card += (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔴 *BUNDLE DETECTED*\n"
            f"   Coordinated buys: {bundle.get('bundle_wallets', 0)} wallets at launch\n"
        )

    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [Full Analysis](https://solscan.io/token/{addr}#holders)"
    )
    await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)


async def pnl_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/pnl — show your linked wallet's PnL (requires /mywallet first)."""
    uid  = str(u.effective_user.id)
    addr = user_wallets.get(uid, "")
    if not addr:
        await u.effective_message.reply_text(
            "Link your wallet first: `/mywallet <solana_address>`",
            parse_mode="Markdown"
        ); return

    msg = await u.effective_message.reply_text("📊 *Calculating your PnL...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)

    pnl_data = await dex_wallet_pnl(addr)
    if not pnl_data or not pnl_data.get("positions"):
        await msg.edit_text(
            f"⚠️ No position data found for your wallet.\n"
            f"Wallet: `{addr[:10]}...{addr[-4:]}`\n\n"
            f"This wallet may be new or have no DexScreener-tracked positions.",
            parse_mode="Markdown"
        ); return

    positions = pnl_data.get("positions", [])
    total_upnl = sum(float(p.get("unrealizedPnl", 0) or 0) for p in positions)
    total_rpnl = sum(float(p.get("realizedPnl",   0) or 0) for p in positions)
    total_pnl  = total_upnl + total_rpnl
    pnl_icon   = "🟢" if total_pnl >= 0 else "🔴"

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *YOUR PnL DASHBOARD*\n"
        f"👛 `{addr[:6]}...{addr[-4:]}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Unrealized: {pnl_icon} `{_usd(total_upnl)}`\n"
        f"✅ Realized:   `{_usd(total_rpnl)}`\n"
        f"📈 Total PnL:  {pnl_icon} `{_usd(total_pnl)}`\n"
        f"📦 Positions:  `{len(positions)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Open Positions*\n"
    )
    winners = [p for p in positions if float(p.get("unrealizedPnl", 0) or 0) > 0]
    losers  = [p for p in positions if float(p.get("unrealizedPnl", 0) or 0) < 0]
    for p in sorted(positions, key=lambda x: float(x.get("unrealizedPnl", 0) or 0), reverse=True)[:8]:
        sym  = (p.get("token") or {}).get("symbol", "???")
        upnl = float(p.get("unrealizedPnl", 0) or 0)
        icon = "🟢" if upnl >= 0 else "🔴"
        card += f"  {icon} `${sym}` — `{_usd(upnl)}`\n"
    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 Winners: {len(winners)}  ·  💀 Losers: {len(losers)}\n"
        f"[Full Portfolio](https://dexscreener.com/solana/{addr})"
    )
    await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)


async def smart_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/smart — tokens being accumulated by smart money right now."""
    msg = await u.effective_message.reply_text("🧠 *Scanning smart money signals...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 3)

    smart_tokens = await fetch_smart_money_tokens()
    if not smart_tokens:
        await msg.edit_text("⚠️ No smart money convergence signals right now. Try again in a few minutes.")
        return

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 *SMART MONEY RADAR*\n"
        f"Tokens appearing across multiple alpha data sources\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    for i, t in enumerate(smart_tokens[:8], 1):
        addr = t.get("address", "")
        card += f"  {i}. `{addr[:12]}...`  — `/scan {addr[:12]}...`\n"
    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Tap a CA or use `/scan <CA>` for full analysis_"
    )
    await msg.edit_text(card, parse_mode="Markdown")


async def copy_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/copy <wallet_address> — get AI breakdown of a wallet's recent moves."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/copy <wallet_address>`\nAnalyze a wallet's recent trades.", parse_mode="Markdown"); return

    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("🔍 *Analyzing wallet moves...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 4)

    txns = await solanafm_wallet_txns(addr, limit=20)
    if not txns:
        txns = await solscan_wallet_txns(addr, limit=20)

    short = f"{addr[:8]}...{addr[-4:]}"
    if not txns:
        await msg.edit_text(
            f"⚠️ No recent transactions found for `{short}`.\n"
            f"Wallet may be inactive or API is rate limited.",
            parse_mode="Markdown"
        ); return

    # Ask AI to analyze the moves
    txn_summary = f"Wallet {short} had {len(txns)} recent transactions on Solana."
    ai_analysis = await ai_ask(
        f"This Solana wallet just made {len(txns)} transactions: {txn_summary}. "
        f"Based on the wallet having {len(txns)} recent moves, give a sharp analysis: "
        f"Is this likely a smart trader, bot, or retail? What copy trading strategy would you suggest? "
        f"What signals to watch for? 3-4 direct sentences.",
        fallback="Analysis unavailable.",
        max_tokens=250,
        inject_market=False
    )

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔁 *COPY TRADE ANALYSIS*\n"
        f"👛 `{short}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Recent Txns: `{len(txns)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 *AI Verdict:*\n_{ai_analysis}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👁️ Track this wallet: `/trackwallet {addr} KOL`\n"
        f"🔗 [View on Solscan](https://solscan.io/account/{addr})"
    )
    await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)


async def bundle_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/bundle <CA> — detect if token was bundled at launch."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/bundle <CA>`\nDetects coordinated launch bundles.", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("🔍 *Checking for bundle...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)

    bundle, holders = await asyncio.gather(
        detect_bundled_launch(addr),
        fetch_token_holders(addr),
        return_exceptions=True
    )

    h10  = holders.get("top10_pct", 0) if not isinstance(holders, Exception) else 0
    is_b = bundle.get("is_bundled", False) if not isinstance(bundle, Exception) else False
    bw   = bundle.get("bundle_wallets", 0) if not isinstance(bundle, Exception) else 0

    verdict = "🔴 BUNDLED — HIGH RISK" if is_b else "🟢 No bundle signals detected"

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔎 *BUNDLE CHECK*\n"
        f"📋 `{addr[:20]}...`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Verdict: {verdict}\n"
    )
    if is_b:
        card += f"  ↳ Coordinated wallets at launch: {bw}\n"
    card += (
        f"\n👥 Top 10 Holder Control: `{h10:.1f}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'⚠️ Avoid or degen small — this was coordinated' if is_b else '✅ Looks organic — standard DYOR applies'}"
    )
    await msg.edit_text(card, parse_mode="Markdown")


async def snipe_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/snipe — find the freshest tokens on pump.fun + DexScreener right now."""
    msg = await u.effective_message.reply_text("🎯 *Finding snipe targets...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 3)

    try:
        pf_new, gt_new = await asyncio.gather(
            _fetch_pumpfun_trending(),
            gt_new_pools(page=1),
            return_exceptions=True
        )

        card = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *SNIPE RADAR*\n"
            f"Freshest launches right now\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Pump.fun newest
        if not isinstance(pf_new, Exception) and pf_new:
            card += "🟣 *Pump.fun Newest*\n"
            for c2 in pf_new[:5]:
                sym  = c2.get("symbol", "???")
                name = c2.get("name", "")[:20]
                mcap = float(c2.get("usd_market_cap", 0) or 0)
                mint = c2.get("mint", "")
                card += f"  • `${sym}` ({name}) — `{_usd(mcap)}`\n"
                if mint: card += f"    `{mint}`\n"

        # GeckoTerminal newest
        if not isinstance(gt_new, Exception) and gt_new:
            card += "\n📊 *GT New Pools*\n"
            for pool in gt_new[:5]:
                tok = gt_parse_pool(pool)
                if tok:
                    card += (
                        f"  • `${tok['sym']}` — MCap `{_usd(tok['fdv'])}`  "
                        f"1h: {'+' if tok['ch1h'] >= 0 else ''}{tok['ch1h']:.1f}%\n"
                        f"    `{tok['address']}`\n"
                    )

        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⚠️ DYOR. New = risky."
        await msg.edit_text(card, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"❌ Snipe scan failed: {e}")


# ── LIVE BACKGROUND: WALLET TRACKER ──────────────────────────────────

async def bg_wallet_tracker(app):
    """
    Every 3 min: polls tracked wallets for new activity using SolanaFM.
    Alerts group chat when a tracked wallet makes a move.
    """
    await asyncio.sleep(180)
    wallet_last_seen: Dict[str, str] = {}  # addr -> last txn sig

    while True:
        try:
            if not tracked_wallets or not GROUP_CHAT_ID:
                await asyncio.sleep(180); continue

            for addr, info in list(tracked_wallets.items()):
                try:
                    txns = await asyncio.wait_for(
                        solanafm_wallet_txns(addr, limit=3), timeout=10
                    )
                    if not txns:
                        txns = await asyncio.wait_for(
                            solscan_wallet_txns(addr, limit=3), timeout=10
                        )
                    if not txns:
                        continue

                    latest = txns[0] if txns else {}
                    sig = str(latest.get("signature", latest.get("hash", "")) or "")
                    if not sig:
                        continue

                    last = wallet_last_seen.get(addr, "")
                    if sig == last:
                        continue  # No new txn

                    wallet_last_seen[addr] = sig
                    if not last:
                        continue  # First time seeing this wallet — just record, don't alert

                    label = info.get("label", addr[:8])
                    short = f"{addr[:6]}...{addr[-4:]}"
                    t_ms  = int(latest.get("blockTime", latest.get("timestamp", 0)) or 0)
                    t_str = datetime.fromtimestamp(t_ms).strftime("%H:%M:%S") if t_ms > 1e9 else "just now"

                    alert_text = (
                        f"👛 *WALLET MOVE*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🏷️ Label: *{_md(label)}*\n"
                        f"📋 `{short}`\n"
                        f"⏰ Time: {t_str}\n"
                        f"🔗 [View Txn](https://solscan.io/tx/{sig})\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Use `/wallet {addr}` for full analysis"
                    )
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=alert_text,
                            parse_mode="Markdown",
                            disable_web_page_preview=True,
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
                except Exception:
                    continue

            await asyncio.sleep(180)
        except Exception as e:
            logger.error(f"[WALLET TRACKER] {e}")
            await asyncio.sleep(180)


# ── ENHANCED /scan THAT USES ELITE DATA ───────────────────────────────

async def escan_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /escan <CA> — Elite scan with holder analysis, bundle check, dev wallet.
    Richer than /scan. All free APIs.
    """
    if not c.args:
        await u.effective_message.reply_text("Usage: `/escan <CA>`\nElite scan with holder + bundle analysis.", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("🔬 *Running elite scan...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 8)

    t = await asyncio.wait_for(full_enhanced_scan(addr), timeout=25)
    if t.get("error"):
        await msg.edit_text(f"❌ {t['error']}"); return

    _track_scan(t, u.effective_user.id)
    buttons = scan_buttons(addr, t.get("sym", ""), t.get("pair_addr", ""))

    sent = await msg.edit_text(
        build_elite_scan_card(t, ""),
        parse_mode="Markdown",
        reply_markup=buttons,
        disable_web_page_preview=True,
    )

    # AI verdict async
    async def _ai_verdict():
        try:
            ai_v = await asyncio.wait_for(
                ai_ask(
                    f"Elite scan on ${t['sym']}: MCap {_usd(t['mcap'])}, liq {_usd(t['liq'])}, "
                    f"age {_age(t['created'])}, holders {t.get('holder_count',0):,}, "
                    f"top10 hold {t.get('top10_pct',0):.0f}%, bundled={t.get('is_bundled',False)}, "
                    f"1h change {_pct(t['ch1h'])}, buy% {t.get('buy_pct',50):.0f}%. "
                    f"Is this worth aping? Any red flags? 2-3 sharp sentences.",
                    fallback="", max_tokens=200, inject_market=True
                ), timeout=15
            )
            if ai_v:
                try:
                    await c.bot.edit_message_text(
                        build_elite_scan_card(t, ai_v),
                        chat_id=u.effective_chat.id,
                        message_id=sent.message_id,
                        parse_mode="Markdown",
                        reply_markup=buttons,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
        except Exception:
            pass
    asyncio.create_task(_ai_verdict())



# ═══════════════════════════════════════════════════════════════════════
# KAYO v40 — MISSING 8 FEATURES
# ═══════════════════════════════════════════════════════════════════════

# ── 1. SNIPER DETECTION ──────────────────────────────────────────────
async def detect_snipers(addr: str) -> Dict:
    """
    Detect wallets that bought within the first 30s of token launch.
    Uses DexScreener txn data — free, no key.
    Returns: sniper_count, sniper_pct_supply, risk_level
    """
    result = {"sniper_count": 0, "sniper_pct": 0.0, "risk": "unknown"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs", [])
                    if not pairs: return result
                    p = pairs[0]
                    created_ms = int(p.get("pairCreatedAt", 0) or 0)
                    b5m = int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
                    b1h = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                    age_min = (time.time() - created_ms / 1000) / 60 if created_ms else 999
                    # Heuristic: if >40% of 1h buys happened in first 5m = sniper heavy
                    if b1h > 0:
                        snipe_ratio = b5m / max(b1h, 1) * 100
                        result["sniper_count"] = b5m
                        result["sniper_pct"] = snipe_ratio
                        if snipe_ratio > 60:   result["risk"] = "HIGH — heavy sniper load"
                        elif snipe_ratio > 30: result["risk"] = "MEDIUM — some snipers"
                        else:                  result["risk"] = "LOW — organic launch"
    except Exception:
        pass
    return result

# ── 2. TOKEN VELOCITY (holder growth rate) ───────────────────────────
async def calc_token_velocity(addr: str) -> Dict:
    """
    Estimate holder growth velocity using CoinGecko + DexScreener data.
    Returns: velocity_score (0-100), trend direction
    """
    result = {"velocity_score": 0, "trend": "unknown", "holder_growth": "unknown"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs", [])
                    if not pairs: return result
                    p = pairs[0]
                    b5m  = int(((p.get("txns") or {}).get("m5")  or {}).get("buys", 0) or 0)
                    b1h  = int(((p.get("txns") or {}).get("h1")  or {}).get("buys", 0) or 0)
                    b6h  = int(((p.get("txns") or {}).get("h6")  or {}).get("buys", 0) or 0)
                    b24h = int(((p.get("txns") or {}).get("h24") or {}).get("buys", 0) or 0)
                    # Velocity = rate of acceleration in buyer count
                    hourly_avg = b24h / 24 if b24h > 0 else 1
                    current_rate = b1h
                    velocity = min(100, int((current_rate / max(hourly_avg, 1)) * 20))
                    result["velocity_score"] = velocity
                    if velocity >= 70:   result["trend"] = "🚀 ACCELERATING"
                    elif velocity >= 40: result["trend"] = "📈 GROWING"
                    elif velocity >= 15: result["trend"] = "➡️ STEADY"
                    else:                result["trend"] = "📉 SLOWING"
                    # Holder growth estimate
                    if b5m > b1h / 12 * 2:
                        result["holder_growth"] = "Accelerating in last 5m"
                    else:
                        result["holder_growth"] = "Normal pace"
    except Exception:
        pass
    return result

# ── 3. QUICKCHART.IO — visual price chart ────────────────────────────
def build_quickchart_url(sym: str, prices: List[float], labels: List[str] = None) -> str:
    """
    Generate a free chart URL from quickchart.io — no API key needed.
    Returns a URL to a PNG chart image embeddable in Telegram.
    """
    import urllib.parse
    if not labels:
        labels = [str(i) for i in range(len(prices))]
    color = "#00ff88" if prices[-1] >= prices[0] else "#ff4444"
    chart_config = {
        "type": "line",
        "data": {
            "labels": labels[-20:],
            "datasets": [{
                "label": f"${sym}",
                "data": prices[-20:],
                "borderColor": color,
                "backgroundColor": color + "22",
                "fill": True,
                "tension": 0.4,
                "pointRadius": 0,
                "borderWidth": 2,
            }]
        },
        "options": {
            "plugins": {"legend": {"display": False}},
            "scales": {
                "x": {"ticks": {"color": "#aaaaaa"}, "grid": {"color": "#333333"}},
                "y": {"ticks": {"color": "#aaaaaa"}, "grid": {"color": "#333333"}}
            },
            "backgroundColor": "#1a1a2e"
        }
    }
    cfg_str = json.dumps(chart_config, separators=(',',':'))
    return f"https://quickchart.io/chart?w=600&h=300&c={urllib.parse.quote(cfg_str)}"

async def fetch_price_history(addr: str) -> tuple:
    """Get price history from DexScreener candles endpoint."""
    prices, labels = [], []
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs", [])
                    if pairs:
                        p = pairs[0]
                        # Build synthetic 24h price history from % changes
                        current_price = float(p.get("priceUsd", 0) or 0)
                        ch1h  = float((p.get("priceChange") or {}).get("h1",  0) or 0)
                        ch6h  = float((p.get("priceChange") or {}).get("h6",  0) or 0)
                        ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
                        if current_price > 0:
                            p24 = current_price / (1 + ch24h/100) if ch24h != -100 else current_price * 0.1
                            p6  = current_price / (1 + ch6h/100)  if ch6h  != -100 else p24
                            p1  = current_price / (1 + ch1h/100)  if ch1h  != -100 else p6
                            prices = [p24, p6, p1, current_price]
                            labels = ["-24h", "-6h", "-1h", "Now"]
    except Exception:
        pass
    return prices, labels

# ── 4. /vchart — visual chart command ────────────────────────────────
async def vchart_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/vchart <CA> — visual price chart via quickchart.io (free, no key)."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/vchart <CA>`\nShows a visual 24h price chart.", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.effective_message.reply_text("📊 *Generating chart...*", parse_mode="Markdown")

    prices, labels = await fetch_price_history(addr)
    if not prices or len(prices) < 2:
        await msg.edit_text("❌ Not enough price data to chart this token."); return

    chart_url = build_quickchart_url("TOKEN", prices, labels)
    trend = "📈" if prices[-1] >= prices[0] else "📉"
    chg = ((prices[-1] - prices[0]) / max(prices[0], 1e-18)) * 100

    await msg.edit_text(
        f"📊 *24H PRICE CHART*\n"
        f"📋 `{addr[:20]}...`\n"
        f"{trend} 24h Change: `{chg:+.1f}%`\n\n"
        f"[View Chart]({chart_url})",
        parse_mode="Markdown",
        disable_web_page_preview=False
    )

# ── 5. /migrate — Pump.fun → Raydium migration detector ──────────────
async def _fetch_pump_graduated() -> List[Dict]:
    """Get tokens that recently graduated from Pump.fun to Raydium."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://frontend-api-v3.pump.fun/coins",
                params={"offset": "0", "limit": "50", "sort": "market_cap", "order": "DESC"},
                headers=_PUMPFUN_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    coins = await r.json()
                    if isinstance(coins, list):
                        return [c for c in coins if c.get("raydium_pool")]
    except Exception:
        pass
    return []

async def migrate_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/migrate — show tokens that just graduated from Pump.fun to Raydium."""
    msg = await u.effective_message.reply_text("🔄 *Fetching Pump.fun graduates...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 3)

    graduated = await _fetch_pump_graduated()
    if not graduated:
        await msg.edit_text("⚠️ No recent migrations found. Try again in a minute."); return

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 *PUMP.FUN → RAYDIUM*\n"
        f"Recently graduated tokens\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    for i, coin in enumerate(graduated[:8], 1):
        sym  = coin.get("symbol", "???")
        name = coin.get("name", "")[:18]
        mcap = float(coin.get("usd_market_cap", 0) or 0)
        mint = coin.get("mint", "")
        pool = coin.get("raydium_pool", "")
        card += (
            f"{i}. *${sym}* — _{name}_\n"
            f"   MCap: `{_usd(mcap)}`\n"
            f"   CA: `{mint}`\n"
        )
    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Use `/scan <CA>` to analyze any token above_"
    )
    await msg.edit_text(card, parse_mode="Markdown")

# ── 6. bg_migrate_monitor — auto-alert on Pump.fun graduations ────────
async def bg_migrate_monitor(app):
    """
    Every 5 min: detect new Pump.fun → Raydium migrations.
    Alerts group when a token graduates with MCap < $500k.
    """
    await asyncio.sleep(300)
    seen_pools: Set[str] = set()

    while True:
        try:
            if not GROUP_CHAT_ID:
                await asyncio.sleep(300); continue

            graduated = await _fetch_pump_graduated()
            for coin in graduated:
                pool = coin.get("raydium_pool", "")
                mint = coin.get("mint", "")
                if not pool or pool in seen_pools: continue
                seen_pools.add(pool)

                sym  = coin.get("symbol", "???")
                name = coin.get("name", "")[:20]
                mcap = float(coin.get("usd_market_cap", 0) or 0)
                if mcap > 500_000 or mcap < 1000: continue

                alert = (
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔄 *PUMP → RAYDIUM*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🪙 *${sym}* — _{_md(name)}_\n"
                    f"📊 MCap: `{_usd(mcap)}`\n"
                    f"📋 `{mint}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"_Just graduated to Raydium — early entry window_\n"
                    f"Use `/scan {mint}` to analyze"
                )
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=alert,
                        parse_mode="Markdown",
                    )
                    await asyncio.sleep(1)
                except Exception:
                    pass

            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"[MIGRATE MONITOR] {e}")
            await asyncio.sleep(300)

# ── 7. /kol — track known alpha callers ──────────────────────────────
# KOL wallet registry (editable)
KOL_WALLETS: Dict[str, str] = {
    # "label": "wallet_address"
    # User can add via /addkol
}

async def kol_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/kol — show recent moves from known KOL wallets."""
    if not KOL_WALLETS and not tracked_wallets:
        await u.effective_message.reply_text(
            "No KOL wallets tracked yet.\n"
            "Add one: `/trackwallet <address> KOL: <name>`",
            parse_mode="Markdown"
        ); return

    msg = await u.effective_message.reply_text("🎯 *Checking KOL wallets...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)

    # Combine KOL_WALLETS + tracked_wallets tagged with "kol"
    all_kols = {**KOL_WALLETS}
    for addr, info in tracked_wallets.items():
        label = info.get("label", "")
        if "kol" in label.lower() or "alpha" in label.lower():
            all_kols[label] = addr

    if not all_kols:
        await msg.edit_text(
            "No KOL wallets found.\n"
            "Track a wallet as KOL: `/trackwallet <address> KOL:<name>`",
            parse_mode="Markdown"
        ); return

    card = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 *KOL WALLET INTEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    for label, addr in list(all_kols.items())[:5]:
        try:
            txns = await asyncio.wait_for(solanafm_wallet_txns(addr, limit=3), timeout=8)
            if not txns:
                txns = await asyncio.wait_for(solscan_wallet_txns(addr, limit=3), timeout=8)
            latest = txns[0] if txns else {}
            t_ms  = int(latest.get("blockTime", latest.get("timestamp", 0)) or 0)
            t_str = datetime.fromtimestamp(t_ms).strftime("%m/%d %H:%M") if t_ms > 1e9 else "unknown"
            sig   = str(latest.get("signature", latest.get("hash", "")) or "?")[:12]
            short = f"{addr[:6]}...{addr[-4:]}"
            card += (
                f"🎯 *{_md(label)}*\n"
                f"   `{short}`\n"
                f"   Last move: {t_str}\n"
                f"   [View]({f'https://solscan.io/account/{addr}'})\n"
            )
        except Exception:
            card += f"🎯 *{_md(label)}* — ⏳ fetching...\n"

    card += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Add KOL: `/trackwallet <address> KOL:<name>`_"
    )
    await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)

# ── 8. WEEKLY LEADERBOARD AUTO-POST ──────────────────────────────────
async def bg_weekly_leaderboard(app):
    """
    Every Sunday at ~midnight Lagos time (23:00 UTC): post weekly leaderboard.
    """
    await asyncio.sleep(60)

    while True:
        try:
            now = datetime.utcnow()
            # Sunday = weekday 6, post at 23:00 UTC (midnight Lagos WAT = UTC+1)
            target_weekday = 6
            target_hour    = 23

            # Calculate seconds until next Sunday 23:00 UTC
            days_ahead = (target_weekday - now.weekday()) % 7
            if days_ahead == 0 and now.hour >= target_hour:
                days_ahead = 7  # already passed this week — wait for next
            next_run = now.replace(hour=target_hour, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
            wait_secs = (next_run - now).total_seconds()
            logger.info(f"[WEEKLY LB] Next post in {wait_secs/3600:.1f}h")
            await asyncio.sleep(max(wait_secs, 60))

            if not GROUP_CHAT_ID or not xp_db:
                continue

            # Build leaderboard
            sorted_xp = sorted(xp_db.items(), key=lambda x: x[1], reverse=True)[:10]
            card = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏆 *WEEKLY LEADERBOARD*\n"
                f"Week of {now.strftime('%b %d, %Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
            for i, (uid, xp) in enumerate(sorted_xp):
                card += f"{medals[i]} `User {uid}` — `{xp} XP`\n"
            card += (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💬 Stay active, scan tokens, and make calls to earn XP!\n"
                f"Use `/rank` to check your standing."
            )
            try:
                await app.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=card,
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"[WEEKLY LB] Send error: {e}")

        except Exception as e:
            logger.error(f"[WEEKLY LB] {e}")
            await asyncio.sleep(3600)

# ── VELOCITY SCORE added to build_alert_card enrichment ─────────────
async def enrich_alert_with_velocity(tok_dict: Dict) -> Dict:
    """Add velocity score to a token dict in place — non-blocking."""
    try:
        vel = await asyncio.wait_for(
            calc_token_velocity(tok_dict.get("address", "")), timeout=8
        )
        tok_dict["velocity_score"] = vel.get("velocity_score", 0)
        tok_dict["velocity_trend"] = vel.get("trend", "")
    except Exception:
        tok_dict["velocity_score"] = 0
        tok_dict["velocity_trend"] = ""
    return tok_dict



async def bg_main_scanner(app: Application):
    """
    PRIMARY SCANNER — every 60s
    Pulls ALL Solana coins from GeckoTerminal (new+trending) + DexScreener (profiles+boosts).
    NO keyword filtering. 150-200+ unique coins per cycle.
    Detects: Pump | Gem | New Launch | Whale | Micro Gem | Unusual
    """
    await asyncio.sleep(15)
    cooldown: Dict[str, float] = {}

    while True:
        try:
            now = time.time()

            # Fetch all sources in parallel (module-level helpers — no closure issues)
            # Fetch GT + DexScreener + Pump.fun all in parallel
            batches = await asyncio.gather(
                _fetch_gt_new(1), _fetch_gt_new(2), _fetch_gt_new(3),
                _fetch_gt_new(4), _fetch_gt_new(5), _fetch_gt_new(6),
                _fetch_gt_trend(1), _fetch_gt_trend(2), _fetch_gt_trend(3),
                _fetch_dex_profiles(),
                _fetch_dex_boosts(),
                pumpfun_latest(50),      # v40: pump.fun newest launches
                pumpfun_trending(20),     # v40: pump.fun trending
                return_exceptions=True,
            )

            all_gt_pools = []
            dex_profiles_raw = []
            dex_boosts_raw   = []
            pumpfun_new_raw  = []
            pumpfun_trend_raw = []
            for idx, batch in enumerate(batches):
                if isinstance(batch, Exception):
                    logger.debug(f"Scanner source {idx} error: {batch}")
                    continue
                if idx < 9:
                    all_gt_pools += batch
                elif idx == 9:
                    dex_profiles_raw = batch
                elif idx == 10:
                    dex_boosts_raw   = batch
                elif idx == 11:
                    pumpfun_new_raw = batch if isinstance(batch, list) else []
                elif idx == 12:
                    pumpfun_trend_raw = batch if isinstance(batch, list) else []

            boosted_addrs  = {b.get("tokenAddress", "") for b in dex_boosts_raw}
            profiled_addrs = {p.get("tokenAddress", "") for p in dex_profiles_raw}

            # Build unified coin map from GT pools
            pairs_map: Dict[str, Dict] = {}
            for pool in all_gt_pools:
                tok = gt_parse_pool(pool)
                if not tok:
                    continue
                addr = tok["address"]
                if addr not in pairs_map:
                    pairs_map[addr] = tok

            # Add Pump.fun coins — extract narrative from description
            pumpfun_narratives: Dict[str, str] = {}  # addr → narrative
            pumpfun_meta: Dict[str, Dict] = {}       # addr → {desc, twitter, telegram, creator, reply_count}
            for coin in (pumpfun_new_raw + pumpfun_trend_raw):
                if not isinstance(coin, dict): continue
                if coin.get("is_banned"): continue   # skip banned tokens
                tok = pumpfun_to_token(coin)
                if not tok: continue
                addr = tok["address"]
                if addr not in pairs_map:
                    pairs_map[addr] = tok
                pumpfun_narratives[addr] = tok.get("narrative", "")
                pumpfun_meta[addr] = {
                    "description": tok.get("description", ""),
                    "tw_link": tok.get("tw_link", ""),
                    "tg_link": tok.get("tg_link", ""),
                    "web_link": tok.get("web_link", ""),
                    "creator": tok.get("creator", ""),
                    "reply_count": tok.get("reply_count", 0),
                    "is_pumpfun": True,
                    "is_graduated": tok.get("is_graduated", False),
                }

            # Also fetch DexScreener detail for profiled/boosted coins not in GT
            extra_addrs = list((profiled_addrs | boosted_addrs) - set(pairs_map.keys()))
            if extra_addrs:
                try:
                    async with aiohttp.ClientSession() as s:
                        chunk = ",".join(extra_addrs[:30])
                        async with s.get(
                            f"https://api.dexscreener.com/latest/dex/tokens/{chunk}",
                            timeout=aiohttp.ClientTimeout(total=12)
                        ) as r:
                            d = await r.json()
                            for p in (d.get("pairs") or []):
                                if p.get("chainId") != "solana":
                                    continue
                                a = (p.get("baseToken") or {}).get("address", "")
                                if a and a not in pairs_map:
                                    pairs_map[a] = {
                                        "address": a,
                                        "sym": (p.get("baseToken") or {}).get("symbol", "?"),
                                        "name": (p.get("baseToken") or {}).get("name", ""),
                                        "price": float(p.get("priceUsd", 0) or 0),
                                        "fdv": float(p.get("fdv", 0) or 0),
                                        "mcap": float(p.get("marketCap", 0) or 0),
                                        "liq": float((p.get("liquidity") or {}).get("usd", 0) or 0),
                                        "ch5m": float((p.get("priceChange") or {}).get("m5", 0) or 0),
                                        "ch1h": float((p.get("priceChange") or {}).get("h1", 0) or 0),
                                        "ch6h": float((p.get("priceChange") or {}).get("h6", 0) or 0),
                                        "ch24h": float((p.get("priceChange") or {}).get("h24", 0) or 0),
                                        "v5m": float((p.get("volume") or {}).get("m5", 0) or 0),
                                        "v1h": float((p.get("volume") or {}).get("h1", 0) or 0),
                                        "v24h": float((p.get("volume") or {}).get("h24", 0) or 0),
                                        "b5m": int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0),
                                        "s5m": int(((p.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0),
                                        "b1h": int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0),
                                        "s1h": int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0),
                                        "buy_pct": 0, "vol_spike": 0,
                                        "pair_addr": p.get("pairAddress", ""),
                                        "created_str": "",
                                    }
                except Exception as e:
                    logger.debug(f"dex_batch_extra: {e}")

            logger.info(f"[SCANNER] {len(pairs_map)} unique coins fetched. Running detection...")

            # Evaluate each coin
            alert_count = 0
            for addr, tok in pairs_map.items():
                if addr in blacklist:
                    continue
                if now - cooldown.get(addr, 0) < 3600:
                    continue

                sym   = tok.get("sym", "???")
                name  = tok.get("name", sym)
                fdv   = float(tok.get("fdv", 0) or 0)
                mcap  = float(tok.get("mcap", 0) or fdv)
                liq   = float(tok.get("liq", 0) or 0)
                ch5m  = float(tok.get("ch5m", 0) or 0)
                ch1h  = float(tok.get("ch1h", 0) or 0)
                ch6h  = float(tok.get("ch6h", 0) or 0)
                ch24h = float(tok.get("ch24h", 0) or 0)
                v5m   = float(tok.get("v5m", 0) or 0)
                v1h   = float(tok.get("v1h", 0) or 0)
                v24h  = float(tok.get("v24h", 0) or 0)
                b5m   = int(tok.get("b5m", 0) or 0)
                s5m   = int(tok.get("s5m", 0) or 0)
                b1h   = int(tok.get("b1h", 0) or 0)
                s1h   = int(tok.get("s1h", 0) or 0)
                price = float(tok.get("price", 0) or 0)
                pair_addr = tok.get("pair_addr", "")

                # Quality filter — use effective cap (fdv or mcap or liq*3)
                eff_cap = max(fdv, mcap, liq * 3)
                if eff_cap > 500_000: continue          # above $500k cap
                if eff_cap < 200 and liq < 50: continue  # truly zero — skip
                if liq < 80: continue                   # need some liquidity

                avg_5m_vol = v1h / 12 if v1h > 0 else 1
                vol_spike  = v5m / max(avg_5m_vol, 1)
                # buy_pct: use 5m data for brand-new coins with no h1 txns yet
                if b1h + s1h >= 3:
                    buy_pct = b1h / max(b1h + s1h, 1) * 100
                elif b5m + s5m >= 1:
                    buy_pct = b5m / max(b5m + s5m, 1) * 100
                else:
                    buy_pct = 60.0  # brand new — assume bullish if liq exists

                if buy_pct < 40: continue   # drop hard sell-pressure only
                # skip dead coins only if ALL signals are flat
                if ch1h == 0 and ch5m == 0 and b1h == 0 and b5m == 0 and vol_spike < 1.05: continue
                if eff_cap > 50_000 and liq / max(eff_cap, 1) < 0.002: continue

                # Narrative + flags — enriched with pump.fun data
                nar = detect_narrative(f"{name} {sym}")
                # Override with pump.fun narrative if available (more accurate)
                if addr in pumpfun_narratives and pumpfun_narratives[addr]:
                    nar = pumpfun_narratives[addr]
                pf_meta = pumpfun_meta.get(addr, {})
                is_pumpfun = bool(pf_meta)
                is_pumpfun_live = pf_meta.get("is_pumpfun", False) and pairs_map[addr].get("is_pumpfun_live", False)
                is_graduated = pf_meta.get("is_graduated", False)
                pf_reply_count = pf_meta.get("reply_count", 0)
                pf_desc = pf_meta.get("description", "")
                is_boosted  = addr in boosted_addrs
                is_rebranded = any(kw in name.lower() for kw in ["trump","maga","ai","agent","dog","cat","frog","ape","pepe","elon"])

                # Pattern detection
                alert_type = None
                is_fresh = (b1h + s1h) < 5  # brand-new token with almost no h1 history

                # ── PUMP.FUN NEW LAUNCH — catch first, these are the freshest ──
                if is_pumpfun and not is_graduated:
                    # Active pump.fun bonding curve token with some market cap
                    if mcap >= 1000 and mcap <= 500_000 and not pairs_map[addr].get("is_banned", False):
                        # If it has a narrative description, it's a strong signal
                        if pf_desc or pf_reply_count >= 5:
                            alert_type = "new"
                        elif mcap >= 5000:  # some traction even without description
                            alert_type = "new"

                # ── PUMP.FUN GRADUATION — token just moved to Raydium ──
                elif is_pumpfun and is_graduated and mcap <= 500_000:
                    alert_type = "migration"

                # ── NEW LAUNCH: fresh token — most important to catch first ──
                elif is_fresh and liq >= 100 and buy_pct >= 50:
                    alert_type = "new"  # any fresh token with liquidity + buy pressure
                elif is_fresh and is_boosted and liq >= 80:
                    alert_type = "new"

                # ── PUMP: fast 5m price spike ─────────────────────────────
                elif ch5m >= 5 and buy_pct >= 50:                              alert_type = "pump"
                elif ch5m >= 3 and b5m >= 2 and buy_pct >= 50:                alert_type = "pump"
                elif ch5m >= 10 and buy_pct >= 40:                             alert_type = "pump"

                # ── MOMENTUM: sustained 1h grind ─────────────────────────
                elif ch1h >= 15 and buy_pct >= 45:                             alert_type = "momentum"
                elif ch1h >= 8  and ch5m >= 1 and buy_pct >= 48:              alert_type = "momentum"
                elif ch1h >= 6  and vol_spike >= 1.3 and buy_pct >= 48:       alert_type = "momentum"

                # ── GEM: micro-cap mover with real buyers ─────────────────
                elif ch1h >= 5  and buy_pct >= 50 and liq >= 200:             alert_type = "gem"
                elif eff_cap < 50_000 and ch1h >= 3 and buy_pct >= 52:        alert_type = "gem"

                # ── ESTABLISHED NEW: small cap with real h1 buys ─────────
                elif b1h >= 3   and buy_pct >= 52 and liq >= 150 and ch1h >= 1: alert_type = "new"
                elif is_boosted and buy_pct >= 48 and b1h >= 1:               alert_type = "new"

                # ── WHALE: heavy accumulation ─────────────────────────────
                elif buy_pct >= 65 and b1h >= 3 and vol_spike >= 1.2:         alert_type = "whale"

                # ── UNUSUAL: vol spike or narrative play ──────────────────
                elif vol_spike >= 1.4 and b1h >= 2 and buy_pct >= 48:         alert_type = "unusual"
                elif is_rebranded and buy_pct >= 48 and (ch1h >= 1 or ch5m >= 1): alert_type = "rebrand"

                if not alert_type: continue

                # Pattern memory (kept for stats but NO gating — don't block alerts)
                pm_key  = f"{alert_type}:{nar}"
                pm_info = pattern_memory.get(pm_key, {})
                # NOTE: win-rate gate removed — it was silently killing too many alerts

                # Dropped calls gate
                if addr in dropped_calls:
                    if now - dropped_calls[addr].get("time", 0) < 3600: continue
                    if abs(price - dropped_calls[addr].get("entry_price", 0)) / max(dropped_calls[addr].get("entry_price", 1e-12), 1e-12) * 100 < 10: continue

                # Dedup
                alert_id = hashlib.md5(f"{addr}:{alert_type}:{int(now/3600)}".encode()).hexdigest()[:16]
                if _seen_check(seen_alert_ids, alert_id): continue
                _seen_add(seen_alert_ids, alert_id)
                asyncio.create_task(_save())

                cooldown[addr] = now

                tok_dict = {
                    "address": addr, "sym": sym, "name": name,
                    "price": price, "fdv": fdv, "mcap": mcap, "liq": liq,
                    "ch5m": ch5m, "ch1h": ch1h, "ch6h": ch6h, "ch24h": ch24h,
                    "v5m": v5m, "v1h": v1h, "v24h": v24h,
                    "b5m": b5m, "s5m": s5m, "b1h": b1h, "s1h": s1h,
                    "b24h": 0, "s24h": 0,
                    "buy_pct": buy_pct, "vol_spike": vol_spike,
                    "risk_score": 30, "red_flags": [], "green_flags": [],
                    "sell_tax": 0, "buy_tax": 0, "is_honeypot": False,
                    "lp_locked": False, "is_renounced": False,
                    "created": pairs_map[addr].get("created", 0) if is_pumpfun else 0,
                    "narrative": nar,
                    "tw_link": pf_meta.get("tw_link", "") if is_pumpfun else "",
                    "tg_link": pf_meta.get("tg_link", "") if is_pumpfun else "",
                    "web_link": pf_meta.get("web_link", "") if is_pumpfun else "",
                    "boost_active": 1 if is_boosted else 0,
                    "has_profile": addr in profiled_addrs, "has_ad": False,
                    "pair_addr": pair_addr,
                    "mscore": min(100, int(abs(ch1h) + buy_pct / 2 + vol_spike * 10)),
                    "is_pumpfun": is_pumpfun,
                    "is_graduated": is_graduated,
                    "pf_reply_count": pf_reply_count,
                    "pf_description": pf_desc,
                }
                # Cap alerts per cycle to prevent flooding
                if alert_count >= 15:
                    break

                # Build card WITHOUT AI verdict first — send IMMEDIATELY
                card = build_alert_card(tok_dict, alert_type, "")
                buttons = scan_buttons(addr, sym)

                if GROUP_CHAT_ID:
                    sent_msg = None
                    try:
                        sent_msg = await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=card,
                            parse_mode="Markdown",
                            reply_markup=buttons,
                            disable_web_page_preview=True,
                        )
                    except Exception as md_err:
                        # Markdown failed — retry as plain text
                        logger.warning(f"[ALERT MD FAIL] {sym}: {md_err} — retrying plain text")
                        try:
                            plain_card = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', card)
                            sent_msg = await app.bot.send_message(
                                chat_id=GROUP_CHAT_ID,
                                text=plain_card,
                                reply_markup=buttons,
                                disable_web_page_preview=True,
                            )
                        except Exception as e2:
                            logger.error(f"[ALERT SEND ERROR] {sym}: {e2}")
                    
                    if sent_msg:
                        alert_count += 1
                        logger.info(f"[ALERT] {alert_type.upper()} ${sym} fdv={fdv:,.0f} liq={liq:,.0f} 1h={ch1h:.1f}% buy%={buy_pct:.0f}%")
                        dropped_calls[addr] = {
                            "sym": sym, "alert_type": alert_type, "time": now,
                            "entry_price": price, "entry_mcap": mcap,
                            "peak_price": price, "narrative": nar,
                        }
                        active_calls.append({
                            "addr": addr, "sym": sym, "alert_type": alert_type,
                            "entry_price": price, "entry_mcap": mcap, "time": now,
                            "narrative": nar, "status": "open",
                        })
                        if len(active_calls) > 200:
                            active_calls.pop(0)
                        asyncio.create_task(_save())
                        
                        # Fire AI verdict as NON-BLOCKING task — edit message when ready
                        async def _add_ai_verdict(msg_id: int, chat_id: int, sym_: str, at: str,
                                                   fdv_: float, liq_: float, ch5m_: float, ch1h_: float,
                                                   b1h_: int, s1h_: int, bp_: float, vs_: float, nar_: str):
                            try:
                                ai_v = await asyncio.wait_for(
                                    ai_ask(
                                        f"Solana token ${sym_} | Type: {at.upper()} | MCap {_usd(fdv_)} | "
                                        f"Liq {_usd(liq_)} | 5m {_pct(ch5m_)} | 1h {_pct(ch1h_)} | "
                                        f"Buys {b1h_} Sells {s1h_} | Buy% {bp_:.0f}% | Vol spike {vs_:.1f}x | "
                                        f"Narrative #{nar_}. Is this worth aping? 1 sharp sentence — max 15 words.",
                                        fallback="",
                                        max_tokens=60,
                                        inject_market=False
                                    ),
                                    timeout=12
                                )
                                if ai_v and ai_v.strip():
                                    try:
                                        await app.bot.edit_message_text(
                                            chat_id=chat_id, message_id=msg_id,
                                            text=card + f"\n\U0001f9e0 *Kayo:* _{ai_v}_",
                                            parse_mode="Markdown",
                                            reply_markup=buttons,
                                            disable_web_page_preview=True,
                                        )
                                    except Exception:
                                        # Edit failed — try plain text
                                        try:
                                            plain_ai = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', ai_v)
                                            await app.bot.edit_message_text(
                                                chat_id=chat_id, message_id=msg_id,
                                                text=card + f"\n🧠 Kayo: {plain_ai}",
                                                reply_markup=buttons,
                                                disable_web_page_preview=True,
                                            )
                                        except Exception:
                                            pass
                            except Exception:
                                pass  # AI verdict is optional — alert already sent
                        
                        asyncio.create_task(_add_ai_verdict(
                            sent_msg.message_id, GROUP_CHAT_ID,
                            sym, alert_type, fdv, liq, ch5m, ch1h,
                            b1h, s1h, buy_pct, vol_spike, nar
                        ))
                        
                        await asyncio.sleep(1)  # rate limit between alerts

            logger.info(f"[SCANNER] Cycle done — {alert_count} alerts sent")

        except Exception as e:
            logger.error(f"[bg_main_scanner] {e}", exc_info=True)

        await asyncio.sleep(30)


async def bg_followup_tracker(app: Application):
    """
    Tracks tokens Kayo dropped — fires celebratory or warning follow-ups.
    Checks every 5 minutes. Fires:
    • 5x — "heads up, this is 5x from my call"
    • 10x — "yo remember this? 10x already 🚀"
    • Rug — "🚨 rug alert — the one I flagged earlier just died"
    Auto-expires entries older than 7 days.
    """
    await asyncio.sleep(120)  # wait 2min after boot
    while True:
        try:
            now = time.time()
            expired = []
            for addr, info in list(dropped_calls.items()):
                # Expire after 7 days
                if now - info.get("time", now) > 604800:
                    expired.append(addr); continue

                entry = info.get("entry_price", 0)
                if not entry: continue

                # Fetch current price
                try:
                    pairs = await dex_pairs_by_token(addr)
                    if not pairs: continue
                    cur_price = float(pairs[0].get("priceUsd", 0) or 0)
                    cur_mcap  = float(pairs[0].get("fdv", 0) or 0)
                    ch24h_now = float((pairs[0].get("priceChange") or {}).get("h24", 0) or 0)
                    liq_now   = float((pairs[0].get("liquidity") or {}).get("usd", 0) or 0)
                except Exception:
                    continue

                if cur_price <= 0: continue
                mult = cur_price / entry  # how many X since drop

                sym  = info.get("sym", "???")
                name = info.get("name", "")
                chat = info.get("chat_id", GROUP_CHAT_ID)
                age_since = _age(int(info.get("time", now) * 1000))

                # ── 10x milestone ────────────────────────────────────────
                if mult >= 10 and not info.get("alerted_10x"):
                    ai_verdict = await ai_ask(
                        f"Token ${sym} that was called at ${entry:.6f} is now ${cur_price:.6f} — that's {mult:.1f}x. "
                        f"MCap now {_usd(cur_mcap)}, was {_usd(info.get('mcap_entry',0))}. "
                        f"24h change: {ch24h_now:+.1f}%. "
                        "Give one hype but grounded sentence about this move. Did they take profit already?",
                        inject_market=True
                    )
                    msg = (
                        f"\U0001f525\U0001f680 *REMEMBER THIS ONE?*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Kayo dropped *${sym}* {age_since} ago — it just hit *{mult:.1f}x* \U0001f4b0\n"
                        f"Entry: `{_price(entry)}` \u2192 Now: `{_price(cur_price)}`\n"
                        f"MCap then: `{_usd(info.get('mcap_entry',0))}` \u2192 Now: `{_usd(cur_mcap)}`\n"
                        f"`{addr}`\n\n"
                        f"\U0001f9e0 _{ai_verdict}_"
                    )
                    try:
                        await app.bot.send_message(chat, msg, parse_mode="Markdown",
                                                   reply_markup=scan_buttons(addr, sym),
                                                   disable_web_page_preview=True)
                        dropped_calls[addr]["alerted_10x"] = True
                        # ── Feed pattern memory (win) ───────────────
                        pm_key = f"{info.get('alert_type','?')}:{info.get('narrative','?')}"
                        pm = pattern_memory.setdefault(pm_key, {"wins":0,"losses":0,"total":0,"avg_mult":0.0,"last_updated":0})
                        pm["wins"]  += 1
                        pm["total"] += 1
                        pm["avg_mult"] = (pm["avg_mult"] * (pm["total"]-1) + mult) / pm["total"]
                        pm["last_updated"] = time.time()
                        asyncio.create_task(_save())
                    except Exception as e:
                        logger.debug(f"followup 10x: {e}")

                # ── 5x milestone (only if 10x not fired yet) ─────────────
                elif mult >= 5 and not info.get("alerted_5x") and not info.get("alerted_10x"):
                    msg = (
                        f"\U0001f4c8 *KAYO CALL UPDATE — ${sym}*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Called {age_since} ago at `{_price(entry)}` \u2014 now *{mult:.1f}x* \U0001f4b0\n"
                        f"Current: `{_price(cur_price)}`  MCap: `{_usd(cur_mcap)}`\n"
                        f"24h: {_pct(ch24h_now)}\n"
                        f"`{addr}`"
                    )
                    try:
                        await app.bot.send_message(chat, msg, parse_mode="Markdown",
                                                   reply_markup=scan_buttons(addr, sym),
                                                   disable_web_page_preview=True)
                        dropped_calls[addr]["alerted_5x"] = True
                        asyncio.create_task(_save())
                    except Exception as e:
                        logger.debug(f"followup 5x: {e}")

                # ── Rug / dump alert ─────────────────────────────────────
                elif (mult < 0.15 or liq_now < 500 or ch24h_now < -80) and not info.get("alerted_rug"):
                    rug_reason = "liquidity pulled" if liq_now < 500 else f"price down {(1-mult)*100:.0f}% from call"
                    mcap_entry = info.get("mcap_entry", 0)
                    # Estimate current mcap from price multiple × entry mcap
                    mcap_now   = mcap_entry * mult if mcap_entry > 0 else 0
                    mcap_drop_pct = (1 - mult) * 100 if mult < 1 else 0
                    liq_ratio_now = (liq_now / max(mcap_now, 1) * 100) if mcap_now > 0 else 0
                    msg = (
                        f"\U0001f6a8 *RUG ALERT — ${sym}*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Kayo flagged *${sym}* {age_since} ago — {rug_reason}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"\U0001f4b5 Price:  `{_price(entry)}` \u2192 `{_price(cur_price)}`  ({_pct((mult-1)*100)})\n"
                        + (f"\U0001f4a0 MCap:   `{_usd(mcap_entry)}` \u2192 `{_usd(mcap_now)}`  (\u2193{mcap_drop_pct:.0f}%)\n" if mcap_entry > 0 else "")
                        + f"\U0001f30a Liq:    `{_usd(liq_now)}`"
                        + (f"  ({liq_ratio_now:.1f}% of MCap)" if mcap_now > 0 else "")
                        + f"\n\U0001f4c8 24h:    {_pct(ch24h_now)}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"\U0001f64f GG if you exited early. Always set a stop loss.\n"
                        f"`{addr}`"
                    )
                    try:
                        await app.bot.send_message(chat, msg, parse_mode="Markdown",
                                                   disable_web_page_preview=True)
                        dropped_calls[addr]["alerted_rug"] = True
                        # ── Feed pattern memory (loss) ──────────────
                        pm_key2 = f"{info.get('alert_type','?')}:{info.get('narrative','?')}"
                        pm2 = pattern_memory.setdefault(pm_key2, {"wins":0,"losses":0,"total":0,"avg_mult":0.0,"last_updated":0})
                        pm2["losses"] += 1
                        pm2["total"]  += 1
                        pm2["last_updated"] = time.time()
                        asyncio.create_task(_save())
                    except Exception as e:
                        logger.debug(f"followup rug: {e}")

            for addr in expired:
                dropped_calls.pop(addr, None)

        except Exception as e:
            logger.error(f"bg_followup_tracker: {e}", exc_info=True)
        await asyncio.sleep(300)  # check every 5 minutes


async def bg_new_launch_scanner(app: Application):
    """
    Every 60s: polls DexScreener token-profiles/latest + boosts/latest
    Scores each new token and alerts if score >= 40
    Dedup via seen_alert_ids (Redis-persisted)
    """
    await asyncio.sleep(60)

    while True:
        try:
            now_ms = time.time() * 1000

            profiles, boosts = await asyncio.gather(
                dex_token_profiles_latest(),
                dex_boosts_latest(),
            )
            sol_prof  = [p for p in profiles if p.get("chainId") == "solana"]
            sol_boost = [b for b in boosts   if b.get("chainId") == "solana"]

            addrs = list(set(
                [p.get("tokenAddress", "") for p in sol_prof  if p.get("tokenAddress")] +
                [b.get("tokenAddress", "") for b in sol_boost if b.get("tokenAddress")]
            ))
            addrs = [a for a in addrs if a and a not in blacklist]

            if not addrs:
                await asyncio.sleep(60); continue

            boost_map  = {b.get("tokenAddress", ""): b.get("amount", 0) for b in sol_boost}
            prof_links = {p.get("tokenAddress", ""): p.get("links", []) or [] for p in sol_prof}

            pairs_data = await dex_batch(addrs[:20])

            for pd in pairs_data:
                addr  = (pd.get("baseToken") or {}).get("address", "")
                if not addr or addr in blacklist: continue

                alert_id = hashlib.md5(f"{addr}:newlaunch:{int(time.time()//86400)}".encode()).hexdigest()[:16]
                if _seen_check(seen_alert_ids, alert_id): continue

                base    = pd.get("baseToken", {})
                sym     = base.get("symbol", "???")
                name    = base.get("name", "")
                fdv     = float(pd.get("fdv", 0) or 0)
                liq     = float((pd.get("liquidity") or {}).get("usd", 0) or 0)
                ch1h    = float((pd.get("priceChange") or {}).get("h1", 0) or 0)
                b1h     = int(((pd.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                s1h     = int(((pd.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                created = int(pd.get("pairCreatedAt", 0) or 0)
                age_min = (now_ms - created) / 60000 if created else 9999
                links   = prof_links.get(addr, [])
                boost   = boost_map.get(addr, 0)
                buy_pct = b1h / max(b1h + s1h, 1) * 100

                eff_fdv = max(fdv, liq * 3)
                if liq < 200: continue                            # need liquidity
                if eff_fdv > 500_000: continue                    # hard $500k cap
                if eff_fdv > 0 and eff_fdv < 500: continue        # dust token
                # fdv=0 is OK for brand new tokens — skip the $2000 floor

                # Scoring
                score = 0
                if age_min < 30:   score += 40
                elif age_min < 90:  score += 25
                elif age_min < 240: score += 10
                if ch1h > 100: score += 35
                elif ch1h > 50: score += 25
                elif ch1h > 15: score += 15
                if buy_pct > 65: score += 20
                elif buy_pct > 55: score += 10
                if any(l.get("type") == "twitter" for l in links):  score += 15
                if any(l.get("type") == "telegram" for l in links): score += 10
                if boost > 0: score += min(20, boost)
                if liq / max(fdv, 1) > 0.05: score += 10
                if b1h > 20: score += 10

                if score < 10: continue  # catch anything with ANY signal
                if GROUP_CHAT_ID == 0: continue

                # ── Anti-spam: never re-drop the same coin ─────────────
                if addr in dropped_calls:
                    last_drop = dropped_calls[addr].get("time", 0)
                    last_price = dropped_calls[addr].get("entry_price", 0)
                    cur_p = float(pd.get("priceUsd", 0) or 0)
                    price_change_since = abs(cur_p - last_price) / max(last_price, 1e-12) * 100
                    if time.time() - last_drop < 21600: continue   # 6h hard cooldown
                    if price_change_since < 20: continue            # 20%+ move required

                _seen_add(seen_alert_ids, alert_id)
                asyncio.create_task(_save())

                tw_link = next((lk.get("url", "") for lk in links if lk.get("type") == "twitter"), "")
                tg_link = next((lk.get("url", "") for lk in links if lk.get("type") == "telegram"), "")
                web_link= next((lk.get("url", "") for lk in links if lk.get("type") == "website"), "")
                nar     = detect_narrative(f"{name} {sym}")
                v5m_new = float((pd.get("volume") or {}).get("m5", 0) or 0)
                v1h_new = float((pd.get("volume") or {}).get("h1", 0) or 0)
                v24h_new= float((pd.get("volume") or {}).get("h24", 0) or 0)
                ch5m_new= float((pd.get("priceChange") or {}).get("m5", 0) or 0)
                ch6h_new= float((pd.get("priceChange") or {}).get("h6", 0) or 0)
                ch24h_new=float((pd.get("priceChange") or {}).get("h24", 0) or 0)
                b5m_new = int(((pd.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
                s5m_new = int(((pd.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0)
                avg_5m  = v1h_new / 12 if v1h_new > 0 else 1
                vs_new  = v5m_new / max(avg_5m, 1)
                cur_price_nl = float(pd.get("priceUsd", 0) or 0)
                mscore_nl = min(100, int(abs(ch1h) + buy_pct/2 + vs_new*10))

                tok_nl = {
                    "address": addr, "sym": sym, "name": name,
                    "price": cur_price_nl, "fdv": fdv, "mcap": fdv,
                    "liq": liq, "liq_ratio": liq/max(fdv,1)*100,
                    "ch5m": ch5m_new, "ch1h": ch1h, "ch6h": ch6h_new, "ch24h": ch24h_new,
                    "v5m": v5m_new, "v1h": v1h_new, "v24h": v24h_new,
                    "b5m": b5m_new, "s5m": s5m_new, "b1h": b1h, "s1h": s1h, "b24h": 0, "s24h": 0,
                    "buy_pct": buy_pct, "vol_spike": vs_new,
                    "risk_score": max(0, 70 - score),
                    "red_flags": [], "green_flags": [],
                    "sell_tax": 0, "buy_tax": 0, "is_honeypot": False,
                    "lp_locked": liq/max(fdv,1) > 0.08,
                    "is_renounced": False,
                    "created": created, "narrative": nar,
                    "tw_link": tw_link, "tg_link": tg_link, "web_link": web_link,
                    "boost_active": boost, "has_profile": True, "has_ad": boost > 0,
                    "pair_addr": pd.get("pairAddress", ""),
                    "mscore": mscore_nl,
                }
                if boost > 0:  tok_nl["green_flags"].append(f"\U0001f4b0 Boosted ({boost} pts)")
                if tw_link:    tok_nl["green_flags"].append("\U0001f426 Twitter active")
                if tg_link:    tok_nl["green_flags"].append("\U0001f4e8 Telegram community")

                # Determine display type — new vs unusual
                nl_type = "new" if age_min < 60 else ("unusual" if vs_new >= 3 else "gem")

                ai = await ai_ask(
                    f"Solana token ${sym} (age {int(age_min)}min, score {score}/100): "
                    f"MCap {_usd(fdv)}, liq {_usd(liq)}, 1h {_pct(ch1h)}, "
                    f"buys {b1h} / sells {s1h}, buy% {buy_pct:.0f}%, vol spike {vs_new:.1f}x. "
                    f"Narrative: #{nar}. Boosted: {boost > 0}. Has Twitter: {bool(tw_link)}. "
                    "Is this worth aping? 1 sharp sentence — what's the play.",
                    fallback="",
                    inject_market=True
                )
                card_nl = build_alert_card(tok_nl, nl_type, ai)
                try:
                    msg_sent_nl = await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=card_nl,
                        parse_mode="Markdown",
                        reply_markup=scan_buttons(addr, sym, tok_nl.get("pair_addr","")),
                        disable_web_page_preview=True,
                    )
                    dropped_calls[addr] = {
                        "sym": sym, "name": name,
                        "entry_price": cur_price_nl,
                        "mcap_entry": fdv,
                        "time": time.time(),
                        "alert_type": nl_type,
                        "msg_id": msg_sent_nl.message_id,
                        "chat_id": GROUP_CHAT_ID,
                        "alerted_10x": False,
                        "alerted_5x":  False,
                        "alerted_rug": False,
                    }
                    asyncio.create_task(_save())
                    logger.info(f"[NEW LAUNCH] ${sym} score={score} type={nl_type}")
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.warning(f"new launch send: {e}")

        except Exception as e:
            logger.error(f"bg_new_launch_scanner: {e}", exc_info=True)
        await asyncio.sleep(90)



async def bg_established_scanner(app: Application):
    """
    Scans for ESTABLISHED coins (age >2h, mcap <$500k) that are
    suddenly pumping again — catches rebounded coins, reactivated old gems,
    and migrated tokens that weren't new when they moved.
    Runs every 3 minutes.
    """
    await asyncio.sleep(180)
    seen_est: dict = {}  # addr -> last_alert_time

    while True:
        try:
            now = time.time()
            QUERIES = [
                "solana meme", "solana ai", "solana dog", "solana cat",
                "solana gaming", "solana pump", "solana pepe", "solana frog"
            ]
            pairs_map = await dex_multi_search(QUERIES)

            for addr, p in pairs_map.items():
                if addr in blacklist: continue
                if now - seen_est.get(addr, 0) < 10800: continue  # 3h per coin

                base    = p.get("baseToken", {})
                sym     = base.get("symbol", "???")
                name    = base.get("name", "")
                fdv     = float(p.get("fdv", 0) or 0)
                mcap    = float(p.get("marketCap", 0) or fdv)
                liq     = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                ch5m    = float((p.get("priceChange") or {}).get("m5", 0) or 0)
                ch1h    = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                ch6h    = float((p.get("priceChange") or {}).get("h6", 0) or 0)
                ch24h   = float((p.get("priceChange") or {}).get("h24", 0) or 0)
                v5m     = float((p.get("volume") or {}).get("m5", 0) or 0)
                v1h     = float((p.get("volume") or {}).get("h1", 0) or 0)
                v24h    = float((p.get("volume") or {}).get("h24", 0) or 0)
                b1h     = int(((p.get("txns") or {}).get("h1") or {}).get("buys",  0) or 0)
                s1h     = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                b5m     = int(((p.get("txns") or {}).get("m5") or {}).get("buys",  0) or 0)
                s5m     = int(((p.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0)
                created = int(p.get("pairCreatedAt", 0) or 0)
                age_min = (now * 1000 - created) / 60000 if created else 9999

                # Only ESTABLISHED coins: >2 hours old, sub $500k, real liq
                if age_min < 120:   continue  # too new — main scanner handles those
                if fdv > 500_000:   continue  # above our cap
                if fdv < 5_000:     continue  # ghost token
                if liq < 500:      continue  # too thin

                avg_5m_vol = v1h / 12 if v1h > 0 else 1
                vol_spike  = v5m / max(avg_5m_vol, 1)
                buy_pct    = b1h / max(b1h + s1h, 1) * 100

                # Must be buying, not selling
                if buy_pct < 50: continue

                # Must have a real move — not just noise
                qualifies = False
                est_type  = None

                # Pattern 1: Old coin suddenly pumping hard — classic "second wind"
                if ch1h >= 8 and buy_pct >= 52 and b1h >= 5:
                    qualifies = True; est_type = "pump"

                # Pattern 2: Volume explosion on quiet coin (possible manipulation or kol call)
                elif vol_spike >= 3 and abs(ch5m) < 8 and b1h > 8 and buy_pct > 55:
                    qualifies = True; est_type = "whale"

                # Pattern 3: Consistent 6h grind — real accumulation in progress
                elif ch6h >= 15 and ch1h >= 5 and buy_pct >= 55 and b1h > 8:
                    qualifies = True; est_type = "gem"

                # Pattern 4: Rebound after dump — was down 24h but now recovering
                elif ch24h < -30 and ch1h >= 15 and ch5m >= 5 and buy_pct >= 60:
                    qualifies = True; est_type = "unusual"

                if not qualifies: continue

                # Anti-spam: never re-alert same coin from here if main scanner already got it
                alert_id_est = re.sub(r"[^a-z0-9]","",sym.lower())[:8] + f":{est_type}:{int(now/3600)}"
                alert_id_est = hashlib.md5(alert_id_est.encode()).hexdigest()[:16]
                if _seen_check(seen_alert_ids, alert_id_est): continue
                _seen_add(seen_alert_ids, alert_id_est)

                seen_est[addr] = now

                nar    = detect_narrative(f"{name} {sym}")
                info   = p.get("info") or {}
                links  = info.get("socials") or []
                tw_lnk = next((s.get("url","") for s in links if s.get("type","") in ("twitter","x")), "")
                tg_lnk = next((s.get("url","") for s in links if s.get("type","") == "telegram"), "")
                liq_ratio = liq / max(fdv, 1) * 100
                mscore = min(100, int(abs(ch1h) + buy_pct/2 + vol_spike*10))

                tok_est = {
                    "address": addr, "sym": sym, "name": name,
                    "price": float(p.get("priceUsd", 0) or 0),
                    "fdv": fdv, "mcap": mcap, "liq": liq, "liq_ratio": liq_ratio,
                    "ch5m": ch5m, "ch1h": ch1h, "ch6h": ch6h, "ch24h": ch24h,
                    "v5m": v5m, "v1h": v1h, "v24h": v24h,
                    "b5m": b5m, "s5m": s5m, "b1h": b1h, "s1h": s1h, "b24h": 0, "s24h": 0,
                    "buy_pct": buy_pct, "vol_spike": vol_spike,
                    "risk_score": 35, "red_flags": [], "green_flags": [],
                    "sell_tax": 0, "buy_tax": 0, "is_honeypot": False,
                    "lp_locked": liq_ratio > 8,
                    "is_renounced": False,
                    "created": created, "narrative": nar,
                    "tw_link": tw_lnk, "tg_link": tg_lnk, "web_link": "",
                    "boost_active": 0, "has_profile": False, "has_ad": False,
                    "pair_addr": p.get("pairAddress", ""),
                    "mscore": mscore,
                }

                ai_est = await ai_ask(
                    f"Established Solana token ${sym} (age {int(age_min)}min, ${_usd(fdv)} mcap) "
                    f"just lit up: 1h {_pct(ch1h)}, 5m {_pct(ch5m)}, 6h {_pct(ch6h)}, "
                    f"24h {_pct(ch24h)}, buy% {buy_pct:.0f}%, vol spike {vol_spike:.1f}x. "
                    f"This is an older coin picking up steam again — #{nar} narrative. "
                    "Is this a real second wind or a dead cat bounce? "
                    "1 sharp degen sentence — ape or skip?",
                    fallback="",
                    inject_market=True
                )
                # Prefix to show this is an ESTABLISHED coin (not new)
                age_label = f"{int(age_min//60)}h{int(age_min%60)}m" if age_min > 60 else f"{int(age_min)}m"
                ai_est_full = f"[Aged {age_label} — not new] {ai_est}"

                card_est = build_alert_card(tok_est, est_type, ai_est_full)
                if GROUP_CHAT_ID != 0:
                    try:
                        msg_est = await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=card_est,
                            parse_mode="Markdown",
                            reply_markup=scan_buttons(addr, sym, tok_est.get("pair_addr","")),
                            disable_web_page_preview=True,
                        )
                        dropped_calls[addr] = {
                            "sym": sym, "name": name,
                            "entry_price": tok_est["price"],
                            "mcap_entry": mcap,
                            "time": now,
                            "alert_type": est_type,
                            "msg_id": msg_est.message_id,
                            "chat_id": GROUP_CHAT_ID,
                            "alerted_10x": False,
                            "alerted_5x":  False,
                            "alerted_rug": False,
                        }
                        asyncio.create_task(_save())
                        logger.info(f"[ESTABLISHED] {est_type} ${sym} age={age_label} {_usd(mcap)}")
                        await asyncio.sleep(4)
                    except Exception as e:
                        logger.warning(f"established alert: {e}")

        except Exception as e:
            logger.error(f"bg_established_scanner: {e}", exc_info=True)
        await asyncio.sleep(180)  # run every 3 minutes



async def bg_narrative_news_scanner(app: Application):
    """
    Every 10 min: pulls REAL social signals from Pump.fun + RSS news + CoinGecko
    → feeds to AI → AI generates DexScreener search terms → hunts pumping tokens.
    No Twitter needed. All free, all working.
    """
    await asyncio.sleep(90)

    while True:
        try:
            signals = await fetch_social_signals()

            # Build context from all sources
            headlines   = signals.get("news", [])[:12]
            pump_latest = signals.get("pump_latest", [])[:10]
            pump_trend  = signals.get("pump_trending", [])[:5]
            cg_coins    = signals.get("cg_trending", [])[:5]

            # Extract pump.fun coin names/themes as social signal
            pump_names = [f"${c.get('symbol','?')} ({c.get('name','?')})" for c in pump_latest]
            pump_trend_names = [f"${c.get('symbol','?')}" for c in pump_trend]
            cg_names = [c.get("item",{}).get("symbol","?") for c in cg_coins]

            context = ""
            if headlines:
                context += "NEWS HEADLINES:\n" + "\n".join(f"- {h}" for h in headlines[:8]) + "\n\n"
            if pump_names:
                context += f"PUMP.FUN LATEST LAUNCHES: {', '.join(pump_names[:8])}\n"
            if pump_trend_names:
                context += f"PUMP.FUN TRENDING: {', '.join(pump_trend_names)}\n"
            if cg_names:
                context += f"COINGECKO TRENDING: {', '.join(cg_names)}\n"

            if not context.strip():
                await asyncio.sleep(600); continue

            logger.info(f"[NARRATIVE] Signal context built — {len(headlines)} headlines, {len(pump_latest)} pump launches")

            # Ask AI to extract DexScreener search terms from all signals
            ai_terms_raw = await ai_ask(
                f"Here are REAL-TIME crypto social signals:\n{context}\n"
                "Based on these signals, what 6 short search terms (1-2 words each) would find "
                "pumping Solana meme coins on DexScreener right now? "
                "Focus on themes from the news headlines and trending coin names. "
                "Output ONLY the terms, one per line. No explanations.",
                fallback="",
                max_tokens=100,
                inject_market=False
            )
            if not ai_terms_raw:
                await asyncio.sleep(600); continue

            search_terms = [t.strip().lower() for t in ai_terms_raw.strip().split("\n")
                           if t.strip() and len(t.strip()) > 1][:6]
            logger.info(f"[NARRATIVE] AI terms: {search_terms}")

            if GROUP_CHAT_ID == 0:
                await asyncio.sleep(600); continue

            found_count = 0
            now = time.time()
            for term in search_terms:
                try:
                    pairs = await dex_search_pairs(f"solana {term}")
                    if not pairs: continue
                    for p in pairs[:3]:
                        addr = (p.get("baseToken") or {}).get("address", "")
                        if not addr or addr in blacklist: continue
                        fdv  = float(p.get("fdv", 0) or 0)
                        liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                        if not (1_000 <= fdv <= 500_000) or liq < 500: continue
                        ch1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                        if ch1h < 5: continue
                        b1h  = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                        s1h  = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                        buy_pct = b1h / max(b1h+s1h, 1) * 100
                        if buy_pct < 52: continue

                        alert_id = hashlib.md5(f"{addr}:nar:{int(now//7200)}".encode()).hexdigest()[:16]
                        if _seen_check(seen_alert_ids, alert_id): continue
                        _seen_add(seen_alert_ids, alert_id)

                        sym   = (p.get("baseToken") or {}).get("symbol", "???")
                        name  = (p.get("baseToken") or {}).get("name", "")
                        nar   = detect_narrative(f"{sym} {name} {term}")
                        price = float(p.get("priceUsd", 0) or 0)
                        v5m   = float((p.get("volume") or {}).get("m5", 0) or 0)
                        v1h   = float((p.get("volume") or {}).get("h1", 0) or 0)
                        ch5m  = float((p.get("priceChange") or {}).get("m5", 0) or 0)
                        avg5m = v1h / 12 if v1h > 0 else 1
                        vs    = v5m / max(avg5m, 1)

                        ai_why = await ai_ask(
                            f"Token ${sym} ({name}) is up +{ch1h:.0f}% on Solana. "
                            f"The current narrative/theme is: \'{term}\'. "
                            "Why might this be relevant right now? 1 sharp sentence, max 12 words.",
                            fallback="Riding current narrative — strong buy pressure.",
                            max_tokens=50, inject_market=False
                        )
                        tok = {
                            "address": addr, "sym": sym, "name": name,
                            "price": price, "fdv": fdv, "mcap": fdv, "liq": liq,
                            "liq_ratio": liq/max(fdv,1)*100,
                            "ch5m": ch5m, "ch1h": ch1h, "ch6h": 0.0, "ch24h": 0.0,
                            "v5m": v5m, "v1h": v1h, "v24h": 0,
                            "b5m": 0, "s5m": 0, "b1h": b1h, "s1h": s1h, "b24h": 0, "s24h": 0,
                            "buy_pct": buy_pct, "vol_spike": vs,
                            "risk_score": 40, "red_flags": [], "green_flags": [],
                            "sell_tax": 0, "buy_tax": 0, "is_honeypot": False,
                            "lp_locked": False, "is_renounced": False,
                            "created": int(p.get("pairCreatedAt", 0) or 0),
                            "narrative": nar, "tw_link": "", "tg_link": "", "web_link": "",
                            "boost_active": 0, "has_profile": False, "has_ad": False,
                            "pair_addr": p.get("pairAddress", ""),
                            "mscore": min(100, int(abs(ch1h) + buy_pct/2 + vs*10)),
                        }
                        try:
                            await app.bot.send_message(
                                chat_id=GROUP_CHAT_ID,
                                text=build_alert_card(tok, "narrative", ai_why),
                                parse_mode="Markdown",
                                reply_markup=scan_buttons(addr, sym, tok["pair_addr"]),
                                disable_web_page_preview=True,
                            )
                            found_count += 1
                            logger.info(f"[NARRATIVE] ${sym} via '{term}': +{ch1h:.0f}%")
                            await asyncio.sleep(2)
                        except Exception as e:
                            logger.warning(f"narrative send {sym}: {e}")
                except Exception as e:
                    logger.debug(f"narrative term '{term}': {e}")
                await asyncio.sleep(1)

            logger.info(f"[NARRATIVE] Cycle done — {found_count} alerts, {len(search_terms)} AI terms")

        except Exception as e:
            logger.error(f"bg_narrative_news_scanner: {e}", exc_info=True)

        await asyncio.sleep(600)


async def bg_trending_metas_scanner(app: Application):
    """
    Every 2h: post a TRENDING METAS digest with DIFFERENT coins.
    Sources GeckoTerminal trending (not keyword search) so same coin
    can't spam. Each coin only appears ONCE. Min 3 unique coins to post.
    """
    await asyncio.sleep(300)  # wait 5min after startup
    last_run     = 0
    posted_addrs: Dict[str, float] = {}  # addr → timestamp, 6h cooldown

    while True:
        try:
            now = time.time()
            # Only run every 2 hours
            if now - last_run < 7200:
                await asyncio.sleep(120)
                continue
            last_run = now

            if GROUP_CHAT_ID == 0:
                await asyncio.sleep(120)
                continue

            # ── Pull from GeckoTerminal trending (real coins, no keyword search) ──
            gt_trend_pools: List[Dict] = []
            try:
                async with aiohttp.ClientSession() as s:
                    for pg in [1, 2]:
                        async with s.get(
                            f"https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page={pg}",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            d = await r.json()
                            gt_trend_pools += d.get("data", [])
                        await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"metas GT fetch: {e}")

            if not gt_trend_pools:
                await asyncio.sleep(120)
                continue

            # ── Filter: sub-$500k, real buyers, positive momentum ──
            candidates = []
            seen_in_this_run: set = set()

            for pool in gt_trend_pools:
                tok = gt_parse_pool(pool)
                if not tok:
                    continue
                addr = tok["address"]
                # Skip duplicates within this run
                if addr in seen_in_this_run:
                    continue
                seen_in_this_run.add(addr)
                # Skip recently posted coins (6h cooldown per coin)
                if now - posted_addrs.get(addr, 0) < 21600:
                    continue
                if addr in blacklist:
                    continue

                fdv     = float(tok.get("fdv", 0) or 0)
                liq     = float(tok.get("liq", 0) or 0)
                ch1h    = float(tok.get("ch1h", 0) or 0)
                b1h     = int(tok.get("b1h", 0) or 0)
                s1h     = int(tok.get("s1h", 0) or 0)
                buy_pct = b1h / max(b1h + s1h, 1) * 100
                sym     = tok.get("sym", "?")
                nar     = detect_narrative(f"{tok.get('name','')} {sym}")

                if not (1_000 < fdv <= 500_000):  continue
                if liq < 500:                      continue
                if buy_pct < 52:                   continue
                if ch1h < 3:                       continue
                if b1h < 3:                        continue

                candidates.append((ch1h, addr, sym, fdv, liq, buy_pct, nar))

            # Need at least 3 DIFFERENT coins to be worth posting
            if len(candidates) < 3:
                logger.info(f"[METAS] Only {len(candidates)} unique coins — skipping post")
                await asyncio.sleep(120)
                continue

            candidates.sort(reverse=True)
            top = candidates[:6]  # max 6 coins per digest

            # ── AI summary of what's hot ──
            coin_desc = ", ".join(
                f"${s[2]} {s[6]} +{s[0]:.0f}% buy%={s[5]:.0f}%"
                for s in top[:4]
            )
            ai_summary = await ai_ask(
                f"Solana degen plays right now: {coin_desc}. "
                "Which 1-2 have the best momentum? 2 sentences max, be sharp.",
                fallback="",
                inject_market=False,
            )

            # ── Build and send the card ──
            lines_out = [
                "\U0001f525 *TRENDING META — DEGEN PLAYS* _(sub-$500k only)_",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ]
            for ch, addr, sym, fdv, liq, bp, nar in top:
                emoji = "🟢" if ch >= 10 else "🟡"
                lines_out.append(
                    f"• *${sym}* #{nar.upper()}  MCap:`{_usd(fdv)}`  "
                    f"1h:{emoji} {ch:+.1f}%  Buy:{bp:.0f}%"
                )
                posted_addrs[addr] = now  # mark as posted

            if ai_summary:
                lines_out.append(f"\n🧠 _{ai_summary}_")

            try:
                await app.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text="\n".join(lines_out),
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                logger.info(f"[METAS] Posted {len(top)} unique coins — next in 2h")
            except Exception as e:
                logger.warning(f"metas send: {e}")

        except Exception as e:
            logger.error(f"bg_trending_metas: {e}", exc_info=True)

        await asyncio.sleep(120)


async def bg_price_alert_checker(app: Application):
    """Check price alerts every 45s."""
    while True:
        try:
            active = [a for a in user_alerts if not a.get("triggered")]
            if active:
                addrs  = list(set([a["addr"] for a in active]))
                pairs  = await dex_batch(addrs[:20])
                prices = {
                    pd.get("baseToken", {}).get("address", ""): float(pd.get("priceUsd", 0) or 0)
                    for pd in pairs
                }
                for alert in active:
                    cur = prices.get(alert["addr"], 0)
                    if cur <= 0: continue
                    hit = (alert["direction"] == "above" and cur >= alert["target"]) or \
                          (alert["direction"] == "below" and cur <= alert["target"])
                    if hit:
                        alert["triggered"] = True; _save()
                        try:
                            await app.bot.send_message(
                                chat_id=alert["uid"],
                                text=(
                                    f"🔔 *PRICE ALERT TRIGGERED*\n"
                                    f"*${alert['sym']}* hit {_price(cur)}\n"
                                    f"Your target: {alert['direction']} {_price(alert['target'])}"
                                ),
                                parse_mode="Markdown",
                            )
                        except Exception as e:
                            logger.debug(f"price alert send: {e}")
        except Exception as e:
            logger.error(f"bg_price_alert: {e}", exc_info=True)
        await asyncio.sleep(45)


async def bg_watchlist_scanner(app: Application):
    """Every 60s: check watched Twitter accounts for CA drops."""
    await asyncio.sleep(120)
    while True:
        if TWITTER_AUTH_TOKEN and watchlist:
            for username, data in list(watchlist.items()):
                try:
                    tweets = await tw_user_tweets(username, limit=5)
                    for tweet in tweets:
                        tid  = tweet.get("id", "")
                        text = tweet.get("text", "")
                        tid_key = hashlib.md5(f"{username}:{tid}".encode()).hexdigest()[:16]
                        if not tid or _seen_check(watchlist_seen, tid_key): continue
                        _seen_add(watchlist_seen, tid_key)
                        cas = extract_cas(text)
                        if not cas: continue
                        watchlist[username]["hits"] = watchlist[username].get("hits", 0) + 1
                        await _save()
                        for ca in cas[:2]:
                            pairs = await dex_pairs_by_token(ca)
                            if not pairs: continue
                            pd   = pairs[0]
                            sym  = pd.get("baseToken", {}).get("symbol", "???")
                            fdv  = float(pd.get("fdv", 0) or 0)
                            liq  = float((pd.get("liquidity") or {}).get("usd", 0) or 0)
                            ch1h = float((pd.get("priceChange") or {}).get("h1", 0) or 0)
                            if GROUP_CHAT_ID != 0:
                                await app.bot.send_message(
                                    chat_id=GROUP_CHAT_ID,
                                    text=(
                                        f"🚨 *CA DROP — @{username}*\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"*${sym}*\n"
                                        f"MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
                                        f"1h: {_pct(ch1h)}\n"
                                        f"_Tweet: {text[:160]}_\n"
                                        f"`{ca}`"
                                    ),
                                    parse_mode="Markdown",
                                    reply_markup=scan_buttons(ca, sym),
                                    disable_web_page_preview=True,
                                )
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"watchlist @{username}: {e}")
        # OrderedDict auto-trims via _seen_add; no manual clear needed
        await asyncio.sleep(60)


async def bg_reminder_checker(app: Application):
    """Check and fire reminders every 30s."""
    while True:
        try:
            now = datetime.utcnow()
            due = [r for r in reminders if datetime.fromisoformat(r["fire_at"]) <= now]
            for r in due:
                try:
                    await app.bot.send_message(
                        chat_id=r["chat_id"],
                        text=f"⏰ *REMINDER*\n{r['text']}",
                        parse_mode="Markdown",
                    )
                    reminders.remove(r)
                    await _save()
                except Exception as e:
                    logger.debug(f"reminder: {e}")
        except Exception as e:
            logger.error(f"bg_reminder: {e}", exc_info=True)
        await asyncio.sleep(30)


# ═══════════════════════════════════════════════════════════════
# POST INIT & MAIN
# ═══════════════════════════════════════════════════════════════
async def post_init(app: Application):
    global _redis
    _redis = _make_redis()
    if _redis:
        try:
            await _redis.ping()
            logger.info("✅ Redis connected and pinged OK")
        except Exception as e:
            logger.warning(f"Redis ping failed at startup ({e}) — will retry on save/load")
            # Don't set to None — let it try on actual operations
    await _load()
    # ── Full command list (all 48) — shown in private chat menu ──────────
    all_cmds = [
        # Scan & Analyze
        BotCommand("start",         "🦅 Welcome & quick menu"),
        BotCommand("help",          "📋 Full command list"),
        BotCommand("scan",          "🔬 Full token scan + AI verdict"),
        BotCommand("c",             "💰 Quick price check"),
        BotCommand("chart",         "📊 In-app chart (no DexScreener)"),
        BotCommand("price",         "💵 Live price: btc sol eth etc"),
        BotCommand("verify",        "🛡 Rug & honeypot check"),
        # Discover
        BotCommand("runners",       "🏃 Top Solana gainers now"),
        BotCommand("new",           "🆕 Brand new launches"),
        BotCommand("pump",          "🚀 Fresh 5m pumps"),
        BotCommand("gems",          "💎 Hidden gem finder"),
        BotCommand("boosted",       "⚡ Boosted tokens"),
        BotCommand("takeover",      "🏴 Community takeovers"),
        # Narratives & Trends
        BotCommand("trending",      "🔥 Trending metas now"),
        BotCommand("narrative",     "📖 Coins in a narrative"),
        BotCommand("explain",       "🧠 AI explains a narrative"),
        # News & AI
        BotCommand("news",          "📰 Latest news + AI summary"),
        BotCommand("ask",           "🤖 Ask Kayo AI anything"),
        BotCommand("sentiment",     "😤 Market mood & risk"),
        BotCommand("macro",         "🌍 Macro briefing"),
        BotCommand("markets",       "📈 Global market data"),
        BotCommand("index",         "😨 Fear & Greed index"),
        BotCommand("a",             "🔍 CoinGecko coin lookup"),
        # Twitter
        BotCommand("tt",            "🐦 Twitter sentiment for CA"),
        BotCommand("moni",          "👁 Monitor a KOL account"),
        BotCommand("watch",         "📡 Watch account for CA drops"),
        BotCommand("unwatch",       "🚫 Stop watching account"),
        BotCommand("watchlist",     "📋 Your watched accounts"),
        # Alerts & Portfolio
        BotCommand("alert",         "🔔 Set a price alert"),
        BotCommand("myalerts",      "🔔 View your price alerts"),
        BotCommand("delalert",      "🗑 Delete a price alert"),
        BotCommand("addport",       "➕ Add token to portfolio"),
        BotCommand("portfolio",     "💼 View portfolio P&L"),
        BotCommand("blacklist",     "⛔ Blacklist a rug token"),
        # Calls
        BotCommand("call",          "📢 Make a public alpha call"),
        BotCommand("mycalls",       "📂 Your calls history"),
        BotCommand("stop",          "🏁 Close a call + P&L"),
        BotCommand("leaderboard",   "🏆 Top callers leaderboard"),
        # Wallets
        BotCommand("trackwallet",   "👛 Track a Solana wallet"),
        BotCommand("mywallet",      "🔗 Link your wallet"),
        # XP & Social
        BotCommand("rank",          "⭐ Your XP rank"),
        BotCommand("gp",            "🏅 Group XP leaderboard"),
        BotCommand("dubs",          "🎉 Celebrate a win (+XP)"),
        BotCommand("gsum",          "💬 AI group chat summary"),
        BotCommand("remindme",      "⏰ Set a reminder"),
        # System
        BotCommand("autoresponder", "🤖 Toggle CA auto-scan"),
        BotCommand("status",        "⚙️ Bot health check"),
        BotCommand("ping",          "📶 Latency check"),
    ]

    # ── Group chat menu — top discovery commands only (keeps it clean) ──
    group_cmds = [
        BotCommand("scan",      "🔬 Full token scan + AI verdict"),
        BotCommand("c",         "💰 Quick price check"),
        BotCommand("chart",     "📊 In-app chart"),
        BotCommand("price",     "💵 Live price: btc sol eth"),
        BotCommand("verify",    "🛡 Rug & honeypot check"),
        BotCommand("runners",   "🏃 Top Solana gainers now"),
        BotCommand("new",       "🆕 Brand new launches"),
        BotCommand("pump",      "🚀 Fresh 5m pumps"),
        BotCommand("gems",      "💎 Hidden gem finder"),
        BotCommand("trending",  "🔥 Trending metas"),
        BotCommand("news",      "📰 News + AI summary"),
        BotCommand("ask",       "🤖 Ask Kayo AI anything"),
        BotCommand("sentiment", "😤 Market mood"),
        BotCommand("call",      "📢 Make a public call"),
        BotCommand("leaderboard","🏆 Top callers"),
        BotCommand("gsum",      "💬 AI group chat summary"),
        BotCommand("gp",        "🏅 Group XP leaderboard"),
        BotCommand("help",      "📋 Full command list"),
    ]

    try:
        from telegram import (
            BotCommandScopeAllPrivateChats,
            BotCommandScopeAllGroupChats,
            BotCommandScopeDefault,
        )
        # Private chats: full list — tap any command without typing
        await app.bot.set_my_commands(all_cmds, scope=BotCommandScopeAllPrivateChats())
        # Group chats: curated top commands
        await app.bot.set_my_commands(group_cmds, scope=BotCommandScopeAllGroupChats())
        # Default fallback
        await app.bot.set_my_commands(all_cmds, scope=BotCommandScopeDefault())
        logger.info(f"✅ {len(all_cmds)} private + {len(group_cmds)} group commands registered")
    except Exception as e:
        logger.warning(f"set_my_commands: {e}")
    logger.info(
        f"🦅 Kayo Brain v40 ready — "
        f"Groq: {'✅' if GROQ_API_KEY else '❌'} | "
        f"Gemini: {'✅' if GEMINI_API_KEY else '❌'} | "
        f"Group alerts: {'✅ '+str(GROUP_CHAT_ID) if GROUP_CHAT_ID != 0 else '❌ set GROUP_CHAT_ID'}"
    )


async def global_error_handler(u: Update, context):
    """Catch ALL errors from command handlers and show feedback to the user."""
    error = context.error
    logger.error(f"Handler error: {error}", exc_info=True)
    try:
        if u and u.effective_chat:
            await context.bot.send_message(
                chat_id=u.effective_chat.id,
                text=f"⚠️ Command failed: {str(error)[:100]}\nTry again or use /help"
            )
    except Exception:
        pass


def safe_command(fn):
    """Decorator that wraps any command handler with try/except.
    If the command crashes, the user gets an error message instead of silence."""
    async def wrapper(u: Update, c: ContextTypes.DEFAULT_TYPE):
        try:
            return await fn(u, c)
        except Exception as e:
            logger.error(f"Command {fn.__name__} failed: {e}", exc_info=True)
            try:
                if u and u.message:
                    await u.effective_message.reply_text(
                        f"⚠️ `{fn.__name__}` failed: {str(e)[:80]}\nTry /help or /ping"
                    )
            except Exception:
                pass
    return wrapper


# ═══════════════════════════════════════════════════════════════
# RICK-STYLE FEATURES — all powered by FREE APIs
# ═══════════════════════════════════════════════════════════════

# Track scanned tokens for /last, /hot, /ath, /groupburp
_scan_history = []  # [{ca, sym, mcap, ch1h, ch24h, time, uid}]
_ath_tracker = {}   # {ca: {sym, first_mcap, ath_mcap, first_seen}}

def _track_scan(t: Dict, uid: int = 0):
    """Record a token scan for leaderboards and history."""
    ca = t.get("address", "")
    if not ca: return
    entry = {
        "ca": ca, "sym": t.get("sym", "???"), "mcap": t.get("mcap", 0),
        "ch1h": t.get("ch1h", 0), "ch24h": t.get("ch24h", 0),
        "liq": t.get("liq", 0), "mscore": t.get("mscore", 0),
        "time": time.time(), "uid": uid
    }
    _scan_history.append(entry)
    if len(_scan_history) > 500: _scan_history.pop(0)
    # ATH tracking
    if ca not in _ath_tracker:
        _ath_tracker[ca] = {"sym": t.get("sym", "???"), "first_mcap": t.get("mcap", 0), "ath_mcap": t.get("mcap", 0), "first_seen": time.time()}
    else:
        if t.get("mcap", 0) > _ath_tracker[ca]["ath_mcap"]:
            _ath_tracker[ca]["ath_mcap"] = t.get("mcap", 0)

# ─── /dev — Deployer History ───────────────────────────────────
async def dex_token_creator(ca: str) -> str:
    """Get token deployer address from DexScreener."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{_DSX}/latest/dex/tokens/{ca}",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs") or []
                    if pairs:
                        # DexScreener doesn't expose deployer directly
                        # but we can show the base token info
                        return pairs[0].get("baseToken", {}).get("address", "")
    except: pass
    return ""

async def dev_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show deployer history — other tokens by same dev."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/dev <contract_address>`", parse_mode="Markdown"); return
    ca = c.args[0].strip()
    msg = await u.effective_message.reply_text("🔍 *Checking deployer history...*", parse_mode="Markdown")
    try:
        # Search DexScreener for tokens with similar names (proxy for same dev)
        # Also get the token info
        t = await asyncio.wait_for(full_token_scan(ca), timeout=15)
        if t.get("error"):
            await msg.edit_text(f"❌ {t['error']}"); return
        sym = t.get("sym", "???")
        # Search for other tokens with same symbol (potential dev connections)
        pairs = await dex_search_pairs(sym)
        same_name = [p for p in pairs if p.get("baseToken",{}).get("symbol","").upper() == sym.upper() and p.get("baseToken",{}).get("address","") != ca]
        # Security info
        sec_info = []
        if t.get("is_renounced"): sec_info.append("✅ Renounced")
        if t.get("lp_locked"): sec_info.append("🔒 LP Locked")
        if t.get("is_honeypot"): sec_info.append("🚨 Honeypot")
        if t.get("buy_tax",0) > 0 or t.get("sell_tax",0) > 0:
            sec_info.append(f"🧾 Tax: {t.get('buy_tax',0):.0f}%/{t.get('sell_tax',0):.0f}%")
        sec_str = "  ".join(sec_info) if sec_info else "⚠️ Unverified"
        card = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  👷 *DEPLOYER CHECK*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 *${_md(sym)}* — _{ _md(t.get('name',''))}_\n"
            f"📋 `{ca}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 MCap: `{_usd(t.get('mcap',0))}`  ·  Liq: `{_usd(t.get('liq',0))}`\n"
            f"🛡️ {sec_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 *Token Info*\n"
            f"  Age: {_age(t.get('created',0))}\n"
            f"  24h: {_pct(t.get('ch24h',0))}  ·  1h: {_pct(t.get('ch1h',0))}\n"
            f"  Momentum: {t.get('mscore',0)}/100\n"
        )
        if same_name:
            card += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            card += f"🔍 *Same-name tokens ({len(same_name)}):*\n"
            for p in same_name[:5]:
                p_sym = p.get("baseToken",{}).get("symbol","?")
                p_ca = p.get("baseToken",{}).get("address","")
                p_mcap = float(p.get("marketCap",0) or p.get("fdv",0) or 0)
                p_ch = (p.get("priceChange") or {}).get("h24",0)
                card += f"  ${_md(p_sym)} — {_usd(p_mcap)} — {_pct(p_ch)}\n"
            card += f"_⚠️ Same name ≠ same dev. Verify on-chain._\n"
        else:
            card += f"\n✅ No other tokens found with same name — likely unique.\n"
        card += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        # Track scan
        _track_scan(t, u.effective_user.id)
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True,
                            reply_markup=scan_buttons(ca, sym, t.get("pair_addr","")))
    except asyncio.TimeoutError:
        await msg.edit_text("❌ Scan timed out. Try again.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")

# ─── /top — Top Traders ────────────────────────────────────────
async def top_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show top traders for a token using DexScreener volume data."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/top <contract_address>`", parse_mode="Markdown"); return
    ca = c.args[0].strip()
    msg = await u.effective_message.reply_text("🔍 *Fetching top traders...*", parse_mode="Markdown")
    try:
        t = await asyncio.wait_for(full_token_scan(ca), timeout=15)
        if t.get("error"):
            await msg.edit_text(f"❌ {t['error']}"); return
        sym = t.get("sym", "???")
        # Buy/sell data as proxy for trader activity
        b1h, s1h = t.get("b1h",0), t.get("s1h",0)
        b24h = t.get("b24h",0)
        s24h = t.get("s24h",0)
        bp = t.get("buy_pct", 50)
        card = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  🏆 *TOP TRADER ACTIVITY*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 *${_md(sym)}*\n"
            f"📋 `{ca}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 *Transaction Activity*\n"
            f"  1h: 🟢 {b1h} buys  ·  🔴 {s1h} sells  ({bp:.0f}% buy)\n"
            f"  24h: 🟢 {b24h} buys  ·  🔴 {s24h} sells\n"
            f"  Vol: `{_usd(t.get('v24h',0))}` (24h)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Momentum: {t.get('mscore',0)}/100\n"
            f"🌊 Liq: `{_usd(t.get('liq',0))}`  ·  MCap: `{_usd(t.get('mcap',0))}`\n"
        )
        if bp > 70:
            card += f"🔥 Strong buy pressure — whales accumulating\n"
        elif bp < 40:
            card += f"❄️ Heavy sell pressure — exits ongoing\n"
        card += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        _track_scan(t, u.effective_user.id)
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True,
                            reply_markup=scan_buttons(ca, sym, t.get("pair_addr","")))
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")

# ─── /soc — Quick Socials ──────────────────────────────────────
async def soc_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Quick socials lookup for any token."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/soc <contract_address>`", parse_mode="Markdown"); return
    ca = c.args[0].strip()
    msg = await u.effective_message.reply_text("🔍 *Finding socials...*", parse_mode="Markdown")
    try:
        pairs = await asyncio.wait_for(dex_pairs_by_token(ca), timeout=12)
        if not pairs:
            await msg.edit_text("❌ Token not found"); return
        p = pairs[0]
        info = p.get("info") or {}
        socials = info.get("socials") or []
        sites = info.get("websites") or []
        sym = p.get("baseToken",{}).get("symbol","???")
        links = []
        for s in socials:
            t_type = s.get("type","")
            url = s.get("url","")
            if t_type == "twitter": links.append(f"🐦 [Twitter]({url})")
            elif t_type == "telegram": links.append(f"💬 [Telegram]({url})")
            elif t_type == "discord": links.append(f"🎮 [Discord]({url})")
        for w in sites[:2]:
            links.append(f"🌐 [Website]({w.get('url','')})")
        if not links:
            await msg.edit_text(f"❌ No socials found for ${_md(sym)}"); return
        card = f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  🔗 *SOCIALS — ${_md(sym)}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        card += "\n".join(f"  {l}" for l in links)
        card += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📋 `{ca}`"
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")

# ─── /ath — ATH Leaderboard ────────────────────────────────────
async def ath_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show ATH leaderboard from group scans."""
    if not _ath_tracker:
        await u.effective_message.reply_text("📊 No tokens tracked yet. Scan some tokens first!"); return
    # Sort by ATH mcap
    sorted_ath = sorted(_ath_tracker.items(), key=lambda x: x[1]["ath_mcap"], reverse=True)[:15]
    card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  📈 *ATH LEADERBOARD*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, (ca, data) in enumerate(sorted_ath, 1):
        sym = data["sym"]
        ath = data["ath_mcap"]
        first = data["first_mcap"]
        gain = ((ath - first) / max(first, 1) * 100) if first > 0 else 0
        card += f"{i}. *${_md(sym)}* — ATH: `{_usd(ath)}` (+{gain:.0f}%)\n"
    card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    await u.effective_message.reply_text(card, parse_mode="Markdown", disable_web_page_preview=True)

# ─── /last — Recent Scans ──────────────────────────────────────
async def last_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show last 10 tokens scanned in the group."""
    if not _scan_history:
        await u.effective_message.reply_text("📊 No tokens scanned yet."); return
    recent = _scan_history[-10:][::-1]  # most recent first
    card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  📋 *RECENT SCANS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, e in enumerate(recent, 1):
        ago = int((time.time() - e["time"]) / 60)
        card += f"{i}. *${_md(e['sym'])}* — {_usd(e['mcap'])} · {_pct(e['ch1h'])} · {ago}m ago\n"
    card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    await u.effective_message.reply_text(card, parse_mode="Markdown", disable_web_page_preview=True)

# ─── /hot — Most Scanned ───────────────────────────────────────
async def hot_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show most scanned tokens in the last hour."""
    cutoff = time.time() - 3600
    recent = [e for e in _scan_history if e["time"] > cutoff]
    if not recent:
        await u.effective_message.reply_text("📊 No scans in the last hour."); return
    # Count by CA
    counts = {}
    for e in recent:
        ca = e["ca"]
        if ca not in counts:
            counts[ca] = {"sym": e["sym"], "count": 0, "mcap": e["mcap"], "ch1h": e["ch1h"]}
        counts[ca]["count"] += 1
    sorted_hot = sorted(counts.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
    card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  🔥 *HOT IN LAST 1H*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, (ca, d) in enumerate(sorted_hot, 1):
        card += f"{i}. *${_md(d['sym'])}* — {d['count']}x scans · {_usd(d['mcap'])} · {_pct(d['ch1h'])}\n"
    card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    await u.effective_message.reply_text(card, parse_mode="Markdown", disable_web_page_preview=True)

# ─── /best — Top Gainers (CoinGecko) ───────────────────────────
async def best_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show top gainers from CoinGecko."""
    msg = await u.effective_message.reply_text("🔍 *Fetching top gainers...*", parse_mode="Markdown")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&price_change_percentage=24h",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                coins = await r.json()
        gainers = sorted(coins, key=lambda x: float(x.get("price_change_percentage_24h",0) or 0), reverse=True)[:10]
        card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  🚀 *TOP GAINERS (24h)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        for i, c2 in enumerate(gainers, 1):
            sym = c2.get("symbol","?").upper()
            ch = float(c2.get("price_change_percentage_24h",0) or 0)
            mcap = float(c2.get("market_cap",0) or 0)
            card += f"{i}. *${_md(sym)}* — 🟢 +{ch:.1f}% · {_usd(mcap)}\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /worst — Top Losers (CoinGecko) ───────────────────────────
async def worst_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show top losers from CoinGecko."""
    msg = await u.effective_message.reply_text("🔍 *Fetching top losers...*", parse_mode="Markdown")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&price_change_percentage=24h",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                coins = await r.json()
        losers = sorted(coins, key=lambda x: float(x.get("price_change_percentage_24h",0) or 0))[:10]
        card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  💀 *TOP LOSERS (24h)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        for i, c2 in enumerate(losers, 1):
            sym = c2.get("symbol","?").upper()
            ch = float(c2.get("price_change_percentage_24h",0) or 0)
            mcap = float(c2.get("market_cap",0) or 0)
            card += f"{i}. *${_md(sym)}* — 🔴 {ch:.1f}% · {_usd(mcap)}\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /dub — Chat Summary ───────────────────────────────────────
async def dub_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """AI summary of recent chat messages."""
    if len(group_messages) < 5:
        await u.effective_message.reply_text("💬 Not enough messages to summarize yet."); return
    msg = await u.effective_message.reply_text("🧠 *Summarizing chat...*", parse_mode="Markdown")
    try:
        recent = group_messages[-50:]
        texts = [f"[{m['uid']}]: {m['text']}" for m in recent]
        ai = await ai_ask(
            f"Summarize this Telegram crypto chat in 3-4 bullet points. Key topics, tokens mentioned, sentiment:\n\n"
            + "\n".join(texts),
            fallback="Could not generate summary.",
            max_tokens=250
        )
        await msg.edit_text(f"📝 *Chat Summary*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}",
                           parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /tldr — URL Summary ───────────────────────────────────────
async def tldr_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """AI summary of any URL (article, tweet, YouTube)."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/tldr <url>`", parse_mode="Markdown"); return
    url = c.args[0].strip()
    msg = await u.effective_message.reply_text("📄 *Fetching and summarizing...*", parse_mode="Markdown")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=12),
                            headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status != 200:
                    await msg.edit_text("❌ Could not fetch URL"); return
                html = await r.text()
        # Extract text from HTML (simple strip)
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()[:3000]
        if len(text) < 100:
            await msg.edit_text("❌ Not enough text content to summarize"); return
        ai = await ai_ask(
            f"Summarize this article in 3-4 key points. Be concise:\n\n{text}",
            fallback="Could not summarize.",
            max_tokens=300
        )
        await msg.edit_text(f"📄 *TL;DR*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}",
                           parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /metas — Trending Categories ──────────────────────────────
async def metas_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show trending DexScreener categories/metas."""
    msg = await u.effective_message.reply_text("🔍 *Fetching trending categories...*", parse_mode="Markdown")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{_DSX}/token-profiles/latest/v1",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                profiles = await r.json()
        # Group by chain
        chains = {}
        for p in profiles:
            chain = p.get("chainId", "unknown")
            chains[chain] = chains.get(chain, 0) + 1
        sorted_chains = sorted(chains.items(), key=lambda x: x[1], reverse=True)[:10]
        card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  📊 *TRENDING NETWORKS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        for i, (chain, count) in enumerate(sorted_chains, 1):
            card += f"{i}. *{chain.title()}* — {count} new profiles\n"
        # Also show top Solana tokens
        sol = [p for p in profiles if p.get("chainId") == "solana"][:5]
        if sol:
            card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🔥 *New Solana Profiles:*\n"
            for p in sol:
                sym = p.get("symbol","?")
                card += f"  ${_md(sym)} — {p.get('name','')[:20]}\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /pvp — Similar Tokens ─────────────────────────────────────
async def pvp_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Find similar/newer tokens with same name."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/pvp <contract_address or symbol>`", parse_mode="Markdown"); return
    query = " ".join(c.args)
    msg = await u.effective_message.reply_text("🔍 *Finding PvP tokens...*", parse_mode="Markdown")
    try:
        pairs = await asyncio.wait_for(dex_search_pairs(query), timeout=12)
        if not pairs:
            await msg.edit_text("❌ No tokens found"); return
        # Sort by age (newest first)
        card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  ⚔️ *PVP — SIMILAR TOKENS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        for i, p in enumerate(pairs[:10], 1):
            sym = p.get("baseToken",{}).get("symbol","?")
            name = p.get("baseToken",{}).get("name","")
            mcap = float(p.get("marketCap",0) or p.get("fdv",0) or 0)
            liq = float((p.get("liquidity") or {}).get("usd",0) or 0)
            ch24 = (p.get("priceChange") or {}).get("h24",0)
            created = int(p.get("pairCreatedAt",0) or 0)
            age = _age(created)
            ca = p.get("baseToken",{}).get("address","")
            card += f"{i}. *${_md(sym)}* — {_usd(mcap)} · Liq {_usd(liq)} · {_pct(ch24)} · {age}\n"
            card += f"   `{ca[:12]}...`\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")

# ─── /groupburp — Active Plays ─────────────────────────────────
async def groupburp_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Show best active plays from group scans."""
    if not _scan_history:
        await u.effective_message.reply_text("📊 No tokens scanned yet."); return
    # Get unique tokens scanned in last 24h, sorted by 1h change
    cutoff = time.time() - 86400
    seen = {}
    for e in _scan_history:
        if e["time"] > cutoff:
            ca = e["ca"]
            if ca not in seen or e["ch1h"] > seen[ca]["ch1h"]:
                seen[ca] = e
    plays = sorted(seen.values(), key=lambda x: x.get("ch1h",0), reverse=True)[:10]
    card = "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  🎯 *ACTIVE PLAYS (24h)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, e in enumerate(plays, 1):
        card += f"{i}. *${_md(e['sym'])}* — {_pct(e['ch1h'])} 1h · {_usd(e['mcap'])}\n"
    card += "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    await u.effective_message.reply_text(card, parse_mode="Markdown", disable_web_page_preview=True)

# ─── /s — Stock Lookup (Yahoo Finance free) ────────────────────
async def stock_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Quick stock lookup via Yahoo Finance."""
    if not c.args:
        await u.effective_message.reply_text("Usage: `/s <ticker>` — e.g. `/s AAPL`", parse_mode="Markdown"); return
    ticker = c.args[0].strip().upper()
    msg = await u.effective_message.reply_text(f"🔍 *Looking up {ticker}...*", parse_mode="Markdown")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    await msg.edit_text(f"❌ Stock {ticker} not found"); return
                d = await r.json()
        result = d.get("chart",{}).get("result",[{}])[0]
        meta = result.get("meta",{})
        price = meta.get("regularMarketPrice", 0)
        prev = meta.get("chartPreviousClose", price)
        ch = ((price - prev) / prev * 100) if prev > 0 else 0
        name = meta.get("symbol", ticker)
        currency = meta.get("currency", "USD")
        card = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  📈 *{name}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Price: `{currency}{price:,.2f}`\n"
            f"📊 Change: {'🟢' if ch >= 0 else '🔴'} {ch:+.2f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await msg.edit_text(card, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await msg.edit_text(f"❌ {str(e)[:100]}")




def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()


    CMDS = [
        ("start", start), ("help", help_cmd),
        ("scan", scan_cmd), ("c", c_cmd), ("verify", verify_cmd),
        ("runners", runners_cmd), ("new", new_cmd), ("pump", pump_cmd),
        ("gems", gems_cmd), ("trending", trending_cmd), ("narrative", narrative_cmd),
        ("explain", explain_cmd), ("boosted", boosted_cmd), ("takeover", takeover_cmd),
        ("ask", ask_cmd), ("news", news_cmd), ("sentiment", sentiment_cmd),
        ("macro", macro_cmd), ("markets", markets_cmd), ("index", index_cmd), ("a", a_cmd),
        ("tt", tt_cmd), ("moni", moni_cmd),
        ("watch", watch_cmd), ("unwatch", unwatch_cmd), ("watchlist", watchlist_cmd),
        ("alert", alert_cmd), ("myalerts", myalerts_cmd), ("delalert", delalert_cmd),
        ("addport", addport_cmd), ("portfolio", portfolio_cmd), ("blacklist", blacklist_cmd),
        ("call", call_cmd), ("mycalls", mycalls_cmd), ("stop", stop_cmd),
        ("leaderboard", leaderboard_cmd),
        ("trackwallet", trackwallet_cmd), ("mywallet", mywallet_cmd),
        ("rank", rank_cmd), ("gp", gp_cmd), ("gsum", gsum_cmd),
        ("dubs", dubs_cmd), ("remindme", remindme_cmd),
        ("chart", chart_cmd),
        ("price", price_cmd),
        ("autoresponder", autoresponder_cmd),
        ("smartscan", smartscan_cmd), ("status", status_cmd), ("ping", ping_cmd),
        ("dev", dev_cmd), ("top", top_cmd), ("soc", soc_cmd),
        ("ath", ath_cmd), ("last", last_cmd), ("hot", hot_cmd),
        ("best", best_cmd), ("worst", worst_cmd),
        ("dub", dub_cmd), ("tldr", tldr_cmd),
        ("metas", metas_cmd), ("pvp", pvp_cmd),
        ("groupburp", groupburp_cmd), ("s", stock_cmd),
        # v40 Elite Features
        ("wallet", wallet_cmd), ("holders", holders_cmd),
        ("pnl", pnl_cmd), ("smart", smart_cmd),
        ("copy", copy_cmd), ("bundle", bundle_cmd),
        ("snipe", snipe_cmd), ("escan", escan_cmd),
        # v40 — remaining elite features
        ("vchart", vchart_cmd), ("migrate", migrate_cmd),
        ("kol", kol_cmd),
    ]
    for name, fn in CMDS:
        app.add_handler(CommandHandler(name, safe_command(fn)))
    # CallbackQuery handler for inline chart button
    # CallbackQuery handlers
    app.add_handler(CallbackQueryHandler(handle_refresh_callback, pattern=r"^refresh:"))
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(handle_help_callback, pattern=r"^help:"))
    app.add_handler(CallbackQueryHandler(handle_chart_callback, pattern=r"^chart:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(global_error_handler)

    async def run():
        async with app:
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            asyncio.create_task(bg_main_scanner(app))
            asyncio.create_task(bg_followup_tracker(app))
            asyncio.create_task(bg_established_scanner(app))
            asyncio.create_task(bg_new_launch_scanner(app))
            asyncio.create_task(bg_narrative_news_scanner(app))
            asyncio.create_task(bg_trending_metas_scanner(app))
            asyncio.create_task(bg_price_alert_checker(app))
            asyncio.create_task(bg_watchlist_scanner(app))
            asyncio.create_task(bg_reminder_checker(app))
            asyncio.create_task(bg_wallet_tracker(app))  # v40: live wallet monitoring
            asyncio.create_task(bg_migrate_monitor(app)) # v40: pump→raydium migration alerts
            asyncio.create_task(bg_weekly_leaderboard(app)) # v40: sunday leaderboard post
            logger.info("12 scanners started OK — v40 Elite")
            if GROUP_CHAT_ID:
                logger.info("GROUP_CHAT_ID=%s — alerts ENABLED", GROUP_CHAT_ID)
            else:
                logger.warning("GROUP_CHAT_ID not set — scanner alerts DISABLED")
            logger.info("🚀 All scanners started")
            while True:
                await asyncio.sleep(3600)

    asyncio.run(run())


if __name__ == "__main__":
    main()
