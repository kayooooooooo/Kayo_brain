"""
╔══════════════════════════════════════════════════════════════════════╗
║                    KAYO BRAIN v29 — PRO REBUILD                     ║
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
import redis as sync_redis
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
def _root(): return "🦅 Kayo Brain v29", 200

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
    """Create async Redis client; returns None if REDIS_URL not set."""
    if not REDIS_URL:
        return None
    try:
        # Quick sync ping to confirm connectivity before we hand it to async
        r_test = sync_redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
        r_test.ping()
        r_test.close()
        return aioredis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
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
                await _redis.set(REDIS_KEY, raw)
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
            raw = await _redis.get(REDIS_KEY)
        if not raw and os.path.exists(STATE_FILE):
            raw = open(STATE_FILE).read()
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
                "?ids=bitcoin,solana,ethereum,binancecoin"
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

                    ctx = (
                        f"[LIVE MARKET DATA - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}]\n"
                        f"BTC: ${btc.get('usd',0):,.0f} ({btc.get('usd_24h_change',0):+.2f}% 24h) | MCap ${btc.get('usd_market_cap',0)/1e9:.1f}B\n"
                        f"ETH: ${eth.get('usd',0):,.0f} ({eth.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"SOL: ${sol.get('usd',0):,.2f} ({sol.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"BNB: ${bnb.get('usd',0):,.2f} ({bnb.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"Fear & Greed: {fg_v}/100 - {fg_c}\n"
                        f"{trending_line}"
                        f"{sol_gainers}"
                        f"---\n"
                        f"You are Kayo — a sharp, witty Solana alpha intelligence. "
                        f"ALWAYS use the live data above for prices. Never hallucinate prices. "
                        f"You can chat casually, explain web3 terms, talk like a degen pro, "
                        f"and answer any question — crypto or not. Be helpful, real, and direct."
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
GROQ_MODELS = ["llama-3.3-70b-versatile", "llama3-70b-8192", "llama3-8b-8192"]

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
        "You are Kayo — an intelligent, witty AI living inside a Telegram group. \n"
        "You are BOTH a Web3 alpha expert AND a knowledgeable friend. \n"
        "PERSONALITY: Sharp, warm, real. Talks like a smart person texting — not a bot. \n"
        "CAPABILITIES: You know crypto, DeFi, Solana, NFTs, live prices, AND sports, "
        "pop culture, news, science, history, coding, life advice — everything. \n"
        "TONE RULES: \n"
        "- Casual messages (gm, vibes, jokes) → short, human, maybe funny. 1-2 sentences. \n"
        "- Crypto/trading questions → use LIVE price data above, be sharp and cite numbers. \n"
        "- General questions → be helpful, smart, direct. No disclaimers. \n"
        "- NEVER say you cannot answer. NEVER say you are just a bot. NEVER refuse. \n"
        "- Match the person's energy — if they're hyped, be hyped. If chill, be chill. \n"
        "FORMAT: Plain text for casual. Use *bold* sparingly for crypto key numbers only."
    )
    system_msg = {"role": "system", "content": system_content}

    if GROQ_API_KEY:
        for model in GROQ_MODELS:
            try:
                async with aiohttp.ClientSession() as s:
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
                            "temperature": 0.65,
                        },
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            text_out = d["choices"][0]["message"]["content"].strip()
                            if text_out:
                                return text_out
                        elif r.status == 429:
                            logger.warning(f"Groq 429 rate-limit on {model} — waiting 2s")
                            await asyncio.sleep(2)
                            continue
                        else:
                            err_body = await r.text()
                            logger.error(f"Groq {model} HTTP {r.status}: {err_body[:200]}")
                            # Don't break — try next model
            except asyncio.TimeoutError:
                logger.warning(f"Groq {model} timed out — trying next model")
            except Exception as e:
                logger.error(f"Groq {model} exception: {e}")

    if GEMINI_API_KEY:
        try:
            full_prompt = f"{system_ctx}\n\n{prompt}"
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": full_prompt}]}],
                          "generationConfig": {"maxOutputTokens": max_tokens}},
                    timeout=aiohttp.ClientTimeout(total=22),
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        text_out = d["candidates"][0]["content"]["parts"][0]["text"].strip()
                        if text_out:
                            return text_out
                    else:
                        err_body = await r.text()
                        logger.error(f"Gemini HTTP {r.status}: {err_body[:200]}")
        except asyncio.TimeoutError:
            logger.warning("Gemini timed out")
        except Exception as e:
            logger.error(f"Gemini exception: {e}")

    logger.error(f"ai_ask: ALL backends failed. Groq key set={bool(GROQ_API_KEY)}, Gemini key set={bool(GEMINI_API_KEY)}")
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
    """
    Twitter auth via cookie-based internal GraphQL API.
    TWITTER_AUTH_TOKEN = the `auth_token` cookie from your logged-in browser session.
    The guest-bearer token below is the public app-level token required alongside the cookie.
    """
    if not TWITTER_AUTH_TOKEN: return None
    # This guest bearer + auth_token cookie combo targets the internal API
    # (same as what the Twitter web app uses) — not the v2 REST API.
    return {
        "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
        "Cookie": f"auth_token={TWITTER_AUTH_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "x-twitter-active-user": "yes",
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-client-language": "en",
        "Referer": "https://twitter.com/",
        "Origin": "https://twitter.com",
    }

async def _tw_guest_token() -> str:
    """Fetch a guest token required for Twitter's internal API calls."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://api.twitter.com/1.1/guest/activate.json",
                headers={
                    "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
                    "User-Agent": "Mozilla/5.0",
                },
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    return (await r.json()).get("guest_token", "")
    except Exception as e:
        logger.debug(f"guest_token: {e}")
    return ""

async def tw_search(query: str, limit: int = 15) -> List[Dict]:
    """Search Twitter via the internal adaptive search API (no v2 API key needed)."""
    h = _tw_headers()
    if not h: return []
    try:
        guest = await _tw_guest_token()
        if not guest: return []
        h["x-guest-token"] = guest
        params = {
            "q": query,
            "count": str(min(limit, 20)),
            "result_type": "recent",
            "tweet_mode": "extended",
        }
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.twitter.com/1.1/search/tweets.json",
                headers=h,
                params=params,
                timeout=aiohttp.ClientTimeout(total=12),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    tweets = data.get("statuses", [])
                    return [
                        {"id": t.get("id_str", ""),
                         "text": t.get("full_text", t.get("text", ""))}
                        for t in tweets
                    ]
    except Exception as e:
        logger.debug(f"tw_search: {e}")
    return []

async def tw_user_tweets(username: str, limit: int = 10) -> List[Dict]:
    """Get user tweets via Twitter 1.1 timeline API with cookie auth."""
    h = _tw_headers()
    if not h: return []
    try:
        guest = await _tw_guest_token()
        if not guest: return []
        h["x-guest-token"] = guest
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.twitter.com/1.1/statuses/user_timeline.json",
                headers=h,
                params={
                    "screen_name": username,
                    "count": str(min(limit, 20)),
                    "tweet_mode": "extended",
                    "include_rts": "false",
                },
                timeout=aiohttp.ClientTimeout(total=12),
            ) as r:
                if r.status == 200:
                    tweets = await r.json()
                    return [
                        {"id": t.get("id_str", ""),
                         "text": t.get("full_text", t.get("text", ""))}
                        for t in tweets
                    ]
    except Exception as e:
        logger.debug(f"tw_user: {e}")
    return []

def extract_cas(text: str) -> List[str]:
    return list(set(re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)))

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
    """Full GMGN-style deep scan card — rich formatting matching screenshot style."""
    age    = _age(t["created"])
    risk   = _risk(t["risk_score"])
    bp     = t["buy_pct"]
    press  = ("\U0001f525 BUY PRESSURE" if bp > 60
              else "\U0001f53b SELL PRESSURE" if bp < 40
              else "\u2696\ufe0f NEUTRAL")

    # Pressure bar — green/red circles
    bull_b = int(bp / 10)
    bear_b = 10 - bull_b
    pbar   = "\U0001f7e2" * bull_b + "\U0001f534" * bear_b

    tags = []
    if t["boost_active"] > 0: tags.append("\U0001f4b0 BOOSTED")
    if t["has_profile"]:       tags.append("\u2705 VERIFIED")
    if t["is_honeypot"]:       tags.append("\U0001f6a8 HONEYPOT")
    tag_str = "  ".join(tags)

    nar  = f"#{t['narrative'].upper()}" if t.get("narrative") else ""
    liq_ratio = t.get("liq_ratio", 0)
    liq_tag   = "\U0001f512 LP Locked" if t.get("lp_locked") else ""

    # Social links inline
    slinks = []
    if t.get("tw_link"):  slinks.append(f"[\U0001f426 Twitter]({t['tw_link']})")
    if t.get("tg_link"):  slinks.append(f"[\U0001f4e8 TG]({t['tg_link']})")
    if t.get("web_link"): slinks.append(f"[\U0001f310 Web]({t['web_link']})")
    social_str = "  ".join(slinks) if slinks else "_(no socials)_"

    card = (
        f"\U0001f985 *KAYO DEEP SCAN*  {tag_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4b0 *${t['sym']}* — _{t['name']}_ {nar}\n"
        f"\U0001f517 `{t['address']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4b5 Price: *{_price(t['price'])}*\n"
        f"\U0001f4c8 24h High: `{_price(t.get('ath_24h', t['price']))}` · "
        f"24h Ago: `{_price(t.get('price_24h_ago', 0))}`\n"
        f"\U0001f4a0 MCap: `{_usd(t['mcap'])}` · FDV: `{_usd(t['fdv'])}`\n"
        f"\U0001f30a Liq: `{_usd(t['liq'])}` ({liq_ratio:.1f}% of MCap)\n"
        f"\u23f1\ufe0f Age: {age}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4c8 *Price Change*\n"
        f"  5m: {_pct(t['ch5m'])}  ·  1h: {_pct(t['ch1h'])}\n"
        f"  6h: {_pct(t['ch6h'])}  ·  24h: {_pct(t['ch24h'])}\n"
        f"\U0001f4b9 *Volume*\n"
        f"  5m: `{_usd(t['v5m'])}`  1h: `{_usd(t['v1h'])}`  24h: `{_usd(t['v24h'])}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f504 *Txns (1h):* \U0001f7e2 {t['b1h']} buys  \U0001f534 {t['s1h']} sells\n"
        f"  Buy ratio: {bp:.0f}%  ·  Vol spike: {t['vol_spike']:.1f}x\n"
        f"  {pbar}  {press}\n"
        f"\u26a1 Momentum: [{_bar(t['mscore'])}] {t['mscore']}/100\n"
        f"\U0001f6e1\ufe0f Security: {risk}  (score {t['risk_score']}/100)\n"
    )
    if t.get("sell_tax", 0) > 0 or t.get("buy_tax", 0) > 0:
        card += f"  \U0001f9fe Tax: Buy {t['buy_tax']}%  Sell {t['sell_tax']}%\n"
    if t["lp_locked"]:    card += "  \U0001f512 LP Locked\n"
    if t["is_renounced"]: card += "  \u2705 Contract Renounced\n"
    if t["red_flags"]:
        card += "\n*\U0001f6a9 Risk Flags:*\n" + "\n".join(f"  {f}" for f in t["red_flags"][:3]) + "\n"
    if t["green_flags"]:
        card += "\n*\u2705 Green Flags:*\n" + "\n".join(f"  {f}" for f in t["green_flags"][:2]) + "\n"
    card += f"\n\U0001f30e Socials: {social_str}\n"
    card += f"\n`{t['address']}`\n"
    if ai:
        card += f"\n🧠 *Kayo AI:*\n_{ai}_"
    return card

def build_alert_card(t: Dict, alert_type: str, ai: str = "") -> str:
    """
    Rich GMGN-style alert card — shows everything from the screenshot:
    token image text, mcap/liq/vol, buys/sells, age, wallet holders, momentum bar,
    social links, contract, AI verdict.
    """
    icons = {
        "pump":      "\U0001f680 *PUMP ALERT*",
        "dump":      "\U0001f480 *DUMP ALERT*",       # only from followup tracker
        "whale":     "\U0001f433 *WHALE ACCUMULATION*",
        "gem":       "\U0001f48e *HIDDEN GEM*",
        "new":       "\U0001f195 *NEW LAUNCH*",
        "narrative": "\U0001f4d6 *NARRATIVE PLAY*",
        "rug":       "\U000026a0\ufe0f *RUG ALERT*",
        "unusual":   "\U000026a1 *UNUSUAL ACTIVITY*",
        "migration": "\U0001f504 *MIGRATION ALERT*",   # pump.fun -> Raydium
        "rebrand":   "\U0001f3f7 *REBRAND ALERT*",     # renamed to trending narrative
    }
    header = icons.get(alert_type, "\u26a1 *KAYO ALERT*")
    age    = _age(t["created"])
    boost  = " \U0001f4b0 BOOSTED" if t.get("boost_active", 0) > 0 else ""
    hp_tag = " \U0001f6a8 HONEYPOT" if t.get("is_honeypot") else ""
    ren_tag= " \u2705 Renounced" if t.get("is_renounced") else ""
    lp_tag = " \U0001f512 LP Locked" if t.get("lp_locked") else ""
    nar    = f"#{t.get('narrative','').upper()}" if t.get('narrative') else ""

    # Buy/sell pressure bar
    bp     = t.get("buy_pct", 50)
    bull_blocks = int(bp / 10)
    bear_blocks = 10 - bull_blocks
    pressure_bar = "\U0001f7e2" * bull_blocks + "\U0001f534" * bear_blocks
    press_label = "BUY PRESSURE" if bp > 60 else ("SELL PRESSURE" if bp < 40 else "NEUTRAL")

    # Liquidity ratio
    liq_ratio = (t.get("liq", 0) / max(t.get("mcap", 1), 1) * 100)
    liq_x     = t.get("liq", 0) / max(t.get("v5m", 1), 1) if t.get("v5m", 0) > 0 else 0
    liq_tag   = "\U0001f512" if t.get("lp_locked") else ""

    # Security flags
    sec_line = ""
    if t.get("buy_tax", 0) > 0 or t.get("sell_tax", 0) > 0:
        sec_line = f"\U0001f9fe Tax: Buy {t.get('buy_tax',0):.1f}% / Sell {t.get('sell_tax',0):.1f}%\n"
    if t.get("red_flags"):
        sec_line += "\U0001f6a9 " + " · ".join(t["red_flags"][:2]) + "\n"

    # Social line
    socials = []
    if t.get("tw_link"):  socials.append(f"[\U0001f426]({t['tw_link']})")
    if t.get("tg_link"):  socials.append(f"[\U0001f4e8]({t['tg_link']})")
    if t.get("web_link"): socials.append(f"[\U0001f310]({t['web_link']})")
    social_str = "  ".join(socials) if socials else ""

    card = (
        f"{header}{boost}{hp_tag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4b0 *${t['sym']}* — _{t['name']}_ {nar}\n"
        f"\U0001f517 `{t['address']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4b5 USD: `{_price(t['price'])}`\n"
        f"\U0001f4a0 FDV: `{_usd(t['fdv'])}` \u21d2 MCap: `{_usd(t['mcap'])}`\n"
        f"\U0001f30a Liq: `{_usd(t['liq'])}` [x{liq_x:.0f}] {liq_tag}\n"
        f"\U0001f4ca Vol: `{_usd(t['v5m'])}` (5m) · `{_usd(t['v1h'])}` (1h) · Age: {age}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4c8 1h: {_pct(t['ch1h'])}  ·  5m: {_pct(t['ch5m'])}  ·  6h: {_pct(t.get('ch6h',0))}\n"
        f"\U0001f504 Buys/Sells (1h): {t['b1h']} / {t['s1h']}  ·  Vol spike: {t['vol_spike']:.1f}x\n"
        f"\U0001f7e2 {pressure_bar}  {bp:.0f}% {press_label}\n"
        f"\u26a1 Momentum: [{_bar(t['mscore'])}] {t['mscore']}/100  ·  {_risk(t['risk_score'])}\n"
    )
    if sec_line:
        card += sec_line
    if ren_tag or lp_tag:
        card += f"{ren_tag}{lp_tag}\n"
    if social_str:
        card += f"\n{social_str}\n"
    if ai:
        card += f"\n\U0001f9e0 *Kayo:* _{ai}_"
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
            # Banana Gun — Telegram bot link (no web app for this one)
            InlineKeyboardButton(
                "\U0001f34c Banana",
                url=f"https://t.me/BananaGunSolana_bot?start=snipe_{addr}"
            ),
            # Trojan — Telegram bot link
            InlineKeyboardButton(
                "\U0001f5e1 Trojan",
                url=f"https://t.me/hector_trojanbot?start=snipe-SOL-{addr}"
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
    await u.message.reply_text(
        f"\U0001f985 *KAYO BRAIN v29*\n"
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
    await u.message.reply_text(
        "\U0001f985 *KAYO BRAIN v29 — COMMANDS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "Tap a category \U0001f447 to see its commands.\n"
        "Or type `/` in the chat bar to tap any command directly.",
        parse_mode="Markdown",
        reply_markup=markup,
    )

async def scan_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/scan <contract_address>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.message.reply_text("🔍 *Scanning...*", parse_mode="Markdown")
    t    = await full_token_scan(addr)
    if t.get("error"):
        await msg.edit_text(f"❌ {t['error']}"); return
    add_xp(u.effective_user.id, 5)
    ai = await ai_ask(
        f"Solana token ${t['sym']} — MCap {_usd(t['mcap'])}, liq {_usd(t['liq'])}, "
        f"age {_age(t['created'])}, 5m {_pct(t['ch5m'])}, 1h {_pct(t['ch1h'])}, "
        f"24h {_pct(t['ch24h'])}, buy ratio {t['buy_pct']:.0f}%, vol spike {t['vol_spike']:.1f}x, "
        f"momentum {t['mscore']}/100, risk {t['risk_score']}/100, "
        f"narrative #{t['narrative']}, honeypot={t['is_honeypot']}, lp_locked={t['lp_locked']}. "
        "Give a sharp alpha verdict: is this worth aping right now? "
        "Consider the current market conditions from your live context. "
        "Call out any red flags. 2-3 direct sentences.",
        fallback="",
        inject_market=True
    )
    await msg.edit_text(
        build_scan_card(t, ai),
        parse_mode="Markdown",
        reply_markup=scan_buttons(addr, t["sym"]),
        disable_web_page_preview=True,
    )

async def c_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/c <ca>`", parse_mode="Markdown"); return
    addr  = c.args[0].strip()
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await u.message.reply_text("❌ Token not found."); return
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
    await u.message.reply_text(
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
        await u.message.reply_text("Usage: `/verify <ca>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    msg  = await u.message.reply_text("🛡 *Running security check...*", parse_mode="Markdown")
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
    msg = await u.message.reply_text("🏃 *Scanning for top runners...*", parse_mode="Markdown")
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
    msg      = await u.message.reply_text("🆕 *Fetching new launches...*", parse_mode="Markdown")
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
    msg = await u.message.reply_text("🚀 *Finding fresh pumps...*", parse_mode="Markdown")
    QUERIES = ["solana meme", "solana new", "solana dog", "solana ai", "solana pump fun"]
    pairs_map = await dex_multi_search(QUERIES)
    pumping = [
        p for p in pairs_map.values()
        if float((p.get("priceChange") or {}).get("m5", 0) or 0) >= 5
        and float((p.get("liquidity") or {}).get("usd", 0) or 0) >= 800
        and (p.get("baseToken") or {}).get("address", "") not in blacklist
    ]
    pumping.sort(key=lambda p: float((p.get("priceChange") or {}).get("m5", 0) or 0), reverse=True)
    if not pumping:
        await msg.edit_text("😴 Nothing pumping hard right now."); return
    add_xp(u.effective_user.id, 2)
    lines = ["🚀 *FRESH PUMPS — 5M*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for p in pumping[:8]:
        base = p.get("baseToken", {})
        sym  = base.get("symbol", "???")
        addr = base.get("address", "")
        ch5m = float((p.get("priceChange") or {}).get("m5", 0) or 0)
        ch1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        fdv  = float(p.get("fdv", 0) or 0)
        liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        b5m  = int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
        s5m  = int(((p.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0)
        lines.append(
            f"\n*${sym}*\n"
            f"  5m: {_pct(ch5m)}  1h: {_pct(ch1h)}\n"
            f"  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
            f"  Buys/Sells (5m): {b5m}/{s5m}\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def gems_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg = await u.message.reply_text("💎 *Hunting hidden gems...*", parse_mode="Markdown")
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
    msg   = await u.message.reply_text("🔥 *Fetching trending metas...*", parse_mode="Markdown")
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
        await u.message.reply_text("Usage: `/narrative <word>` e.g. `/narrative ai`", parse_mode="Markdown"); return
    slug = c.args[0].lower().strip()
    msg  = await u.message.reply_text(f"📖 *Finding #{slug} tokens...*", parse_mode="Markdown")
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
        await u.message.reply_text("Usage: `/explain <narrative>` e.g. `/explain RWA`", parse_mode="Markdown"); return
    topic = " ".join(c.args)
    msg   = await u.message.reply_text(f"🧠 *Explaining #{topic}...*", parse_mode="Markdown")
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
    msg    = await u.message.reply_text("💰 *Fetching boosted tokens...*", parse_mode="Markdown")
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
    msg  = await u.message.reply_text("🫧 *Fetching community takeovers...*", parse_mode="Markdown")
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
    msg   = await u.message.reply_text("📰 *Fetching latest news...*", parse_mode="Markdown")
    items = await fetch_news(8)
    if not items:
        await msg.edit_text("❌ No news available right now."); return
    add_xp(u.effective_user.id, 1)
    titles = "\n".join([i["title"] for i in items[:6]])
    ai_sum = await ai_ask(
        f"Crypto headlines just in:\n{titles}\n\n"
        "As Kayo, give a sharp intelligence briefing: "
        "(1) The single most important macro story and why it moves markets, "
        "(2) Direct impact on SOL/Solana ecosystem specifically, "
        "(3) Any narrative plays — coins/sectors that could pump from this news. "
        "Keep it tight, specific, and actionable. Cross-reference with live prices.",
        fallback="",
        inject_market=True
    )
    lines = ["📰 *CRYPTO NEWS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for item in items[:6]:
        lines.append(f"\n• [{item['title'][:72]}]({item['link']})\n  _— {item['source']}_")
    if ai_sum:
        lines.append(f"\n🧠 *Kayo AI Summary:*\n_{ai_sum}_")
    await msg.edit_text("\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)

# ── Smart crypto vs casual detector — used in ask_cmd + handle_message ──
_CRYPTO_KWS = frozenset([
    "price","mcap","market cap","btc","bitcoin","sol","solana","eth","ethereum",
    "bnb","token","coin","pump","dump","chart","wallet","defi","nft","dao",
    "alpha","buy","sell","trade","ape","degen","rug","honeypot","liq","liquidity",
    "fdv","volume","candle","narrative","trending","gem","launch","migrate",
    "raydium","jupiter","dexscreener","birdeye","gmgn","bullx","whale","kol",
    "call","entry","exit","fear","greed","dominance","airdrop","staking","yield",
    "1h","5m","24h","10x","100x","rekt","ngmi","wagmi","memecoin","web3",
    "blockchain","contract","ca","address","photon","trojan","banana","snipe",
])
_CASUAL_KWS = frozenset([
    "sup","gm","gn","hi","hello","hey","yo","wassup","lol","haha","hahaha",
    "how are you","how r u","you good","bored","hungry","tired","what time",
    "joke","funny","mood","vibe","chill","love","miss","feel","today","tomorrow",
    "weather","sports","game","movie","music","food","drink","sleep","wake",
    "morning","night","evening","afternoon","weekend","busy","free","work",
])

def _is_crypto_q(text: str) -> bool:
    tl = text.lower()
    return any(kw in tl for kw in _CRYPTO_KWS)

def _is_casual_q(text: str) -> bool:
    tl = text.lower()
    if len(text.split()) <= 4 and not _is_crypto_q(text): return True
    return any(kw in tl for kw in _CASUAL_KWS) and not _is_crypto_q(text)

async def ask_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/ask <question>`", parse_mode="Markdown"); return
    q   = " ".join(c.args)
    msg = await u.message.reply_text("\U0001f9e0 *Kayo thinking...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)
    import random as _rand
    _CASUAL_FALLBACKS = [
        "haha yeah that checks out 😂", "lol true dat", "gm ser 🌅",
        "vibing — what's good?", "lol no cap 😭", "yo 👋 what's the move?",
        "facts 🤝", "based ngl", "haha 💀 real ones know",
    ]
    # Unified prompt — no keyword routing. Always inject live market data.
    # AI decides tone: casual = short+human, crypto = sharp+data, general = helpful.
    prompt = (
        f"User asks via /ask: \"{q}\"\n\n"
        "You are Kayo — sharp Solana alpha bot AND knowledgeable friend. "
        "Answer anything: crypto prices (use LIVE data), web3 alpha, general knowledge, casual chat. "
        "Be direct. No disclaimers. No refusals. Match the tone of the question. "
        "For prices: cite the exact live numbers from your context. "
        "For casual: be short, warm, witty. For deep questions: be thorough but concise."
    )
    ans = await ai_ask(prompt, max_tokens=500, inject_market=True,
                       fallback=_rand.choice(_CASUAL_FALLBACKS))
    ts     = datetime.utcnow().strftime("%H:%M UTC")
    footer = f"\n\n_Live data as of {ts}_"

    if not ans or not ans.strip():
        ans = _rand.choice(_CASUAL_FALLBACKS)
    import re as _re3
    # Always try markdown first (AI may use bold for crypto), fall back to plain
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
    msg = await u.message.reply_text("📊 *Reading market sentiment...*", parse_mode="Markdown")
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
    msg      = await u.message.reply_text("📉 *Analyzing macro...*", parse_mode="Markdown")
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
    msg  = await u.message.reply_text("🌍 *Loading market data...*", parse_mode="Markdown")
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
        await u.message.reply_text("❌ F&G index unavailable."); return
    val   = int(fg.get("value", 0) or 0)
    cls   = fg.get("value_classification", "?")
    emoji = "😱" if val < 25 else "😰" if val < 40 else "😐" if val < 60 else "😊" if val < 75 else "🤑"
    add_xp(u.effective_user.id, 1)
    await u.message.reply_text(
        f"{emoji} *FEAR & GREED INDEX*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Score: *{val}/100*\n"
        f"Classification: *{cls}*\n"
        f"[{_bar(val)}]",
        parse_mode="Markdown"
    )

async def a_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/a <coin_id>` e.g. `/a solana`", parse_mode="Markdown"); return
    coin_id = c.args[0].lower()
    msg     = await u.message.reply_text(f"💰 *Looking up {coin_id}...*", parse_mode="Markdown")
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
        await u.message.reply_text("Usage: `/watch @username` — watch a Twitter account for CA drops", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@").lower()
    watchlist[username] = {"added": time.time(), "by": u.effective_user.id, "hits": 0}
    await _save()
    add_xp(u.effective_user.id, 5)
    await u.message.reply_text(
        f"👁 *Watching @{username}*\n"
        f"I'll alert the group the moment they drop a CA.\n"
        f"_Requires TWITTER\\_AUTH\\_TOKEN to be set in Render env vars_",
        parse_mode="Markdown"
    )

async def unwatch_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/unwatch @username`", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@").lower()
    if username in watchlist:
        del watchlist[username]; _save()
        await u.message.reply_text(f"✅ Stopped watching @{username}")
    else:
        await u.message.reply_text(f"@{username} is not in your watchlist.")

async def watchlist_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not watchlist:
        await u.message.reply_text("Watchlist empty. Use `/watch @username` to add.", parse_mode="Markdown"); return
    lines = ["👁 *WATCHLIST*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for un, data in watchlist.items():
        added = datetime.fromtimestamp(data.get("added", 0)).strftime("%d/%m")
        hits  = data.get("hits", 0)
        lines.append(f"• @{un} — added {added}, {hits} CA drops caught")
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def tt_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/tt <ca_or_symbol>`", parse_mode="Markdown"); return
    query = " ".join(c.args)
    msg   = await u.message.reply_text(f"🐦 *Searching Twitter for {query}...*", parse_mode="Markdown")
    if not TWITTER_AUTH_TOKEN:
        await msg.edit_text(
            "⚠️ *Twitter not configured*\n\n"
            "Add `TWITTER_AUTH_TOKEN` to Render env vars.\n"
            "Get it: twitter.com → DevTools → Application → Cookies → `auth_token`",
            parse_mode="Markdown"
        ); return
    tweets = await tw_search(f"{query} solana", limit=20)
    if not tweets:
        await msg.edit_text(f"No recent tweets found for `{query}`."); return
    texts = " ".join([t.get("text", "") for t in tweets])
    cas   = extract_cas(texts)
    ai    = await ai_ask(
        f"Analyze {len(tweets)} recent tweets about {query} on Solana. "
        f"What's the sentiment (bullish/bearish/neutral)? Key themes? Any alpha? "
        f"Tweets: {texts[:800]}",
        fallback="Could not analyze."
    )
    lines = [f"🐦 *TWITTER: {query.upper()}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{len(tweets)} tweets analyzed"]
    if cas: lines.append("\n📋 *CAs found:*\n" + "\n".join([f"`{ca}`" for ca in cas[:3]]))
    lines.append(f"\n🧠 *AI Analysis:*\n_{ai}_")
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def moni_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/moni @username`", parse_mode="Markdown"); return
    username = c.args[0].lstrip("@")
    msg      = await u.message.reply_text(f"👤 *Checking @{username}...*", parse_mode="Markdown")
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
        await u.message.reply_text("Usage: `/alert <ca> <target_price>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try:   target = float(c.args[1])
    except: await u.message.reply_text("❌ Invalid price."); return
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await u.message.reply_text("❌ Token not found."); return
    p     = pairs[0]
    sym   = p.get("baseToken", {}).get("symbol", "???")
    price = float(p.get("priceUsd", 0) or 0)
    direction = "above" if target > price else "below"
    user_alerts.append({"uid": u.effective_user.id, "addr": addr, "sym": sym, "target": target, "direction": direction, "triggered": False})
    await _save()
    add_xp(u.effective_user.id, 3)
    await u.message.reply_text(
        f"🔔 *Alert set for ${sym}*\n"
        f"Current: {_price(price)}\n"
        f"Alert when price goes *{direction}* {_price(target)}",
        parse_mode="Markdown"
    )

async def myalerts_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid    = u.effective_user.id
    alerts = [a for a in user_alerts if a.get("uid") == uid and not a.get("triggered")]
    if not alerts:
        await u.message.reply_text("No active alerts. Use `/alert <ca> <price>`.", parse_mode="Markdown"); return
    lines = ["🔔 *YOUR ALERTS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, a in enumerate(alerts, 1):
        lines.append(f"{i}. *${a['sym']}* — alert {a['direction']} {_price(a['target'])}")
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def delalert_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/delalert <number>` — see numbers with /myalerts", parse_mode="Markdown"); return
    uid = u.effective_user.id
    my  = [a for a in user_alerts if a.get("uid") == uid and not a.get("triggered")]
    try:   idx = int(c.args[0]) - 1
    except: await u.message.reply_text("❌ Invalid number."); return
    if idx < 0 or idx >= len(my):
        await u.message.reply_text("❌ Alert not found."); return
    user_alerts.remove(my[idx]); _save()
    await u.message.reply_text(f"✅ Alert for *${my[idx]['sym']}* deleted.", parse_mode="Markdown")

async def call_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.message.reply_text("Usage: `/call <ca> <entry_price>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try:  entry = float(c.args[1])
    except: await u.message.reply_text("❌ Invalid price."); return
    pairs = await dex_pairs_by_token(addr)
    sym   = pairs[0].get("baseToken", {}).get("symbol", "???") if pairs else "???"
    user  = u.effective_user
    active_calls.append({
        "uid": user.id, "username": user.username or user.first_name,
        "addr": addr, "sym": sym, "entry": entry,
        "time": time.time(), "status": "open", "exit": None, "pnl": None
    })
    asyncio.create_task(_save()); add_xp(user.id, 10)
    await u.message.reply_text(
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
        await u.message.reply_text("No calls yet. Use `/call <ca> <price>`.", parse_mode="Markdown"); return
    lines = ["📋 *YOUR CALLS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for cl in sorted(mine, key=lambda x: x["time"], reverse=True)[:10]:
        status = cl.get("status", "open")
        pnl    = f" → {cl['pnl']}" if cl.get("pnl") else ""
        date   = datetime.fromtimestamp(cl["time"]).strftime("%d/%m")
        lines.append(f"• *${cl['sym']}* @ {_price(cl['entry'])} [{status}]{pnl} — {date}")
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def stop_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/stop <symbol_or_ca> <exit_price>`", parse_mode="Markdown"); return
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
            await u.message.reply_text(
                f"🛑 *Call closed — ${cl['sym']}*\n"
                f"Entry: {_price(cl['entry'])}  Exit: {_price(exit_p) if exit_p else 'N/A'}\n"
                f"P&L: {cl.get('pnl', 'N/A')}",
                parse_mode="Markdown"
            )
            return
    await u.message.reply_text(f"❌ No open call for {target}.")

async def leaderboard_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    closed = [cl for cl in active_calls if cl.get("status") == "closed" and cl.get("pnl")]
    if not closed:
        await u.message.reply_text("No closed calls yet."); return
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
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def addport_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.message.reply_text("Usage: `/addport <ca> <amount_usd>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    try: amount = float(c.args[1])
    except: await u.message.reply_text("❌ Invalid amount."); return
    pairs = await dex_pairs_by_token(addr)
    sym   = pairs[0].get("baseToken", {}).get("symbol", "???") if pairs else "???"
    price = float(pairs[0].get("priceUsd", 0) or 0) if pairs else 0
    uid   = str(u.effective_user.id)
    if uid not in portfolios: portfolios[uid] = []
    portfolios[uid].append({"addr": addr, "sym": sym, "amount": amount, "entry_price": price, "time": time.time()})
    asyncio.create_task(_save()); add_xp(u.effective_user.id, 3)
    await u.message.reply_text(f"✅ Added *${sym}* — ${amount:.2f} at {_price(price)}", parse_mode="Markdown")

async def portfolio_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = str(u.effective_user.id)
    port = portfolios.get(uid, [])
    if not port:
        await u.message.reply_text("Portfolio empty. Use `/addport <ca> <amount>`.", parse_mode="Markdown"); return
    msg = await u.message.reply_text("💼 *Loading portfolio...*", parse_mode="Markdown")
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
        await u.message.reply_text("Usage: `/blacklist <ca>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    blacklist.add(addr); _save()
    add_xp(u.effective_user.id, 2)
    await u.message.reply_text(f"🚫 `{addr[:20]}...` blacklisted — filtered from all scans.", parse_mode="Markdown")

async def rank_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid  = str(u.effective_user.id)
    xp   = xp_db.get(uid, 0)
    rank = sum(1 for v in xp_db.values() if v > xp) + 1
    lvl  = xp // 100
    await u.message.reply_text(
        f"⭐ *YOUR RANK*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"XP: {xp}  Level: {lvl}\n"
        f"[{_bar(xp % 100)}] → {(lvl+1)*100} XP next level\n"
        f"Group rank: #{rank}",
        parse_mode="Markdown"
    )

async def gp_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not xp_db:
        await u.message.reply_text("No XP recorded yet!"); return
    top    = sorted(xp_db.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"]
    lines  = ["🏆 *XP LEADERBOARD*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, (uid, xp) in enumerate(top):
        m = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{m} User ...{uid[-4:]} — {xp} XP  (Lv {xp//100})")
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def trackwallet_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/trackwallet <address> <label>`", parse_mode="Markdown"); return
    addr  = c.args[0].strip()
    label = " ".join(c.args[1:]) or addr[:8]
    tracked_wallets[addr] = {"label": label, "by": u.effective_user.id, "added": time.time()}
    asyncio.create_task(_save()); add_xp(u.effective_user.id, 5)
    await u.message.reply_text(f"👛 Tracking *{label}*\n`{addr}`", parse_mode="Markdown")

async def mywallet_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/mywallet <solana_address>`", parse_mode="Markdown"); return
    addr = c.args[0].strip()
    user_wallets[str(u.effective_user.id)] = addr; _save()
    await u.message.reply_text(f"✅ Wallet linked: `{addr}`", parse_mode="Markdown")

async def dubs_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/dubs <your win story>`", parse_mode="Markdown"); return
    text = " ".join(c.args)
    user = u.effective_user
    add_xp(user.id, 20)
    await u.message.reply_text(
        f"🎉 *W ALERT*\n"
        f"@{user.username or user.first_name} is celebrating!\n\n_{text}_\n\n🏆 +20 XP",
        parse_mode="Markdown"
    )

async def gsum_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(group_messages) < 5:
        await u.message.reply_text("Not enough messages to summarize yet."); return
    msgs = group_messages[-50:]
    ai   = await ai_ask(
        f"Summarize this Telegram crypto group conversation. What coins were discussed? "
        f"Any alpha or CAs dropped? Key themes? "
        f"Messages: {chr(10).join([m['text'] for m in msgs][:2000])}",
        fallback="Summary unavailable.",
        max_tokens=350
    )
    add_xp(u.effective_user.id, 3)
    await u.message.reply_text(f"📝 *GROUP SUMMARY*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{ai}", parse_mode="Markdown")

async def remindme_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if len(c.args) < 2:
        await u.message.reply_text("Usage: `/remindme <minutes> <message>`", parse_mode="Markdown"); return
    try:  mins = int(c.args[0])
    except: await u.message.reply_text("❌ Invalid time."); return
    text = " ".join(c.args[1:])
    fire = (datetime.utcnow() + timedelta(minutes=mins)).isoformat()
    reminders.append({"chat_id": u.effective_chat.id, "text": text, "fire_at": fire}); _save()
    await u.message.reply_text(f"⏰ Reminder set for *{mins} minutes*\n_{text}_", parse_mode="Markdown")

async def ping_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    t   = time.time()
    msg = await u.message.reply_text("🏓")
    ms  = int((time.time() - t) * 1000)
    await msg.edit_text(f"🏓 *Pong!* {ms}ms — Kayo Brain v29 alive.", parse_mode="Markdown")

async def price_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /price btc  or  /price sol  — live price from CoinGecko
    Always accurate, always real-time. Never relies on AI training data.
    """
    if not c.args:
        await u.message.reply_text(
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
    msg = await u.message.reply_text(f"💰 *Fetching live price...*", parse_mode="Markdown")

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
        await u.message.reply_text(
            "Usage: `/chart <contract_address>`\n"
            "Example: `/chart EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`",
            parse_mode="Markdown"
        ); return

    addr = c.args[0].strip()
    msg  = await u.message.reply_text("📊 *Loading chart...*", parse_mode="Markdown")

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
            await u.message.reply_photo(
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
    await u.message.reply_text(f"🤖 Auto CA-scanner turned *{state}*", parse_mode="Markdown")

async def smartscan_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Manual trigger of the live scanner — shows what the bot would alert right now."""
    msg = await u.message.reply_text("🔍 *Running live GeckoTerminal scan...*", parse_mode="Markdown")
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
    redis_ok  = "✅" if _redis else "❌"
    groq_ok   = "✅" if GROQ_API_KEY else "❌"
    gemini_ok = "✅" if GEMINI_API_KEY else "❌"
    tw_ok     = "✅" if TWITTER_AUTH_TOKEN else "❌"
    group_ok  = "✅" if GROUP_CHAT_ID != 0 else f"❌ (set GROUP_CHAT_ID)"
    await u.message.reply_text(
        f"⚙️ *KAYO BRAIN v29 STATUS*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{redis_ok} Redis\n"
        f"{groq_ok} Groq AI (primary)\n"
        f"{gemini_ok} Gemini AI (fallback)\n"
        f"{tw_ok} Twitter auth\n"
        f"{group_ok} Group alerts (ID: {GROUP_CHAT_ID})\n\n"
        f"📊 Watchlist: {len(watchlist)} accounts\n"
        f"🔔 Active alerts: {sum(1 for a in user_alerts if not a.get('triggered'))}\n"
        f"📢 Open calls: {sum(1 for cl in active_calls if cl.get('status')=='open')}\n"
        f"🚫 Blacklisted: {len(blacklist)}\n"
        f"💾 Seen alerts (Redis): {len(seen_alert_ids)}",
        parse_mode="Markdown"
    )

# ═══════════════════════════════════════════════════════════════
# AUTO-RESPONDER
# ═══════════════════════════════════════════════════════════════
HELP_PAGES = {
    "scan": (
        "\U0001f52c *SCAN & ANALYZE*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/scan <CA>` — Full token deep scan + AI verdict\n"        "_(Bot auto-drops: pumps, gems, new launches, whale moves, unusual activity)_\n"
        "`/c <CA>` — Quick price snapshot\n"
        "`/chart <CA>` — In-app chart image (no DexScreener needed)\n"
        "`/price btc` — Live price for any coin (btc, sol, eth, bnb...)\n"
        "`/verify <CA>` — Rug & honeypot check via GoPlus\n"
        "`/a <coin-id>` — Full CoinGecko coin lookup"
    ),
    "discover": (
        "\U0001f50d *DISCOVER*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/runners` — Top Solana gainers right now\n"
        "`/new` — Brand new token launches\n"
        "`/pump` — Fresh 5-minute pumps\n"
        "`/gems` — Hidden gems (low cap, good momentum)\n"
        "`/boosted` — Tokens teams are actively promoting\n"
        "`/takeover` — Community takeover tokens"
    ),
    "narrative": (
        "\U0001f4d6 *NARRATIVES & TRENDS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/trending` — Trending metas on DexScreener\n"
        "`/narrative <word>` — Tokens matching a narrative\n"
        "  e.g. `/narrative ai` `/narrative gaming`\n"
        "`/explain <narrative>` — AI professional breakdown of a narrative\n"
        "  e.g. `/explain defi` `/explain meme`"
    ),
    "ai": (
        "\U0001f4f0 *NEWS & AI*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/news` — Latest 5-source news + AI intelligence briefing\n"
        "`/ask <question>` — Ask Kayo AI anything (uses live prices)\n"
        "`/sentiment` — Market mood, F&G, BTC dom + AI verdict\n"
        "`/macro` — Macro briefing: BTC, SOL, risk environment\n"
        "`/markets` — Global market cap & volume data\n"
        "`/index` — Fear & Greed index"
    ),
    "twitter": (
        "\U0001f426 *TWITTER / SOCIAL*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/tt <CA>` — Twitter sentiment for a token\n"
        "`/moni @user` — Analyze a KOL account (tweet history + CAs)\n"
        "`/watch @user` — Monitor account for CA drops\n"
        "`/unwatch @user` — Stop monitoring\n"
        "`/watchlist` — Your monitored accounts"
    ),
    "alerts": (
        "\U0001f514 *ALERTS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/alert <CA> <price>` — Set a price alert\n"
        "  e.g. `/alert EPjF... 0.05` — fires when price hits target\n"
        "`/myalerts` — View all your active alerts\n"
        "`/delalert <number>` — Delete an alert by its number\n"
        "`/blacklist <CA>` — Blacklist a rug (filtered from all scans)"
    ),
    "calls": (
        "\U0001f4e2 *CALLS*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/call <CA> <entry>` — Make a public alpha call\n"
        "  e.g. `/call EPjF... 0.042`\n"
        "`/mycalls` — Your call history\n"
        "`/stop <symbol> <exit>` — Close a call + auto P&L\n"
        "  e.g. `/stop WIF 0.08`\n"
        "`/leaderboard` — Top callers ranked by P&L & win rate"
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
        "\U00002699\ufe0f *SYSTEM*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "`/autoresponder` — Toggle auto-scan when CA is pasted\n"
        "`/status` — Full bot health check (Redis, AI, Twitter, group)\n"
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
        # Re-show the help category menu
        await help_cmd(u, c)
        return

    page = HELP_PAGES.get(data)
    if page:
        try:
            await query.message.edit_text(page, parse_mode="Markdown", reply_markup=BACK_BTN)
        except Exception:
            await query.message.reply_text(page, parse_mode="Markdown", reply_markup=BACK_BTN)


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

    if cmd == "help":
        await help_cmd(u, c)
        return

    if cmd in NO_ARG_CMDS:
        await NO_ARG_CMDS[cmd](u, c)
        return

    if cmd in ARG_PROMPTS and ARG_PROMPTS[cmd]:
        icon, prompt = ARG_PROMPTS[cmd]
        await query.message.reply_text(
            f"{icon} {prompt}",
            parse_mode="Markdown"
        )
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
    if not u.message or not u.message.text: return
    text = u.message.text.strip()
    uid  = u.effective_user.id
    chat = u.effective_chat

    # Always store for group summary
    group_messages.append({"uid": uid, "text": text, "time": time.time()})
    if len(group_messages) > 300: group_messages.pop(0)

    # ── 1. CA auto-scan — full deep scan (same as /scan command) ────
    if get_setting(uid, "autoresponder", True):
        for ca in extract_cas(text)[:1]:
            try:
                scanning_msg = await u.message.reply_text(
                    "\U0001f50d *Scanning...*", parse_mode="Markdown"
                )
                t = await full_token_scan(ca)
                if t.get("error"):
                    await scanning_msg.edit_text(f"\u274c {t['error']}")
                    return
                # AI verdict — same as /scan
                ai_verdict = await ai_ask(
                    f"Solana token ${t['sym']} — MCap {_usd(t['mcap'])}, liq {_usd(t['liq'])}, "
                    f"age {_age(t['created'])}, 5m {_pct(t['ch5m'])}, 1h {_pct(t['ch1h'])}, "
                    f"24h {_pct(t['ch24h'])}, buy ratio {t['buy_pct']:.0f}%, vol spike {t['vol_spike']:.1f}x, "
                    f"momentum {t['mscore']}/100, risk {t['risk_score']}/100, "
                    f"narrative #{t['narrative']}, honeypot={t['is_honeypot']}, lp_locked={t['lp_locked']}. "
                    "Give a sharp alpha verdict: is this worth aping right now? "
                    "Call out any red flags. 2-3 direct sentences.",
                    fallback="",
                    inject_market=True
                )
                await scanning_msg.delete()
                await u.message.reply_text(
                    build_scan_card(t, ai_verdict),
                    parse_mode="Markdown",
                    reply_markup=scan_buttons(ca, t["sym"]),
                    disable_web_page_preview=True,
                )
                add_xp(uid, 5)
            except Exception as _ca_err:
                logger.error(f"CA auto-scan error for {ca}: {_ca_err}", exc_info=True)
                try:
                    await scanning_msg.edit_text(
                        f"\u274c Scan failed for this token. Try `/scan {ca}` instead."
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
        if _now - _last < 6:
            return  # silently skip — don't spam
        _ai_reply_cooldown[uid] = _now

    # Show typing indicator
    try:
        await u.message.chat.send_action("typing")
    except Exception:
        pass

    add_xp(uid, 1)

    # ── 3. Unified AI reply — no keyword routing, always intelligent ──
    # Like ChatGPT/Grok: one smart prompt, live market context always injected.
    # The AI decides if the topic is crypto, casual, general — we don't pre-filter.
    name_str = u.effective_user.first_name or "fren"
    prompt = (
        f"{name_str} says: \"{text}\"\n\n"
        "Reply naturally. You are Kayo — a sharp, witty Solana alpha intelligence AND a "
        "knowledgeable friend. You can talk about ANYTHING: crypto, web3, sports, life, "
        "memes, news, culture — whatever the person brings. "
        "Rules: "
        "1. If it's casual chat (gm, lol, vibes, jokes) → be short, warm, human. 1-2 sentences. "
        "2. If it's a crypto/web3 question → use the LIVE prices from your context, cite real numbers, "
        "   drop alpha like a degen pro. Be sharp and direct. "
        "3. If it's a general knowledge question → answer like a smart helpful friend, no disclaimers. "
        "4. NEVER say you can't answer. NEVER say 'I'm just a bot'. NEVER refuse. "
        "5. For price questions: ALWAYS use the live market data provided to you. "
        "6. Plain text for casual, markdown *bold* allowed for crypto analysis. "
        "Match the energy of what they're saying."
    )
    reply = await ai_ask(
        prompt,
        fallback="yo, brain glitched for a sec — say that again?",
        max_tokens=450,
        inject_market=True  # always inject live prices — AI ignores them for casual chat
    )

    if not reply or not reply.strip():
        reply = "yo, say that again? 🤔"
    # Always try markdown (AI uses bold for crypto analysis), fall back to plain
    import re as _re
    try:
        await u.message.reply_text(reply, parse_mode="Markdown",
                                   disable_web_page_preview=True)
    except Exception:
        plain = _re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', reply)
        await u.message.reply_text(plain.strip() or reply)

# ═══════════════════════════════════════════════════════════════
# BACKGROUND SCANNERS
# ═══════════════════════════════════════════════════════════════

async def bg_main_scanner(app: Application):
    """
    PRIMARY SCANNER — every 60s
    Sources ALL Solana coins with NO keyword filtering:
      - GeckoTerminal new_pools (pages 1-6) = newest launches
      - GeckoTerminal trending_pools (pages 1-3) = momentum plays
      - DexScreener token-profiles/latest = coins with new profiles
      - DexScreener boosts/latest = boosted coins (paid attention)
    Detects: Pump | Gem | New Launch | Whale | Migration | Rebrand | Micro Gem
    """
    await asyncio.sleep(15)
    cooldown: Dict[str, float] = {}

    while True:
        try:
            now = time.time()

            # ── FETCH ALL SOURCES IN PARALLEL ────────────────────────────
            async def _fetch_gt_new(pg):
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            f"https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page={pg}",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            d = await r.json()
                            return d.get("data", [])
                except Exception as e:
                    logger.debug(f"GT new_pools pg{pg}: {e}")
                    return []

            async def _fetch_gt_trend(pg):
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            f"https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page={pg}",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            d = await r.json()
                            return d.get("data", [])
                except Exception as e:
                    logger.debug(f"GT trending pg{pg}: {e}")
                    return []

            async def _fetch_dex_profiles():
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            "https://api.dexscreener.com/token-profiles/latest/v1",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            d = await r.json()
                            return [x for x in (d if isinstance(d, list) else []) if x.get("chainId") == "solana"]
                except Exception as e:
                    logger.debug(f"dex_profiles: {e}")
                    return []

            async def _fetch_dex_boosts():
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            "https://api.dexscreener.com/token-boosts/latest/v1",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            d = await r.json()
                            return [x for x in (d if isinstance(d, list) else []) if x.get("chainId") == "solana"]
                except Exception as e:
                    logger.debug(f"dex_boosts: {e}")
                    return []

            # Parallel fetch — all sources at once
            (
                gt_new1, gt_new2, gt_new3, gt_new4, gt_new5, gt_new6,
                gt_trend1, gt_trend2, gt_trend3,
                dex_profiles, dex_boosts,
            ) = await asyncio.gather(
                _fetch_gt_new(1), _fetch_gt_new(2), _fetch_gt_new(3),
                _fetch_gt_new(4), _fetch_gt_new(5), _fetch_gt_new(6),
                _fetch_gt_trend(1), _fetch_gt_trend(2), _fetch_gt_trend(3),
                _fetch_dex_profiles(),
                _fetch_dex_boosts(),
            )

            all_gt_pools = (
                gt_new1 + gt_new2 + gt_new3 + gt_new4 + gt_new5 + gt_new6 +
                gt_trend1 + gt_trend2 + gt_trend3
            )
            boosted_addrs = {b.get("tokenAddress", "") for b in dex_boosts}
            profiled_addrs = {p.get("tokenAddress", "") for p in dex_profiles}

            # ── BUILD UNIFIED COIN MAP FROM GT POOLS ─────────────────────
            pairs_map: Dict[str, Dict] = {}
            for pool in all_gt_pools:
                tok = gt_parse_pool(pool)
                if not tok:
                    continue
                addr = tok["address"]
                if addr in pairs_map:
                    continue  # dedup — keep first occurrence (newest)
                pairs_map[addr] = tok

            # ── Also fetch DexScreener detail for profiled/boosted coins ──
            # These are coins teams paid attention to — worth scanning even if not in GT
            extra_addrs = list((profiled_addrs | boosted_addrs) - set(pairs_map.keys()))
            if extra_addrs:
                # Batch fetch up to 30 extra coins from DexScreener
                batch = extra_addrs[:30]
                try:
                    async with aiohttp.ClientSession() as s:
                        chunk = ",".join(batch)
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
                                    # Convert DexScreener pair → gt_parse_pool-compatible dict
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
                                        "pair_addr": p.get("pairAddress", ""),
                                    }
                except Exception as e:
                    logger.debug(f"dex_batch_extra: {e}")

            logger.info(f"[SCANNER] {len(pairs_map)} unique Solana coins (GT {len(all_gt_pools)} pools + {len(extra_addrs)} profiled/boosted)")

            # ── EVALUATE EACH COIN ────────────────────────────────────────
            for addr, tok in pairs_map.items():
                if addr in blacklist:
                    continue
                if now - cooldown.get(addr, 0) < 10800:  # 3h cooldown
                    continue

                sym    = tok.get("sym", "???")
                name   = tok.get("name", "")
                fdv    = float(tok.get("fdv", 0) or 0)
                mcap   = float(tok.get("mcap", 0) or fdv)
                liq    = float(tok.get("liq", 0) or 0)
                ch5m   = float(tok.get("ch5m", 0) or 0)
                ch1h   = float(tok.get("ch1h", 0) or 0)
                ch6h   = float(tok.get("ch6h", 0) or 0)
                ch24h  = float(tok.get("ch24h", 0) or 0)
                v5m    = float(tok.get("v5m", 0) or 0)
                v1h    = float(tok.get("v1h", 0) or 0)
                v24h   = float(tok.get("v24h", 0) or 0)
                b5m    = int(tok.get("b5m", 0) or 0)
                s5m    = int(tok.get("s5m", 0) or 0)
                b1h    = int(tok.get("b1h", 0) or 0)
                s1h    = int(tok.get("s1h", 0) or 0)
                price  = float(tok.get("price", 0) or 0)
                pair_addr = tok.get("pair_addr", "")

                # ── QUALITY FILTER ────────────────────────────────────────
                if fdv > 500_000: continue   # hard $500k cap
                if fdv < 1_000:   continue   # ghost token
                if liq < 300:     continue   # no real liquidity

                avg_5m_vol = v1h / 12 if v1h > 0 else 1
                vol_spike  = v5m / max(avg_5m_vol, 1)
                buy_pct    = b1h / max(b1h + s1h, 1) * 100

                if buy_pct < 48: continue
                # Must have SOME signal
                if ch1h < 2 and ch5m < 1 and b1h < 3 and vol_spike < 1.3: continue

                # ── NARRATIVE + MIGRATION DETECT ─────────────────────────
                nar = detect_narrative(f"{name} {sym}")
                b24h_sc = 0  # GT doesn't give 24h txn count
                s24h_sc = 0
                is_migrated = False  # can't detect without pair age from GT
                name_lower  = name.lower()
                sym_lower   = sym.lower()
                trending_kws = ["trump","maga","ai","agent","dog","cat","frog","ape","pepe","pump","elon","doge"]
                is_rebranded = (
                    any(kw in name_lower for kw in trending_kws) and
                    not any(kw in sym_lower for kw in trending_kws)
                )
                is_boosted  = addr in boosted_addrs
                has_profile = addr in profiled_addrs

                # ── PATTERN DETECTION ─────────────────────────────────────
                alert_type = None

                # 🚀 PUMP — strong 5m move with net buyers
                if ch5m >= 3 and b5m > s5m and buy_pct >= 52:
                    alert_type = "pump"

                # 🚀 BIG PUMP — explosive 5m or 1h move
                elif ch5m >= 20 and buy_pct >= 50:
                    alert_type = "pump"
                elif ch1h >= 40 and buy_pct >= 50:
                    alert_type = "pump"

                # 💎 GEM — low cap with sustained 1h momentum
                elif ch1h >= 10 and buy_pct >= 55 and liq >= 1000:
                    alert_type = "gem"

                # 🆕 NEW LAUNCH — early traction
                elif b1h >= 5 and buy_pct >= 55 and liq >= 500 and ch1h >= 5:
                    alert_type = "new"

                # 🔄 REBRAND — trending narrative play
                elif is_rebranded and buy_pct >= 52 and ch1h >= 4 and liq >= 500:
                    alert_type = "rebrand"

                # 🐳 WHALE — heavy buying, price barely moved (stealth accumulation)
                elif buy_pct >= 62 and b1h >= 15 and abs(ch5m) < 8:
                    alert_type = "whale"

                # ⚡ MOMENTUM — sustained buying pressure
                elif ch1h >= 8 and buy_pct >= 55 and b1h >= 8:
                    alert_type = "pump"

                # ⚠️ UNUSUAL — volume spike with buyers
                elif vol_spike >= 3.0 and b1h >= 5 and buy_pct >= 55:
                    alert_type = "unusual"

                # 🌱 MICRO GEM — tiny mcap, real buyers showing up
                elif fdv < 80_000 and b1h >= 10 and buy_pct >= 60:
                    alert_type = "unusual"

                # ⭐ BOOSTED — team paid for a boost + some traction
                elif is_boosted and buy_pct >= 52 and b1h >= 5 and ch1h >= 3:
                    alert_type = "new"

                if not alert_type:
                    continue

                # ── QUALITY GATE ──────────────────────────────────────────
                if fdv > 50_000 and liq / fdv < 0.003: continue  # rug risk (relaxed for micro caps)
                if buy_pct < 48: continue

                # ── PATTERN MEMORY GATE ───────────────────────────────────
                pm_key = f"{alert_type}:{nar}"
                pm_info = pattern_memory.get(pm_key, {})
                if pm_info.get("total", 0) >= 5:
                    win_rate = pm_info["wins"] / max(pm_info["total"], 1)
                    if win_rate < 0.25:
                        logger.info(f"[PATTERN SKIP] {pm_key} wr={win_rate:.0%}")
                        continue

                # ── DROPPED CALLS GATE ────────────────────────────────────
                if addr in dropped_calls:
                    last_dropped = dropped_calls[addr].get("time", 0)
                    last_price   = dropped_calls[addr].get("entry_price", 0)
                    price_change_since = abs(price - last_price) / max(last_price, 1e-12) * 100
                    if now - last_dropped < 10800: continue       # 3h hard cooldown
                    if price_change_since < 15: continue          # must move 15%+ to re-drop

                # ── DEDUP ─────────────────────────────────────────────────
                alert_id = hashlib.md5(f"{addr}:{alert_type}:{int(now/3600)}".encode()).hexdigest()[:16]
                if _seen_check(seen_alert_ids, alert_id): continue
                _seen_add(seen_alert_ids, alert_id)
                asyncio.create_task(_save())

                cooldown[addr] = now

                # ── BUILD TOKEN DICT & SEND ALERT ─────────────────────────
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
                    "created": 0, "narrative": nar,
                    "tw_link": "", "tg_link": "", "web_link": "",
                    "boost_active": 1 if is_boosted else 0,
                    "has_profile": has_profile, "has_ad": False,
                    "pair_addr": pair_addr,
                    "mscore": min(100, int(abs(ch1h) + buy_pct / 2 + vol_spike * 10)),
                }
                pm_hint = ""
                if pm_info.get("total", 0) >= 3:
                    wr = pm_info["wins"] / max(pm_info["total"], 1)
                    pm_hint = f" | Pattern wr={wr:.0%} ({pm_info['total']} calls)"

                card = build_alert_card(tok_dict, alert_type, pm_hint)
                buttons = scan_buttons(addr, sym)

                if GROUP_CHAT_ID:
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=card,
                            parse_mode="Markdown",
                            reply_markup=buttons,
                            disable_web_page_preview=True,
                        )
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
                    except Exception as e:
                        logger.error(f"[ALERT SEND ERROR] {sym}: {e}")

        except Exception as e:
            logger.error(f"[bg_main_scanner] {e}", exc_info=True)

        await asyncio.sleep(60)


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

                alert_id = hashlib.md5(f"{addr}:newlaunch".encode()).hexdigest()[:16]
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

                if liq < 500 or fdv < 2000 or fdv > 500_000: continue  # hard $500k cap

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

                if score < 15: continue  # very low — catch everything with any real signal
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
    AI-DRIVEN NARRATIVE SCANNER — every 8 minutes.

    The old approach (keyword dict matching) is replaced entirely.

    HOW IT WORKS:
    1. Fetch latest headlines from 5 crypto/news RSS feeds
    2. Feed the RAW headlines to the AI — no keyword matching at all
    3. AI reads them like a human journalist + degen caller combined
    4. AI generates fresh DexScreener search terms from scratch based on
       what it actually understands — not what is in a predefined list
    5. Search DexScreener with AI-generated terms
    6. Find Solana tokens under $500k matching the live narrative
    7. AI connects the specific news story to the specific token
    8. Alert to group with full context + thesis

    Example (no keywords needed):
    - Headline: "Elon Musk announces SpaceX lands on Mars in 2027"
    - AI generates: ["mars","space","elon","rocket","spacex","musk"]
    - Finds $MARS, $SPACE tokens on Solana with buy pressure
    - Explains: "SpaceX Mars timeline = meme coin catalyst. $MARS at
      $180k mcap with 67% buys is the play before CT wakes up."
    """
    await asyncio.sleep(90)
    last_run       = 0
    last_headlines: List[str] = []

    while True:
        try:
            now = time.time()
            if now - last_run < 480:  # 8 min
                await asyncio.sleep(30); continue
            last_run = now

            # ── Step 1: Fetch latest news ────────────────────────────────
            items     = await fetch_news(20)
            headlines = [f"[{it['source']}] {it['title']}" for it in items]
            if not headlines:
                await asyncio.sleep(60); continue

            new_headlines = [h for h in headlines if h not in last_headlines]
            if len(new_headlines) < 2:
                await asyncio.sleep(60); continue
            last_headlines = headlines

            logger.info(f"[NARRATIVE AI] {len(new_headlines)} new headlines to process")

            # ── Step 2: AI reads headlines and generates search terms ────
            # Key change: AI understands the STORY, not a keyword dict
            headlines_block = "\n".join([f"- {h}" for h in new_headlines[:15]])

            ai_terms_raw = await ai_ask(
                f"You are a crypto narrative hunter looking for Solana pump opportunities.\n"
                f"Latest news headlines:\n{headlines_block}\n\n"
                "Find headlines that could create a Solana meme coin or token narrative.\n"
                "A narrative = real-world news (political event, viral moment, celebrity, "
                "sports result, tech launch, cultural trend) that makes people create or "
                "buy tokens with related names.\n\n"
                "For EACH narrative you spot, output EXACTLY (one per line):\n"
                "NARRATIVE: <short name> | TERMS: <word1>,<word2>,<word3>,<word4>,<word5> | STORY: <one sentence>\n\n"
                "TERMS must be short words someone would use in a Solana meme coin ticker/name.\n"
                "Example: Elon Mars news → TERMS: elon,mars,space,rocket,spacex,musk\n"
                "Output 2-4 narratives max. If none have pump potential output: NONE\n"
                "Do NOT explain. Output the formatted lines only.",
                fallback="NONE",
                max_tokens=350,
                inject_market=False
            )

            if not ai_terms_raw or ai_terms_raw.strip().upper() == "NONE":
                logger.info("[NARRATIVE AI] No pump narratives this cycle")
                await asyncio.sleep(60); continue

            # ── Step 3: Parse AI output ──────────────────────────────────
            narrative_plays = []
            for line in ai_terms_raw.strip().split("\n"):
                line = line.strip()
                if "NARRATIVE:" not in line: continue
                try:
                    parts    = line.split("|")
                    nar_name = parts[0].replace("NARRATIVE:", "").strip()
                    terms    = [t.strip().lower() for t in parts[1].replace("TERMS:", "").strip().split(",") if t.strip()][:6]
                    story    = parts[2].replace("STORY:", "").strip() if len(parts) > 2 else nar_name
                    if nar_name and terms:
                        narrative_plays.append({"name": nar_name, "terms": terms, "story": story})
                except Exception:
                    continue

            if not narrative_plays:
                await asyncio.sleep(60); continue

            logger.info(f"[NARRATIVE AI] Narratives found: {[p['name'] for p in narrative_plays]}")

            # ── Step 4: Search DexScreener per narrative ─────────────────
            for play in narrative_plays[:3]:
                nar_name = play["name"]
                terms    = play["terms"]
                story    = play["story"]

                queries   = [f"solana {t}" for t in terms[:4]]
                pairs_map = await dex_multi_search(queries)
                if not pairs_map: continue

                # ── Step 5: Filter — must actually match narrative terms ──
                candidates = []
                for addr, p in pairs_map.items():
                    if addr in blacklist: continue
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
                    b1h     = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                    s1h     = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                    v1h     = float((p.get("volume") or {}).get("h1", 0) or 0)
                    v24h    = float((p.get("volume") or {}).get("h24", 0) or 0)

                    if liq < 1500 or fdv < 5_000 or fdv > 500_000: continue
                    buy_pct = b1h / max(b1h + s1h, 1) * 100
                    if buy_pct < 48: continue

                    # Token must actually reference the narrative in name/symbol
                    token_text = f"{sym} {name}".lower()
                    term_hits  = sum(1 for t in terms if t in token_text)
                    if term_hits == 0: continue  # completely unrelated = skip

                    # Score
                    score = term_hits * 25
                    if ch1h  > 20:  score += 30
                    elif ch1h > 5:  score += 15
                    if ch6h  > 50:  score += 25
                    if buy_pct > 65: score += 20
                    elif buy_pct > 55: score += 10
                    if v24h > 20_000: score += 15
                    if b1h > 15: score += 10
                    created = int(p.get("pairCreatedAt", 0) or 0)
                    age_min = (now * 1000 - created) / 60000 if created else 9999
                    if age_min < 60:   score += 30  # new token in hot narrative = jackpot
                    elif age_min < 240: score += 12

                    if score >= 30:
                        candidates.append((score, addr, p, sym, name, fdv, liq,
                                           ch1h, ch6h, ch24h, b1h, s1h, buy_pct, age_min))

                if not candidates: continue
                candidates.sort(reverse=True)

                # ── Step 6 + 7: Alert top 2 with AI thesis ───────────────
                for (score, addr, p, sym, name, fdv, liq, ch1h, ch6h, ch24h,
                     b1h, s1h, buy_pct, age_min) in candidates[:2]:

                    # Cooldown — 3h per addr
                    alert_id = hashlib.md5(
                        f"{addr}:nar_ai:{nar_name}:{int(now/10800)}".encode()
                    ).hexdigest()[:16]
                    if _seen_check(seen_alert_ids, alert_id): continue
                    if addr in dropped_calls and now - dropped_calls[addr].get("time",0) < 10800:
                        continue

                    _seen_add(seen_alert_ids, alert_id)
                    asyncio.create_task(_save())

                    price_now = float(p.get("priceUsd", 0) or 0)
                    age_str   = f"{age_min:.0f}min" if age_min < 1440 else f"{age_min/1440:.1f}d"

                    # AI writes the thesis connecting THIS news to THIS token
                    ai_thesis = await ai_ask(
                        f"NEWS: {story}\n"
                        f"TOKEN: ${sym} ({name})\n"
                        f"MCap {_usd(fdv)} | Liq {_usd(liq)} | Age {age_str}\n"
                        f"1h: {_pct(ch1h)} | 6h: {_pct(ch6h)} | Buys: {b1h} | Buy%: {buy_pct:.0f}%\n\n"
                        "As a degen alpha caller: 1-2 sharp sentences.\n"
                        "1) WHY this token fits this specific news narrative\n"
                        "2) Is it worth aping right now — entry thesis, risk, potential?\n"
                        "Cite the actual numbers. Be direct.",
                        fallback="",
                        inject_market=True
                    )

                    bp_arrow = "\U0001f7e2" if buy_pct > 60 else "\U0001f534" if buy_pct < 40 else "\u26aa"
                    msg_text = (
                        f"\U0001f4f0 *NARRATIVE PLAY — {nar_name.upper()}*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"\U0001f4f0 *Story:* _{story[:120]}_\n\n"
                        f"*${sym}* | _{name}_\n"
                        f"\U0001f4a0 MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`  Age: {age_str}\n"
                        f"\U0001f4b5 Price: `{_price(price_now)}`\n"
                        f"1h: {_pct(ch1h)}  6h: {_pct(ch6h)}  24h: {_pct(ch24h)}\n"
                        f"{bp_arrow} Buys/Sells (1h): {b1h}/{s1h} → *{buy_pct:.0f}% buys*\n"
                        f"\U0001f511 Matched: {' | '.join(terms[:4])}\n"
                        f"\n`{addr}`"
                    )
                    if ai_thesis:
                        msg_text += f"\n\n\U0001f9e0 {ai_thesis}"

                    if GROUP_CHAT_ID != 0:
                        try:
                            nar_msg = await app.bot.send_message(
                                chat_id=GROUP_CHAT_ID,
                                text=msg_text,
                                parse_mode="Markdown",
                                reply_markup=scan_buttons(addr, sym, p.get("pairAddress", "")),
                                disable_web_page_preview=True,
                            )
                            logger.info(f"[NARRATIVE AI] Alerted ${sym} | {nar_name} | score={score}")
                            dropped_calls[addr] = {
                                "sym": sym, "name": name,
                                "entry_price": price_now,
                                "mcap_entry":  fdv,
                                "time":        now,
                                "alert_type":  "narrative",
                                "msg_id":      nar_msg.message_id,
                                "chat_id":     GROUP_CHAT_ID,
                                "alerted_10x": False, "alerted_5x": False, "alerted_rug": False,
                            }
                            asyncio.create_task(_save())
                            await asyncio.sleep(3)
                        except Exception as e:
                            try:
                                import re as _ren
                                plain = _ren.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', msg_text)
                                await app.bot.send_message(
                                    chat_id=GROUP_CHAT_ID, text=plain,
                                    reply_markup=scan_buttons(addr, sym),
                                )
                            except Exception:
                                logger.warning(f"narrative alert: {e}")

                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"bg_narrative_news_scanner: {e}", exc_info=True)
        await asyncio.sleep(30)



async def bg_trending_metas_scanner(app: Application):
    """Every 20min: post trending metas digest to group."""
    await asyncio.sleep(180)
    last_run = 0
    while True:
        try:
            now = time.time()
            if now - last_run < 1200:
                await asyncio.sleep(60); continue
            last_run = now

            # Get trending metas narrative names — used to find sub-$500k tokens IN those metas
            metas = await dex_trending_metas()
            if not metas or GROUP_CHAT_ID == 0:
                await asyncio.sleep(60); continue

            # Only post metas digest if we have actionable tokens under $500k in the meta
            # (The trending digest of $36B coins is useless for degen trading — skip it)
            meta_names = [m.get("name","?") for m in metas[:6]]
            nar_slugs  = [m.get("slug", m.get("name","")).lower() for m in metas[:6]]

            # Search for sub-$500k tokens in each trending meta
            meta_finds = []
            for slug in nar_slugs[:4]:
                kws_meta = NARRATIVES.get(slug, [slug])
                pairs_m  = await dex_multi_search([f"solana {kw}" for kw in kws_meta[:2]])
                for addr_m, p_m in pairs_m.items():
                    fdv_m  = float(p_m.get("fdv", 0) or 0)
                    liq_m  = float((p_m.get("liquidity") or {}).get("usd", 0) or 0)
                    ch1h_m = float((p_m.get("priceChange") or {}).get("h1", 0) or 0)
                    b1h_m  = int(((p_m.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                    s1h_m  = int(((p_m.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                    bp_m   = b1h_m / max(b1h_m + s1h_m, 1) * 100
                    sym_m  = (p_m.get("baseToken") or {}).get("symbol","?")
                    if fdv_m > 500_000 or fdv_m < 5_000: continue  # HARD $500k cap
                    if liq_m < 2000: continue
                    if bp_m < 55: continue  # buy-only
                    if ch1h_m < 5: continue
                    meta_finds.append((ch1h_m, slug, sym_m, fdv_m, liq_m, bp_m))

            if not meta_finds:
                # No actionable sub-$500k tokens in trending metas — skip posting
                logger.info("[METAS] No sub-$500k tokens found in trending metas this run")
            else:
                meta_finds.sort(reverse=True)
                ai_meta = await ai_ask(
                    f"Trending metas: {meta_names[:4]}. "
                    f"Sub-$500k degen plays in these metas: "
                    + ", ".join(f"${s[2]} (#{s[1]}, 1h {s[0]:+.0f}%)" for s in meta_finds[:3])
                    + ". Which meta/token has the best momentum for a quick degen flip? "
                    "2 sentences, be direct.",
                    fallback="", inject_market=True
                )
                meta_lines = ["\U0001f525 *TRENDING META — DEGEN PLAYS* _(sub-$500k only)_\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
                for ch, slug_l, sym_l, fdv_l, liq_l, bp_l in meta_finds[:4]:
                    meta_lines.append(f"• *${sym_l}* #{slug_l.upper()}  MCap:`{_usd(fdv_l)}`  1h:{_pct(ch)}  Buy:{bp_l:.0f}%")
                if ai_meta: meta_lines.append(f"\n\U0001f9e0 _{ai_meta}_")
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text="\n".join(meta_lines),
                        parse_mode="Markdown",
                    )
                    logger.info(f"[METAS] Posted {len(meta_finds)} sub-$500k meta plays")
                except Exception as e:
                    logger.warning(f"metas post: {e}")
        except Exception as e:
            logger.error(f"bg_trending_metas: {e}", exc_info=True)
        await asyncio.sleep(60)


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
        logger.info("✅ Async Redis connected")
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
        f"🦅 Kayo Brain v29 ready — "
        f"Groq: {'✅' if GROQ_API_KEY else '❌'} | "
        f"Gemini: {'✅' if GEMINI_API_KEY else '❌'} | "
        f"Group alerts: {'✅ '+str(GROUP_CHAT_ID) if GROUP_CHAT_ID != 0 else '❌ set GROUP_CHAT_ID'}"
    )


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
    ]
    for name, fn in CMDS:
        app.add_handler(CommandHandler(name, fn))
    # CallbackQuery handler for inline chart button
    # CallbackQuery handlers
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(handle_help_callback, pattern=r"^help:"))
    app.add_handler(CallbackQueryHandler(handle_chart_callback, pattern=r"^chart:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

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
            logger.info("🚀 All scanners started")
            while True:
                await asyncio.sleep(3600)

    asyncio.run(run())


if __name__ == "__main__":
    main()
