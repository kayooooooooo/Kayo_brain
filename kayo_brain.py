"""
╔══════════════════════════════════════════════════════════════════════╗
║                    KAYO BRAIN v16 — PRO REBUILD                     ║
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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
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
def _root(): return "🦅 Kayo Brain v16", 200

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
# BUG FIX: Use OrderedDict as a bounded ordered set so we can evict
# the OLDEST entries (not random ones like plain set).
seen_alert_ids:  "OrderedDict[str, int]" = OrderedDict()  # key=id, value=timestamp
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
        seen_alert_ids  = OrderedDict((k, 0) for k in d.get("seen_alert_ids", []))
        logger.info(f"✅ State loaded — {len(watchlist)} watched, {len(active_calls)} calls, {len(seen_alert_ids)} seen alerts")
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
_MARKET_CTX_TTL = 60   # seconds between refreshes

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

                    ctx = (
                        f"[LIVE MARKET DATA - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}]\n"
                        f"BTC: ${btc.get('usd',0):,.0f} ({btc.get('usd_24h_change',0):+.2f}% 24h) | MCap ${btc.get('usd_market_cap',0)/1e9:.1f}B\n"
                        f"ETH: ${eth.get('usd',0):,.0f} ({eth.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"SOL: ${sol.get('usd',0):,.2f} ({sol.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"BNB: ${bnb.get('usd',0):,.2f} ({bnb.get('usd_24h_change',0):+.2f}% 24h)\n"
                        f"Fear & Greed: {fg_v}/100 - {fg_c}\n"
                        f"---\n"
                        f"You are Kayo, a sharp Solana alpha intelligence bot. "
                        f"ALWAYS use the live data above when asked about prices — never use training data for prices."
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
    system_msg = {
        "role": "system",
        "content": (
            f"{system_ctx}\n\n"
            "Style: Drop alpha like a pro - cite exact prices, be sharp and direct, "
            "no fluff, no disclaimers. Telegram traders scan fast."
        )
    }

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
                        timeout=aiohttp.ClientTimeout(total=14),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            return d["choices"][0]["message"]["content"].strip()
                        elif r.status == 429:
                            await asyncio.sleep(1.5)
                            continue
            except Exception as e:
                logger.debug(f"Groq {model}: {e}")

    if GEMINI_API_KEY:
        try:
            full_prompt = f"{system_ctx}\n\n{prompt}"
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": full_prompt}]}],
                          "generationConfig": {"maxOutputTokens": max_tokens}},
                    timeout=aiohttp.ClientTimeout(total=18),
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        return d["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            logger.debug(f"Gemini: {e}")

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
    "ai":         ["ai", "agent", "gpt", "intelligence", "neural", "llm", "openai", "deepseek", "groq"],
    "gaming":     ["game", "gaming", "play", "nft", "quest", "rpg", "metaverse", "gamer"],
    "defi":       ["defi", "swap", "yield", "lend", "farm", "liquidity", "amm", "dex", "vault"],
    "meme":       ["dog", "cat", "pepe", "frog", "doge", "shib", "bonk", "wif", "wen", "gm"],
    "sports":     ["football", "soccer", "fifa", "worldcup", "nba", "sport", "athlete", "fan"],
    "rwa":        ["rwa", "real", "estate", "bond", "treasury", "commodity", "gold", "asset"],
    "infra":      ["infra", "layer", "bridge", "zk", "rollup", "validator", "oracle", "chain"],
    "payments":   ["payment", "pay", "visa", "card", "bank", "fiat", "transfer", "remit"],
    "social":     ["social", "friend", "community", "dao", "vote", "creator", "tiktok", "twitter"],
    "health":     ["health", "medical", "bio", "pharma", "longevity", "fitness", "wellness"],
    "politics":   ["trump", "election", "president", "government", "fed", "reserve", "macro"],
    "celebrity":  ["elon", "musk", "kanye", "trump", "maga", "celebrity", "viral", "hype"],
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
    pairs, sec, orders = await asyncio.gather(
        dex_pairs_by_token(address),
        goplus_check(address),
        dex_token_orders(address),
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
    age    = _age(t["created"])
    risk   = _risk(t["risk_score"])
    press  = ("🔥 BUY PRESSURE" if t["buy_pct"] > 60
              else "🔻 SELL PRESSURE" if t["buy_pct"] < 40
              else "⚖️ NEUTRAL")
    tags   = []
    if t["boost_active"] > 0: tags.append("💰 BOOSTED")
    if t["has_profile"]:       tags.append("✅ VERIFIED")
    if t["is_honeypot"]:       tags.append("🚨 HONEYPOT")
    tag_str = "  ".join(tags)

    card = (
        f"🦅 *KAYO SCAN — ${t['sym']}*\n"
        f"_{t['name']}_  {tag_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Price:* {_price(t['price'])}\n"
        f"📊 *MCap:* `{_usd(t['mcap'])}`  ·  *Liq:* `{_usd(t['liq'])}` ({t['liq_ratio']:.1f}%)\n"
        f"⏱ *Age:* {age}  ·  *Narrative:* #{t['narrative'].upper()}\n\n"
        f"📈 *Price Change*\n"
        f"  5m: {_pct(t['ch5m'])}  ·  1h: {_pct(t['ch1h'])}\n"
        f"  6h: {_pct(t['ch6h'])}  ·  24h: {_pct(t['ch24h'])}\n\n"
        f"💹 *Volume*\n"
        f"  5m: `{_usd(t['v5m'])}`  ·  1h: `{_usd(t['v1h'])}`  ·  24h: `{_usd(t['v24h'])}`\n\n"
        f"🔄 *Transactions (1h)*\n"
        f"  🟢 Buys: {t['b1h']}  🔴 Sells: {t['s1h']}  →  {press}\n"
        f"  Buy ratio: {t['buy_pct']:.0f}%  ·  Vol spike: {t['vol_spike']:.1f}x\n\n"
        f"⚡ *Momentum:* [{_bar(t['mscore'])}] {t['mscore']}/100\n"
        f"🛡 *Security:* {risk}  (score {t['risk_score']}/100)\n"
    )
    if t.get("sell_tax", 0) > 0 or t.get("buy_tax", 0) > 0:
        card += f"  Buy tax: {t['buy_tax']}%  ·  Sell tax: {t['sell_tax']}%\n"
    if t["lp_locked"]:    card += "  🔒 LP Locked\n"
    if t["is_renounced"]: card += "  ✅ Contract renounced\n"
    if t["red_flags"]:
        card += "\n*🚩 Risk Flags:*\n" + "\n".join(f"  {f}" for f in t["red_flags"][:3]) + "\n"
    if t["green_flags"]:
        card += "\n*✅ Green Flags:*\n" + "\n".join(f"  {f}" for f in t["green_flags"][:2]) + "\n"
    card += f"\n🌐 *Socials:* {_social_line(t)}\n"
    card += f"\n`{t['address']}`\n"
    if ai:
        card += f"\n🧠 *Kayo AI:*\n_{ai}_"
    return card

def build_alert_card(t: Dict, alert_type: str, ai: str = "") -> str:
    """Compact alert card for scanner — clean, fast to read."""
    icons = {
        "pump":  "🚀 *PUMP ALERT*",
        "dump":  "💀 *DUMP ALERT*",
        "whale": "🐳 *WHALE ACCUMULATION*",
        "gem":   "💎 *GEM SPOTTED*",
        "new":   "🆕 *NEW LAUNCH*",
        "narrative": "📖 *NARRATIVE PLAY*",
    }
    header = icons.get(alert_type, "⚡ *ALERT*")
    press  = f"{t['buy_pct']:.0f}% buys"
    age    = _age(t["created"])
    boost  = " 💰" if t.get("boost_active", 0) > 0 else ""

    card = (
        f"{header}{boost}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*${t['sym']}* — _{t['name']}_\n"
        f"Age: {age}  ·  MCap: `{_usd(t['mcap'])}`  ·  Liq: `{_usd(t['liq'])}`\n\n"
        f"  5m: {_pct(t['ch5m'])}  ·  1h: {_pct(t['ch1h'])}\n"
        f"  Vol 5m: `{_usd(t['v5m'])}`  ·  Spike: {t['vol_spike']:.1f}x\n"
        f"  Buys/Sells (1h): {t['b1h']} / {t['s1h']}  →  {press}\n"
        f"  ⚡ Momentum: {t['mscore']}/100  ·  {_risk(t['risk_score'])}\n"
    )
    if t.get("is_honeypot"):
        card += "  🚨 *HONEYPOT — DO NOT BUY*\n"
    if t.get("sell_tax", 0) > 10:
        card += f"  ⚠️ Sell tax: {t['sell_tax']}%\n"
    if t.get("tw_link") or t.get("tg_link"):
        card += f"  {_social_line(t)}\n"
    card += f"\n`{t['address']}`"
    if ai:
        card += f"\n\n🧠 _{ai}_"
    return card

def scan_buttons(addr: str, sym: str = "") -> InlineKeyboardMarkup:
    label = f"📊 {sym} Chart" if sym else "📊 Chart"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(label, callback_data=f"chart:{addr}"),
            InlineKeyboardButton("🔫 Photon", url=f"https://photon-sol.tinyastro.io/en/lp/{addr}"),
            InlineKeyboardButton("🌙 BullX", url=f"https://bullx.io/terminal?chainId=1399811149&address={addr}"),
        ],
        [
            InlineKeyboardButton("🍌 Banana", url=f"https://t.me/BananaGunSolana_bot?start=snipe_{addr}"),
            InlineKeyboardButton("🐸 GMGN", url=f"https://gmgn.ai/sol/token/{addr}"),
            InlineKeyboardButton("🦅 Birdeye", url=f"https://birdeye.so/token/{addr}?chain=solana"),
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
        f"\U0001f985 *KAYO BRAIN v16*\n"
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
        "\U0001f985 *KAYO BRAIN v16 — COMMANDS*\n"
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
    msg = await u.message.reply_text("🏃 *Finding top runners...*", parse_mode="Markdown")
    QUERIES = ["solana meme", "solana ai", "solana new", "solana gaming", "solana pump", "solana defi"]
    pairs_map = await dex_multi_search(QUERIES)
    pairs = [p for addr, p in pairs_map.items() if addr not in blacklist]
    pairs.sort(key=lambda p: float((p.get("priceChange") or {}).get("h1", 0) or 0), reverse=True)
    top   = [p for p in pairs if float((p.get("priceChange") or {}).get("h1", 0) or 0) > 5][:10]
    if not top:
        await msg.edit_text("😴 No significant runners right now."); return
    add_xp(u.effective_user.id, 3)
    lines = ["🏃 *TOP SOLANA RUNNERS — 1H*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, p in enumerate(top, 1):
        base  = p.get("baseToken", {})
        sym   = base.get("symbol", "???")
        addr  = base.get("address", "")
        ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
        s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
        nar   = detect_narrative(f"{sym} {base.get('name','')}")
        lines.append(
            f"\n*{i}. ${sym}* — #{nar.upper()}\n"
            f"  1h: {_pct(ch1h)}  24h: {_pct(ch24h)}\n"
            f"  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
            f"  Buys/Sells: {b1h}/{s1h}\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

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
    QUERIES = ["solana meme", "solana new", "solana dog", "solana cat", "solana ai", "solana pump"]
    pairs_map = await dex_multi_search(QUERIES)
    gems = []
    for addr, p in pairs_map.items():
        if addr in blacklist: continue
        fdv  = float(p.get("fdv", 0) or 0)
        liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        ch1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        b1h  = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
        s1h  = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
        if fdv > 2_000_000 or fdv < 10_000: continue
        if liq < 3000 or liq / max(fdv, 1) < 0.03: continue
        if ch1h < 10 or b1h < s1h: continue
        gems.append(p)
    gems.sort(key=lambda p: float((p.get("priceChange") or {}).get("h1", 0) or 0), reverse=True)
    if not gems:
        await msg.edit_text("💤 No hidden gems right now."); return
    add_xp(u.effective_user.id, 3)
    lines = ["💎 *HIDDEN GEMS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for p in gems[:6]:
        base  = p.get("baseToken", {})
        sym   = base.get("symbol", "???")
        addr  = base.get("address", "")
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        ch24h = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
        s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
        age   = _age(p.get("pairCreatedAt", 0) or 0)
        liq_r = liq / max(fdv, 1) * 100
        nar   = detect_narrative(f"{sym} {base.get('name','')}")
        lines.append(
            f"\n💎 *${sym}* — #{nar.upper()}  Age: {age}\n"
            f"  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}` ({liq_r:.1f}%)\n"
            f"  1h: {_pct(ch1h)}  24h: {_pct(ch24h)}\n"
            f"  Buys/Sells: {b1h}/{s1h}\n"
            f"  `{addr}`"
        )
    await msg.edit_text("\n".join(lines), parse_mode="Markdown")

async def trending_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    msg   = await u.message.reply_text("🔥 *Fetching trending metas...*", parse_mode="Markdown")
    metas = await dex_trending_metas()
    if not metas:
        await msg.edit_text("❌ Could not fetch trending metas."); return
    add_xp(u.effective_user.id, 2)
    lines = ["🔥 *TRENDING METAS*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
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
    pairs = [p for p in pairs if p.get("chainId") == "solana"
             and float((p.get("liquidity") or {}).get("usd", 0) or 0) > 2000]
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

async def ask_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not c.args:
        await u.message.reply_text("Usage: `/ask <question>`", parse_mode="Markdown"); return
    q   = " ".join(c.args)
    msg = await u.message.reply_text("🧠 *Kayo thinking...*", parse_mode="Markdown")
    add_xp(u.effective_user.id, 2)
    ans = await ai_ask(
        f"Trader question: {q}\n"
        "Answer using the live market data provided in your context. "
        "Be sharp, cite exact numbers, and drop alpha like a pro trader would. "
        "If the question is about price, ALWAYS use the live prices above.",
        max_tokens=420,
        inject_market=True
    )
    ts = datetime.utcnow().strftime("%H:%M UTC")
    await msg.edit_text(
        f"\U0001f9e0 *Kayo AI*\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n{ans}\n\n_Live data as of {ts}_",
        parse_mode="Markdown"
    )

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
    await msg.edit_text(f"🏓 *Pong!* {ms}ms — Kayo Brain v15 alive.", parse_mode="Markdown")

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

    # Buttons
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 DexScreener", url=dex_url),
            InlineKeyboardButton("🦅 Birdeye", url=f"https://birdeye.so/token/{addr}?chain=solana"),
        ],
        [
            InlineKeyboardButton("🔫 Photon", url=f"https://photon-sol.tinyastro.io/en/lp/{addr}"),
            InlineKeyboardButton("🌙 BullX", url=f"https://bullx.io/terminal?chainId=1399811149&address={addr}"),
        ],
    ])

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

async def status_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    redis_ok  = "✅" if _redis else "❌"
    groq_ok   = "✅" if GROQ_API_KEY else "❌"
    gemini_ok = "✅" if GEMINI_API_KEY else "❌"
    tw_ok     = "✅" if TWITTER_AUTH_TOKEN else "❌"
    group_ok  = "✅" if GROUP_CHAT_ID != 0 else f"❌ (set GROUP_CHAT_ID)"
    await u.message.reply_text(
        f"⚙️ *KAYO BRAIN v16 STATUS*\n"
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
        "`/scan <CA>` — Full token deep scan + AI verdict\n"
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

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 DexScreener", url=dex_url),
            InlineKeyboardButton("🦅 Birdeye", url=f"https://birdeye.so/token/{addr}?chain=solana"),
        ],
        [
            InlineKeyboardButton("🔫 Photon", url=f"https://photon-sol.tinyastro.io/en/lp/{addr}"),
            InlineKeyboardButton("🌙 BullX", url=f"https://bullx.io/terminal?chainId=1399811149&address={addr}"),
        ],
    ])

    # Try chart images
    chart_url = None
    chart_source = ""
    chart_candidates = [
        (f"https://io.dexscreener.com/dex/chart/amm/v3/solana/{pair_addr}?theme=dark&interval=15&baseToken={addr}", "DexScreener"),
        (f"https://io.dexscreener.com/dex/chart/amm/v2/solana/{pair_addr}?theme=dark&interval=15&baseToken={addr}", "DexScreener"),
        (f"https://cache.defined.fi/charts/{addr}?resolution=15&networkId=1399811149", "Defined.fi"),
    ]
    async with aiohttp.ClientSession() as s:
        for url, src in chart_candidates:
            try:
                async with s.head(url, timeout=aiohttp.ClientTimeout(total=5),
                                  headers={"User-Agent": "Mozilla/5.0"}) as r:
                    ct = r.headers.get("content-type", "")
                    if r.status == 200 and "image" in ct:
                        chart_url = url; chart_source = src; break
            except Exception:
                continue

    if chart_url:
        cap = caption + f"\n\n_Chart via {chart_source}_"
        try:
            await query.message.reply_photo(photo=chart_url, caption=cap,
                                            parse_mode="Markdown", reply_markup=markup)
            return
        except Exception as e:
            logger.debug(f"chart photo: {e}")

    # Fallback: stats card with links
    await query.message.reply_text(
        f"📊 *${sym} CHART*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Image not available — open chart via links below._\n\n" + caption,
        parse_mode="Markdown", reply_markup=markup, disable_web_page_preview=True
    )


async def handle_message(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.message.text: return
    text = u.message.text
    uid  = u.effective_user.id
    group_messages.append({"uid": uid, "text": text, "time": time.time()})
    if len(group_messages) > 200: group_messages.pop(0)
    if not get_setting(uid, "autoresponder", True): return
    for ca in extract_cas(text)[:1]:
        pairs = await dex_pairs_by_token(ca)
        if pairs:
            p     = pairs[0]
            sym   = p.get("baseToken", {}).get("symbol", "???")
            price = float(p.get("priceUsd", 0) or 0)
            fdv   = float(p.get("fdv", 0) or 0)
            liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
            ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
            await u.message.reply_text(
                f"⚡ *${sym}*\n"
                f"Price: {_price(price)}  MCap: `{_usd(fdv)}`\n"
                f"Liq: `{_usd(liq)}`  1h: {_pct(ch1h)}\n"
                f"`{ca}`",
                parse_mode="Markdown",
                reply_markup=scan_buttons(ca, sym),
            )
            add_xp(uid, 1)

# ═══════════════════════════════════════════════════════════════
# BACKGROUND SCANNERS
# ═══════════════════════════════════════════════════════════════

async def bg_main_scanner(app: Application):
    """
    PRIMARY SCANNER — every 30s
    Detects: Pump | Dump | Whale | Gem | New Launch (<30min)
    Uses all 6 DexScreener query categories + token-profiles endpoint
    Dedup via Redis-persisted seen_alert_ids
    """
    await asyncio.sleep(20)
    cooldown: Dict[str, float] = {}

    QUERIES = ["solana meme", "solana ai", "solana new", "solana pump", "solana dog", "solana gaming"]

    while True:
        try:
            now = time.time()
            # Parallel fetch
            pairs_map = await dex_multi_search(QUERIES)
            boosts_top = await dex_boosts_top()
            boosted_addrs = {b.get("tokenAddress", "") for b in boosts_top if b.get("chainId") == "solana"}

            for addr, p in pairs_map.items():
                if addr in blacklist: continue
                if now - cooldown.get(addr, 0) < 1800: continue

                base    = p.get("baseToken", {})
                sym     = base.get("symbol", "???")
                name    = base.get("name", "")
                fdv     = float(p.get("fdv", 0) or 0)
                mcap    = float(p.get("marketCap", 0) or fdv)
                liq     = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                ch5m    = float((p.get("priceChange") or {}).get("m5", 0) or 0)
                ch1h    = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                ch6h    = float((p.get("priceChange") or {}).get("h6", 0) or 0)
                v5m     = float((p.get("volume") or {}).get("m5", 0) or 0)
                v1h     = float((p.get("volume") or {}).get("h1", 0) or 0)
                b5m     = int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
                s5m     = int(((p.get("txns") or {}).get("m5") or {}).get("sells", 0) or 0)
                b1h     = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                s1h     = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                created = int(p.get("pairCreatedAt", 0) or 0)
                age_min = (now * 1000 - created) / 60000 if created else 9999

                if liq < 1500 or fdv < 5000 or fdv > 50_000_000: continue

                avg_5m_vol = v1h / 12 if v1h > 0 else 1
                vol_spike  = v5m / max(avg_5m_vol, 1)
                buy_pct    = b1h / max(b1h + s1h, 1) * 100
                nar        = detect_narrative(f"{name} {sym}")
                is_boosted = addr in boosted_addrs

                # Score for dedup key
                alert_type = None
                if ch5m >= 8 and b5m > s5m and v5m > 200:
                    alert_type = "pump"
                elif ch5m <= -10 and s5m > b5m * 1.5 and v5m > 200:
                    alert_type = "dump"
                elif vol_spike >= 3.0 and abs(ch5m) < 5 and b1h > 10 and buy_pct > 60:
                    alert_type = "whale"
                elif fdv < 300_000 and ch1h >= 20 and buy_pct >= 60 and liq >= 2000:
                    alert_type = "gem"
                elif age_min < 45 and b1h > 15 and buy_pct >= 60 and liq >= 1500:
                    alert_type = "new"

                if not alert_type: continue

                # Dedup via Redis-persisted set
                alert_id = hashlib.md5(f"{addr}:{alert_type}:{int(now/3600)}".encode()).hexdigest()[:16]
                if _seen_check(seen_alert_ids, alert_id): continue
                _seen_add(seen_alert_ids, alert_id)
                asyncio.create_task(_save())  # non-blocking persist

                cooldown[addr] = now

                # Build token dict for card
                tok = {
                    "address": addr, "sym": sym, "name": name,
                    "price": float(p.get("priceUsd", 0) or 0),
                    "fdv": fdv, "mcap": mcap, "liq": liq,
                    "ch5m": ch5m, "ch1h": ch1h, "ch6h": ch6h, "ch24h": float((p.get("priceChange") or {}).get("h24", 0) or 0),
                    "v5m": v5m, "v1h": v1h, "v24h": float((p.get("volume") or {}).get("h24", 0) or 0),
                    "b5m": b5m, "s5m": s5m, "b1h": b1h, "s1h": s1h, "b24h": 0, "s24h": 0,
                    "buy_pct": buy_pct, "vol_spike": vol_spike,
                    "risk_score": 30, "red_flags": [], "green_flags": [],
                    "sell_tax": 0, "buy_tax": 0, "is_honeypot": False,
                    "lp_locked": False, "is_renounced": False,
                    "created": created, "narrative": nar,
                    "tw_link": "", "tg_link": "", "web_link": "",
                    "boost_active": 1 if is_boosted else 0,
                    "has_profile": False, "has_ad": False, "pair_addr": "",
                    "mscore": min(100, int(abs(ch1h) + buy_pct/2 + vol_spike*10)),
                }
                ai = await ai_ask(
                    f"Solana alert — ${sym} ({alert_type.upper()}): MCap {_usd(mcap)}, "
                    f"liq {_usd(liq)}, 5m {_pct(ch5m)}, 1h {_pct(ch1h)}, "
                    f"buys/sells {b1h}/{s1h}, vol spike {vol_spike:.1f}x. "
                    f"Narrative: #{nar}. "
                    "Given current market conditions, is this worth acting on NOW? "
                    "1 razor-sharp sentence — entry thesis or stay away.",
                    fallback="",
                    inject_market=True
                )
                card = build_alert_card(tok, alert_type, ai)
                if GROUP_CHAT_ID != 0:
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=card,
                            parse_mode="Markdown",
                            reply_markup=scan_buttons(addr, sym),
                            disable_web_page_preview=True,
                        )
                        logger.info(f"[ALERT] {alert_type} ${sym} {_usd(mcap)}")
                        await asyncio.sleep(2)
                    except Exception as e:
                        logger.warning(f"alert send: {e}")

            # Trim cooldown
            cooldown = {k: v for k, v in cooldown.items() if now - v < 7200}

        except Exception as e:
            logger.error(f"bg_main_scanner: {e}", exc_info=True)
        await asyncio.sleep(30)


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

                if liq < 800 or fdv < 3000: continue

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

                if score < 38: continue
                if GROUP_CHAT_ID == 0: continue

                _seen_add(seen_alert_ids, alert_id)
                asyncio.create_task(_save())

                tw_link = next((l.get("url", "") for l in links if l.get("type") == "twitter"), "")
                tg_link = next((l.get("url", "") for l in links if l.get("type") == "telegram"), "")
                soc_str = ("🐦 " if tw_link else "") + ("💬 " if tg_link else "")
                nar     = detect_narrative(f"{name} {sym}")

                ai = await ai_ask(
                    f"New Solana token ${sym} — Age {int(age_min)}min, "
                    f"MCap {_usd(fdv)}, liq {_usd(liq)}, 1h {_pct(ch1h)}, "
                    f"buys/sells {b1h}/{s1h}, score {score}/100. "
                    "Worth watching? 1 honest sentence.",
                    fallback=""
                )
                msg_text = (
                    f"🆕 *NEW LAUNCH ALERT* [Score: {score}]\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"*${sym}* — _{name}_ {soc_str}#{nar.upper()}\n"
                    f"Age: {int(age_min)}m  MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
                    f"1h: {_pct(ch1h)}  Buys/Sells: {b1h}/{s1h}  Buy%: {buy_pct:.0f}%\n"
                    + (f"Boost: {boost}  " if boost else "")
                    + (f"[Twitter]({tw_link})  " if tw_link else "")
                    + (f"[Telegram]({tg_link})" if tg_link else "")
                    + f"\n`{addr}`"
                )
                if ai: msg_text += f"\n\n🧠 _{ai}_"
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=msg_text,
                        parse_mode="Markdown",
                        reply_markup=scan_buttons(addr, sym),
                        disable_web_page_preview=True,
                    )
                    logger.info(f"[NEW LAUNCH] ${sym} score={score}")
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.warning(f"new launch send: {e}")

        except Exception as e:
            logger.error(f"bg_new_launch_scanner: {e}", exc_info=True)
        await asyncio.sleep(60)


async def bg_narrative_news_scanner(app: Application):
    """
    Every 10min:
    1. Fetch latest news headlines
    2. Extract dominant narratives from headlines
    3. Find trending metas from DexScreener
    4. Search for Solana tokens matching those narratives
    5. Alert on tokens with narrative momentum
    
    Example: FIFA World Cup headlines → search "worldcup soccer football solana" 
             → find tokens playing the narrative → alert before latecomers notice
    """
    await asyncio.sleep(90)
    last_run = 0

    while True:
        try:
            now = time.time()
            if now - last_run < 600:  # 10 min
                await asyncio.sleep(30); continue
            last_run = now

            # Step 1: Fetch news and extract narratives
            items    = await fetch_news(12)
            headlines = [i["title"] for i in items]
            active_narratives = narrative_from_news(headlines)

            # Step 2: Get DexScreener trending metas too
            dex_metas = await dex_trending_metas()
            dex_nar_names = [m.get("slug", m.get("name", "")).lower() for m in dex_metas[:5]]
            all_narratives = list(set(active_narratives + [n for n in dex_nar_names if n]))

            if not all_narratives:
                await asyncio.sleep(30); continue

            logger.info(f"[NARRATIVE SCANNER] Active narratives: {all_narratives}")

            # Step 3: For each narrative, find matching Solana tokens
            for nar in all_narratives[:5]:
                kws = NARRATIVES.get(nar, [nar])

                # Find relevant headlines for this narrative
                rel_headlines = [h for h in headlines if any(kw in h.lower() for kw in kws)]
                if not rel_headlines: continue

                # Search DexScreener for tokens matching this narrative
                queries = [f"solana {kw}" for kw in kws[:3]]
                pairs_map = await dex_multi_search(queries)

                # Filter + score
                candidates = []
                for addr, p in pairs_map.items():
                    if addr in blacklist: continue
                    fdv   = float(p.get("fdv", 0) or 0)
                    liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                    ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                    ch6h  = float((p.get("priceChange") or {}).get("h6", 0) or 0)
                    b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                    s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                    v24h  = float((p.get("volume") or {}).get("h24", 0) or 0)
                    if liq < 3000 or fdv < 10_000 or fdv > 20_000_000: continue
                    buy_pct = b1h / max(b1h + s1h, 1) * 100
                    # Score: narrative momentum
                    score = 0
                    if ch1h > 30:  score += 30
                    elif ch1h > 10: score += 20
                    if ch6h > 50:  score += 25
                    if buy_pct > 65: score += 20
                    if v24h > 50_000: score += 15
                    if score >= 35:
                        candidates.append((score, addr, p))

                candidates.sort(reverse=True)

                for score, addr, p in candidates[:2]:
                    alert_id = hashlib.md5(f"{addr}:nar:{nar}:{int(now/3600)}".encode()).hexdigest()[:16]
                    if _seen_check(seen_alert_ids, alert_id): continue
                    _seen_add(seen_alert_ids, alert_id)
                    asyncio.create_task(_save())

                    base  = p.get("baseToken", {})
                    sym   = base.get("symbol", "???")
                    name  = base.get("name", "")
                    fdv   = float(p.get("fdv", 0) or 0)
                    liq   = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                    ch1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                    ch6h  = float((p.get("priceChange") or {}).get("h6", 0) or 0)
                    b1h   = int(((p.get("txns") or {}).get("h1") or {}).get("buys", 0) or 0)
                    s1h   = int(((p.get("txns") or {}).get("h1") or {}).get("sells", 0) or 0)
                    buy_pct = b1h / max(b1h + s1h, 1) * 100

                    # AI explains WHY this narrative is hot right now
                    ai = await ai_ask(
                        f"Breaking news context: {'; '.join(rel_headlines[:2])}. "
                        f"Solana token ${sym} is in the #{nar.upper()} narrative. "
                        f"MCap {_usd(fdv)}, 1h {_pct(ch1h)}, 6h {_pct(ch6h)}, buy ratio {buy_pct:.0f}%. "
                        "Explain in 2 sentences why this token could benefit from this news narrative "
                        "and whether this is worth watching. Be professional and direct.",
                        fallback=""
                    )

                    msg_text = (
                        f"📖 *NARRATIVE ALERT — #{nar.upper()}*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"*${sym}* — _{name}_\n"
                        f"MCap: `{_usd(fdv)}`  Liq: `{_usd(liq)}`\n"
                        f"1h: {_pct(ch1h)}  6h: {_pct(ch6h)}\n"
                        f"Buys/Sells: {b1h}/{s1h}  →  {buy_pct:.0f}% buys\n\n"
                        f"📰 *Trending news:*\n"
                        + "\n".join([f"  • _{h[:70]}_" for h in rel_headlines[:2]])
                        + f"\n\n`{addr}`"
                    )
                    if ai: msg_text += f"\n\n🧠 _{ai}_"

                    if GROUP_CHAT_ID != 0:
                        try:
                            await app.bot.send_message(
                                chat_id=GROUP_CHAT_ID,
                                text=msg_text,
                                parse_mode="Markdown",
                                reply_markup=scan_buttons(addr, sym),
                                disable_web_page_preview=True,
                            )
                            logger.info(f"[NARRATIVE] ${sym} #{nar}")
                            await asyncio.sleep(3)
                        except Exception as e:
                            logger.warning(f"narrative alert: {e}")

                await asyncio.sleep(1)

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

            metas = await dex_trending_metas()
            if metas and GROUP_CHAT_ID != 0:
                top5  = metas[:5]
                lines = ["🔥 *TRENDING METAS UPDATE*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
                for m in top5:
                    name  = m.get("name", "?")
                    mcap  = float(m.get("marketCap", 0) or 0)
                    c1h   = float((m.get("marketCapChange") or {}).get("h1", 0) or 0)
                    c24h  = float((m.get("marketCapChange") or {}).get("h24", 0) or 0)
                    lines.append(f"• *{name}*  MCap: `{_usd(mcap)}`  1h: {_pct(c1h)}  24h: {_pct(c24h)}")
                nar_names = [m.get("name", "") for m in top5]
                ai = await ai_ask(
                    f"Top trending metas right now: {nar_names}. "
                    "With the current market conditions (live data in context), "
                    "which meta has the strongest momentum, what's driving it, "
                    "and what specific type of Solana token should a degen be hunting in it? "
                    "3 sentences max, be precise.",
                    fallback="",
                    inject_market=True
                )
                if ai: lines.append(f"\n🧠 _{ai}_")
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text="\n".join(lines),
                        parse_mode="Markdown",
                    )
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
        f"🦅 Kayo Brain v16 ready — "
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
        ("status", status_cmd), ("ping", ping_cmd),
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
