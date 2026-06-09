"""
KAYO BRAIN - COMPLETE WEB3 INTELLIGENCE BOT
VERSION: 13.0 - WATCHLIST EDITION
- Every feature from v12 intact
- NEW: /watch, /unwatch, /watchlist — monitor specific Twitter accounts
- Bot checks watched accounts every 60s, instantly drops CA to group
- Tracks win rates per watched account over time
"""

import asyncio
import logging
import re
import time
import json
import hashlib
import os
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import quote_plus

import aiohttp
import redis
import xml.etree.ElementTree as ET
from google import genai as genai_sdk
from google.genai import types as genai_types
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, MenuButtonCommands, WebAppInfo,
)
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters,
)
from flask import Flask
import threading

# ── Config ────────────────────────────────────────────────────
BOT_TOKEN     = os.environ.get("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID", "0"))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable is not set! Set it before running.")


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)
@flask_app.route('/')
def health(): return "🦅 Kayo Brain v13 alive!", 200
@flask_app.route('/health')
def hc(): return "OK", 200
threading.Thread(
    target=lambda: flask_app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False, use_reloader=False),
    daemon=True
).start()
logger.info(f"🌐 Web server started on port {os.environ.get('PORT', 8080)}")


# ── Redis (for persistent state on Render) ────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "")
_redis = None
if REDIS_URL:
    try:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
        _redis.ping()
        logger.info("✅ Redis connected — state will persist across restarts")
    except Exception as e:
        logger.warning(f"⚠️  Redis connection failed: {e} — falling back to local JSON")
        _redis = None
else:
    logger.info("ℹ️  No REDIS_URL set — using local JSON for state (will reset on redeploy)")


# ── Gemini AI ─────────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
_gemini = None
if GEMINI_API_KEY:
    try:
        _gemini = genai_sdk.Client(api_key=GEMINI_API_KEY)
        logger.info("✅ Gemini AI connected (google-genai SDK)")
    except Exception as e:
        logger.warning(f"⚠️  Gemini init failed: {e}")
else:
    logger.warning("🔴 GEMINI_API_KEY not set — ALL AI features disabled. Add it in Render env vars!")

async def gemini_ask(prompt: str, fallback: str = "") -> str:
    """Ask Gemini a question using the new google-genai SDK."""
    if not _gemini:
        return fallback
    try:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            None,
            lambda: _gemini.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt
            )
        )
        return resp.text.strip()[:1000]
    except Exception as e:
        logger.warning(f"Gemini error: {type(e).__name__}: {e}")
        return fallback

# ── Global state ──────────────────────────────────────────────
settings:         Dict[int, Dict]  = {}
active_calls:     Dict[int, Dict]  = {}
closed_calls:     Dict[int, List]  = {}
user_xp:          Dict[int, int]   = {}
tracked_wallets:  Dict[str, int]   = {}   # wallet_address -> chat_id
my_wallets:       Dict[int, str]   = {}   # uid -> wallet_address
seen_tokens:      set = set()
seen_news:        set = set()
kayo_knowledge:   List[str] = []
strategy_records: List[Dict] = []
strategy_weights: Dict[str, float] = {
    "momentum": 0.3, "volume_spike": 0.25,
    "narrative_strength": 0.2, "whale_activity": 0.15, "social_sentiment": 0.1,
}
reminders: List[Dict] = []

# ── Enhanced tracking state ───────────────────────────────────
pump_seen:         set  = set()          # pumpfun tokens already alerted
dex_baseline:      Dict = {}             # address -> {price, vol, buys} for movement detection
gem_alerts_sent:   set  = set()          # avoid duplicate gem alerts
news_cache:        List = []             # last CryptoPanic headlines
last_news_fetch:   float = 0.0          # timestamp of last news fetch
price_alerts:      Dict = {}            # uid -> [{address, symbol, target, direction}]
portfolio:         Dict = {}            # uid -> [{address, symbol, amount, buy_price}]
blacklist:         set  = set()         # addresses user flagged as rugs
top_callers:       Dict = {}            # uid -> {calls, wins, total_profit}
group_messages:    List = []            # rolling last 50 group messages for summary

# ── Persistence helpers ───────────────────────────────────────
STATE_FILE  = "kayo_state.json"
REDIS_KEY   = "kayo_brain_state"

def _state_dict():
    return {
        "settings":         {str(k): v for k, v in settings.items()},
        "active_calls":     {str(k): v for k, v in active_calls.items()},
        "closed_calls":     {str(k): v for k, v in closed_calls.items()},
        "user_xp":          {str(k): v for k, v in user_xp.items()},
        "tracked_wallets":  tracked_wallets,
        "my_wallets":       {str(k): v for k, v in my_wallets.items()},
        "kayo_knowledge":   kayo_knowledge,
        "strategy_records": strategy_records,
        "strategy_weights": strategy_weights,
        "reminders":        reminders,
        "watchlist":        watchlist,
        "price_alerts":     {str(k): v for k, v in price_alerts.items()},
        "portfolio":        {str(k): v for k, v in portfolio.items()},
        "blacklist":        list(blacklist),
    }

def _apply_state(data: dict):
    global settings, active_calls, closed_calls, user_xp
    global tracked_wallets, my_wallets, kayo_knowledge
    global strategy_records, reminders, watchlist
    settings         = {int(k): v for k, v in data.get("settings", {}).items()}
    active_calls     = {int(k): v for k, v in data.get("active_calls", {}).items()}
    closed_calls     = {int(k): v for k, v in data.get("closed_calls", {}).items()}
    user_xp          = {int(k): v for k, v in data.get("user_xp", {}).items()}
    tracked_wallets  = data.get("tracked_wallets", {})
    my_wallets       = {int(k): v for k, v in data.get("my_wallets", {}).items()}
    kayo_knowledge   = data.get("kayo_knowledge", [])
    strategy_records = data.get("strategy_records", [])
    strategy_weights.update(data.get("strategy_weights", {}))
    reminders        = data.get("reminders", [])
    watchlist        = data.get("watchlist", {})
    price_alerts.update({int(k): v for k, v in data.get("price_alerts", {}).items()})
    portfolio.update({int(k): v for k, v in data.get("portfolio", {}).items()})
    blacklist.update(set(data.get("blacklist", [])))

def save_state():
    """Save state to Redis (primary) or local JSON (fallback)."""
    try:
        data = json.dumps(_state_dict())
        if _redis:
            _redis.set(REDIS_KEY, data)
        else:
            with open(STATE_FILE, "w") as f:
                f.write(data)
    except Exception as e:
        logger.warning(f"save_state error: {e}")

def load_state():
    """Load state from Redis (primary) or local JSON (fallback)."""
    try:
        if _redis:
            raw = _redis.get(REDIS_KEY)
            if raw:
                data = json.loads(raw)
                _apply_state(data)
                logger.info(f"✅ State loaded from Redis: {len(watchlist)} watched, {len(user_xp)} users, {len(kayo_knowledge)} knowledge items")
                return
        # Fallback to local JSON
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
            _apply_state(data)
            logger.info(f"✅ State loaded from JSON: {len(watchlist)} watched, {len(user_xp)} users")
    except Exception as e:
        logger.warning(f"load_state error: {e}")


# ── Watchlist state ───────────────────────────────────────────
# { "username": { "chat_id": int, "added_by": int, "added_at": str,
#                 "last_tweet_id": str, "calls": int, "wins": int } }
watchlist: Dict[str, Dict] = {}
watchlist_seen_tweets: set = set()   # tweet hashes already processed

start_time = time.time()

# ── Formatters ────────────────────────────────────────────────
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
    try: return f"{'+' if sign and v > 0 else ''}{v:.1f}%"
    except: return "N/A"

def safety_emoji(s):
    if s >= 80: return "🟢"
    if s >= 50: return "🟡"
    if s >= 20: return "🟠"
    return "🔴"

def add_xp(uid: int, amount: int = 5):
    user_xp[uid] = user_xp.get(uid, 0) + amount

def kayo_opinion(momentum: float, rug: float, vol_ratio: float) -> str:
    """Basic synchronous opinion (used as fallback when Gemini unavailable)."""
    w_mom = strategy_weights.get("momentum", 0.3)
    w_vol = strategy_weights.get("volume_spike", 0.25)
    score = momentum * w_mom + min(100, vol_ratio * 20) * w_vol + rug * 0.2
    learned = f"\n🧠 Kayo tracking: {kayo_knowledge[-1]}" if kayo_knowledge else ""
    if score >= 70 and rug >= 70:
        return f"🟢 **KAYO SAYS: APE** — Strong momentum, clean safety. High conviction.{learned}"
    elif score >= 55 and rug >= 50:
        return f"🟡 **KAYO SAYS: WATCH** — Decent setup. Wait for 5m confirmation.{learned}"
    elif rug < 40:
        return f"🔴 **KAYO SAYS: AVOID** — Safety red flags. Not worth the risk."
    elif momentum < 20:
        return f"🟠 **KAYO SAYS: DEAD** — No momentum. Find a better coin.{learned}"
    else:
        return f"🟠 **KAYO SAYS: CAUTION** — Mixed signals. Small size only.{learned}"

async def kayo_opinion_ai(token_data: dict) -> str:
    """Gemini-powered deep analysis of a token."""
    if not _gemini:
        return kayo_opinion(
            token_data.get("momentum_score", 0),
            token_data.get("rug_score", 100),
            token_data.get("vol_ratio", 1)
        )
    news_ctx = ""
    if news_cache:
        headlines = [n["title"] for n in news_cache[:5]]
        news_ctx = "Recent crypto news: " + " | ".join(headlines)

    prompt = f"""You are Kayo, a sharp Web3 alpha analyst. Analyze this Solana token and give a short, punchy verdict (max 3 sentences). Be direct — ape, watch, or avoid. Use emojis.

Token: ${token_data.get('symbol','?')} ({token_data.get('name','?')})
Price: {fmt_price(token_data.get('price',0))}
Market Cap: {fmt_usd(token_data.get('fdv',0))}
Liquidity: {fmt_usd(token_data.get('liq',0))}
1h change: {fmt_pct(token_data.get('ch_1h',0))}
5m change: {fmt_pct(token_data.get('ch_5m',0))}
24h change: {fmt_pct(token_data.get('ch_24h',0))}
Momentum score: {token_data.get('momentum_score',0)}/100
Safety/Rug score: {token_data.get('rug_score',0)}/100
Volume spike: {token_data.get('vol_ratio',1):.1f}x
Buys (1h): {token_data.get('buys_1h',0)} | Sells (1h): {token_data.get('sells_1h',0)}
Narrative: {token_data.get('narrative','unknown')}
Liq/MCap ratio: {token_data.get('liq_ratio',0)*100:.1f}%
{news_ctx}

Give your verdict starting with 🟢 APE / 🟡 WATCH / 🟠 CAUTION / 🔴 AVOID:"""

    result = await gemini_ask(prompt, fallback=kayo_opinion(
        token_data.get("momentum_score", 0),
        token_data.get("rug_score", 100),
        token_data.get("vol_ratio", 1)
    ))
    return f"🧠 **KAYO AI:** {result}"

# ── API helpers ───────────────────────────────────────────────
async def dex_token(session, address: str) -> Optional[Dict]:
    try:
        async with session.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{address}",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200: return None
            data  = await r.json()
            pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
            if not pairs: return None
            pairs.sort(key=lambda x: float(x.get("liquidity",{}).get("usd",0) or 0), reverse=True)
            return pairs[0]
    except: return None

async def dex_search(session, query: str = "solana") -> List[Dict]:
    try:
        async with session.get(
            f"https://api.dexscreener.com/latest/dex/search?q={quote_plus(query)}",
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200: return []
            data = await r.json()
            return [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
    except: return []

async def dex_boosted(session) -> List[Dict]:
    try:
        async with session.get(
            "https://api.dexscreener.com/token-boosts/latest/v1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status == 200:
                data = await r.json()
                return data if isinstance(data, list) else []
    except: pass
    return []

async def goplus_sec(session, address: str) -> Dict:
    try:
        async with session.get(
            f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={address}",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200: return {}
            data   = await r.json()
            result = data.get("result", {})
            return result.get(address.lower(), result.get(address, {}))
    except: return {}


# ── Crypto News RSS (CoinDesk + Decrypt + Cointelegraph) ─────
async def fetch_cryptopanic(session, filter_type="rising") -> List[Dict]:
    """Fetch latest crypto news from multiple free RSS feeds.
    CryptoPanic now blocks scrapers — replaced with CoinDesk, Decrypt, Cointelegraph.
    """
    rss_sources = [
        ("CoinDesk",       "https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ("Cointelegraph",  "https://cointelegraph.com/rss"),
        ("Decrypt",        "https://decrypt.co/feed"),
    ]
    results = []
    for source_name, url in rss_sources:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "Mozilla/5.0 (compatible; KayoBrain/1.0)"}
            ) as r:
                if r.status != 200:
                    logger.debug(f"News RSS {source_name}: HTTP {r.status}")
                    continue
                text = await r.text()
                # Fix unescaped & chars in XML
                fixed = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', text)
                root  = ET.fromstring(fixed)
                items = root.findall('.//item')
                for item in items[:8]:
                    title = item.findtext('title', '').strip()
                    link  = item.findtext('link',  '').strip()
                    pub   = item.findtext('pubDate', '').strip()
                    desc  = item.findtext('description', '').strip()
                    # Strip CDATA wrappers
                    title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title).strip()
                    desc  = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc).strip()
                    desc  = re.sub(r'<[^>]+>', '', desc).strip()[:200]
                    if title and len(title) > 10:
                        results.append({
                            "title":  title,
                            "link":   link,
                            "pub":    pub,
                            "desc":   desc,
                            "source": source_name
                        })
        except Exception as e:
            logger.debug(f"News RSS {source_name} error: {e}")
            continue
    # Deduplicate by title
    seen = set()
    unique = []
    for a in results:
        key = a['title'].lower()[:50]
        if key not in seen:
            seen.add(key)
            unique.append(a)
    return unique[:20]

# ── PumpFun API ───────────────────────────────────────────────
async def pumpfun_new_tokens(session, limit=20) -> List[Dict]:
    """Fetch newest Solana tokens. PumpFun API is Cloudflare-blocked, use DexScreener new pairs."""
    try:
        # DexScreener /new-pairs endpoint for Solana
        async with session.get(
            "https://api.dexscreener.com/latest/dex/search?q=solana+new&chainIds=solana",
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200: return []
            data = await r.json()
            pairs = data.get("pairs", [])
            result = []
            for p in pairs[:limit]:
                base = p.get("baseToken", {})
                result.append({
                    "mint":         base.get("address",""),
                    "symbol":       base.get("symbol","???"),
                    "name":         base.get("name","Unknown"),
                    "market_cap":   float(p.get("fdv",0) or 0),
                    "reply_count":  int(p.get("txns",{}).get("h1",{}).get("buys",0) or 0),
                    "king_of_the_hill_timestamp": None,
                    "liq":          float(p.get("liquidity",{}).get("usd",0) or 0),
                    "ch_1h":        float(p.get("priceChange",{}).get("h1",0) or 0),
                })
            return result
    except Exception as e:
        logger.error(f"pumpfun_new_tokens: {e}")
        return []


async def pumpfun_graduating(session) -> List[Dict]:
    """Find tokens close to graduating to Raydium (near $69k bonding curve).
    PumpFun API is Cloudflare-blocked — use DexScreener tokens with $50k-$80k mcap on Solana."""
    try:
        async with session.get(
            "https://api.dexscreener.com/latest/dex/search?q=solana+pump+graduating&chainIds=solana",
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200: return []
            data = await r.json()
            result = []
            for p in data.get("pairs", []):
                fdv = float(p.get("fdv",0) or 0)
                liq = float(p.get("liquidity",{}).get("usd",0) or 0)
                # Graduating tokens: $40k-$80k mcap range (approaching $69k bonding curve)
                if 40_000 <= fdv <= 80_000 and liq > 5000:
                    base = p.get("baseToken",{})
                    result.append({
                        "mint":       base.get("address",""),
                        "symbol":     base.get("symbol","???"),
                        "market_cap": fdv,
                        "liq":        liq,
                    })
            return result[:10]
    except Exception as e:
        logger.error(f"pumpfun_graduating: {e}")
        return []


async def dex_trending_solana(session) -> List[Dict]:
    """Get broad Solana pair data via multiple DexScreener queries (returns 100+ unique pairs)."""
    queries = ["solana meme", "solana ai", "solana pump", "solana new", "solana defi", "solana gaming"]
    seen: dict = {}
    for q in queries:
        try:
            async with session.get(
                f"https://api.dexscreener.com/latest/dex/search?q={q.replace(' ','+')}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200: continue
                data = await r.json()
                for p in data.get("pairs", []):
                    addr = p.get("baseToken", {}).get("address", "")
                    if addr and p.get("chainId") == "solana":
                        seen[addr] = p
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug(f"dex_trending_solana query '{q}': {e}")
    return list(seen.values())


async def dex_new_pairs(session) -> List[Dict]:
    """Get newest Solana pairs from DexScreener."""
    try:
        async with session.get(
            "https://api.dexscreener.com/token-profiles/latest/v1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
            if isinstance(data, list):
                return [t for t in data if t.get("chainId") == "solana"][:20]
            return []
    except:
        return []

async def coingecko_coin(session, coin_id: str) -> Optional[Dict]:
    try:
        async with session.get(
            f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200: return None
            return await r.json()
    except: return None

async def coingecko_top(session, limit=10) -> List[Dict]:
    try:
        async with session.get(
            f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={limit}&page=1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200: return []
            return await r.json()
    except: return []

async def coingecko_global(session) -> Dict:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200: return {}
            return (await r.json()).get("data", {})
    except: return {}

async def coingecko_trending(session) -> List:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200: return []
            return (await r.json()).get("coins", [])
    except: return []

# ── Twitter / X scraping via twscrape ─────────────────────────
TWITTER_AUTH_TOKEN = os.environ.get("TWITTER_AUTH_TOKEN", "")
_tw_api = None
_tw_ready = False

async def _ensure_twscrape():
    """Initialize twscrape account pool from TWITTER_AUTH_TOKEN env var."""
    global _tw_api, _tw_ready
    if _tw_ready:
        return True
    if not TWITTER_AUTH_TOKEN:
        logger.warning("⚠️ TWITTER_AUTH_TOKEN not set — Twitter features disabled")
        return False
    try:
        from twscrape import API as TwAPI
        _tw_api = TwAPI()
        await _tw_api.pool.add_account(
            username="kayo_bot_scraper",
            password="placeholder_pass",
            email="kayo@placeholder.bot",
            email_password="placeholder_pass",
            cookies={"auth_token": TWITTER_AUTH_TOKEN}
        )
        await _tw_api.pool.login_all()
        _tw_ready = True
        logger.info("✅ twscrape Twitter scraper ready")
    except Exception as e:
        logger.warning(f"⚠️ twscrape init failed: {e}")
        _tw_ready = False
    return _tw_ready

async def tw_search(query: str, limit: int = 10) -> List[Dict]:
    """Search Twitter/X for tweets by keyword. Returns list of tweet dicts."""
    if not TWITTER_AUTH_TOKEN:
        return []
    if not _tw_ready:
        await _ensure_twscrape()
    if not _tw_ready or not _tw_api:
        return []
    results = []
    try:
        async for tweet in _tw_api.search(f"{query} lang:en", limit=limit):
            results.append({
                "text":     tweet.rawContent,
                "user":     tweet.user.username,
                "likes":    tweet.likeCount,
                "retweets": tweet.retweetCount,
                "date":     str(tweet.date)[:10],
                "url":      f"https://x.com/{tweet.user.username}/status/{tweet.id}"
            })
    except Exception as e:
        logger.warning(f"tw_search('{query}') error: {e}")
    return results

async def tw_user_tweets(username: str, limit: int = 15) -> List[Dict]:
    """Get recent tweets from a Twitter/X user timeline."""
    if not TWITTER_AUTH_TOKEN:
        return []
    if not _tw_ready:
        await _ensure_twscrape()
    if not _tw_ready or not _tw_api:
        return []
    results = []
    try:
        uname = username.lstrip("@")
        user  = await _tw_api.user_by_login(uname)
        if not user:
            return []
        async for tweet in _tw_api.user_tweets(user.id, limit=limit):
            results.append({
                "text":     tweet.rawContent,
                "user":     tweet.user.username,
                "likes":    tweet.likeCount,
                "retweets": tweet.retweetCount,
                "date":     str(tweet.date)[:10],
                "url":      f"https://x.com/{tweet.user.username}/status/{tweet.id}"
            })
    except Exception as e:
        logger.warning(f"tw_user_tweets('{username}') error: {e}")
    return results

def _tw_not_configured() -> str:
    return (
        "⚠️ **Twitter/X not configured**\n\n"
        "To enable Twitter commands:\n"
        "1. Log into x.com in your browser\n"
        "2. Press F12 → Application → Cookies → x.com\n"
        "3. Copy the `auth\_token` value\n"
        "4. Add it to Render env vars as `TWITTER_AUTH_TOKEN`\n\n"
        "Then restart the bot — all Twitter commands will work instantly."
    )

async def scrape_nitter(session, query: str, limit=10) -> List[Dict]:
    """Search tweets via twscrape (replaces dead Nitter). Falls back to empty list."""
    return await tw_search(query, limit=limit)

async def scrape_nitter_user(session, username: str, limit=15) -> List[Dict]:
    """Get user timeline tweets via twscrape (replaces dead Nitter)."""
    return await tw_user_tweets(username, limit=limit)

# ── Smart scan ────────────────────────────────────────────────
async def smart_scan(address: str) -> Dict:
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(
            dex_token(session, address),
            goplus_sec(session, address)
        )
    if not pair:
        return {"error": "Token not found on Solana"}
    base     = pair.get("baseToken", {})
    symbol   = base.get("symbol", "???")
    name     = base.get("name", "Unknown")
    price    = float(pair.get("priceUsd", 0) or 0)
    fdv      = float(pair.get("fdv", 0) or 0)
    liq      = float(pair.get("liquidity", {}).get("usd", 0) or 0)
    ch_1h    = float(pair.get("priceChange", {}).get("h1", 0) or 0)
    ch_5m    = float(pair.get("priceChange", {}).get("m5", 0) or 0)
    ch_24h   = float(pair.get("priceChange", {}).get("h24", 0) or 0)
    buys_1h  = int(pair.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells_1h = int(pair.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    vol_1h   = float(pair.get("volume", {}).get("h1", 0) or 0)
    vol_5m   = float(pair.get("volume", {}).get("m5", 0) or 0)
    vol_24h  = float(pair.get("volume", {}).get("h24", 0) or 0)
    narrative, narrative_score = "Meme", 5
    text = f"{name} {symbol}".lower()
    if any(w in text for w in ['ai','agent','gpt','intelligence']): narrative, narrative_score = "AI", 9
    elif any(w in text for w in ['game','play','gaming','nft','quest']): narrative, narrative_score = "Gaming", 8
    elif any(w in text for w in ['defi','swap','yield','lend','farm']): narrative, narrative_score = "DeFi", 8
    elif any(w in text for w in ['rwa','real','asset','estate']): narrative, narrative_score = "RWA", 9
    vol_ratio      = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 1
    momentum_score = min(100, max(0,
        (min(50, ch_1h * 2) if ch_1h > 0 else 0) +
        (min(30, vol_ratio * 10) if vol_ratio > 1 else 0) +
        (min(20, buys_1h / 2) if buys_1h > 20 else 0)
    ))
    rug_score = 100
    if sec.get("is_honeypot") == "1":      rug_score -= 60
    if sec.get("cannot_sell_all") == "1":  rug_score -= 40
    if float(sec.get("sell_tax", 0) or 0) > 10: rug_score -= 20
    if sec.get("lp_locked") == "1":        rug_score += 10
    rug_score  = max(0, min(100, rug_score))
    liq_ratio  = liq / fdv if fdv > 0 else 0
    opinion    = kayo_opinion(momentum_score, rug_score, vol_ratio)
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
    pressure = ("🔥 BUY PRESSURE" if a['buys_1h'] > a['sells_1h'] * 1.5
                else "🔻 SELL PRESSURE" if a['sells_1h'] > a['buys_1h'] * 1.5
                else "⚖️ BALANCED")
    return (
        f"🦅 **KAYO SCAN — ${a['symbol']}** ({a['name']})\n"
        f"{'═'*42}\n\n"
        f"💰 **Price:** {fmt_price(a['price'])}\n"
        f"📊 **MCap:** {fmt_usd(a['fdv'])}  |  **Liq:** {fmt_usd(a['liq'])}\n"
        f"📈 **5m:** {fmt_pct(a['ch_5m'])}  |  **1h:** {fmt_pct(a['ch_1h'])}  |  **24h:** {fmt_pct(a['ch_24h'])}\n\n"
        f"⚡ **Momentum:** {a['momentum_score']}/100  |  Vol spike: {a['vol_ratio']:.1f}x\n"
        f"🅱 Buys: {a['buys_1h']}  🆂 Sells: {a['sells_1h']}  →  {pressure}\n\n"
        f"🔮 **Narrative:** {a['narrative']} ({a['narrative_score']}/10)\n"
        f"🛡️ **Safety:** {safety_emoji(a['rug_score'])} {a['rug_score']}/100\n"
        f"💧 **Liq/MCap:** {a['liq_ratio']*100:.1f}%\n\n"
        f"🧠 {a['opinion']}\n\n"
        f"`{a['address']}`"
    )

def get_chart_buttons(address: str, symbol: str) -> InlineKeyboardMarkup:
    dex_url = f"https://dexscreener.com/solana/{address}"
    birdeye = f"https://birdeye.so/token/{address}?chain=solana"
    photon  = f"https://photon-sol.tinyastro.io/en/lp/{address}"
    pumpfun = f"https://pump.fun/{address}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 DEX Chart",  web_app=WebAppInfo(url=dex_url)),
            InlineKeyboardButton("🦅 Birdeye",    web_app=WebAppInfo(url=birdeye)),
        ],
        [
            InlineKeyboardButton("⚡ Photon",     url=photon),
            InlineKeyboardButton("🎰 Pump.fun",   url=pumpfun),
        ],
        [
            InlineKeyboardButton("🔍 Full Scan",  callback_data=f"scan:{address}"),
            InlineKeyboardButton("🛡️ Rug Check",  callback_data=f"rug:{address}"),
        ],
    ])

# ════════════════════════════════════════════════════════════
#  COMMANDS
# ════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_xp(update.effective_user.id, 2)
    await update.message.reply_text(
        "🦅 **KAYO BRAIN v13 — WEB3 INTELLIGENCE**\n\n"
        "**📊 Core Analysis:**\n"
        "• `/scan <ca>` — Full scan + Kayo opinion\n"
        "• `/smartscan` — Best coins right now\n"
        "• `/runners [min%]` — Today's runners\n"
        "• `/momentum` — Volume spike coins\n"
        "• `/verify <ca>` — Rug check\n\n"
        "**📈 Charts (inside Telegram):**\n"
        "• `/chart <ca>` — DEX chart inline\n"
        "• `/c <ca>` — Quick chart\n\n"
        "**📰 Twitter Intel:**\n"
        "• `/watch @account` — 🆕 Watch account for CA drops\n"
        "• `/unwatch @account` — Stop watching\n"
        "• `/watchlist` — See who you're watching\n"
        "• `/news` — Latest CA drops from Twitter\n"
        "• `/trending` — Hot narratives\n"
        "• `/tt` — Trending tweets\n"
        "• `/moni @account` — Scan any account\n"
        "• `/insiders` — Top alpha accounts\n"
        "• `/copy @account` — Copy their last CA\n"
        "• `/twittersearch <coin>` — Sentiment\n\n"
        "**🔮 Narrative & Learning:**\n"
        "• `/narrative <topic>` — ai, meme, defi, gaming, rwa\n"
        "• `/learn` — Force Kayo to learn now\n"
        "• `/mystats` — Your stats + Kayo's brain\n"
        "• `/strategies` — Strategy win rates\n"
        "• `/record <strategy> <won/lost> <profit%>` — Teach Kayo\n\n"
        "**💰 Trading:**\n"
        "• `/call <ca>` — Register entry\n"
        "• `/mycalls` — Live P&L\n"
        "• `/stop <ca>` — Close call\n"
        "• `/leaderboard` — Top traders\n\n"
        "**👛 Wallet:**\n"
        "• `/w <address>` — Wallet overview\n"
        "• `/trackwallet <address>` — Get activity alerts\n"
        "• `/mywallet <address>` — Set your wallet\n"
        "• `/walletpnl` — Your closed trade P&L\n"
        "• `/untrackwallet <address>` — Stop tracking\n\n"
        "**🌍 Market:**\n"
        "• `/macro` — Global overview\n"
        "• `/a <coin>` — CoinGecko price\n"
        "• `/index` — Top 10 by MCap\n"
        "• `/markets` — Market summary\n"
        "• `/dt` — Trending DEX\n\n"
        "**⚡ Quick DEX:**\n"
        "• `/x <ca>` — Quick query\n"
        "• `/z <ca>` — Ultra quick price\n"
        "• `/p <symbol>` — Simple price\n"
        "• `/s <symbol>` — Search token\n\n"
        "**👥 Group:**\n"
        "• `/gp` — Group points\n"
        "• `/ping` — Ping chat\n"
        "• `/dubs` — Chat summary\n"
        "• `/rank` — Your XP\n"
        "• `/remindme <time> <msg>` — Reminder\n"
        "• `/tz` — World timezones\n"
        "• `/status` — Bot status\n"
        "• `/buttons` — Toggle chart buttons\n"
        "• `/autoresponder` — Toggle CA auto-scan\n\n"
        "**🤖 AI Features (Gemini):**\n"
        "• `/ask <question>` — Ask Kayo AI anything\n"
        "• `/sentiment` — AI market analysis\n"
        "• `/gems` — AI gem finder\n"
        "• `/gsum` — AI group chat summary\n"
        "• `/cryptonews` — AI-summarized crypto news\n\n"
        "**🎰 PumpFun:**\n"
        "• `/pump` — Newest launches\n"
        "• `/graduating` — About to hit DEX\n\n"
        "**🔔 Alerts & Portfolio:**\n"
        "• `/alert <ca> <above|below> <price>` — Price alert\n"
        "• `/myalerts` — Your alerts\n"
        "• `/delalert <n>` — Remove alert\n"
        "• `/addport <ca> <amount> <price>` — Add to portfolio\n"
        "• `/portfolio` — Live P&L\n"
        "• `/blacklist <ca>` — Flag a rug\n\n"
        "Drop any CA in chat for instant scan 🦅",
        parse_mode="Markdown"
    )

# ── Core Analysis ─────────────────────────────────────────────
async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/scan <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    wait    = await update.message.reply_text("🔍 Scanning" + (" + asking Kayo AI..." if _gemini else "..."))
    a       = await smart_scan(address)
    if a.get("error"):
        await wait.edit_text(f"❌ {a['error']}"); return
    add_xp(update.effective_user.id, 5)
    # Use Gemini AI opinion if available
    if _gemini:
        a["opinion"] = await kayo_opinion_ai(a)
    card = build_scan_card(a)
    await wait.edit_text(
        card,
        reply_markup=get_chart_buttons(address, a['symbol']),
        parse_mode="Markdown", disable_web_page_preview=True
    )

async def smartscan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🎯 Smart scanning market...")
    queries = ["solana meme", "solana ai", "solana pump", "solana new", "solana defi", "solana gaming"]
    seen: dict = {}
    async with aiohttp.ClientSession() as session:
        for q in queries:
            try:
                async with session.get(
                    f"https://api.dexscreener.com/latest/dex/search?q={q.replace(' ','+')}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        for p in data.get("pairs",[]):
                            addr = p.get("baseToken",{}).get("address","")
                            if addr and p.get("chainId") == "solana":
                                seen[addr] = p
                await asyncio.sleep(0.3)
            except: pass
    pairs = list(seen.values())
    candidates = []
    for p in pairs:
        base  = p.get("baseToken", {})
        fdv   = float(p.get("fdv", 0) or 0)
        liq   = float(p.get("liquidity",{}).get("usd",0) or 0)
        ch_1h = float(p.get("priceChange",{}).get("h1",0) or 0)
        buys  = int(p.get("txns",{}).get("h1",{}).get("buys",0) or 0)
        if fdv < 1000 or liq < 1000 or ch_1h < -50: continue
        candidates.append({
            "address": base.get("address",""), "symbol": base.get("symbol","???"),
            "fdv": fdv, "liq": liq, "ch_1h": ch_1h,
            "score": ch_1h * 2 + buys / 5 + liq / 10000
        })
    candidates.sort(key=lambda x: x["score"], reverse=True)
    if not candidates:
        await wait.edit_text("❌ No coins found right now. Try again shortly."); return
    lines = ["🎯 **SMART SCAN RESULTS**\n" + "═"*32 + "\n"]
    for i, c in enumerate(candidates[:10], 1):
        e = "🚀" if c["ch_1h"] > 20 else "📈" if c["ch_1h"] > 5 else "📊"
        lines.append(f"{e} **{i}. ${c['symbol']}**\n   MCap: {fmt_usd(c['fdv'])} | Liq: {fmt_usd(c['liq'])} | 1h: {fmt_pct(c['ch_1h'])}\n   `/scan {c['address']}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def runners_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Find top runners on Solana right now."""
    min_ch  = float(context.args[0]) if context.args else 5.0
    wait = await update.message.reply_text(f"🏃 Scanning Solana for runners (+{min_ch}%+ in 1h)...")
    queries = ["solana meme", "solana ai", "solana pump", "solana new", "solana defi", "solana gaming"]
    seen: dict = {}
    async with aiohttp.ClientSession() as session:
        for q in queries:
            try:
                async with session.get(
                    f"https://api.dexscreener.com/latest/dex/search?q={q.replace(' ','+')}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200: continue
                    data = await r.json()
                    for p in data.get("pairs", []):
                        addr = p.get("baseToken",{}).get("address","")
                        if addr and p.get("chainId") == "solana":
                            seen[addr] = p
                await asyncio.sleep(0.3)
            except: pass
    runners = []
    for p in seen.values():
        base  = p.get("baseToken", {})
        ch_1h = float(p.get("priceChange", {}).get("h1", 0) or 0)
        ch_5m = float(p.get("priceChange", {}).get("m5", 0) or 0)
        liq   = float(p.get("liquidity", {}).get("usd", 0) or 0)
        vol_1h= float(p.get("volume", {}).get("h1", 0) or 0)
        fdv   = float(p.get("fdv", 0) or 0)
        buys  = int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        sells = int(p.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
        sym   = base.get("symbol", "???")
        addr  = base.get("address", "")
        if ch_1h < min_ch or liq < 5000 or vol_1h < 5000: continue
        if fdv > 50_000_000: continue
        runners.append({"sym": sym, "addr": addr, "ch_1h": ch_1h, "ch_5m": ch_5m,
                        "liq": liq, "fdv": fdv, "buys": buys, "sells": sells})
    runners.sort(key=lambda x: -x["ch_1h"])
    if not runners:
        await wait.edit_text(f"❌ No runners found with >{min_ch}% gain in 1h. Market might be quiet — try `/runners 2`"); return
    add_xp(update.effective_user.id, 3)
    lines_out = [f"🏃 **SOLANA RUNNERS** (+{min_ch}%+ in 1h)\n{'═'*35}\n"]
    for i, r in enumerate(runners[:12], 1):
        trend = "🔥" if r["ch_1h"] > 20 else "📈"
        lines_out.append(
            f"{trend} **${r['sym']}** — +{r['ch_1h']:.1f}% 1h | {fmt_pct(r['ch_5m'])} 5m\n"
            f"   MCap: {fmt_usd(r['fdv'])} | Liq: {fmt_usd(r['liq'])}\n"
            f"   💚{r['buys']} / 🔴{r['sells']} | `{r['addr']}`\n"
        )
    msg = "\n".join(lines_out)
    if _gemini and runners:
        top3 = ", ".join([f"${r['sym']} +{r['ch_1h']:.0f}%" for r in runners[:3]])
        ai_take = await gemini_ask(
            f"Top Solana runners right now: {top3}. One sentence: what's driving this and should traders act?",
            fallback=""
        )
        if ai_take:
            msg += f"\n🧠 **Kayo AI:** {ai_take}"
    await wait.edit_text(msg[:4000], parse_mode="Markdown", disable_web_page_preview=True)


async def momentum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("⚡ Scanning momentum...")
    queries = ["solana meme", "solana ai", "solana pump", "solana new", "solana defi", "solana gaming"]
    seen: dict = {}
    async with aiohttp.ClientSession() as session:
        for q in queries:
            try:
                async with session.get(
                    f"https://api.dexscreener.com/latest/dex/search?q={q.replace(' ','+')}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        for p in data.get("pairs",[]):
                            addr = p.get("baseToken",{}).get("address","")
                            if addr and p.get("chainId") == "solana":
                                seen[addr] = p
                await asyncio.sleep(0.3)
            except: pass
    pairs = list(seen.values())
    spikes = []
    for p in pairs:
        base   = p.get("baseToken", {})
        ch_5m  = float(p.get("priceChange",{}).get("m5",0) or 0)
        ch_1h  = float(p.get("priceChange",{}).get("h1",0) or 0)
        vol_5m = float(p.get("volume",{}).get("m5",0) or 0)
        vol_1h = float(p.get("volume",{}).get("h1",0) or 0)
        liq    = float(p.get("liquidity",{}).get("usd",0) or 0)
        fdv    = float(p.get("fdv",0) or 0)
        if liq < 500: continue
        vr = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 0
        if (ch_5m > 3 and vr > 1.5) or ch_1h > 10:
            spikes.append({"address": base.get("address",""), "symbol": base.get("symbol","???"),
                           "ch_5m": ch_5m, "ch_1h": ch_1h, "fdv": fdv, "vr": vr})
    spikes.sort(key=lambda x: x["ch_5m"], reverse=True)
    if not spikes:
        await wait.edit_text("No momentum spikes right now. Markets quiet."); return
    lines = ["⚡ **MOMENTUM SPIKES**\n" + "═"*30 + "\n"]
    for s in spikes[:10]:
        lines.append(f"🔥 **${s['symbol']}**\n   5m: {fmt_pct(s['ch_5m'])} | 1h: {fmt_pct(s['ch_1h'])} | Vol: {s['vr']:.1f}x\n   `/scan {s['address']}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def verify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/verify <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    wait    = await update.message.reply_text("🔍 Running rug check...")
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
    if not pair:
        await wait.edit_text("❌ Token not found"); return
    base = pair.get("baseToken",{})
    rug, red, green = 0, [], []
    if sec.get("is_honeypot") == "1":          rug += 60; red.append("🚨 HONEYPOT — Cannot sell!")
    st = float(sec.get("sell_tax",0) or 0)
    if st > 20:    rug += 40; red.append(f"💸 Extreme sell tax: {st}%")
    elif st > 10:  rug += 20; red.append(f"⚠️ High sell tax: {st}%")
    if sec.get("lp_locked") == "1": green.append("🔒 Liquidity locked")
    else:           rug += 35; red.append("⚠️ LP NOT locked")
    if sec.get("owner_change_balance") == "1": rug += 30; red.append("👑 Owner can change balances")
    if sec.get("is_blacklisted") == "1":       rug += 40; red.append("🚫 Contract blacklisted")
    liq = float(pair.get("liquidity",{}).get("usd",0) or 0)
    fdv = float(pair.get("fdv",0) or 0)
    if fdv > 0 and liq > 0 and liq/fdv < 0.02:
        rug += 25; red.append(f"💧 Shallow liq ({liq/fdv*100:.1f}%)")
    rug     = min(100, rug)
    verdict = ("🔴 CONFIRMED RUG" if rug >= 70 else "🟠 HIGH RISK" if rug >= 50
               else "🟡 SUSPICIOUS" if rug >= 30 else "🟢 CLEAN")
    await wait.edit_text(
        f"🔍 **RUG CHECK — ${base.get('symbol','???')}**\n{'═'*35}\n\n"
        f"**Verdict:** {verdict}\n**Score:** {rug}/100\n\n"
        f"🚩 **Red Flags:**\n" + ("\n".join([f"  • {f}" for f in red]) or "  None") + "\n\n"
        f"✅ **Green Flags:**\n" + ("\n".join([f"  • {f}" for f in green]) or "  None"),
        parse_mode="Markdown"
    )

# ── Charts ────────────────────────────────────────────────────
async def chart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/chart <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    await update.message.reply_text(
        f"📊 **DEX Chart** — tap to open inside Telegram:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Open DEX Chart", web_app=WebAppInfo(url=f"https://dexscreener.com/solana/{address}"))],
            [InlineKeyboardButton("🦅 Birdeye Chart",  web_app=WebAppInfo(url=f"https://birdeye.so/token/{address}?chain=solana"))],
        ])
    )

async def dex_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await chart_cmd(update, context)

async def c_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await chart_cmd(update, context)

# ── Twitter / News ────────────────────────────────────────────
async def news_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("📰 Scanning Twitter alpha...")
    async with aiohttp.ClientSession() as session:
        tweets = []
        for q in ["solana ca drop alpha", "new gem solana", "alpha call solana pump"]:
            batch = await scrape_nitter(session, q, limit=6)
            tweets.extend(batch)
            await asyncio.sleep(0.5)
    lines = ["📰 **TWITTER ALPHA**\n" + "═"*30 + "\n"]
    seen  = set()
    cas_found = []
    for tw in tweets:
        text = tw.get("text","")
        tid  = hashlib.md5(text.encode()).hexdigest()
        if tid in seen: continue
        seen.add(tid)
        cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
        user    = tw.get("user","unknown")
        snippet = text[:120].replace('\n',' ')
        if cas:
            cas_found.extend(cas)
            lines.append(f"🚨 **@{user}**\n{snippet}\n📌 CA: `{cas[0]}`\n")
        elif any(kw in text.lower() for kw in ['launch','gem','alpha','solana','pump']):
            lines.append(f"📢 **@{user}**\n{snippet}\n")
    if len(lines) == 1:
        await wait.edit_text("No fresh alpha found right now. Try again shortly."); return
    result = "\n".join(lines[:8])
    if cas_found:
        result += "\n\n💡 `/scan <ca>` for full analysis"
    await wait.edit_text(result, parse_mode="Markdown")

async def trending_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔥 Fetching trends...")
    async with aiohttp.ClientSession() as session:
        cg_trending, pairs = await asyncio.gather(coingecko_trending(session), dex_search(session, "solana"))
    lines = ["🔥 **TRENDING**\n" + "═"*35 + "\n"]
    if cg_trending:
        lines.append("**📈 CoinGecko Trending:**")
        for c in cg_trending[:5]:
            item = c.get("item",{})
            lines.append(f"  • **${item.get('symbol','?').upper()}** — {item.get('name','')} (Rank #{item.get('market_cap_rank','?')})")
        lines.append("")
    nc = Counter()
    for p in pairs[:100]:
        base = p.get("baseToken",{})
        t    = f"{base.get('name','')} {base.get('symbol','')}".lower()
        if any(w in t for w in ['ai','agent','gpt']): nc['🤖 AI'] += 1
        elif any(w in t for w in ['game','play','nft']): nc['🎮 Gaming'] += 1
        elif any(w in t for w in ['meme','doge','pepe','cat','dog']): nc['🐸 Meme'] += 1
        elif any(w in t for w in ['defi','swap','yield']): nc['💰 DeFi'] += 1
        else: nc['🎲 Other'] += 1
    lines.append("**🔮 Active Narratives:**")
    for narr, count in nc.most_common(5):
        bar = "█" * min(10, count // 2)
        lines.append(f"  {narr}: {bar} ({count})")
    if kayo_knowledge:
        lines.append(f"\n**🧠 Kayo Learned:**")
        for k in kayo_knowledge[-3:]:
            lines.append(f"  • {k}")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def tt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🐦 Scanning trending tweets...")
    async with aiohttp.ClientSession() as session:
        tweets = await scrape_nitter(session, "solana crypto trending", limit=8)
    if not tweets:
        await wait.edit_text("Nitter unavailable right now. Try `/news` instead."); return
    lines = ["🐦 **TRENDING TWEETS**\n" + "═"*30 + "\n"]
    for tw in tweets[:6]:
        lines.append(f"📢 **@{tw.get('user','unknown')}**\n{tw.get('text','')[:120]}\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def twittersearch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/twittersearch <coin>`\nExample: `/twittersearch bonk`", parse_mode="Markdown"); return
    query = " ".join(context.args)
    wait  = await update.message.reply_text(f"🔍 Searching Twitter for {query}...")
    async with aiohttp.ClientSession() as session:
        tweets = await scrape_nitter(session, f"{query} solana", limit=10)
    bullish = bearish = neutral = 0
    bull_kw = ['bullish','pump','moon','ape','buy','gem','up','🚀','🟢']
    bear_kw = ['bearish','dump','sell','avoid','down','rug','💀','🔴']
    for tw in tweets:
        t = tw.get("text","").lower()
        if any(k in t for k in bull_kw): bullish += 1
        elif any(k in t for k in bear_kw): bearish += 1
        else: neutral += 1
    total = max(bullish + bearish + neutral, 1)
    bull_pct = bullish / total * 100
    sent = "🟢 BULLISH" if bull_pct >= 60 else "🔴 BEARISH" if bull_pct <= 30 else "🟡 NEUTRAL"
    lines = [f"🔍 **TWITTER SENTIMENT — {query.upper()}**\n" + "═"*35 + "\n",
             f"**Sentiment:** {sent}",
             f"🟢 Bullish: {bullish} ({bull_pct:.0f}%)",
             f"🔴 Bearish: {bearish}",
             f"⚪ Neutral: {neutral}\n"]
    for tw in tweets[:3]:
        lines.append(f"📢 @{tw.get('user','?')}: {tw.get('text','')[:100]}\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def moni_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/moni @username`", parse_mode="Markdown"); return
    username = context.args[0].lstrip('@')
    wait     = await update.message.reply_text(f"👤 Scanning @{username}...")
    async with aiohttp.ClientSession() as session:
        tweets = await scrape_nitter_user(session, username, limit=15)
    if not tweets:
        await wait.edit_text(f"❌ Couldn't fetch @{username}'s tweets. Nitter may be down or account is private."); return
    cas = []
    for tw in tweets:
        found = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', tw.get("text",""))
        cas.extend(found)
    lines = [f"👤 **@{username} PROFILE SCAN**\n" + "═"*35 + "\n",
             f"📊 Recent tweets: {len(tweets)}",
             f"📌 CAs found: {len(cas)}\n"]
    if cas:
        lines.append("**Recent CA drops:**")
        for ca in cas[:3]:
            lines.append(f"  • `{ca}`\n  `/scan {ca}`")
        lines.append(f"\n💡 Use `/copy @{username}` to instantly scan their latest CA")
    else:
        lines.append("No CA drops found in recent tweets.")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def insiders_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = ["🧠 **TOP INSIDER ACCOUNTS**\n" + "═"*35 + "\n",
             "_Add real accounts you trust with `/watch @account`_\n",
             "**📋 Currently Watched Accounts:**\n"]
    if watchlist:
        for i, (uname, data) in enumerate(list(watchlist.items())[:10], 1):
            calls = data.get("calls", 0)
            wins  = data.get("wins", 0)
            wr    = f"{wins/calls*100:.0f}%" if calls > 0 else "No data yet"
            lines.append(f"{i}. **@{uname}**\n   Calls tracked: {calls} | Win rate: {wr}\n   `/moni @{uname}`\n")
    else:
        lines.append("No accounts on watchlist yet.\n\n"
                     "Add alpha accounts:\n"
                     "• `/watch @solanatrader`\n"
                     "• `/watch @alphadrops`\n"
                     "• `/watch @solanagems`")
    lines.append("\n💡 `/watch @account` — auto-detect their CA drops instantly")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def copy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/copy @account`\nFetches and scans their latest CA drop.", parse_mode="Markdown"); return
    username = context.args[0].lstrip('@')
    wait     = await update.message.reply_text(f"📋 Copying from @{username}...")
    async with aiohttp.ClientSession() as session:
        tweets = await scrape_nitter_user(session, username, limit=20)
    cas = []
    for tw in tweets:
        found = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', tw.get("text",""))
        cas.extend(found)
    if not cas:
        await wait.edit_text(f"No CA drops found from @{username} recently.\n\nTry `/moni @{username}` to see their latest tweets.", parse_mode="Markdown"); return
    ca = cas[0]
    a  = await smart_scan(ca)
    if a.get("error"):
        await wait.edit_text(f"📋 Found CA from @{username}: `{ca}`\n\n❌ {a['error']}", parse_mode="Markdown"); return
    await wait.edit_text(
        f"📋 **COPY FROM @{username}**\n{'═'*30}\n\n" + build_scan_card(a),
        reply_markup=get_chart_buttons(ca, a['symbol']),
        parse_mode="Markdown", disable_web_page_preview=True
    )

# ── Watchlist ─────────────────────────────────────────────────
async def watch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a Twitter account to the watchlist for instant CA drop alerts."""
    if not context.args:
        await update.message.reply_text(
            "Usage: `/watch @account`\nExample: `/watch @solanatrader`\n\n"
            "Bot will check their tweets every 60s and instantly drop any CA they post.",
            parse_mode="Markdown"); return
    username = context.args[0].lstrip('@').lower()
    if username in watchlist:
        await update.message.reply_text(f"👁️ **@{username}** is already on the watchlist!\n\nUse `/watchlist` to see all watched accounts.", parse_mode="Markdown"); return
    watchlist[username] = {
        "chat_id":   GROUP_CHAT_ID if GROUP_CHAT_ID != 0 else update.effective_chat.id,
        "added_by":  update.effective_user.id,
        "added_at":  datetime.utcnow().isoformat(),
        "calls":     0,
        "wins":      0,
    }
    save_state()
    total = len(watchlist)
    await update.message.reply_text(
        f"👁️ **Now watching @{username}**\n\n"
        f"✅ Added to watchlist ({total} total)\n\n"
        f"The bot will check @{username}'s tweets every 60 seconds.\n"
        f"Any CA they drop will be instantly posted here with a full scan.\n\n"
        f"Use `/watchlist` to see all watched accounts.",
        parse_mode="Markdown"
    )

async def unwatch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an account from the watchlist."""
    if not context.args:
        await update.message.reply_text("Usage: `/unwatch @account`", parse_mode="Markdown"); return
    username = context.args[0].lstrip('@').lower()
    if username not in watchlist:
        await update.message.reply_text(f"❌ @{username} is not on the watchlist.", parse_mode="Markdown"); return
    del watchlist[username]
    save_state()
    await update.message.reply_text(f"✅ Removed **@{username}** from watchlist.", parse_mode="Markdown")

async def watchlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all watched accounts and their stats."""
    if not watchlist:
        await update.message.reply_text(
            "👁️ **WATCHLIST EMPTY**\n\n"
            "Add alpha accounts with `/watch @account`\n\n"
            "The bot will monitor their tweets 24/7 and instantly drop any CA they post.",
            parse_mode="Markdown"); return
    lines = [f"👁️ **WATCHLIST** ({len(watchlist)} accounts)\n" + "═"*35 + "\n"]
    for i, (uname, data) in enumerate(watchlist.items(), 1):
        calls   = data.get("calls", 0)
        wins    = data.get("wins", 0)
        wr      = f"{wins/calls*100:.0f}%" if calls > 0 else "—"
        added   = data.get("added_at","")[:10]
        lines.append(
            f"**{i}. @{uname}**\n"
            f"   📞 CAs dropped: {calls} | 🎯 Win rate: {wr}\n"
            f"   📅 Watching since: {added}\n"
            f"   `/unwatch @{uname}` to remove\n"
        )
    lines.append("\n🔄 Checking every 60 seconds for new CA drops")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ── Narrative & Learning ──────────────────────────────────────
async def narrative_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    topic = " ".join(context.args).lower() if context.args else ""
    if not topic:
        await update.message.reply_text("Usage: `/narrative <topic>`\nTopics: ai, meme, defi, gaming, rwa\nExample: `/narrative ai`", parse_mode="Markdown"); return
    wait = await update.message.reply_text(f"🔮 Scanning {topic} narrative...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, f"{topic} solana")
    kw_map = {
        "ai":     ['ai','agent','gpt','intelligence','neural'],
        "meme":   ['meme','doge','pepe','cat','dog','frog','chad'],
        "defi":   ['defi','swap','yield','lend','vault','farm'],
        "gaming": ['game','play','gaming','quest','rpg','nft'],
        "rwa":    ['rwa','real','asset','estate','bond'],
    }
    keywords = kw_map.get(topic, [topic])
    matches  = []
    for p in pairs[:100]:
        base = p.get("baseToken",{})
        name = f"{base.get('name','')} {base.get('symbol','')}".lower()
        if any(k in name for k in keywords):
            liq = float(p.get("liquidity",{}).get("usd",0) or 0)
            if liq > 500:
                matches.append({
                    "symbol":  base.get("symbol","???"),
                    "address": base.get("address",""),
                    "fdv":     float(p.get("fdv",0) or 0),
                    "ch_1h":   float(p.get("priceChange",{}).get("h1",0) or 0),
                    "liq":     liq,
                })
    matches.sort(key=lambda x: x["ch_1h"], reverse=True)
    if not matches:
        await wait.edit_text(f"No {topic} coins found right now."); return
    lines = [f"🔮 **{topic.upper()} COINS**\n" + "═"*30 + "\n"]
    for i, m in enumerate(matches[:8], 1):
        lines.append(f"{i}. **${m['symbol']}** — {fmt_pct(m['ch_1h'])} | Liq: {fmt_usd(m['liq'])}\n   `/scan {m['address']}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def mystats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    xp    = user_xp.get(uid, 0)
    calls = len(active_calls.get(uid, {}))
    closed= len(closed_calls.get(uid, []))
    wins  = sum(1 for c in closed_calls.get(uid,[]) if c.get("pnl",0) > 0)
    wr    = f"{wins/closed*100:.0f}%" if closed > 0 else "—"
    lines = [
        f"📊 **YOUR STATS**\n{'═'*30}\n",
        f"⭐ XP: {xp}",
        f"📞 Active calls: {calls}",
        f"🔒 Closed calls: {closed}",
        f"🎯 Win rate: {wr}\n",
        f"**🧠 KAYO'S BRAIN**",
        f"📚 Things learned: {len(kayo_knowledge)}",
        f"🎯 Strategies tracked: {len(strategy_records)}",
        f"👁️ Watching accounts: {len(watchlist)}\n",
    ]
    if kayo_knowledge:
        lines.append("**Latest intel:**")
        for k in kayo_knowledge[-5:]:
            lines.append(f"  • {k}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def strategies_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = ["📈 **STRATEGY WIN RATES**\n" + "═"*35 + "\n"]
    if not strategy_records:
        lines.append("No strategies recorded yet.\n\nUse `/record <strategy> won 50` to teach Kayo.")
    else:
        by_strat = defaultdict(lambda: {"wins":0,"total":0,"profit":0.0})
        for r in strategy_records:
            s = r.get("strategy","unknown")
            by_strat[s]["total"]  += 1
            by_strat[s]["wins"]   += 1 if r.get("won") else 0
            by_strat[s]["profit"] += r.get("profit", 0)
        for strat, d in sorted(by_strat.items(), key=lambda x: x[1]["wins"]/max(x[1]["total"],1), reverse=True):
            wr  = d["wins"] / max(d["total"],1) * 100
            bar = "█" * int(wr / 10)
            lines.append(f"**{strat}**\n  WR: {wr:.0f}% {bar} | {d['wins']}/{d['total']} | avg profit: {d['profit']/max(d['total'],1):.1f}%\n")
    lines.append("\n**🧠 Current Strategy Weights:**")
    for k, v in strategy_weights.items():
        lines.append(f"  {k}: {v*100:.0f}%")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def learn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🧠 Forcing Kayo to learn...")
    async with aiohttp.ClientSession() as session:
        tweets = []
        for q in ["solana alpha", "crypto narrative 2025", "solana gem"]:
            batch = await scrape_nitter(session, q, limit=5)
            tweets.extend(batch)
            await asyncio.sleep(0.5)
    learned = []
    for tw in tweets:
        text = tw.get("text","").lower()
        for kw in ['ai agent','rwa','defi summer','meme season','gaming','pump','bullish','bearish']:
            if kw in text:
                entry = f"{kw.title()} spotted ({datetime.utcnow().strftime('%H:%M UTC')})"
                if entry not in kayo_knowledge:
                    kayo_knowledge.append(entry)
                    if len(kayo_knowledge) > 100: kayo_knowledge.pop(0)
                    learned.append(kw)
    if learned:
        await wait.edit_text(f"🧠 **KAYO LEARNED**\n\n" + "\n".join([f"  • {l.title()}" for l in learned[:8]]) + f"\n\nTotal knowledge: {len(kayo_knowledge)} items", parse_mode="Markdown")
    else:
        await wait.edit_text(f"🧠 Scanned Twitter — no new intel found.\nKayo's knowledge base: {len(kayo_knowledge)} items")

async def record_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3:
        await update.message.reply_text("Usage: `/record <strategy> <won/lost> <profit%>`\nExample: `/record momentum won 45`", parse_mode="Markdown"); return
    strat  = context.args[0].lower()
    result = context.args[1].lower()
    try:    profit = float(context.args[2])
    except: await update.message.reply_text("❌ Profit must be a number e.g. `45` for 45%"); return
    won = result in ["won","win","w","yes","true","1"]
    strategy_records.append({"strategy": strat, "won": won, "profit": profit, "time": datetime.utcnow().isoformat()})
    if strat in strategy_weights:
        wins  = sum(1 for r in strategy_records if r["strategy"] == strat and r["won"])
        total = sum(1 for r in strategy_records if r["strategy"] == strat)
        wr    = wins / total
        strategy_weights[strat] = round(strategy_weights[strat] * 0.9 + wr * 0.1, 3)
    add_xp(update.effective_user.id, 15)
    save_state()
    await update.message.reply_text(
        f"📝 **RECORDED**\n\nStrategy: {strat}\nResult: {'✅ WIN' if won else '❌ LOSS'}\nProfit: {fmt_pct(profit)}\n\n🧠 Kayo updated strategy weights.",
        parse_mode="Markdown"
    )

# ── Market Data ───────────────────────────────────────────────
async def a_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/a <coin>`\nExample: `/a solana`", parse_mode="Markdown"); return
    query = " ".join(context.args).lower()
    wait  = await update.message.reply_text("🔍 Checking CoinGecko...")
    async with aiohttp.ClientSession() as session:
        data = await coingecko_coin(session, query)
    if not data:
        await wait.edit_text(f"❌ '{query}' not found.\nTry exact slug: `/a solana` `/a bitcoin` `/a bonk`", parse_mode="Markdown"); return
    m     = data.get("market_data",{})
    price = m.get("current_price",{}).get("usd",0)
    ch24  = m.get("price_change_percentage_24h",0)
    ch7   = m.get("price_change_percentage_7d",0)
    mcap  = m.get("market_cap",{}).get("usd",0)
    vol   = m.get("total_volume",{}).get("usd",0)
    await wait.edit_text(
        f"🪙 **{data.get('name','')} (${data.get('symbol','').upper()})**\n{'═'*35}\n\n"
        f"💰 Price: {fmt_price(price)}\n"
        f"📈 24h: {fmt_pct(ch24)} | 7d: {fmt_pct(ch7)}\n"
        f"📊 MCap: {fmt_usd(mcap)}\n"
        f"🔄 Vol 24h: {fmt_usd(vol)}\n\n"
        f"[CoinGecko](https://coingecko.com/en/coins/{query})",
        parse_mode="Markdown", disable_web_page_preview=True
    )

async def macro_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🌍 Fetching macro...")
    async with aiohttp.ClientSession() as session:
        g = await coingecko_global(session)
    if g:
        mcap    = g.get("total_market_cap",{}).get("usd",0)
        vol     = g.get("total_volume",{}).get("usd",0)
        btc_dom = g.get("market_cap_percentage",{}).get("btc",0)
        eth_dom = g.get("market_cap_percentage",{}).get("eth",0)
        ch24    = g.get("market_cap_change_percentage_24h_usd",0)
        kayo_read = ('🟢 Risk ON — deploy capital' if ch24 > 2
                     else '🔴 Risk OFF — stay cautious' if ch24 < -3
                     else '🟡 Neutral — be selective')
        await wait.edit_text(
            f"🌍 **MACRO OVERVIEW**\n{'═'*35}\n\n"
            f"**Total MCap:** {fmt_usd(mcap)} ({fmt_pct(ch24)})\n"
            f"**24h Volume:** {fmt_usd(vol)}\n"
            f"**BTC Dom:** {btc_dom:.1f}%  |  **ETH Dom:** {eth_dom:.1f}%\n\n"
            f"🧠 Kayo's read: {kayo_read}\n\n"
            f"• `/a bitcoin` • `/a ethereum` • `/a solana`",
            parse_mode="Markdown"
        )
    else:
        await wait.edit_text("🌍 CoinGecko unavailable.\n\n• `/a bitcoin`\n• `/a ethereum`\n• `/a solana`", parse_mode="Markdown")

async def index_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("📊 Fetching top 10...")
    async with aiohttp.ClientSession() as session:
        coins = await coingecko_top(session, 10)
    if not coins:
        await wait.edit_text("❌ CoinGecko unavailable right now."); return
    lines = ["📊 **TOP 10 BY MARKET CAP**\n" + "═"*35 + "\n"]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    for i, c in enumerate(coins):
        ch = c.get("price_change_percentage_24h",0)
        lines.append(f"{medals[i]} **${c.get('symbol','').upper()}** — {fmt_price(c.get('current_price',0))} ({fmt_pct(ch)})\n   MCap: {fmt_usd(c.get('market_cap',0))}\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def markets_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await macro_cmd(update, context)

async def dt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔥 Fetching trending DEX...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    trending = sorted(pairs, key=lambda x: float(x.get("volume",{}).get("h24",0) or 0), reverse=True)[:10]
    lines = ["🔥 **TRENDING DEX (by volume)**\n" + "═"*35 + "\n"]
    for i, p in enumerate(trending, 1):
        base = p.get("baseToken",{})
        ch   = float(p.get("priceChange",{}).get("h24",0) or 0)
        vol  = float(p.get("volume",{}).get("h24",0) or 0)
        lines.append(f"{i}. **${base.get('symbol','???')}** {fmt_pct(ch)} | Vol: {fmt_usd(vol)}\n   `/scan {base.get('address','')}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

# ── Quick DEX ─────────────────────────────────────────────────
async def x_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/x <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    wait    = await update.message.reply_text("⚡ Quick query...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if not pair:
        await wait.edit_text("❌ Not found"); return
    base  = pair.get("baseToken",{})
    price = float(pair.get("priceUsd",0) or 0)
    fdv   = float(pair.get("fdv",0) or 0)
    liq   = float(pair.get("liquidity",{}).get("usd",0) or 0)
    ch_1h = float(pair.get("priceChange",{}).get("h1",0) or 0)
    ch_5m = float(pair.get("priceChange",{}).get("m5",0) or 0)
    await wait.edit_text(
        f"⚡ **${base.get('symbol','???')}**\n"
        f"💰 {fmt_price(price)} | MCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}\n"
        f"📈 5m: {fmt_pct(ch_5m)} | 1h: {fmt_pct(ch_1h)}",
        reply_markup=get_chart_buttons(address, base.get("symbol","???")),
        parse_mode="Markdown"
    )

async def z_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/z <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if not pair:
        await update.message.reply_text("❌ Not found"); return
    base  = pair.get("baseToken",{})
    price = float(pair.get("priceUsd",0) or 0)
    ch_1h = float(pair.get("priceChange",{}).get("h1",0) or 0)
    await update.message.reply_text(f"**${base.get('symbol','???')}** — {fmt_price(price)} ({fmt_pct(ch_1h)})", parse_mode="Markdown")

async def p_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/p <symbol>`\nExample: `/p bonk`", parse_mode="Markdown"); return
    query = context.args[0].upper().replace('$','')
    wait  = await update.message.reply_text(f"💰 Checking ${query}...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, query)
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if not sol_pairs:
        await wait.edit_text(f"❌ ${query} not found on Solana"); return
    p     = sol_pairs[0]
    price = float(p.get("priceUsd",0) or 0)
    ch_1h = float(p.get("priceChange",{}).get("h1",0) or 0)
    ch_24h= float(p.get("priceChange",{}).get("h24",0) or 0)
    fdv   = float(p.get("fdv",0) or 0)
    addr  = p.get("baseToken",{}).get("address","")
    await wait.edit_text(
        f"💰 **${query}**\n{fmt_price(price)}\n1h: {fmt_pct(ch_1h)} | 24h: {fmt_pct(ch_24h)}\nMCap: {fmt_usd(fdv)}\n\n`/scan {addr}`",
        parse_mode="Markdown"
    )

async def s_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/s <symbol or name>`", parse_mode="Markdown"); return
    query = " ".join(context.args)
    wait  = await update.message.reply_text(f"🔍 Searching for {query}...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, query)
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"][:5]
    if not sol_pairs:
        await wait.edit_text(f"❌ No results for '{query}'"); return
    lines = [f"🔍 **SEARCH: {query.upper()}**\n" + "═"*30 + "\n"]
    for p in sol_pairs:
        base  = p.get("baseToken",{})
        price = float(p.get("priceUsd",0) or 0)
        fdv   = float(p.get("fdv",0) or 0)
        ch_1h = float(p.get("priceChange",{}).get("h1",0) or 0)
        lines.append(f"• **${base.get('symbol','???')}** — {fmt_price(price)} ({fmt_pct(ch_1h)})\n  MCap: {fmt_usd(fdv)} | `/scan {base.get('address','')}`\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

# ── Trading ───────────────────────────────────────────────────
async def call_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/call <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    wait    = await update.message.reply_text("📞 Locking entry...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if not pair:
        await wait.edit_text("❌ Token not found"); return
    price  = float(pair.get("priceUsd",0) or 0)
    symbol = pair.get("baseToken",{}).get("symbol","???")
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
    calls = active_calls.get(uid,{})
    if not calls:
        await update.message.reply_text("No active calls. Use `/call <ca>` to register one.", parse_mode="Markdown"); return
    wait  = await update.message.reply_text("📊 Fetching live P&L...")
    lines = ["📊 **YOUR ACTIVE CALLS**\n" + "═"*30 + "\n"]
    async with aiohttp.ClientSession() as session:
        for addr, c in list(calls.items()):
            pair = await dex_token(session, addr)
            if pair:
                curr = float(pair.get("priceUsd",0) or 0)
                pnl  = ((curr - c["entry"]) / c["entry"] * 100) if c["entry"] > 0 else 0
                e    = "🟢" if pnl > 0 else "🔴"
                lines.append(f"{e} **${c['symbol']}**\n   Entry: {fmt_price(c['entry'])} → Now: {fmt_price(curr)}\n   P&L: {fmt_pct(pnl)}\n")
            else:
                lines.append(f"❓ **${c['symbol']}** — price unavailable\n")
    await wait.edit_text("\n".join(lines), parse_mode="Markdown")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/stop <token_address>`", parse_mode="Markdown"); return
    address = context.args[0].strip()
    uid     = update.effective_user.id
    calls   = active_calls.get(uid,{})
    if address not in calls:
        await update.message.reply_text("❌ No active call for this address.", parse_mode="Markdown"); return
    c    = calls.pop(address)
    wait = await update.message.reply_text("🔒 Closing...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if pair:
        curr = float(pair.get("priceUsd",0) or 0)
        pnl  = ((curr - c["entry"]) / c["entry"] * 100) if c["entry"] > 0 else 0
        closed_calls.setdefault(uid,[]).append({"symbol":c["symbol"],"pnl":pnl,"at":datetime.utcnow().isoformat()})
        add_xp(uid, 20)
        await wait.edit_text(
            f"🔒 **CLOSED — ${c['symbol']}**\n\nEntry: {fmt_price(c['entry'])}\nExit:  {fmt_price(curr)}\nP&L:   {fmt_pct(pnl)} {'🎉' if pnl > 0 else '💀'}",
            parse_mode="Markdown"
        )
    else:
        await wait.edit_text(f"✅ ${c['symbol']} call closed.")

async def leaderboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_scores = []
    for uid, calls in closed_calls.items():
        if calls:
            avg_pnl = sum(c.get("pnl",0) for c in calls) / len(calls)
            wins    = sum(1 for c in calls if c.get("pnl",0) > 0)
            all_scores.append({"uid": uid, "avg": avg_pnl, "wins": wins, "total": len(calls)})
    all_scores.sort(key=lambda x: x["avg"], reverse=True)
    if not all_scores:
        await update.message.reply_text("🏆 No closed calls yet. Use `/call` then `/stop` to get on the leaderboard!", parse_mode="Markdown"); return
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines  = ["🏆 **LEADERBOARD**\n" + "═"*30 + "\n"]
    for i, s in enumerate(all_scores[:5]):
        lines.append(f"{medals[i]} User #{s['uid']} — Avg P&L: {fmt_pct(s['avg'])} | {s['wins']}/{s['total']} wins\n")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ── Wallet ────────────────────────────────────────────────────
async def w_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/w <wallet_address>`", parse_mode="Markdown"); return
    wallet = context.args[0].strip()
    await update.message.reply_text(
        f"👛 **Wallet**\n`{wallet[:12]}...{wallet[-6:]}`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👛 Solscan",      url=f"https://solscan.io/account/{wallet}"),
            InlineKeyboardButton("📊 Step Finance", url=f"https://app.step.finance/en/dashboard?watching={wallet}"),
        ]])
    )

async def trackwallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/trackwallet <wallet_address>`", parse_mode="Markdown"); return
    wallet = context.args[0].strip()
    tracked_wallets[wallet] = update.effective_chat.id
    await update.message.reply_text(f"👀 Now tracking `{wallet[:12]}...{wallet[-6:]}`\n\nYou'll get alerts on any new activity.", parse_mode="Markdown")

async def mywallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/mywallet <wallet_address>`", parse_mode="Markdown"); return
    wallet = context.args[0].strip()
    my_wallets[update.effective_user.id] = wallet
    await update.message.reply_text(f"✅ Wallet set: `{wallet[:12]}...{wallet[-6:]}`", parse_mode="Markdown")

async def walletpnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    wallet = my_wallets.get(uid)
    closed = closed_calls.get(uid, [])
    lines  = ["📊 **WALLET P&L**\n" + "═"*30 + "\n"]
    if wallet:
        lines.append(f"👛 Wallet: `{wallet[:12]}...{wallet[-6:]}`\n")
    if not closed:
        lines.append("No closed calls yet.\n\nUse `/call` then `/stop` to track trades.")
    else:
        total_pnl = sum(c.get("pnl",0) for c in closed)
        wins      = sum(1 for c in closed if c.get("pnl",0) > 0)
        lines.append(f"📞 Total trades: {len(closed)}")
        lines.append(f"🎯 Win rate: {wins/len(closed)*100:.0f}%")
        lines.append(f"💰 Total P&L: {fmt_pct(total_pnl)}\n")
        for c in closed[-5:]:
            e = "🟢" if c.get("pnl",0) > 0 else "🔴"
            lines.append(f"{e} **${c.get('symbol','?')}** — {fmt_pct(c.get('pnl',0))}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def untrackwallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/untrackwallet <wallet_address>`", parse_mode="Markdown"); return
    wallet = context.args[0].strip()
    if wallet in tracked_wallets:
        del tracked_wallets[wallet]
        await update.message.reply_text(f"✅ Stopped tracking `{wallet[:12]}...`", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ That wallet isn't being tracked.")

# ── Group commands ────────────────────────────────────────────
async def gp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    top = sorted(user_xp.items(), key=lambda x: x[1], reverse=True)[:5]
    if not top:
        await update.message.reply_text("🏆 No XP yet. Use commands to earn points!", parse_mode="Markdown"); return
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines  = ["🏆 **GROUP POINTS**\n" + "═"*30 + "\n",
              "• `/scan` +5 | `/call` +10 | `/stop` +20 | `/record` +15\n"]
    for i, (uid, xp) in enumerate(top):
        rank = ("💎 Chad" if xp >= 2000 else "🥇 Alpha" if xp >= 500 else "🥈 Degen" if xp >= 100 else "🥉 Rookie")
        lines.append(f"{medals[i]} User #{uid} — {xp} XP ({rank})")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong! Kayo is alive and scanning 24/7 🦅")

async def dubs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤫 **DUBS — SILENT CHAT SUMMARY**\n\n"
        "📊 Current market vibe based on what I've learned:\n\n" +
        (f"• {kayo_knowledge[-1]}\n" if kayo_knowledge else "• No intel yet — use `/learn`\n") +
        f"\n🔥 Watching {len(watchlist)} accounts | 🆕 {len(seen_tokens)} tokens scanned",
        parse_mode="Markdown"
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
        f"{f'Next: {nxt-xp} XP away' if nxt else '🏆 Max rank!'}\n\n"
        f"Earn: `/scan` +5 | `/call` +10 | `/stop` +20 | `/record` +15",
        parse_mode="Markdown"
    )

async def remindme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage: `/remindme <time> <message>`\nExamples:\n• `/remindme 30m Check SOL`\n• `/remindme 2h Buy the dip`\n• `/remindme 1d Weekly review`", parse_mode="Markdown"); return
    time_str = context.args[0].lower()
    msg      = " ".join(context.args[1:])
    minutes  = 0
    if time_str.endswith('m'):
        try: minutes = int(time_str[:-1])
        except: pass
    elif time_str.endswith('h'):
        try: minutes = int(time_str[:-1]) * 60
        except: pass
    elif time_str.endswith('d'):
        try: minutes = int(time_str[:-1]) * 1440
        except: pass
    if minutes <= 0:
        await update.message.reply_text("❌ Invalid time. Use `30m`, `2h`, or `1d`.", parse_mode="Markdown"); return
    fire_at = (datetime.utcnow() + timedelta(minutes=minutes)).isoformat()
    reminders.append({"chat_id": update.effective_chat.id, "msg": msg, "fire_at": fire_at})
    await update.message.reply_text(f"⏰ Reminder set!\n\n**Message:** {msg}\n**In:** {time_str}", parse_mode="Markdown")

async def tz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    zones = [
        ("🇺🇸 New York",  -5), ("🇺🇸 LA",       -8),
        ("🇬🇧 London",    0),  ("🇳🇬 Lagos",     1),
        ("🇦🇪 Dubai",     4),  ("🇮🇳 India",     5.5),
        ("🇸🇬 Singapore", 8),  ("🇯🇵 Tokyo",     9),
        ("🇦🇺 Sydney",    10),
    ]
    lines = [f"🕐 **WORLD TIMEZONES**\nUTC: {now.strftime('%H:%M')}\n" + "═"*30 + "\n"]
    for name, offset in zones:
        h, m = divmod(int(offset * 60), 60)
        local = now + timedelta(hours=h, minutes=m)
        lines.append(f"{name}: **{local.strftime('%H:%M')}**")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime  = int(time.time() - start_time)
    hours   = uptime // 3600
    minutes = (uptime % 3600) // 60
    await update.message.reply_text(
        f"✅ **KAYO STATUS v13**\n{'═'*30}\n\n"
        f"⏱️ Uptime: {hours}h {minutes}m\n"
        f"👁️ Watching: {len(watchlist)} accounts\n"
        f"🔄 Tokens scanned: {len(seen_tokens)}\n"
        f"📰 News seen: {len(seen_news)}\n"
        f"🧠 Knowledge items: {len(kayo_knowledge)}\n"
        f"👛 Tracked wallets: {len(tracked_wallets)}\n"
        f"📊 Active calls: {sum(len(v) for v in active_calls.values())}\n"
        f"⏰ Reminders pending: {len(reminders)}\n\n"
        f"📢 Group reports: {'✅ ON' if GROUP_CHAT_ID != 0 else '❌ Set GROUP_CHAT_ID'}",
        parse_mode="Markdown"
    )

async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s   = settings.get(chat_id, {})
    btn = "✅ ON" if s.get("buttons", True)      else "❌ OFF"
    aut = "✅ ON" if s.get("autoresponder", True) else "❌ OFF"
    await update.message.reply_text(
        f"⚙️ **SETTINGS**\n\nButtons: {btn}\nAuto-scan: {aut}\n\n"
        f"`/buttons` toggle chart buttons\n`/autoresponder` toggle CA auto-scan",
        parse_mode="Markdown"
    )

async def buttons_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    cur = settings[chat_id].get("buttons", True)
    settings[chat_id]["buttons"] = not cur
    await update.message.reply_text(f"🔘 Chart buttons: {'✅ ON' if not cur else '❌ OFF'}")

async def autoresponder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    cur = settings[chat_id].get("autoresponder", True)
    settings[chat_id]["autoresponder"] = not cur
    await update.message.reply_text(f"🤖 Auto address scan: {'✅ ON' if not cur else '❌ OFF'}")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ── Gemini AI Chat ────────────────────────────────────────────
async def ask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask Kayo AI anything about crypto."""
    if not context.args:
        await update.message.reply_text("Usage: `/ask <question>`\nExample: `/ask Is now a good time to ape into meme coins?`", parse_mode="Markdown"); return
    question = " ".join(context.args)
    wait = await update.message.reply_text("🧠 Thinking...")
    news_ctx = ""
    if news_cache:
        headlines = [n["title"] for n in news_cache[:5]]
        news_ctx = "\nRecent news: " + " | ".join(headlines)
    prompt = f"""You are Kayo, a sharp Web3/crypto alpha analyst with deep knowledge of Solana, DeFi, meme coins, and on-chain analysis. Answer concisely and directly. Use emojis. Max 4 sentences.{news_ctx}

Question: {question}"""
    answer = await gemini_ask(prompt, fallback="🧠 Gemini unavailable right now. Try again shortly.")
    await wait.edit_text(f"🧠 **KAYO AI**\n\n{answer}", parse_mode="Markdown")
    add_xp(update.effective_user.id, 3)

# ── CryptoPanic News ──────────────────────────────────────────
async def cryptonews_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetch and AI-summarize latest crypto news + find matching CAs on DexScreener."""
    wait = await update.message.reply_text("📰 Fetching crypto news...")
    async with aiohttp.ClientSession() as session:
        articles = await fetch_cryptopanic(session)
    if not articles:
        await wait.edit_text("❌ News feeds unavailable right now. Try again in a moment."); return
    news_cache = articles
    last_news_fetch = __import__("time").time()
    headlines = "\n".join([f"• [{a.get('source','News')}] {a['title']}" for a in articles[:10]])
    if _gemini:
        prompt = f"""You are Kayo, a crypto analyst. Summarize these crypto news headlines into a sharp 4-line alpha briefing. Mention what's bullish, what's bearish, and what narrative is trending. Use emojis.

Headlines:
{headlines}"""
        summary = await gemini_ask(prompt, fallback="")
        msg = f"📰 **CRYPTO NEWS — AI SUMMARY**\n{'═'*35}\n\n{summary}\n\n**Raw Headlines:**\n{headlines}"
    else:
        msg = f"📰 **LATEST CRYPTO NEWS**\n{'═'*35}\n\n{headlines}"
    await wait.edit_text(msg[:4000], parse_mode="Markdown", disable_web_page_preview=True)

    # ── Now find CAs on Solana matching the news narratives ──
    if not _gemini:
        return
    await asyncio.sleep(1)
    search_msg = await update.message.reply_text("🔍 Searching for CAs matching today's narratives...")
    # Extract key tokens/narratives from headlines using Gemini
    extract_prompt = f"""From these crypto news headlines, extract up to 4 short search keywords that would find relevant Solana tokens on DexScreener. Focus on: coin names, narratives (AI, RWA, gaming), or trending topics. Return ONLY a comma-separated list of keywords, nothing else.

Headlines:
{headlines}"""
    keywords_raw = await gemini_ask(extract_prompt, fallback="")
    if not keywords_raw:
        await search_msg.edit_text("🔍 Could not extract keywords from news."); return

    keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()][:4]
    logger.info(f"News CA search keywords: {keywords}")

    found_tokens = []
    seen_addrs   = set()
    async with aiohttp.ClientSession() as session:
        for kw in keywords:
            try:
                results = await dex_search(session, kw)
                for p in results[:5]:
                    base    = p.get("baseToken", {})
                    address = base.get("address","")
                    symbol  = base.get("symbol","???")
                    name    = base.get("name","")
                    chain   = p.get("chainId","")
                    fdv     = float(p.get("fdv",0) or 0)
                    liq     = float(p.get("liquidity",{}).get("usd",0) or 0)
                    ch_1h   = float(p.get("priceChange",{}).get("h1",0) or 0)
                    ch_24h  = float(p.get("priceChange",{}).get("h24",0) or 0)
                    if not address or address in seen_addrs: continue
                    if liq < 5000 or fdv < 10_000: continue
                    if chain != "solana": continue
                    seen_addrs.add(address)
                    found_tokens.append({
                        "keyword": kw, "symbol": symbol, "name": name,
                        "address": address, "fdv": fdv, "liq": liq,
                        "ch_1h": ch_1h, "ch_24h": ch_24h
                    })
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.debug(f"News CA search '{kw}': {e}")

    if not found_tokens:
        await search_msg.edit_text(f"🔍 Searched for: {', '.join(keywords)}\n❌ No matching Solana CAs found with good liquidity.")
        return

    lines_out = [f"🔍 **CAs MATCHING TODAY'S NEWS**\n{'═'*35}\n"]
    lines_out.append(f"_Keywords searched: {', '.join(keywords)}_\n")
    for t in found_tokens[:8]:
        trend = "📈" if t["ch_1h"] > 0 else "📉"
        lines_out.append(
            f"{trend} **${t['symbol']}** — {t['name']}\n"
            f"   MCap: {fmt_usd(t['fdv'])} | Liq: {fmt_usd(t['liq'])}\n"
            f"   1h: {fmt_pct(t['ch_1h'])} | 24h: {fmt_pct(t['ch_24h'])}\n"
            f"   `{t['address']}`\n"
        )
    await search_msg.edit_text("\n".join(lines_out)[:4000], parse_mode="Markdown", disable_web_page_preview=True)


# ── PumpFun Commands ──────────────────────────────────────────
async def pump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show newest pump.fun launches."""
    wait = await update.message.reply_text("🎰 Fetching new pump.fun launches...")
    async with aiohttp.ClientSession() as session:
        tokens = await pumpfun_new_tokens(session, limit=15)
    if not tokens:
        await wait.edit_text("❌ PumpFun API unavailable right now."); return
    lines = ["🎰 **NEW PUMP.FUN LAUNCHES**\n" + "═"*35 + "\n"]
    for i, t in enumerate(tokens[:10], 1):
        sym   = t.get("symbol","???")
        name  = t.get("name","?")
        mcap  = t.get("market_cap", 0)
        addr  = t.get("mint","")
        king  = "👑" if t.get("king_of_the_hill_timestamp") else ""
        reply = t.get("reply_count",0)
        lines.append(f"{i}. {king}**${sym}** ({name})\n   MCap: {fmt_usd(mcap)} | 💬 {reply} replies\n   `/scan {addr}`\n")
    await wait.edit_text("\n".join(lines)[:4000], parse_mode="Markdown")

async def graduating_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pump.fun tokens near graduation (bonding curve ~complete)."""
    wait = await update.message.reply_text("🎓 Finding tokens about to graduate...")
    async with aiohttp.ClientSession() as session:
        tokens = await pumpfun_graduating(session)
    if not tokens:
        await wait.edit_text("❌ No graduating tokens found right now."); return
    lines = ["🎓 **GRADUATING SOON (PumpFun)**\n" + "═"*35 + "\n",
             "_These tokens are near 69k bonding curve — about to hit DEX_\n"]
    for i, t in enumerate(tokens[:8], 1):
        sym  = t.get("symbol","???")
        mcap = t.get("market_cap",0)
        addr = t.get("mint","")
        pct  = min(100, mcap / 69000 * 100)
        bar  = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        lines.append(f"{i}. **${sym}** — {pct:.0f}% to grad\n   [{bar}]\n   MCap: {fmt_usd(mcap)} | `/scan {addr}`\n")
    await wait.edit_text("\n".join(lines)[:4000], parse_mode="Markdown")

# ── Price Alerts ──────────────────────────────────────────────
async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set a price alert: /alert <ca> <above|below> <price>"""
    if len(context.args) < 3:
        await update.message.reply_text(
            "Usage: `/alert <ca> <above|below> <price>`\n"
            "Example: `/alert EPjFW... above 0.005`\n\n"
            "You\'ll get a DM when the price hits your target.",
            parse_mode="Markdown"); return
    addr      = context.args[0].strip()
    direction = context.args[1].lower()
    try:    target = float(context.args[2])
    except: await update.message.reply_text("❌ Price must be a number."); return
    if direction not in ("above","below"):
        await update.message.reply_text("❌ Use `above` or `below`.", parse_mode="Markdown"); return
    uid = update.effective_user.id
    price_alerts.setdefault(uid, [])
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, addr)
    sym = pair.get("baseToken",{}).get("symbol","?") if pair else "?"
    price_alerts[uid].append({"address": addr, "symbol": sym, "target": target,
                               "direction": direction, "chat_id": update.effective_chat.id})
    save_state()
    await update.message.reply_text(
        f"🔔 **ALERT SET**\n\n${sym} — notify when price goes **{direction}** {fmt_price(target)}\n\nUse `/myalerts` to see all alerts.",
        parse_mode="Markdown"
    )

async def myalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    alerts = price_alerts.get(uid, [])
    if not alerts:
        await update.message.reply_text("No alerts set.\n\nUse `/alert <ca> <above|below> <price>` to set one.", parse_mode="Markdown"); return
    lines = ["🔔 **YOUR PRICE ALERTS**\n"]
    for i, a in enumerate(alerts, 1):
        lines.append(f"{i}. **${a['symbol']}** — {a['direction']} {fmt_price(a['target'])}")
    lines.append("\nUse `/delalert <number>` to remove an alert.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def delalert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Usage: `/delalert <number>`\nSee your alerts: `/myalerts`", parse_mode="Markdown"); return
    try:    idx = int(context.args[0]) - 1
    except: await update.message.reply_text("❌ Invalid number."); return
    alerts = price_alerts.get(uid, [])
    if idx < 0 or idx >= len(alerts):
        await update.message.reply_text("❌ Alert not found. Use `/myalerts` to see your list.", parse_mode="Markdown"); return
    removed = alerts.pop(idx)
    save_state()
    await update.message.reply_text(f"✅ Removed alert for **${removed['symbol']}**.", parse_mode="Markdown")

# ── Portfolio Tracker ─────────────────────────────────────────
async def addport_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add token to personal portfolio: /addport <ca> <amount> <buy_price>"""
    if len(context.args) < 3:
        await update.message.reply_text(
            "Usage: `/addport <ca> <amount_usd> <buy_price>`\n"
            "Example: `/addport EPjFW... 100 0.00042`",
            parse_mode="Markdown"); return
    addr = context.args[0].strip()
    try:
        amount    = float(context.args[1])
        buy_price = float(context.args[2])
    except:
        await update.message.reply_text("❌ Amount and price must be numbers."); return
    uid = update.effective_user.id
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, addr)
    sym = pair.get("baseToken",{}).get("symbol","?") if pair else "?"
    portfolio.setdefault(uid, [])
    portfolio[uid].append({"address": addr, "symbol": sym, "amount": amount,
                           "buy_price": buy_price, "added": __import__("datetime").datetime.utcnow().isoformat()})
    save_state()
    await update.message.reply_text(
        f"💼 **Added to portfolio**\n\n${sym} — ${amount:.2f} @ {fmt_price(buy_price)}",
        parse_mode="Markdown"
    )

async def portfolio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show portfolio with live P&L."""
    uid   = update.effective_user.id
    holds = portfolio.get(uid, [])
    if not holds:
        await update.message.reply_text("No portfolio yet.\n\nAdd tokens: `/addport <ca> <amount_usd> <buy_price>`", parse_mode="Markdown"); return
    wait  = await update.message.reply_text("💼 Loading portfolio...")
    lines = ["💼 **YOUR PORTFOLIO**\n" + "═"*35 + "\n"]
    total_invested = total_now = 0
    async with aiohttp.ClientSession() as session:
        for h in holds:
            pair     = await dex_token(session, h["address"])
            cur_price= float(pair.get("priceUsd",0) or 0) if pair else 0
            tokens   = h["amount"] / h["buy_price"] if h["buy_price"] > 0 else 0
            cur_val  = tokens * cur_price
            pnl      = ((cur_price - h["buy_price"]) / h["buy_price"] * 100) if h["buy_price"] > 0 else 0
            total_invested += h["amount"]
            total_now      += cur_val
            e = "🟢" if pnl > 0 else "🔴"
            lines.append(f"{e} **${h['symbol']}**\n   In: ${h['amount']:.2f} → Now: {fmt_usd(cur_val)} ({fmt_pct(pnl)})\n")
    total_pnl = ((total_now - total_invested) / total_invested * 100) if total_invested > 0 else 0
    e = "🟢" if total_pnl > 0 else "🔴"
    lines.append(f"\n{e} **TOTAL:** ${total_invested:.2f} → {fmt_usd(total_now)} ({fmt_pct(total_pnl)})")
    await wait.edit_text("\n".join(lines)[:4000], parse_mode="Markdown")

# ── Blacklist / Rug Flag ──────────────────────────────────────
async def blacklist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        if blacklist:
            lines = ["🚫 **BLACKLISTED TOKENS**\n"] + [f"• `{a}`" for a in list(blacklist)[:10]]
            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await update.message.reply_text("No tokens blacklisted yet.\n\nUse `/blacklist <ca>` to flag a rug.", parse_mode="Markdown")
        return
    addr = context.args[0].strip()
    blacklist.add(addr)
    save_state()
    await update.message.reply_text(f"🚫 Token blacklisted: `{addr[:12]}...`\n\nBot will warn your group if anyone pastes this.", parse_mode="Markdown")

# ── Market Sentiment (Gemini) ─────────────────────────────────
async def sentiment_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🧠 Analyzing market sentiment...")
    async with aiohttp.ClientSession() as session:
        global_data, trending, articles = await asyncio.gather(
            coingecko_global(session),
            coingecko_trending(session),
            fetch_cryptopanic(session)
        )
    mcap_ch  = global_data.get("market_cap_change_percentage_24h_usd", 0)
    btc_dom  = global_data.get("market_cap_percentage",{}).get("btc",0)
    trend_names = [c.get("item",{}).get("name","") for c in trending[:5]]
    headlines   = [a["title"] for a in articles[:8]]

    if _gemini:
        prompt = f"""You are Kayo, a sharp crypto analyst. Give an overall market sentiment analysis in 5 bullet points. Be direct and actionable.

Data:
- Total market 24h change: {fmt_pct(mcap_ch)}
- BTC dominance: {btc_dom:.1f}%
- Trending coins: {', '.join(trend_names)}
- Latest news: {' | '.join(headlines[:5])}

Format: use 🟢 bullish / 🔴 bearish / 🟡 neutral emojis. End with a 1-line action recommendation."""
        analysis = await gemini_ask(prompt, fallback="")
        msg = f"🧠 **KAYO AI MARKET SENTIMENT**\n{'═'*35}\n\n{analysis}"
    else:
        mood = "🟢 BULLISH" if mcap_ch > 2 else "🔴 BEARISH" if mcap_ch < -3 else "🟡 NEUTRAL"
        msg  = f"📊 **MARKET SENTIMENT**\n\nMood: {mood}\n24h MCap change: {fmt_pct(mcap_ch)}\nBTC Dom: {btc_dom:.1f}%\nTrending: {', '.join(trend_names)}"
    await wait.edit_text(msg[:4000], parse_mode="Markdown")

# ── Gem Finder (AI-powered) ───────────────────────────────────
async def gems_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """AI-powered hidden gem finder across DexScreener + PumpFun."""
    wait = await update.message.reply_text("💎 Hunting for gems...")
    async with aiohttp.ClientSession() as session:
        pairs, pump_tokens = await asyncio.gather(
            dex_search(session, "solana"),
            pumpfun_new_tokens(session, 20)
        )
    candidates = []
    for p in pairs[:150]:
        base  = p.get("baseToken",{})
        fdv   = float(p.get("fdv",0) or 0)
        liq   = float(p.get("liquidity",{}).get("usd",0) or 0)
        ch_1h = float(p.get("priceChange",{}).get("h1",0) or 0)
        ch_5m = float(p.get("priceChange",{}).get("m5",0) or 0)
        buys  = int(p.get("txns",{}).get("h1",{}).get("buys",0) or 0)
        sells = int(p.get("txns",{}).get("h1",{}).get("sells",0) or 0)
        if fdv < 5000 or fdv > 5_000_000: continue
        if liq < 2000: continue
        if ch_1h < 5: continue
        buy_pressure = buys / max(sells, 1)
        gem_score = (ch_1h * 0.3) + (ch_5m * 0.2) + (buy_pressure * 10) + (liq / fdv * 100)
        if gem_score > 15:
            candidates.append({
                "symbol": base.get("symbol","???"),
                "address": base.get("address",""),
                "fdv": fdv, "liq": liq, "ch_1h": ch_1h, "ch_5m": ch_5m,
                "buy_pressure": buy_pressure, "gem_score": gem_score
            })
    candidates.sort(key=lambda x: x["gem_score"], reverse=True)
    top = candidates[:5]
    if not top:
        await wait.edit_text("💎 No gems found right now — market may be slow. Try again in 5 min."); return

    if _gemini and top:
        # Send all gems in ONE Gemini call (not per-token) to stay within rate limit
        token_summary = "\n".join([
            f"${c['symbol']}: MCap {fmt_usd(c['fdv'])}, Liq {fmt_usd(c['liq'])}, 1h {fmt_pct(c['ch_1h'])}, buy pressure {c['buy_pressure']:.1f}x"
            for c in top
        ])
        prompt = f"""You are Kayo, a Solana gem hunter. Rank and briefly comment on these potential gems (1 sentence each). Be honest — mention any red flags.

{token_summary}

Format each as: 💎 $SYMBOL — your comment"""
        ai_take = await gemini_ask(prompt, fallback="")
    else:
        ai_take = ""

    lines = ["💎 **KAYO GEM FINDER**\n" + "═"*35 + "\n"]
    for i, c in enumerate(top, 1):
        lines.append(f"{i}. **${c['symbol']}** — Score: {c['gem_score']:.0f}\n   MCap: {fmt_usd(c['fdv'])} | Liq: {fmt_usd(c['liq'])} | 1h: {fmt_pct(c['ch_1h'])}\n   Buy pressure: {c['buy_pressure']:.1f}x\n   `/scan {c['address']}`\n")
    if ai_take:
        lines.append(f"\n🧠 **Kayo AI Take:**\n{ai_take}")
    await wait.edit_text("\n".join(lines)[:4000], parse_mode="Markdown")

# ── Summarize Group Chat ──────────────────────────────────────
async def gsum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """AI summary of what's been happening in the group."""
    if not group_messages:
        await update.message.reply_text("Not enough group activity to summarize yet."); return
    if not _gemini:
        await update.message.reply_text("🧠 Gemini not configured — can\'t summarize.", parse_mode="Markdown"); return
    wait = await update.message.reply_text("🧠 Summarizing group activity...")
    recent = group_messages[-30:]
    convo  = "\n".join([f"@{m['user']}: {m['text'][:100]}" for m in recent])
    prompt = f"""Summarize this crypto Telegram group conversation in 5 bullet points. Focus on: which tokens were discussed, what calls were made, general sentiment, any alpha dropped. Be concise.

Conversation:
{convo}"""
    summary = await gemini_ask(prompt, fallback="Could not summarize.")
    await wait.edit_text(f"🧠 **GROUP SUMMARY**\n{'═'*30}\n\n{summary}", parse_mode="Markdown")

# ── Callback handler ──────────────────────────────────────────
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if data.startswith("scan:"):
        address  = data[5:]
        a        = await smart_scan(address)
        if a.get("error"):
            await q.message.reply_text(f"❌ {a['error']}")
        else:
            await q.message.reply_text(
                build_scan_card(a),
                reply_markup=get_chart_buttons(address, a['symbol']),
                parse_mode="Markdown", disable_web_page_preview=True
            )
    elif data.startswith("rug:"):
        address = data[4:]
        async with aiohttp.ClientSession() as session:
            pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
        if not pair:
            await q.message.reply_text("❌ Token not found"); return
        rug, red = 0, []
        if sec.get("is_honeypot") == "1":     rug += 60; red.append("🚨 Honeypot")
        if float(sec.get("sell_tax",0) or 0) > 10: rug += 20; red.append("⚠️ High tax")
        if sec.get("lp_locked") != "1":        rug += 35; red.append("⚠️ LP unlocked")
        rug     = min(100, rug)
        verdict = ("🔴 RUG" if rug >= 70 else "🟠 RISKY" if rug >= 50 else "🟡 CHECK" if rug >= 30 else "🟢 CLEAN")
        grn     = "🔒 LP Locked" if sec.get("lp_locked") == "1" else ""
        await q.message.reply_text(
            f"🔍 **${pair.get('baseToken',{}).get('symbol','?')}** Rug: {verdict} ({rug}/100)\n\n"
            f"🚩 {', '.join(red) or 'None'}\n✅ {grn or 'None'}",
            parse_mode="Markdown"
        )

# ── Auto CA detection in chat ─────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    text    = update.message.text
    chat_id = update.effective_chat.id
    if not settings.get(chat_id, {}).get("autoresponder", True): return
    addresses = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
    # Track group messages for AI summary
    try:
        user = update.effective_user
        uname = user.username or user.first_name or "user"
        group_messages.append({"user": uname, "text": text})
        if len(group_messages) > 50:
            group_messages.pop(0)
    except:
        pass

    if not addresses: return
    address = addresses[0]
    # Warn if blacklisted
    if address in blacklist:
        await update.message.reply_text("🚫 **BLACKLISTED TOKEN** — this address has been flagged as a rug by the community.", parse_mode="Markdown")
        return
    wait    = await update.message.reply_text("🔍 CA detected — scanning...")
    a = await smart_scan(address)
    if not a.get("error"):
        markup = get_chart_buttons(address, a['symbol']) if settings.get(chat_id,{}).get("buttons", True) else None
        await wait.edit_text(build_scan_card(a), reply_markup=markup, parse_mode="Markdown", disable_web_page_preview=True)
    else:
        await wait.edit_text(f"❌ {a['error']}")

# ════════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ════════════════════════════════════════════════════════════

async def bg_reminder_checker(app: Application):
    while True:
        now = datetime.utcnow()
        due = [r for r in reminders if datetime.fromisoformat(r["fire_at"]) <= now]
        for r in due:
            try:
                await app.bot.send_message(chat_id=r["chat_id"], text=f"⏰ **REMINDER**\n\n{r['msg']}", parse_mode="Markdown")
                reminders.remove(r)
            except: pass
        await asyncio.sleep(30)

async def bg_watchlist_scanner(app: Application):
    """
    Core watchlist engine — checks every watched account every 60s.
    The moment a watched account tweets a CA, it gets reported instantly.
    """
    global watchlist_seen_tweets
    await asyncio.sleep(60)
    logger.info("👁️ Watchlist scanner started")
    while True:
        for username, data in list(watchlist.items()):
            try:
                async with aiohttp.ClientSession() as session:
                    tweets = await scrape_nitter_user(session, username, limit=10)
                for tw in tweets:
                    text = tw.get("text","")
                    tid  = hashlib.md5(f"{username}:{text}".encode()).hexdigest()
                    if tid in watchlist_seen_tweets:
                        continue
                    watchlist_seen_tweets.add(tid)
                    # keep seen set lean
                    if len(watchlist_seen_tweets) > 5000:
                        watchlist_seen_tweets = set(list(watchlist_seen_tweets)[-2000:])
                    # look for CAs
                    cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
                    if not cas:
                        continue
                    ca      = cas[0]
                    snippet = text[:200].replace('\n',' ')
                    # increment call counter
                    watchlist[username]["calls"] = watchlist[username].get("calls", 0) + 1
                    # scan the CA
                    a = await smart_scan(ca)
                    target_chat = data.get("chat_id", GROUP_CHAT_ID)
                    if target_chat == 0:
                        continue
                    if a.get("error"):
                        msg = (
                            f"👁️ **WATCHLIST ALERT — @{username}**\n"
                            f"{'═'*35}\n\n"
                            f"📝 {snippet}\n\n"
                            f"📌 CA: `{ca}`\n"
                            f"❌ Token not found on Solana yet — could be very new!\n\n"
                            f"Save CA and check in a few minutes."
                        )
                        try:
                            await app.bot.send_message(
                                chat_id=target_chat,
                                text=msg,
                                parse_mode="Markdown"
                            )
                        except Exception as e:
                            logger.warning(f"Watchlist alert error: {e}")
                    else:
                        # track win rate — if momentum > 50 count as "win"
                        if a.get("momentum_score", 0) > 50:
                            watchlist[username]["wins"] = watchlist[username].get("wins", 0) + 1
                        msg = (
                            f"👁️ **WATCHLIST ALERT — @{username} DROPPED A CA**\n"
                            f"{'═'*35}\n\n"
                            f"📝 {snippet}\n\n" +
                            build_scan_card(a)
                        )
                        try:
                            await app.bot.send_message(
                                chat_id=target_chat,
                                text=msg,
                                reply_markup=get_chart_buttons(ca, a['symbol']),
                                parse_mode="Markdown",
                                disable_web_page_preview=True
                            )
                        except Exception as e:
                            logger.warning(f"Watchlist scan alert error: {e}")
                await asyncio.sleep(2)  # between accounts
            except Exception as e:
                logger.error(f"Watchlist scanner error for @{username}: {e}")
        await asyncio.sleep(60)  # full cycle every 60s


# ── Background: Fast DexScreener Scanner (every 30s) ─────────
async def bg_dex_fast_scanner(app: Application):
    """Scan DexScreener every 30s for new gems and unusual movement."""
    await asyncio.sleep(60)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                pairs = await dex_trending_solana(session)
            for p in pairs[:120]:
                base    = p.get("baseToken", {})
                address = base.get("address", "")
                symbol  = base.get("symbol", "???")
                if not address or address in blacklist:
                    continue
                fdv   = float(p.get("fdv", 0) or 0)
                liq   = float(p.get("liquidity", {}).get("usd", 0) or 0)
                ch_5m = float(p.get("priceChange", {}).get("m5", 0) or 0)
                ch_1h = float(p.get("priceChange", {}).get("h1", 0) or 0)
                vol_5m= float(p.get("volume", {}).get("m5", 0) or 0)
                vol_1h= float(p.get("volume", {}).get("h1", 0) or 0)
                buys  = int(p.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
                sells = int(p.get("txns", {}).get("m5", {}).get("sells", 0) or 0)
                if liq < 2000 or fdv < 5000 or fdv > 20_000_000:
                    continue
                vol_ratio = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 1
                dex_baseline[address] = {"ch_5m": ch_5m, "vol_ratio": vol_ratio, "buys": buys}

                alert_key = f"{address}_{int(__import__('time').time() // 300)}"
                if alert_key in gem_alerts_sent:
                    continue

                alert = None
                alert_type = ""

                # 🚀 Massive pump
                if ch_5m > 20 and vol_ratio > 4 and buys > sells * 2:
                    alert = (f"🚀 **MASSIVE PUMP DETECTED**\n"
                             f"**${symbol}** +{ch_5m:.0f}% in 5m | {vol_ratio:.1f}x volume surge\n"
                             f"💚 Buys: {buys} | 🔴 Sells: {sells}\n"
                             f"MCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}")
                    alert_type = "pump"

                # 💀 Massive dump
                elif ch_5m < -20 and vol_ratio > 4 and sells > buys * 2:
                    alert = (f"💀 **MASSIVE DUMP DETECTED**\n"
                             f"**${symbol}** {ch_5m:.0f}% in 5m | {vol_ratio:.1f}x sell volume\n"
                             f"💚 Buys: {buys} | 🔴 Sells: {sells}\n"
                             f"MCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}")
                    alert_type = "dump"

                # 🐳 Whale accumulation — huge volume, price stable
                elif vol_ratio > 8 and abs(ch_5m) < 5 and buys > 20:
                    alert = (f"🐳 **WHALE ACCUMULATION**\n"
                             f"**${symbol}** — {vol_ratio:.0f}x normal volume, price barely moved\n"
                             f"💚 Buys: {buys} | MCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}\n"
                             f"⚡ Smart money loading?")
                    alert_type = "whale"

                # 💎 New gem — small mcap, strong buy pressure, pumping
                elif fdv < 500_000 and ch_1h > 30 and buys > sells * 1.5 and liq > 5000:
                    alert = (f"💎 **POTENTIAL GEM**\n"
                             f"**${symbol}** +{ch_1h:.0f}% in 1h — tiny mcap, strong buys\n"
                             f"MCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)} | Buy pressure: {buys/max(sells,1):.1f}x")
                    alert_type = "gem"

                if alert and GROUP_CHAT_ID != 0:
                    gem_alerts_sent.add(alert_key)
                    ai_take = ""
                    if _gemini and alert_type in ("pump", "gem", "whale"):
                        await asyncio.sleep(2)  # Gemini rate limit buffer (15 req/min)
                        ai_take = await gemini_ask(
                            f"One sentence: is this {alert_type} on ${symbol} (MCap {fmt_usd(fdv)}, 5m {fmt_pct(ch_5m)}) worth acting on? Be direct.",
                            fallback=""
                        )
                    full_alert = f"{alert}\n`{address}`"
                    if ai_take:
                        full_alert += f"\n\n🧠 Kayo AI: {ai_take}"
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=full_alert,
                            parse_mode="Markdown",
                            reply_markup=get_chart_buttons(address, symbol)
                        )
                    except Exception as e:
                        logger.warning(f"DEX alert send: {e}")

            # Clean old baseline entries
            if len(dex_baseline) > 500:
                keys = list(dex_baseline.keys())
                for k in keys[:100]:
                    del dex_baseline[k]

        except Exception as e:
            logger.error(f"bg_dex_fast_scanner: {e}")
        await asyncio.sleep(30)


# ── Background: PumpFun Scanner (every 25s) ───────────────────
async def bg_pumpfun_scanner(app: Application):
    """Scan pump.fun every 25s for new launches and graduating tokens."""
    await asyncio.sleep(45)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                new_tokens, graduating = await asyncio.gather(
                    pumpfun_new_tokens(session, 15),
                    pumpfun_graduating(session)
                )

            # Alert new launches
            for t in new_tokens:
                addr  = t.get("mint", "")
                sym   = t.get("symbol", "???")
                name  = t.get("name", "?")
                mcap  = t.get("market_cap", 0)
                reply = t.get("reply_count", 0)
                king  = t.get("king_of_the_hill_timestamp")

                if not addr or addr in pump_seen:
                    continue
                pump_seen.add(addr)

                # Only alert if it has some traction
                if mcap < 5000 and reply < 3 and not king:
                    continue

                if GROUP_CHAT_ID == 0:
                    continue

                king_tag = " 👑 KING OF HILL" if king else ""
                msg = (f"🆕 **NEW PUMP.FUN LAUNCH{king_tag}**\n"
                       f"**${sym}** — {name}\n"
                       f"MCap: {fmt_usd(mcap)} | 💬 {reply} replies\n"
                       f"`{addr}`\n"
                       f"/scan {addr}")
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=msg,
                        parse_mode="Markdown",
                        reply_markup=get_chart_buttons(addr, sym)
                    )
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.warning(f"PumpFun alert: {e}")

            # Alert graduating tokens
            for t in graduating:
                addr  = t.get("mint", "")
                sym   = t.get("symbol", "???")
                mcap  = t.get("market_cap", 0)
                pct   = min(100, mcap / 69000 * 100)

                grad_key = f"grad_{addr}"
                if addr in pump_seen or grad_key in gem_alerts_sent:
                    continue
                if pct < 80:
                    continue

                gem_alerts_sent.add(grad_key)
                if GROUP_CHAT_ID == 0:
                    continue

                bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
                msg = (f"🎓 **GRADUATING SOON — {pct:.0f}%**\n"
                       f"**${sym}** is about to hit DEX!\n"
                       f"[{bar}]\n"
                       f"MCap: {fmt_usd(mcap)} / 69k\n"
                       f"`{addr}`")
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=msg,
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.warning(f"Graduating alert: {e}")

            # Trim pump_seen
            if len(pump_seen) > 2000:
                old = list(pump_seen)[:500]
                for o in old:
                    pump_seen.discard(o)

        except Exception as e:
            logger.error(f"bg_pumpfun_scanner: {e}")
        await asyncio.sleep(25)


# ── Background: CryptoPanic News (every 3min) ────────────────
async def bg_news_scanner(app: Application):
    """Refresh internal news cache every 3 minutes. NO auto-posting — use /cryptonews manually."""
    global news_cache, last_news_fetch
    await asyncio.sleep(30)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                articles = await fetch_cryptopanic(session)
            if articles:
                news_cache      = articles
                last_news_fetch = __import__("time").time()
                logger.debug(f"News cache refreshed: {len(articles)} articles")
        except Exception as e:
            logger.error(f"bg_news_scanner: {e}")
        await asyncio.sleep(180)


async def bg_price_alert_checker(app: Application):
    """Check user price alerts every 30s and notify when triggered."""
    await asyncio.sleep(90)
    while True:
        try:
            triggered = []
            async with aiohttp.ClientSession() as session:
                for uid, alerts in list(price_alerts.items()):
                    remaining = []
                    for a in alerts:
                        pair = await dex_token(session, a["address"])
                        if not pair:
                            remaining.append(a)
                            continue
                        price = float(pair.get("priceUsd", 0) or 0)
                        hit = (a["direction"] == "above" and price >= a["target"]) or                               (a["direction"] == "below" and price <= a["target"])
                        if hit:
                            triggered.append((uid, a, price))
                        else:
                            remaining.append(a)
                    price_alerts[uid] = remaining

            for uid, a, price in triggered:
                try:
                    await app.bot.send_message(
                        chat_id=a["chat_id"],
                        text=f"🔔 **PRICE ALERT TRIGGERED**\n\n**${a['symbol']}** is now {fmt_price(price)}\nYour target: {a['direction']} {fmt_price(a['target'])}\n\n`/scan {a['address']}`",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.warning(f"Price alert notify: {e}")
            if triggered:
                save_state()

        except Exception as e:
            logger.error(f"bg_price_alert_checker: {e}")
        await asyncio.sleep(30)

async def bg_twitter_scanner(app: Application):
    """General Twitter scan for CA drops (not watchlist-specific)."""
    global seen_news
    await asyncio.sleep(90)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                tweets = []
                for q in ["solana new token ca drop", "alpha call solana", "gem solana"]:
                    batch = await scrape_nitter(session, q, limit=5)
                    tweets.extend(batch)
                    await asyncio.sleep(1)
            for tw in tweets:
                text = tw.get("text","")
                tid  = hashlib.md5(text.encode()).hexdigest()
                if tid in seen_news: continue
                seen_news.add(tid)
                if len(seen_news) > 5000: seen_news = set(list(seen_news)[-2000:])
                for kw in ['ai agent','rwa','defi','gaming','meme season','pump incoming','bullish solana']:
                    if kw in text.lower():
                        entry = f"{kw.title()} trending ({datetime.utcnow().strftime('%H:%M')})"
                        if entry not in kayo_knowledge:
                            kayo_knowledge.append(entry)
                            if len(kayo_knowledge) > 100: kayo_knowledge.pop(0)
                cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
                if cas and GROUP_CHAT_ID != 0:
                    ca   = cas[0]
                    user = tw.get("user","unknown")
                    # skip if already caught by watchlist
                    if user.lower() in watchlist: continue
                    snippet = text[:150].replace('\n',' ')
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=f"🚨 **TWITTER CA DROP**\n👤 @{user}\n📝 {snippet}\n\n📌 CA: `{ca}`\n\n`/scan {ca}`",
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.warning(f"Twitter alert: {e}")
        except Exception as e:
            logger.error(f"Twitter scanner: {e}")
        await asyncio.sleep(45)

async def bg_new_token_scanner(app: Application):
    """Scan DexScreener every 30s for new Solana tokens. Only posts tokens with good AI score."""
    await asyncio.sleep(120)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                pairs   = await dex_search(session, "solana")
                boosted = await dex_boosted(session)
            for p in pairs[:60] + boosted[:20]:
                base    = p.get("baseToken",{}) if "baseToken" in p else {}
                address = base.get("address", p.get("tokenAddress",""))
                if not address or address in seen_tokens: continue
                fdv  = float(p.get("fdv",0) or 0)
                liq  = float(p.get("liquidity",{}).get("usd",0) or 0)
                ch_5m= float(p.get("priceChange",{}).get("m5",0) or 0)
                ch_1h= float(p.get("priceChange",{}).get("h1",0) or 0)
                vol_5m = float(p.get("volume",{}).get("m5",0) or 0)
                vol_1h = float(p.get("volume",{}).get("h1",0) or 0)
                buys = int(p.get("txns",{}).get("h1",{}).get("buys",0) or 0)
                sells= int(p.get("txns",{}).get("h1",{}).get("sells",0) or 0)
                sym  = base.get("symbol", p.get("symbol","???"))
                name = base.get("name", sym)
                seen_tokens.add(address)
                # Basic quality filters — skip trash
                if liq < 5000 or fdv > 5_000_000: continue
                if fdv < 10_000: continue
                if vol_5m < 200 and buys < 5: continue
                if buys < sells: continue  # net selling — skip
                if GROUP_CHAT_ID == 0: continue
                # Security check
                async with aiohttp.ClientSession() as s2:
                    sec = await goplus_sec(s2, address)
                if sec.get("is_honeypot") == "1": continue
                sell_tax = float(sec.get("sell_tax", 0) or 0)
                if sell_tax > 15: continue  # too high tax
                # Narrative detection from name/symbol
                text = f"{name} {sym}".lower()
                narrative = "Meme"
                if any(w in text for w in ["ai","agent","gpt","neural","mind"]): narrative = "AI"
                elif any(w in text for w in ["game","play","gaming","nft","quest"]): narrative = "Gaming"
                elif any(w in text for w in ["defi","swap","yield","lend","farm"]): narrative = "DeFi"
                elif any(w in text for w in ["rwa","real","estate","asset"]): narrative = "RWA"
                elif any(w in text for w in ["dog","cat","pepe","frog","ape","shib"]): narrative = "Meme"
                # Volume ratio
                vol_ratio = vol_5m / max(vol_1h/12, 1) if vol_1h > 0 else 1
                safety = "🟢 LP Locked" if sec.get("lp_locked") == "1" else "🟡 Check LP"
                # AI opinion — only call if looks promising
                ai_opinion = ""
                if _gemini and ch_1h > 10 and buys > 10:
                    ai_opinion = await gemini_ask(
                        f"New Solana token: ${sym} ({narrative}). MCap {fmt_usd(fdv)}, Liq {fmt_usd(liq)}, +{ch_1h:.0f}% 1h, {buys}B/{sells}S buys. Vol spike {vol_ratio:.1f}x. One sentence: worth watching or skip?",
                        fallback=""
                    )
                    await asyncio.sleep(2)  # rate limit buffer
                msg = (
                    f"🆕 **NEW TOKEN ALERT**\n"
                    f"**${sym}** — {name}\n"
                    f"🏷️ Narrative: {narrative}\n"
                    f"💰 MCap: {fmt_usd(fdv)} | 💧 Liq: {fmt_usd(liq)}\n"
                    f"📈 5m: {fmt_pct(ch_5m)} | 1h: {fmt_pct(ch_1h)}\n"
                    f"⚡ Vol spike: {vol_ratio:.1f}x | 💚{buys} / 🔴{sells}\n"
                    f"🛡️ {safety} | Tax: {sell_tax:.0f}%\n"
                    f"`{address}`"
                )
                if ai_opinion:
                    msg += f"\n\n🧠 Kayo: {ai_opinion}"
                try:
                    await app.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=msg,
                        parse_mode="Markdown",
                        reply_markup=get_chart_buttons(address, sym)
                    )
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.warning(f"New token alert: {e}")
        except Exception as e:
            logger.error(f"Token scanner: {e}")
        await asyncio.sleep(30)


async def bg_unusual_activity(app: Application):
    baseline: Dict[str, Dict] = {}
    alerted:  Dict[str, float] = {}   # address -> last alert timestamp
    await asyncio.sleep(150)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                pairs = await dex_search(session, "solana")
            now = __import__("time").time()
            for p in pairs[:80]:
                base    = p.get("baseToken",{})
                address = base.get("address","")
                symbol  = base.get("symbol","???")
                ch_5m   = float(p.get("priceChange",{}).get("m5",0) or 0)
                vol_5m  = float(p.get("volume",{}).get("m5",0) or 0)
                vol_1h  = float(p.get("volume",{}).get("h1",0) or 0)
                liq     = float(p.get("liquidity",{}).get("usd",0) or 0)
                fdv     = float(p.get("fdv",0) or 0)
                if liq < 2000 or not address: continue
                # 30-minute cooldown per token to avoid spam
                if now - alerted.get(address, 0) < 1800: continue
                vr   = vol_5m / max(vol_1h/12,1) if vol_1h > 0 else 0
                prev = baseline.get(address,{})
                baseline[address] = {"ch_5m": ch_5m, "vr": vr}
                if not prev: continue
                alert = None
                if ch_5m > 15 and vr > 3:
                    alert = f"🚀 **PUMP ALERT** — ${symbol} +{ch_5m:.0f}% in 5m | {vr:.1f}x volume spike"
                elif ch_5m < -15 and vr > 3:
                    alert = f"💀 **DUMP ALERT** — ${symbol} {ch_5m:.0f}% in 5m | {vr:.1f}x sell pressure"
                elif vr > 5 and abs(ch_5m) < 3:
                    alert = f"🐳 **WHALE LOADING** — ${symbol} {vr:.1f}x normal volume, price flat — accumulation?"
                if alert and GROUP_CHAT_ID != 0:
                    alerted[address] = now
                    ai_take = ""
                    if _gemini:
                        ai_take = await gemini_ask(
                            f"One sentence: ${symbol} MCap {fmt_usd(fdv)}, {ch_5m:+.0f}% 5m, {vr:.1f}x volume. Worth watching?",
                            fallback=""
                        )
                    msg = f"{alert}\nMCap: {fmt_usd(fdv)} | Liq: {fmt_usd(liq)}\n`{address}`"
                    if ai_take:
                        msg += f"\n\n🧠 Kayo: {ai_take}"
                    try:
                        await app.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=msg,
                            parse_mode="Markdown",
                            reply_markup=get_chart_buttons(address, symbol)
                        )
                    except Exception as e:
                        logger.warning(f"Activity alert: {e}")
        except Exception as e:
            logger.error(f"Activity scanner: {e}")
        await asyncio.sleep(120)


async def bg_wallet_tracker(app: Application):
    wallet_last_seen: Dict[str, str] = {}
    await asyncio.sleep(180)
    while True:
        try:
            for wallet, chat_id in list(tracked_wallets.items()):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"https://api.mainnet-beta.solana.com",
                            timeout=aiohttp.ClientTimeout(total=8)
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                sig  = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
                                prev = wallet_last_seen.get(wallet)
                                wallet_last_seen[wallet] = sig
                                if prev and prev != sig:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=f"👛 **WALLET ACTIVITY**\n`{wallet[:12]}...{wallet[-6:]}`\n\nNew transaction detected!\n[View on Solscan](https://solscan.io/account/{wallet})",
                                        parse_mode="Markdown"
                                    )
                except: pass
        except Exception as e:
            logger.error(f"Wallet tracker: {e}")
        await asyncio.sleep(60)

# ── Bot setup ─────────────────────────────────────────────────
async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start",         "🏠 Welcome & all commands"),
        BotCommand("scan",          "🔍 Full scan + opinion"),
        BotCommand("smartscan",     "🎯 Best coins NOW"),
        BotCommand("runners",       "🏃 Today's runners"),
        BotCommand("momentum",      "⚡ Momentum spikes"),
        BotCommand("verify",        "🛡️ Rug check"),
        BotCommand("chart",         "📊 DEX chart inside Telegram"),
        BotCommand("watch",         "👁️ Watch account for CA drops"),
        BotCommand("unwatch",       "❌ Stop watching account"),
        BotCommand("watchlist",     "📋 See watched accounts"),
        BotCommand("news",          "📰 Twitter alpha"),
        BotCommand("trending",      "🔥 Hot narratives"),
        BotCommand("tt",            "🐦 Trending tweets"),
        BotCommand("moni",          "👤 Scan any Twitter account"),
        BotCommand("insiders",      "🧠 Insider accounts"),
        BotCommand("copy",          "📋 Copy trade from account"),
        BotCommand("twittersearch", "🔍 Twitter sentiment"),
        BotCommand("narrative",     "🔮 Find coins by narrative"),
        BotCommand("learn",         "🧠 Force Kayo to learn"),
        BotCommand("mystats",       "📊 Your stats + Kayo brain"),
        BotCommand("strategies",    "📈 Strategy win rates"),
        BotCommand("record",        "📝 Teach Kayo from trades"),
        BotCommand("call",          "📞 Register a call"),
        BotCommand("mycalls",       "📊 Your calls + live P&L"),
        BotCommand("stop",          "🔒 Close a call"),
        BotCommand("leaderboard",   "🏆 Top traders"),
        BotCommand("w",             "👛 Wallet overview"),
        BotCommand("trackwallet",   "👀 Track wallet activity"),
        BotCommand("mywallet",      "👛 Set your wallet"),
        BotCommand("walletpnl",     "📊 Your trade P&L"),
        BotCommand("untrackwallet", "❌ Stop tracking wallet"),
        BotCommand("a",             "🪙 CoinGecko price"),
        BotCommand("macro",         "🌍 Global market"),
        BotCommand("index",         "📊 Top 10 by MCap"),
        BotCommand("dt",            "🔥 Trending DEX"),
        BotCommand("x",             "⚡ Quick token query"),
        BotCommand("z",             "⚡ Ultra quick price"),
        BotCommand("p",             "💰 Simple price"),
        BotCommand("s",             "🔍 Search token"),
        BotCommand("gp",            "🏆 Group points"),
        BotCommand("rank",          "⭐ Your XP & rank"),
        BotCommand("remindme",      "⏰ Set a reminder"),
        BotCommand("tz",            "🕐 World timezones"),
        BotCommand("status",        "✅ Bot status"),
        BotCommand("settings",      "⚙️ Settings"),
        BotCommand("buttons",       "🔘 Toggle chart buttons"),
        BotCommand("autoresponder", "🤖 Toggle auto CA scan"),
        BotCommand("help",          "❓ All commands"),
    ])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    logger.info("=" * 60)
    logger.info("🦅 KAYO BRAIN v13.0 - WATCHLIST EDITION")
    logger.info(f"📢 Group: {'ENABLED (' + str(GROUP_CHAT_ID) + ')' if GROUP_CHAT_ID != 0 else 'DISABLED — set GROUP_CHAT_ID'}")
    logger.info("=" * 60)

def main():
    try:
        __import__("urllib.request", fromlist=["urlopen"]).urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=5)
    except: pass

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    for name, fn in [
        ("start", start), ("help", help_cmd),
        ("scan", scan_cmd), ("smartscan", smartscan_cmd),
        ("runners", runners_cmd), ("momentum", momentum_cmd), ("verify", verify_cmd),
        ("chart", chart_cmd), ("dex", dex_cmd), ("c", c_cmd),
        ("watch", watch_cmd), ("unwatch", unwatch_cmd), ("watchlist", watchlist_cmd),
        ("news", news_cmd), ("trending", trending_cmd), ("tt", tt_cmd),
        ("twittersearch", twittersearch_cmd), ("moni", moni_cmd),
        ("insiders", insiders_cmd), ("copy", copy_cmd),
        ("narrative", narrative_cmd), ("learn", learn_cmd),
        ("mystats", mystats_cmd), ("strategies", strategies_cmd), ("record", record_cmd),
        ("a", a_cmd), ("macro", macro_cmd), ("index", index_cmd),
        ("markets", markets_cmd), ("dt", dt_cmd),
        ("x", x_cmd), ("z", z_cmd), ("p", p_cmd), ("s", s_cmd),
        ("call", call_cmd), ("mycalls", mycalls_cmd),
        ("stop", stop_cmd), ("leaderboard", leaderboard_cmd),
        ("w", w_cmd), ("trackwallet", trackwallet_cmd),
        ("mywallet", mywallet_cmd), ("walletpnl", walletpnl_cmd),
        ("untrackwallet", untrackwallet_cmd),
        ("gp", gp_cmd), ("ping", ping_cmd), ("dubs", dubs_cmd),
        ("rank", rank_cmd), ("remindme", remindme_cmd),
        ("tz", tz_cmd), ("status", status_cmd),
        ("settings", settings_cmd), ("buttons", buttons_cmd),
        ("autoresponder", autoresponder_cmd),
        # New commands
        ("ask",         ask_cmd),
        ("cryptonews",  cryptonews_cmd),
        ("pump",        pump_cmd),
        ("graduating",  graduating_cmd),
        ("alert",       alert_cmd),
        ("myalerts",    myalerts_cmd),
        ("delalert",    delalert_cmd),
        ("addport",     addport_cmd),
        ("portfolio",   portfolio_cmd),
        ("blacklist",   blacklist_cmd),
        ("sentiment",   sentiment_cmd),
        ("gems",        gems_cmd),
        ("gsum",        gsum_cmd),
    ]:
        app.add_handler(CommandHandler(name, fn))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def run():
        async with app:
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            logger.info("🚀 Kayo Brain v13 polling started")
            asyncio.create_task(bg_reminder_checker(app))
            asyncio.create_task(bg_watchlist_scanner(app))
            asyncio.create_task(bg_twitter_scanner(app))
            asyncio.create_task(bg_new_token_scanner(app))
            asyncio.create_task(bg_unusual_activity(app))
            asyncio.create_task(bg_wallet_tracker(app))
            # New high-speed scanners
            asyncio.create_task(bg_dex_fast_scanner(app))
            asyncio.create_task(bg_pumpfun_scanner(app))
            asyncio.create_task(bg_news_scanner(app))
            asyncio.create_task(bg_price_alert_checker(app))
            while True:
                await asyncio.sleep(3600)

    asyncio.run(run())

if __name__ == "__main__":
    main()
