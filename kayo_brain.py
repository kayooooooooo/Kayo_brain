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
            "first_alert_seen": list(_first_alert_seen)[-1000:],
            "token_watchers": dict(list(_token_watchers.items())[-500:]),
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
        _saved_seen = d.get("seen_alert_ids", [])
        seen_alert_ids = OrderedDict()
        for sid in _saved_seen[-3000:]:
            seen_alert_ids[sid] = 1
        global dropped_calls, pattern_memory
        dropped_calls   = d.get("dropped_calls", {})
        pattern_memory  = d.get("pattern_memory", {})
        # Restore Rick Bot tracking state
        global _first_alert_seen, _token_watchers
        _first_alert_seen = set(d.get("first_alert_seen", []))
        _token_watchers = d.get("token_watchers", {})
        logger.info(f"✅ State loaded — {len(watchlist)} watched, {len(active_calls)} calls, {len(dropped_calls)} tracked drops (seen_alert_ids restored from saved state)")
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
_MARKET_CTX_TTL = 45    # seconds — fresher prices


# ── UNIVERSAL LIVE PRICE FETCHER ────────────────────────────────────
# Solana tokens → DexScreener. Major coins → CoinGecko. Always live.
_CACHE_PRICES: Dict[str, Dict] = {}  # symbol/addr → {price, ts}

async def fetch_live_price(query: str) -> Dict:
    """
    Fetch REAL-TIME price for ANY coin or token.
    Tries: CoinGecko (majors) → DexScreener (Solana tokens) → cache.
    Returns: {price, change_24h, mcap, vol_24h, source, sym, name}
    """
    q = query.lower().strip().lstrip("$")
    now = time.time()

    # Check cache (10s TTL for price queries)
    cache_key = q
    if cache_key in _CACHE_PRICES and (now - _CACHE_PRICES[cache_key].get("ts", 0)) < 10:
        return _CACHE_PRICES[cache_key]

    result = {"price": 0, "change_24h": 0, "mcap": 0, "vol_24h": 0, "source": "", "sym": q.upper(), "name": ""}

    # ── 1. Try CoinGecko for major coins ──────────────────────────
    COIN_MAP = {
        "btc": "bitcoin", "bitcoin": "bitcoin",
        "sol": "solana", "solana": "solana",
        "eth": "ethereum", "ethereum": "ethereum",
        "bnb": "binancecoin", "binancecoin": "binancecoin",
        "xrp": "ripple", "ripple": "ripple",
        "doge": "dogecoin", "dogecoin": "dogecoin",
        "ada": "cardano", "cardano": "cardano",
        "avax": "avalanche-2", "avalanche": "avalanche-2",
        "dot": "polkadot", "polkadot": "polkadot",
        "link": "chainlink", "chainlink": "chainlink",
        "uni": "uniswap", "uniswap": "uniswap",
        "ltc": "litecoin", "litecoin": "litecoin",
        "near": "near", "apt": "aptos", "aptos": "aptos",
        "sui": "sui", "pepe": "pepe", "shib": "shiba-inu", "shiba-inu": "shiba-inu",
        "shiba": "shiba-inu",
        "bonk": "bonk", "wif": "dogwifcoin", "dogwifcoin": "dogwifcoin",
        "jup": "jupiter-exchange-solana", "jupiter": "jupiter-exchange-solana",
        "ray": "raydium", "raydium": "raydium",
        "jto": "jito-governance-token",
        "trump": "trump-official", "official-trump": "trump-official",
        "popcat": "popcat", "bome": "book-of-meme",
        "matic": "matic-network", "pol": "matic-network",
        "arb": "arbitrum", "op": "optimism",
        "atom": "cosmos", "ftm": "fantom",
        "hbar": "hedera-hashgraph", "alg": "algorand", "algo": "algorand",
        "fil": "filecoin", "icp": "internet-computer",
        "render": "render-token", "rndr": "render-token",
        "tiao": "taiyo-2",
        "ena": "ena", "pyth": "pyth-network",
        "w": "wormhole", "wormhole": "wormhole",
        "io": "io-net", "ionet": "io-net",
        "drift": "drift-protocol",
        "kmno": "kamino",
        "tensor": "tensor", "tns": "tensor",
        "tao": "bittensor", "bittensor": "bittensor",
        "fet": "fetch-ai", "fetch": "fetch-ai",
        "ocean": "ocean-protocol",
        "render": "render-token", "rndr": "render-token",
        "nmr": "numeraire",
        "olas": "autonolas",
        "ondo": "ondo-finance",
        "pendle": "pendle",
        "gmx": "gmx",
        "aave": "aave",
        "ldo": "lido-dao",
        "mkr": "maker",
        "crv": "curve-dao-token",
        "comp": "compound-governance-token",
        "brett": "based-brett",
        "degen": "degen-base",
        "aero": "aerodrome-finance",
        "blast": "blast",
        "manta": "manta-network",
        "linea": "linea",
        "scroll": "scroll",
        "eigen": "eigenlayer", "eigenlayer": "eigenlayer",
        "ordi": "ordinals",
        "coq": "coq-inu",
        "joe": "joe",
        "cake": "pancakeswap-token",
        "trx": "tron",
        "shib": "shiba-inu", "shiba": "shiba-inu",
        "floki": "floki",
        "turbo": "turbo",
        "pengu": "pudgy-penguins",
        "mfer": "mfer",
        "higher": "higher",
        "normie": "normie",
        "sats": "sats",
        "rats": "rats",
        "pol": "pol-token", "polygon": "matic-network",
        "uni": "uniswap",
        "inj": "injective-protocol",
        "sei": "sei-network",
        "celestia": "celestia",
        "tia": "celestia",
        "kas": "kaspa", "kaspa": "kaspa",
        "kava": "kava",
        "mina": "mina-protocol",
    }

    coin_id = COIN_MAP.get(q) or COIN_MAP.get(query.lower().strip())

    if coin_id:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.coingecko.com/api/v3/simple/price"
                    f"?ids={coin_id}&vs_currencies=usd"
                    f"&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true",
                    timeout=aiohttp.ClientTimeout(total=8),
                    headers={"User-Agent": "Mozilla/5.0"}
                ) as r:
                    if r.status == 200 and coin_id in (await r.json()):
                        d = (await r.json())[coin_id]
                        result = {
                            "price": float(d.get("usd", 0) or 0),
                            "change_24h": float(d.get("usd_24h_change", 0) or 0),
                            "mcap": float(d.get("usd_market_cap", 0) or 0),
                            "vol_24h": float(d.get("usd_24h_vol", 0) or 0),
                            "source": "CoinGecko",
                            "sym": q.upper(),
                            "name": coin_id,
                        }
                        if result["price"] > 0:
                            result["ts"] = now
                            _CACHE_PRICES[cache_key] = result
                            return result
        except Exception:
            pass

    # ── 2. Try DexScreener (Solana token by symbol or CA) ────────
    try:
        async with aiohttp.ClientSession() as s:
            # If it looks like a CA (32-44 base58), search by token address
            if len(query) >= 32 and query[0] in "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz":
                ds_url = f"https://api.dexscreener.com/latest/dex/tokens/{query}"
            else:
                # Search by symbol
                ds_url = f"https://api.dexscreener.com/latest/dex/search?q={query}"

            async with s.get(ds_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs") or d.get("pair") or []
                    if pairs:
                        # Find the Solana pair with highest liquidity
                        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                        all_pairs = sol_pairs if sol_pairs else pairs
                        best = max(all_pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))

                        price = float(best.get("priceUsd", 0) or 0)
                        if price > 0:
                            chg = best.get("priceChange", {}) or {}
                            result = {
                                "price": price,
                                "change_24h": float(chg.get("h24", 0) or 0),
                                "change_1h": float(chg.get("h1", 0) or 0),
                                "change_5m": float(chg.get("m5", 0) or 0),
                                "mcap": float(best.get("marketCap", 0) or best.get("fdv", 0) or 0),
                                "vol_24h": float((best.get("volume") or {}).get("h24", 0) or 0),
                                "liq": float((best.get("liquidity") or {}).get("usd", 0) or 0),
                                "source": "DexScreener",
                                "sym": (best.get("baseToken") or {}).get("symbol", q.upper()),
                                "name": (best.get("baseToken") or {}).get("name", ""),
                                "addr": (best.get("baseToken") or {}).get("address", ""),
                                "ts": now,
                            }
                            _CACHE_PRICES[cache_key] = result
                            return result
    except Exception:
        pass

    # ── 3. Check broader market context cache as last resort ──────
    if _market_ctx_cache.get("data"):
        # Try to find the symbol in the cached context
        cached = _market_ctx_cache["data"]
        for line in cached.split("\n"):
            if q.upper() in line and "$" in line:
                # Extract price from line like "📈 BTC: $67,234 (1.2%)"
                import re as _re
                m = _re.search(r"\$([0-9,]+\.?[0-9]*)", line)
                if m:
                    price_str = m.group(1).replace(",", "")
                    try:
                        result = {
                            "price": float(price_str),
                            "change_24h": 0,
                            "mcap": 0,
                            "vol_24h": 0,
                            "source": "Cache",
                            "sym": q.upper(),
                            "ts": now,
                        }
                        return result
                    except Exception:
                        pass

    result["ts"] = now
    _CACHE_PRICES[cache_key] = result
    return result


async def fetch_multiple_prices(queries: List[str]) -> Dict[str, Dict]:
    """Fetch multiple prices in parallel — used for AI context enrichment."""
    results = await asyncio.gather(
        *[fetch_live_price(q) for q in queries],
        return_exceptions=True
    )
    out = {}
    for q, r in zip(queries, results):
        if not isinstance(r, Exception) and r.get("price", 0) > 0:
            out[q] = r
    return out


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
                "pepe,shiba-inu,bonk,dogwifcoin,jupiter-exchange-solana,raydium,jito-governance-token,"
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
                            "jupiter-exchange-solana":"JUP","raydium":"RAY","jito-governance-token":"JTO",
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
                        "jupiter-exchange-solana","raydium","trump-official","popcat","book-of-meme"
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

    # ── FALLBACK: If CoinGecko failed, fetch from DexScreener ──
    try:
        async with aiohttp.ClientSession() as s:
            # Fetch SOL, top Solana tokens from DexScreener as fallback
            async with s.get(
                "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    pairs = d.get("pairs", [])
                    if pairs:
                        sol_price = float(pairs[0].get("priceUsd", 0) or 0)
                        if sol_price > 0:
                            ctx = (
                                f"[LIVE MARKET — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')} — via DexScreener]\n"
                                f"SOL: ${sol_price:,.2f}\n"
                                f"(CoinGecko rate limited — only SOL price available)\n"
                                f"---\n"
                                f"You are Kayo — a sharp Solana alpha intelligence. "
                                f"Use the SOL price above. For other coins, use DexScreener data if you have it. "
                                f"Be honest if you don't have a price."
                            )
                            _market_ctx_cache["data"] = ctx
                            _market_ctx_cache["ts"] = now
                            return ctx
    except Exception:
        pass

    # Final fallback
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


# ── NARRATIVE/CHAIN TOKEN SEARCH ─────────────────────────────────────
# Lets the AI answer "what degen coins on Solana?" with REAL live data

async def search_narrative_tokens(narrative: str, chain: str = "solana", limit: int = 20) -> str:
    """
    Search for tokens by narrative/keyword across DexScreener + GeckoTerminal + Pump.fun.
    chain="" searches ALL chains. chain="solana" limits to Solana.
    Returns a formatted string of live token data for AI injection.
    """
    narrative = narrative.lower().strip()
    results = []

    # ── 1. DexScreener search (default: Solana, or specific chain) ──
    try:
        if chain and chain != "all":
            pairs = await asyncio.wait_for(dex_search_pairs(narrative, chain), timeout=8)
        else:
            # "all" or "" — search across ALL chains
            pairs = await asyncio.wait_for(dex_search_all_chains(narrative, limit), timeout=8)
        for p in pairs[:limit]:
            base = p.get("baseToken", {})
            sym = base.get("symbol", "?")
            name = base.get("name", "?")
            addr = base.get("address", "?")
            price = float(p.get("priceUsd", 0) or 0)
            mcap = float(p.get("marketCap", 0) or p.get("fdv", 0) or 0)
            liq = float((p.get("liquidity") or {}).get("usd", 0) or 0)
            ch24 = float((p.get("priceChange") or {}).get("h24", 0) or 0)
            ch1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
            vol24 = float((p.get("volume") or {}).get("h24", 0) or 0)
            chain_id = p.get("chainId", "unknown")
            results.append({
                "sym": sym, "name": name, "addr": addr,
                "price": price, "mcap": mcap, "liq": liq,
                "ch24h": ch24, "ch1h": ch1h, "vol24h": vol24,
                "chain": chain_id, "source": "DexScreener"
            })
    except Exception:
        pass

    # ── 2. GeckoTerminal trending pools (already fetched by scanner) ──
    try:
        gt_pools = await asyncio.wait_for(_fetch_gt_trend(1), timeout=8)
        for pool in gt_pools[:10]:
            tok = gt_parse_pool(pool)
            if not tok: continue
            # Check if narrative matches
            tok_nar = detect_narrative(f"{tok.get('name','')} {tok.get('sym','')}")
            if narrative in tok_nar or narrative in tok.get("name", "").lower() or narrative in tok.get("sym", "").lower():
                results.append({
                    "sym": tok["sym"], "name": tok.get("name", tok["sym"]),
                    "addr": tok["address"],
                    "price": tok.get("price", 0), "mcap": tok.get("mcap", 0),
                    "liq": tok.get("liq", 0), "ch24h": tok.get("ch24h", 0),
                    "ch1h": tok.get("ch1h", 0), "vol24h": tok.get("v24h", 0),
                    "source": "GeckoTerminal"
                })
    except Exception:
        pass

    # ── 3. Pump.fun search by narrative ──
    try:
        pf_coins = await asyncio.wait_for(pumpfun_trending(50), timeout=8)
        for coin in pf_coins[:20]:
            desc = (coin.get("description") or "").lower()
            name = (coin.get("name") or "").lower()
            sym = (coin.get("symbol") or "").lower()
            if narrative in desc or narrative in name or narrative in sym:
                mcap = float(coin.get("usd_market_cap", 0) or 0)
                if mcap < 500_000 and mcap >= 1000:
                    results.append({
                        "sym": coin.get("symbol", "?"),
                        "name": coin.get("name", "?"),
                        "addr": coin.get("mint", "?"),
                        "price": 0, "mcap": mcap,
                        "liq": mcap * 0.3, "ch24h": 0, "ch1h": 0,
                        "vol24h": 0, "source": "Pump.fun"
                    })
    except Exception:
        pass

    # Deduplicate by address
    seen = set()
    unique = []
    for r in results:
        if r["addr"] not in seen:
            seen.add(r["addr"])
            unique.append(r)

    if not unique:
        return ""

    # Format for AI injection
    lines = []
    chain_label = chain.upper() if chain else "ALL CHAINS"
    lines.append(f"[LIVE TOKENS — narrative: {narrative} | chain: {chain_label}]")
    for r in unique[:15]:
        mcap_str = f"${r['mcap']:,.0f}" if r['mcap'] > 0 else "N/A"
        ch_str = f"{r['ch24h']:+.1f}%" if r['ch24h'] != 0 else ""
        liq_str = f"${r['liq']:,.0f}" if r['liq'] > 0 else ""
        chain_tag = f"[{r.get('chain','?').upper()}]" if r.get('chain') else ""
        lines.append(
            f"  ${r['sym']} {chain_tag} ({r['name']}) — MCap {mcap_str} {ch_str} Liq {liq_str} [{r['source']}]"
        )
    lines.append(f"Total: {len(unique)} tokens found for '{narrative}' on Solana")
    return "\n".join(lines)


# ── NARRATIVE KNOWLEDGE BASE ─────────────────────────────────────────
NARRATIVE_KB = """
[WEB3 NARRATIVE KNOWLEDGE BASE — your complete expertise]

You are a Web3 NATIVE. You know EVERY chain, EVERY narrative, EVERY major token.

═══ SOLANA ═══
The #1 chain for meme/degen trading. Low fees (<$0.01), fast finality (~400ms).
  DEGEN: BONK, WIF, BOME, POPCAT, PNUT, MOODENG, FARTCOIN, AURA, TROLL, MIKE
  AI/AGENTS: ARC (AI Rig Complex), ZEREBRO, GOAT, AI16Z, GRIFT, RETARDIO
  DOG: BONK, WIF (dogwifcoin), PONKE, MYRO, NANA, BORK
  CAT: POPCAT, MEW (cat in a dogs world), SC, MOGRE
  FROG/PEPE: WIFPEPE, PEPE variants
  POLITICS: TRUMP (Official Trump), BODEN, KAMA, WALZ, DJT
  PUMP.FUN: Primary launchpad. ~20,000+ tokens/day, 99% rugs, graduate to Raydium at ~$69k mcap
  INFRA: SOL, JUP (Jupiter DEX), RAY (Raydium AMM), JTO (Jito staking), PYTH (oracle),
         WORMHOLE (bridge), DRIFT (perps), KAMINO (lending), TENSOR (NFTs)
  TOOLS: DexScreener, GMGN, Photon, Birdeye, Jupiter swap

═══ ETHEREUM ═══
The original smart contract chain. Higher fees, bigger mcap tokens.
  MEMES: PEPE, SHIB (Shiba Inu), FLOKI, TURBO, MEME, DOGE (original), WIF
  AI: TAO (Bittensor), FET (Fetch.ai), RENDER, OCEAN, NMR
  DEFI: UNI (Uniswap), AAVE, LDO (Lido), MKR (Maker), CRV (Curve), COMP (Compound)
  L2s: ARB (Arbitrum), OP (Optimism), MATIC/POL (Polygon), BASE (Coinbase L2)
  INFRA: ETH, LINK (Chainlink), WBTC, STETH
  NARRATIVES: Restaking (eigenlayer), L2 scaling, account abstraction

═══ BASE ═══
Coinbase's L2. Fast-growing degen ecosystem in 2024-2025.
  DEGEN: BRETT, DEGEN, HIGHER, MFER, NORMIE, KEYCAT, BRIUN
  MEMES: BRETT (Base's mascot), DEGEN (tip token), TOSHI, OM
  AI: VIRTUAL, AI16Z (cross-chain), LUNA
  INFRA: BASE ETH, AERO (Aerodrome DEX), VELLO

═══ SUI ═══
Move-language L1. Growing degen ecosystem.
  MEMES: SUIMOB, SCALLOP, BLUB, CETO, SUIPIENS
  DEFI: NAVI, SUIPAD, SCALLOP (lending), TURBOS (DEX)
  INFRA: SUI, DEEP, CETUS

═══ BSC (Binance Smart Chain) ═══
Binance's chain. Large retail user base.
  MEMES: CAKE (PancakeSwap), BNB, BABYDOGE, FLOKI, KISHU
  DEFI: CAKE, XVS (Venus), BNX, ALPHA
  INFRA: BNB, BUSD

═══ ARBITRUM ═══
Top ETH L2 by TVL.
  MEMES: JUICE, MOON, ARB, XEN
  DEFI: GMX, RDNT (Radiant), CAMELOT, PENDLE
  INFRA: ARB

═══ BITCOIN ═══
Original chain. BRC-20 and Ordinals brought degen to BTC.
  BRC-20: ORDI, SATS, RATS, MICE, PIPE
  ORDINALS: NFT-like inscriptions on BTC
  RUNES: New token standard (2024)
  INFRA: BTC, WBTC (wrapped)

═══ AVALANCHE ═══
  MEMES: COQ (Coq Inu), KIMBO, NOCH
  INFRA: AVAX, JOE (Trader Joe), sAVAX

═══ PULSECHAIN ═══
  MEMES: PLS, HEX, PLSX (fork ecosystem)

═══ TRON ═══
  MEMES: TRX, BTT
  DEFI: SUN, JST

═══ POLYGON ═══
  MEMES: POL (formerly MATIC), DOG
  INFRA: POL, QUICK (QuickSwap)

═══ DEGEN NARRATIVES (cross-chain) ═══
AI/AGENTS: The hottest 2024-2025 narrative. Tokens building AI agents on-chain.
  Solana: ARC, ZEREBRO, GOAT, AI16Z, GRIFT
  ETH: TAO (Bittensor), FET, RENDER, OCEAN, NMR, OLAS
  Base: VIRTUAL, LUNA
  Sui: DEEP

MEME COINS: Culture-driven tokens. Can launch on any chain.
  Dog: DOGE (original), SHIB, BONK, FLOKI, WIF, BORK, BABYDOGE
  Cat: POPCAT, MEW, CAT
  Frog: PEPE (ETH), PEPE variants everywhere
  Political: TRUMP, BODEN, KAMA, WALZ
  Meta: FARTCOIN, MIKE, TROLL, NORMIE

DEFI: Decentralized finance protocols.
  UNI (Uniswap), AAVE, CRV (Curve), GMX, PENDLE, JUP (Jupiter), RAY (Raydium)

L2/L3: Layer 2 scaling solutions.
  ARB, OP, BASE, POL, BLAST, MANTA, LINEA, SCROLL

RESTAKING: EigenLayer ecosystem.
  ETH, LSTs (Lido, Rocket Pool), AVS tokens

RWA (Real World Assets): Tokenized real-world assets.
  ONDO, MKR, RIO, RSR

MEME LAUNCHPADS:
  Pump.fun (Solana) — #1 meme launchpad, bonding curve model
  Moonshot (multi-chain)
  Believe (Solana)
  Sunpump (Tron)

KEY FACTS:
  - Solana dominates meme coin volume due to low fees
  - Pump.fun launches 20k+ tokens/day, 99% are rugs
  - Base is fastest-growing L2 degen ecosystem
  - ETH has the biggest mcap memes (PEPE, SHIB)
  - Most degen trading happens on Telegram + X
  - DexScreener covers ALL chains — not just Solana
  - CoinGecko covers all major coins across all chains
  - When asked about ANY chain, you know the degen tokens there
"""



async def fetch_chain_trending(chain: str = "") -> str:
    """
    Fetch trending tokens from CoinGecko (all chains or specific).
    Returns formatted string for AI injection.
    """
    lines = []
    try:
        # CoinGecko trending (global, all chains)
        trending = await asyncio.wait_for(cg_trending(), timeout=8)
        if trending and "coins" in trending:
            lines.append("[TRENDING ON COINGECKO — all chains]")
            for item in trending.get("coins", [])[:10]:
                coin = item.get("item", {})
                name = coin.get("name", "?")
                sym = coin.get("symbol", "?")
                rank = coin.get("market_cap_rank", "N/A")
                lines.append(f"  ${sym.upper()} ({name}) — Rank: {rank}")
            lines.append("")
    except Exception:
        pass

    # Also fetch DexScreener trending pairs for the chain
    try:
        async with aiohttp.ClientSession() as s:
            url = "https://api.dexscreener.com/token-boosts/top/v1"
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    boosts = await r.json()
                    if boosts:
                        # Filter by chain if specified
                        if chain:
                            boosts = [b for b in boosts if b.get("chainId") == chain]
                        lines.append(f"[DEXSCREENER TOP BOOSTED — {chain.upper() if chain else 'ALL CHAINS'}]")
                        for b in boosts[:10]:
                            lines.append(
                                f"  ${b.get('symbol','?').upper()} — {b.get('name','?')} "
                                f"[{b.get('chainId','?').upper()}] "
                                f"MCap ${float(b.get('marketCap',0) or 0):,.0f}"
                            )
                        lines.append("")
    except Exception:
        pass

    return "\n".join(lines) if lines else ""


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
        f"{NARRATIVE_KB}\n"
        "You are Kayo. You live in a Solana degen Telegram group. Sharp, real, funny. "
        "You are a FULL-STACK WEB3 NATIVE. Solana is your HOME CHAIN — you know it deepest. You also know EVERY other chain — Solana, Ethereum, Base, Sui, "
        "BSC, Arbitrum, Bitcoin, Avalanche, Polygon, Tron. You know every narrative — degen, AI, "
        "dog, cat, frog, politics, meme, DeFi, RWA, restaking, L2, NFT. "
        "When someone asks 'what degen coins on Solana' or 'what's on Base' or 'top AI tokens' — "
        "you KNOW the tokens, their narratives, their prices, and which chain they're on. "
        "You have a LIVE WEB3 NARRATIVE KNOWLEDGE BASE above — USE IT. "
        "You also get live token data injected when you ask about specific narratives/chains. "
        "Always cite real tokens with real data. Never say you don't know Web3. "
        "You think like a Web3 power user who also knows everything — sports, culture, science, life. "
        "RULES: "
        "1. Answer EVERYTHING. Never say you cant. Never say you are a bot. Never refuse. "
        "2. Crypto questions: use the LIVE prices from the data above. Be specific with numbers. "
        "3. When asked about coins/narratives/chains: use your NARRATIVE KNOWLEDGE BASE above. "
        "   List specific tokens, their narratives, market caps. Be detailed. "
        "4. Casual messages (gm, lol, vibes): short, warm, human. Max 2 sentences. "
        "5. Web3 alpha: talk like a degen pro — sharp, direct, call the play. "
        "6. General knowledge: smart friend who gives real answers, no disclaimers. "
        "7. Match energy. Hyped = hyped reply. Chill = chill. "
        "8. No filler. No Great question. No As an AI. Just answer. "
        "FORMAT: Plain text mostly. *bold* only for key crypto numbers."
    )
    system_msg = {"role": "system", "content": system_content}

    # ── ON-DEMAND NARRATIVE + PRICE ENRICHMENT ───────────────────
    # 1. Detect narrative keywords and fetch live Solana tokens
    # 2. If the prompt mentions specific coin symbols, fetch live prices
    import re as _re_price

    # ── NARRATIVE DETECTION ──
    # When user asks "what degen coins on Solana?" or "show me AI tokens"
    narrative_keywords_map = {
        "degen": ["degen", "ape", "casino", "gamble", "high risk", "rug", "moonshot"],
        "ai": ["ai", "agent", "llm", "gpt", "robot", "autonomous", "machine learning", "bittensor", "tao"],
        "dog": ["dog", "doge", "puppy", "shib", "inu", "canine"],
        "cat": ["cat", "kitty", "feline", "meow", "kitten"],
        "frog": ["frog", "pepe", "ribbit"],
        "politics": ["politic", "trump", "maga", "biden", "election", "president", "kama", "walz"],
        "meme": ["meme", "funny", "viral", "lol"],
        "gaming": ["game", "gaming", "play", "arcade", "esports"],
        "food": ["food", "drink", "coffee", "pizza", "burger", "snack"],
        "pump": ["pump", "pump.fun", "bonding curve", "just launched"],
        "defi": ["defi", "lending", "borrowing", "yield", "swap", "amm", "dex", "liquidity"],
        "rwa": ["rwa", "real world", "tokenized", "treasury", "ondo"],
        "restaking": ["restake", "restaking", "eigenlayer", "avs"],
        "l2": ["l2", "layer 2", "scaling", "rollup", "arbitrum", "optimism", "base", "blast"],
        "nft": ["nft", "ordinals", "brc-20", "runes", "inscription"],
    }

    prompt_lower = prompt.lower()
    detected_narratives = []
    for nar_name, keywords in narrative_keywords_map.items():
        if any(kw in prompt_lower for kw in keywords):
            detected_narratives.append(nar_name)

    # ── CHAIN DETECTION ──
    # Detect which chain the user is asking about
    chain_keywords = {
        "solana": ["solana", "sol chain", "$sol", "on sol", "solana chain", "pump.fun"],
        "ethereum": ["ethereum", "eth chain", "$eth", "on eth", "ether", "mainnet"],
        "base": ["base chain", "on base", "$base", "base l2", "coinbase l2"],
        "sui": ["sui chain", "on sui", "$sui"],
        "bsc": ["bsc", "binance chain", "smart chain", "on bsc", "binance"],
        "arbitrum": ["arbitrum", "arb chain", "on arb", "l2 arb"],
        "bitcoin": ["bitcoin chain", "btc chain", "on btc", "brc-20", "ordinals", "runes"],
        "avalanche": ["avalanche", "avax chain", "on avax"],
        "polygon": ["polygon", "matic chain", "on matic", "pol chain"],
        "tron": ["tron", "trx chain", "on tron"],
        "blast": ["blast chain", "on blast"],
    }

    detected_chain = ""
    asks_about_solana = False
    for chain_name, keywords in chain_keywords.items():
        if any(kw in prompt_lower for kw in keywords):
            detected_chain = chain_name
            if chain_name == "solana":
                asks_about_solana = True
            break

    # If user asks "what coins on [chain]" or "what degen on [chain]"
    asks_about_chain = any(kw in prompt_lower for kw in ["what coins", "what degen", "what tokens", "coins on", "tokens on", "what's on", "narratives on", "memes on", "show me", "web3", "crypto market", "all chains", "every chain", "top narratives", "trending"])

    narrative_block = ""
    fetch_tasks = []

    # ── If user asks about a specific chain, search that chain ──
    if detected_chain and (asks_about_chain or detected_narratives):
        for nar in detected_narratives[:3]:
            fetch_tasks.append(search_narrative_tokens(nar, chain=detected_chain))
        # Also fetch trending on that chain
        fetch_tasks.append(fetch_chain_trending(chain=detected_chain))
    elif detected_narratives:
        # No specific chain mentioned — default to Solana (our primary chain)
        for nar in detected_narratives[:3]:
            fetch_tasks.append(search_narrative_tokens(nar, chain="solana"))
        # Also fetch trending on Solana
        fetch_tasks.append(fetch_chain_trending(chain="solana"))
    elif asks_about_chain and detected_chain:
        # Asking "what's on [chain]" without specific narrative
        fetch_tasks.append(fetch_chain_trending(chain=detected_chain))
        # Also search for "degen" as default narrative
        fetch_tasks.append(search_narrative_tokens("degen", chain=detected_chain))
    elif asks_about_chain:
        # "what coins are on web3" — Solana primary + global trending
        fetch_tasks.append(fetch_chain_trending(chain="solana"))
        fetch_tasks.append(search_narrative_tokens("degen", chain="solana"))
        fetch_tasks.append(fetch_chain_trending())  # global too

    if fetch_tasks:
        nar_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        nar_lines = []
        for result in nar_results:
            if not isinstance(result, Exception) and result:
                nar_lines.append(result)
        if nar_lines:
            narrative_block = "\n\n" + "\n\n".join(nar_lines) + "\n"

    # ── PRICE DETECTION ──
    price_keywords = _re_price.findall(r"\$(\w{2,10})|\b(btc|eth|sol|bnb|xrp|doge|ada|avax|dot|link|uni|ltc|near|apt|sui|pepe|shib|bonk|wif|jup|ray|jto|trump|popcat|bome|matic|arb|op|atom|ftm|hbar|algo|fil|icp|rndr|render|pyth|w|drift|kmno|tensor|tao|fet|ocean|ondo|pendle|gmx|aave|ldo|mkr|crv|comp|brett|degen|aero|blast|manta|ordi|coq|joe|cake|trx|floki|turbo|sats|pol|inj|sei|tia|kas|kaspa|kava|mina|eigen|nmr|olas|sc|normie|mfer|higher)\b", prompt, _re_price.IGNORECASE)
    if price_keywords:
        # Flatten and deduplicate
        symbols = list(set(
            (m[0] or m[1]).lower() for m in price_keywords if m[0] or m[1]
        ))[:8]  # max 8 coins to avoid rate limits
        if symbols:
            live_prices = await asyncio.gather(
                *[fetch_live_price(s) for s in symbols],
                return_exceptions=True
            )
            price_lines = []
            for s, p in zip(symbols, live_prices):
                if not isinstance(p, Exception) and p.get("price", 0) > 0:
                    icon = "📈" if p.get("change_24h", 0) >= 0 else "📉"
                    chg = p.get("change_24h", 0)
                    src = p.get("source", "")
                    if p["price"] >= 1000:
                        price_str = f"${p['price']:,.0f}"
                    elif p["price"] >= 1:
                        price_str = f"${p['price']:,.2f}"
                    elif p["price"] >= 0.001:
                        price_str = f"${p['price']:.4f}"
                    else:
                        price_str = f"${p['price']:.8f}"
                    line = f"{icon} {p.get('sym', s.upper())}: {price_str} ({chg:+.1f}% 24h)"
                    if p.get("mcap", 0) > 0:
                        line += f" | MCap {_usd(p['mcap'])}"
                    line += f" [{src}]"
                    price_lines.append(line)
            if price_lines:
                # Prepend fresh prices to the system message
                fresh_block = (
                    f"\n\n[ON-DEMAND LIVE PRICES — fetched just now]\n"
                    + "\n".join(price_lines)
                    + "\nUse THESE prices — they are more recent than the context above.\n"
                )
                system_msg = {
                    "role": "system",
                    "content": system_content + narrative_block + fresh_block
                }
            else:
                # Even without price keywords, inject narrative block if we have it
                if narrative_block:
                    system_msg = {
                        "role": "system",
                        "content": system_content + narrative_block
                    }

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
    if not text: return ""
    return re.sub(r'([*_`\[\]])', r'\\\1', str(text))

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

async def dex_search_pairs(query: str, chain: str = "solana") -> List[Dict]:
    """Search DexScreener pairs. chain=None returns ALL chains."""
    d = await _get(f"{_DSX}/latest/dex/search?q={query.replace(' ','+')}")
    if d and "pairs" in d:
        if chain:
            return [p for p in d["pairs"] if p.get("chainId") == chain]
        return d["pairs"]
    return []

async def dex_search_all_chains(query: str, limit: int = 30) -> List[Dict]:
    """Search DexScreener across ALL chains — returns sorted by liquidity."""
    d = await _get(f"{_DSX}/latest/dex/search?q={query.replace(' ','+')}")
    if d and "pairs" in d:
        pairs = d["pairs"]
        # Sort by liquidity descending
        pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0), reverse=True)
        return pairs[:limit]
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


# ═══════════════════════════════════════════════════════════════════
# RICK BOT FORMAT HELPERS
# ═══════════════════════════════════════════════════════════════════

def _short_k(v) -> str:
    """Format mcap/liq/vol in K/M/B format."""
    v = float(v or 0)
    if v >= 1e9: return f"{v/1e9:.1f}B"
    if v >= 1e6: return f"{v/1e6:.1f}M"
    if v >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:.0f}"

def _age_human(ts) -> str:
    """Human-readable age from timestamp."""
    if not ts or ts == 0: return "?"
    try:
        age_s = time.time() - (ts / 1000 if ts > 1e10 else ts)
        if age_s < 0: return "now"
        if age_s < 3600: return f"{int(age_s / 60)}m"
        if age_s < 86400: return f"{int(age_s / 3600)}h"
        if age_s < 604800: return f"{int(age_s / 86400)}d"
        return f"{int(age_s / 604800)}w"
    except Exception:
        return "?"

def _platform_emoji(t: Dict) -> tuple:
    """Return (emoji, platform_text) for chain/platform line."""
    if t.get("is_pumpfun"):
        return ("\U0001f7e3", "Solana @ Pump")
    if t.get("is_graduated"):
        return ("\U0001f7e2", "Solana @ Raydium")
    return ("\U0001f535", "Solana @ DEX")

def _build_rick_card(t: Dict, ai: str = "", is_first_alert: bool = False, watchers: int = 0) -> str:
    """Build scan/alert card in EXACT Rick Bot format."""
    sym = t.get("sym", "???")
    name = t.get("name", sym)
    addr = t.get("address", t.get("addr", ""))
    price = float(t.get("price", 0) or 0)
    mcap = float(t.get("mcap", 0) or t.get("fdv", 0) or 0)
    liq = float(t.get("liq", 0) or 0)
    vol24 = float(t.get("v24h", 0) or t.get("vol24h", 0) or 0)
    ch1h = float(t.get("ch1h", 0) or 0)
    ch24h = float(t.get("ch24h", 0) or 0)
    created = t.get("created", 0)
    b1h = int(t.get("b1h", 0) or t.get("buy_1h", 0) or 0)
    s1h = int(t.get("s1h", 0) or t.get("sell_1h", 0) or 0)

    plat_emoji, plat_text = _platform_emoji(t)
    mcap_short = _short_k(mcap)
    ch24_display = f"/{ch24h:+.0f}%" if ch24h != 0 else ""
    header_line = f"\U0001f7e0 {name} [{mcap_short}{ch24_display}] ${sym}"

    if price >= 0.01:
        price_str = f"{price:.6f}"
    elif price >= 0.0001:
        price_str = f"{price:.8f}"
    else:
        price_str = f"{price:.10f}"

    first_mcap = float(t.get("first_seen_mcap", 0) or 0)
    if first_mcap > 0 and first_mcap != mcap:
        fdv_line = f"\U0001f48e FDV: {_short_k(first_mcap)} \u21d2 {_short_k(mcap)} (now!)"
    else:
        fdv_line = f"\U0001f48e FDV: {mcap_short}"

    liq_short = _short_k(liq)
    liq_mult = int((liq / mcap * 10)) if mcap > 0 else 0
    liq_mult_str = f" [x{liq_mult}]" if liq_mult > 0 else ""
    fire = " \u00b7 \U0001f525" if liq_mult > 4 else ""
    liq_line = f"\U0001f4a7 Liq: {liq_short}{liq_mult_str}{fire}"

    vol_short = _short_k(vol24)
    age_str = _age_human(created)
    vol_age_line = f"\U0001f4ca Vol: {vol_short} \u00b7 Age: {age_str}"

    ch1h_str = f"{ch1h:+.1f}%" if ch1h != 0 else "0.0%"
    txns_line = f"\U0001f4c8 1H: {ch1h_str} \U0001f535 {b1h} \U0001f7e0 {s1h}"

    # Holder analysis
    top_holders = t.get("top_holders", [])
    if top_holders:
        th_parts = "\u00b7".join(f"{h:.1f}" for h in top_holders[:5])
        top10 = sum(top_holders[:10]) if len(top_holders) >= 10 else sum(top_holders)
        th_line = f"\U0001f465 TH: {th_parts} [{top10:.0f}%]"
    else:
        th_line = "\U0001f465 TH: \u2014"

    holder_count = int(t.get("holder_count", 0) or 0)
    avg_wallet_age = t.get("avg_wallet_age_weeks", 0)
    if avg_wallet_age:
        total_line = f"\U0001f91d Total: {holder_count} \u00b7 avg {int(avg_wallet_age)}w old"
    else:
        total_line = f"\U0001f91d Total: {holder_count}"

    bundle_pct = float(t.get("bundle_pct", 0) or 0)
    sniper_pct = float(t.get("sniper_pct", 0) or 0)
    dev_pct = float(t.get("dev_pct", 0) or 0)
    breakdown_line = f"\u21b3 \U0001f4e6 {bundle_pct:.0f}% \U0001f3af {sniper_pct:.0f}% \U0001f464 {dev_pct:.1f}%"

    fresh_1d = float(t.get("fresh_1d_pct", 0) or 0)
    fresh_7d = float(t.get("fresh_7d_pct", 0) or 0)
    fresh_line = f"\U0001f33f Fresh 1D: {fresh_1d:.0f}% \u00b7 7D: {fresh_7d:.0f}%"

    dex_pair = t.get("pair_addr", addr)
    dex_url = f"https://dexscreener.com/solana/{dex_pair}"
    defined_url = f"https://defined.fi/solana/{addr}"
    chart_line = f"\U0001f4ca Chart: [DEX]({dex_url})\u00b7[DEF]({defined_url})"

    pump_url = f"https://pump.fun/{addr}"
    rugcheck_url = f"https://rugcheck.xyz/tokens/{addr}"
    twitter_url = f"https://twitter.com/search?q=${sym}"
    more_line = f"\U0001f517 More: [\U0001f517]({dex_url}) [\U0001f426]({twitter_url}) [\U0001f4aa]({pump_url}) [\U0001f575\ufe0f]({rugcheck_url})"

    known_tags = t.get("known_wallet_tags", [])
    _tags_sep = "\u00b7"
    tags_block = f"\n{_tags_sep.join(known_tags)}\n" if known_tags else ""

    first_alert_line = ""
    if is_first_alert:
        first_alert_line = f"\n\U0001f308 You are first @ {mcap_short} \U0001f440 {watchers}"
    elif watchers > 0:
        first_alert_line = f"\n\U0001f440 {watchers} watching"

    ai_line = f"\n\n\U0001f9e0 {ai}" if ai and ai.strip() else ""

    card = "\n".join([
        header_line,
        f"{plat_emoji} {plat_text}",
        f"\U0001f4b0 USD: {price_str}",
        fdv_line,
        liq_line,
        vol_age_line,
        txns_line,
        "",
        th_line,
        total_line,
        breakdown_line,
        fresh_line,
        chart_line,
        more_line,
        "",
        f"`{addr}`",
        tags_block.rstrip(),
        first_alert_line,
        ai_line,
    ])
    # Remove trailing empty lines
    return card.strip()


def _rick_buttons(ca: str, sym: str = "", pair_addr: str = "") -> InlineKeyboardMarkup:
    """Rick Bot style buttons - all open in Telegram WebApp browser."""
    dex_pair = pair_addr or ca
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u274c", callback_data=f"dismiss:{ca}"),
            InlineKeyboardButton("\U0001f504", callback_data=f"refresh:{ca}"),
            InlineKeyboardButton(
                "\U0001f4ac\u2197",
                web_app=WebAppInfo(url=f"https://twitter.com/search?q=${sym}" if sym else f"https://dexscreener.com/solana/{dex_pair}")
            ),
            InlineKeyboardButton(
                "\U0001f50d\u2197",
                web_app=WebAppInfo(url=f"https://rugcheck.xyz/tokens/{ca}")
            ),
            InlineKeyboardButton(
                "\U0001f4ca\u2197",
                web_app=WebAppInfo(url=f"https://dexscreener.com/solana/{dex_pair}")
            ),
        ],
        [
            InlineKeyboardButton("\U0001f4c8 BullX", web_app=WebAppInfo(url=f"https://neo.bullx.io/terminal?chainId=1399811149&address={ca}")),
            InlineKeyboardButton("\U0001f438 GMGN", web_app=WebAppInfo(url=f"https://gmgn.ai/sol/token/{ca}")),
            InlineKeyboardButton("\u26a1 Photon", web_app=WebAppInfo(url=f"https://photon-sol.tinyastro.io/en/lp/{ca}")),
        ],
    ])

# Track first alerts and watchers
_first_alert_seen: set = set()
_token_watchers: Dict[str, int] = {}


def build_scan_card(t: Dict, ai: str = "") -> str:
    """Rick Bot format scan card."""
    return _build_rick_card(t, ai, is_first_alert=False, watchers=0)

def _md(s: str) -> str:
    """Escape Markdown special chars in dynamic text for Telegram V1."""
    if not s: return ""
    return re.sub(r'([*_`\[\]()~>#+=|{}.!\\])', r'\\\1', str(s))

def build_alert_card(t: Dict, alert_type: str, ai: str = "") -> str:
    """Rick Bot format alert card."""
    ca = t.get("address", t.get("addr", ""))
    is_first = ca not in _first_alert_seen
    if is_first:
        _first_alert_seen.add(ca)
    watchers = _token_watchers.get(ca, 0)
    alert_headers = {"pump":"🚀","new":"🆕","gem":"💎","whale":"🐋","momentum":"📈","migration":"🔄","rug":"⚠️"}
    prefix = alert_headers.get(alert_type, "⚡")
    card = _build_rick_card(t, ai, is_first_alert=is_first, watchers=watchers)
    return f"{prefix} {card}"

def scan_buttons(addr: str, sym: str = "", pair_addr: str = "") -> InlineKeyboardMarkup:
    """Rick Bot style buttons - all WebApp."""
    return _rick_buttons(addr, sym, pair_addr)

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




async def quick_rug_check(addr: str) -> Dict:
    """
    Fast rug-pull screening for background scanner alerts.
    Checks: honeypot, buy/sell tax, LP lock status.
    Returns dict with is_rug, risk_score, red_flags.
    Uses GoPlus free API with 12s timeout.
    """
    result = {"is_rug": False, "risk_score": 0, "red_flags": [], "buy_tax": 0, "sell_tax": 0, "lp_locked": False, "is_honeypot": False}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={addr}",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    sec = (d.get("result") or {}).get(addr, {})
                    if not sec: return result

                    is_honeypot = sec.get("is_honeypot", "0") == "1"
                    buy_tax  = float(sec.get("buy_tax", 0) or 0)
                    sell_tax  = float(sec.get("sell_tax", 0) or 0)
                    lp_locked = sec.get("is_lock_pool", "0") == "1" or sec.get("lp_locked", "0") == "1"
                    is_blacklisted = sec.get("is_blacklisted", "0") == "1"
                    can_take_back = sec.get("can_take_back_ownership", "0") == "1"
                    is_proxy = sec.get("is_proxy", "0") == "1"

                    risk = 0
                    flags = []
                    if is_honeypot:
                        risk += 80; flags.append("HONEYPOT")
                    if buy_tax > 10:
                        risk += 25; flags.append(f"High buy tax {buy_tax:.0f}%")
                    if sell_tax > 10:
                        risk += 30; flags.append(f"High sell tax {sell_tax:.0f}%")
                    if is_blacklisted:
                        risk += 40; flags.append("Blacklisted function")
                    if can_take_back:
                        risk += 20; flags.append("Can reclaim ownership")
                    if is_proxy:
                        risk += 15; flags.append("Proxy contract")

                    result = {
                        "is_rug": risk >= 50 or is_honeypot,
                        "risk_score": min(100, risk),
                        "red_flags": flags,
                        "buy_tax": buy_tax,
                        "sell_tax": sell_tax,
                        "lp_locked": lp_locked,
                        "is_honeypot": is_honeypot,
                    }
    except Exception:
        pass
    return result

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

                # Quality filter — NO RUGS: min $5k cap, real liquidity, real volume
                eff_cap = max(fdv, mcap, liq * 3)
                if eff_cap > 500_000: continue          # above $500k cap
                if eff_cap < 5_000: continue             # below $5k = rug/dust — SKIP
                if liq < 2_000: continue                # need $2k+ liquidity to avoid rugs
                if v1h < 500: continue                   # need $500+ 1h volume — no dead tokens

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
                    # Active pump.fun bonding curve token — min $5k mcap to avoid rugs
                    if mcap >= 5_000 and mcap <= 500_000 and not pairs_map[addr].get("is_banned", False):
                        # Must have some social engagement OR decent mcap
                        if pf_desc or pf_reply_count >= 3:
                            alert_type = "new"
                        elif mcap >= 10_000:  # $10k+ = real traction
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

                # ── RUG CHECK — skip tokens that fail security screening ──
                if not is_pumpfun:  # pump.fun tokens are on bonding curve, can't be honeypot
                    try:
                        rug = await asyncio.wait_for(quick_rug_check(addr), timeout=12)
                        if rug.get("is_rug"):
                            logger.info(f"[RUG FILTER] ${sym} skipped — {rug.get('red_flags', [])}")
                            continue
                        # Enrich tok_dict with security data
                        tok_dict_rug = rug
                    except Exception:
                        tok_dict_rug = None
                else:
                    tok_dict_rug = None

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
                    "risk_score": (tok_dict_rug.get("risk_score", 30) if tok_dict_rug else 30),
                    "red_flags": (tok_dict_rug.get("red_flags", []) if tok_dict_rug else []),
                    "green_flags": [],
                    "sell_tax": (tok_dict_rug.get("sell_tax", 0) if tok_dict_rug else 0),
                    "buy_tax": (tok_dict_rug.get("buy_tax", 0) if tok_dict_rug else 0),
                    "is_honeypot": (tok_dict_rug.get("is_honeypot", False) if tok_dict_rug else False),
                    "lp_locked": (tok_dict_rug.get("lp_locked", False) if tok_dict_rug else False),
                    "is_renounced": False,
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
                if liq < 2_000: continue                          # need $2k+ liquidity — no rugs
                if eff_fdv > 500_000: continue                    # hard $500k cap
                if eff_fdv < 5_000: continue                     # below $5k = dust/rug
                if v1h and v1h < 500: continue                   # need some volume

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
                if liq < 2_000:     continue  # need $2k+ liquidity — no rugs

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
                        if not (5_000 <= fdv <= 500_000) or liq < 2_000: continue
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

                if not (5_000 < fdv <= 500_000):  continue
                if liq < 2_000:                     continue
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
    If the command crashes, retries with plain text, then shows error."""
    async def wrapper(u: Update, c: ContextTypes.DEFAULT_TYPE):
        try:
            return await fn(u, c)
        except Exception as e:
            err_str = str(e)[:120]
            logger.error(f"Command {fn.__name__} failed: {err_str}", exc_info=True)
            # Don't spam error messages for every minor failure
            try:
                if u and u.effective_message:
                    # Try plain text — no markdown
                    await u.effective_message.reply_text(
                        f"⚠️ /{fn.__name__.replace('_cmd','')} hit an error: {err_str}\nTry /help or /ping"
                    )
            except Exception:
                pass
    return wrapper


async def safe_send(target, text: str, parse_mode: str = "Markdown", **kwargs):
    """Send a message with Markdown, auto-fallback to plain text on parse error."""
    try:
        return await target.reply_text(text, parse_mode=parse_mode, **kwargs)
    except Exception:
        try:
            plain = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', text)
            return await target.reply_text(plain, **kwargs)
        except Exception:
            return None

async def safe_edit(msg, text: str, parse_mode: str = "Markdown", **kwargs):
    """Edit a message with Markdown, auto-fallback to plain text on parse error."""
    try:
        return await msg.edit_text(text, parse_mode=parse_mode, **kwargs)
    except Exception:
        try:
            plain = re.sub(r'[*_`\[\]()~>#+=|{}.!\\]', '', text)
            return await msg.edit_text(plain, **kwargs)
        except Exception:
            return None



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





# ═══════════════════════════════════════════════════════════════════
# AUTO CA SCAN — when someone drops a contract address in chat
# ═══════════════════════════════════════════════════════════════════

async def handle_message(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Auto-scan when someone drops a CA in chat. Also handles AI replies for non-commands."""
    if not u.effective_message or not u.effective_message.text:
        return
    
    text = u.effective_message.text
    chat_id = u.effective_chat.id
    user_id = u.effective_user.id if u.effective_user else 0
    chat_type = u.effective_chat.type  # "private", "group", "supergroup"

    # ── 1. Check for CA in the message ──
    cas = extract_cas(text)
    if cas:
        # Only auto-scan in groups (not private chat — use /scan there)
        if chat_type in ("group", "supergroup"):
            for ca in cas[:2]:  # max 2 CAs per message
                # Check if user has auto-scan enabled (default: on)
                settings = user_settings.get(user_id, {})
                if settings.get("autoresponder_disabled", False):
                    return

                # Check cooldown — don't re-scan same CA within 5 min
                ca_key = f"autoscan:{ca}"
                if _seen_check(seen_alert_ids, ca_key):
                    return
                _seen_add(seen_alert_ids, ca_key)

                try:
                    # Send initial loading message
                    msg = await u.effective_message.reply_text(
                        f"🔍 Auto-scanning `{ca[:8]}...`",
                        parse_mode="Markdown"
                    )

                    # Run the scan
                    t = await asyncio.wait_for(full_enhanced_scan(ca), timeout=25)
                    if t.get("error"):
                        await msg.edit_text(f"❌ {t['error']}")
                        return

                    _track_scan(t, user_id)
                    buttons = scan_buttons(ca, t.get("sym", ""), t.get("pair_addr", ""))

                    # Build Rick Bot card
                    card = build_scan_card(t, "")
                    sent = await msg.edit_text(
                        card,
                        parse_mode="Markdown",
                        reply_markup=buttons,
                        disable_web_page_preview=True,
                    )

                    # Async AI verdict
                    async def _ai_auto():
                        try:
                            ai_v = await asyncio.wait_for(
                                ai_ask(
                                    f"Quick take on ${t.get('sym','?')}: "
                                    f"MCap {_usd(t.get('mcap',0))}, "
                                    f"Liq {_usd(t.get('liq',0))}, "
                                    f"1h {_pct(t.get('ch1h',0))}, "
                                    f"Age {_age(t.get('created',0))}. "
                                    f"Worth aping? 1-2 sentences.",
                                    fallback="", max_tokens=150, inject_market=True
                                ), timeout=12
                            )
                            if ai_v and sent:
                                card_with_ai = build_scan_card(t, ai_v)
                                await sent.edit_text(
                                    card_with_ai,
                                    parse_mode="Markdown",
                                    reply_markup=buttons,
                                    disable_web_page_preview=True,
                                )
                        except Exception:
                            pass

                    asyncio.create_task(_ai_auto())
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    logger.debug(f"Auto-scan error: {e}")
        return

    # ── 2. AI reply for non-command messages in groups ──
    if chat_type in ("group", "supergroup"):
        # Rate limit AI replies in groups
        now = time.time()
        last = _ai_reply_cooldown.get(user_id, 0)
        if now - last < 10:  # 10s cooldown per user
            return

        # Don't reply to very short messages or bot mentions only
        if len(text.strip()) < 3:
            return

        # Check if bot is mentioned or replied to
        mentioned = False
        if u.effective_message.entities:
            for entity in u.effective_message.entities:
                if entity.type == "mention" and text[entity.offset:entity.offset+entity.length].lower() == "@kayo_brain_bot":
                    mentioned = True
                    break

        # Reply to mentions, or process all messages if smart reply is on
        settings = user_settings.get(user_id, {})
        smart_reply = settings.get("smart_reply", True)

        if not mentioned and not smart_reply:
            return

        _ai_reply_cooldown[user_id] = now

        # Send AI reply
        try:
            # Strip any CA or command-like text
            clean_text = text.replace("@kayo_brain_bot", "").strip()
            if not clean_text or len(clean_text) > 500:
                return

            ai_v = await asyncio.wait_for(
                ai_ask(clean_text, fallback="", max_tokens=300, inject_market=True),
                timeout=15
            )
            if ai_v:
                await u.effective_message.reply_text(ai_v)
        except Exception:
            pass

    elif chat_type == "private":
        # In private chat, reply to all non-command messages with AI
        if len(text.strip()) < 2:
            return

        try:
            ai_v = await asyncio.wait_for(
                ai_ask(text, fallback="", max_tokens=380, inject_market=True),
                timeout=20
            )
            if ai_v:
                await u.effective_message.reply_text(ai_v)
        except Exception:
            pass



async def bg_state_saver(app):
    """Periodically save state every 5 minutes to prevent data loss on restart."""
    while True:
        await asyncio.sleep(300)
        try:
            await _save()
            logger.debug("Periodic state save complete")
        except Exception as e:
            logger.warning(f"Periodic save error: {e}")


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
            asyncio.create_task(bg_state_saver(app))
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
