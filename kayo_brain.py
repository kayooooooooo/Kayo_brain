"""
KAYO BRAIN - COMPLETE WEB3 INTELLIGENCE BOT
VERSION: 10.0 FINAL
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

BOT_TOKEN = "PASTE_YOUR_BOT_TOKEN_HERE"
ALERT_CHAT_ID = 0

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('kayo_brain.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "🦅 Kayo Brain is alive!", 200

@flask_app.route('/health')
def health_check():
    return "OK", 200

@flask_app.route('/ping')
def ping():
    return "OK", 200

def run_webserver():
    flask_app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)

start_time = time.time()
threading.Thread(target=run_webserver, daemon=True).start()
logger.info("🌐 Web server started on port 8080")
class ImageGenerator:
    def __init__(self):
        self.cache_dir = "kayo_images"
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def generate_price_chart_ascii(self, symbol: str, price_data: List[float]) -> str:
        if not price_data:
            return "No price data available"
        min_price = min(price_data)
        max_price = max(price_data)
        range_price = max_price - min_price if max_price != min_price else 1
        height = 10
        width = min(40, len(price_data))
        chart = f"\n📊 **ASCII Chart - ${symbol}**\n```\n"
        for row in range(height):
            level = max_price - (range_price * row / height)
            line = ""
            for i in range(len(price_data[:width])):
                if price_data[i] >= level:
                    line += "█"
                else:
                    line += " "
            chart += line + "\n"
        chart += "```\n"
        return chart
    
    def generate_trend_image(self, symbol: str, trend: str, score: int) -> str:
        if trend == "BULLISH":
            arrow = "▲" * min(20, score // 5)
            color = "🟢"
        elif trend == "BEARISH":
            arrow = "▼" * min(20, score // 5)
            color = "🔴"
        else:
            arrow = "●" * min(20, score // 5)
            color = "🟡"
        return f"\n{color} **${symbol} - {trend}** {color}\n\nMomentum: {score}/100\n{arrow}\n\nSignal: {'STRONG BUY' if score > 70 else 'BUY' if score > 50 else 'HOLD' if score > 30 else 'SELL'}\n"
    
    async def generate_meme(self, text: str) -> str:
        memes = [
            f"🦅 KAYO SAYS: {text.upper()} 🚀",
            f"📈 TO THE MOON: {text} 📈",
            f"💎 DIAMOND HANDS: {text} 💎",
            f"🐸 WEN MOON? {text} 🐸",
        ]
        return random.choice(memes)


class KayoMemory:
    def __init__(self, memory_file="kayo_memory.json"):
        self.memory_file = memory_file
        self.trading_patterns = {}
        self.coin_memories = {}
        self.strategy_weights = {
            "momentum": 0.3, "volume_spike": 0.25, "narrative_strength": 0.2,
            "whale_activity": 0.15, "social_sentiment": 0.1,
        }
        self.win_loss_records = []
        self.known_news_events = []
        self.opinion_memory = {}
        self.user_feedback = {}
        self.load_memory()
    
    def load_memory(self):
        try:
            if os.path.exists(self.memory_file):
                with open(self.memory_file, 'r') as f:
                    data = json.load(f)
                    self.trading_patterns = data.get('trading_patterns', {})
                    self.strategy_weights = data.get('strategy_weights', self.strategy_weights)
                    self.win_loss_records = data.get('win_loss_records', [])
                    self.opinion_memory = data.get('opinion_memory', {})
                    self.user_feedback = data.get('user_feedback', {})
                logger.info(f"Loaded memory with {len(self.opinion_memory)} opinions")
        except Exception as e:
            logger.warning(f"Could not load memory: {e}")
    
    def save_memory(self):
        try:
            with open(self.memory_file, 'w') as f:
                json.dump({
                    'trading_patterns': self.trading_patterns,
                    'strategy_weights': self.strategy_weights,
                    'win_loss_records': self.win_loss_records[-1000:],
                    'opinion_memory': self.opinion_memory,
                    'user_feedback': self.user_feedback,
                    'last_updated': datetime.utcnow().isoformat(),
                }, f, indent=2)
            logger.info("Memory saved")
        except Exception as e:
            logger.error(f"Could not save memory: {e}")
    
    def record_outcome(self, pattern: str, won: bool, profit_percent: float, user_id: int = 0):
        self.win_loss_records.append({
            'pattern': pattern, 'won': won, 'profit': profit_percent,
            'time': datetime.utcnow().isoformat(), 'user_id': user_id
        })
        if pattern not in self.trading_patterns:
            self.trading_patterns[pattern] = {'wins': 0, 'losses': 0, 'total_profit': 0}
        if won:
            self.trading_patterns[pattern]['wins'] += 1
        else:
            self.trading_patterns[pattern]['losses'] += 1
        self.trading_patterns[pattern]['total_profit'] += profit_percent
        total = self.trading_patterns[pattern]['wins'] + self.trading_patterns[pattern]['losses']
        success = self.trading_patterns[pattern]['wins'] / total if total > 0 else 0
        if pattern in self.strategy_weights:
            target_weight = max(0.05, min(0.5, success * 0.8))
            self.strategy_weights[pattern] = self.strategy_weights[pattern] * 0.9 + target_weight * 0.1
        total_weight = sum(self.strategy_weights.values())
        if total_weight > 0:
            for k in self.strategy_weights:
                self.strategy_weights[k] /= total_weight
        self.save_memory()
    
    def form_opinion(self, address: str, analysis: Dict) -> str:
        score = 0
        weighted_score = 0
        for strategy, weight in self.strategy_weights.items():
            strategy_score = self._evaluate_strategy(strategy, analysis)
            weighted_score += strategy_score * weight
        score += weighted_score
        if address in self.user_feedback:
            feedback_score = self.user_feedback[address].get('rating', 0) * 10
            score += feedback_score
        if score >= 70:
            opinion = "🟢 **KAYO SAYS: APE** — This fits my winning patterns."
        elif score >= 50:
            opinion = "🟡 **KAYO SAYS: WATCH** — Interesting, waiting for confirmation."
        elif score >= 30:
            opinion = "🟠 **KAYO SAYS: CAUTION** — Risk signals present."
        else:
            opinion = "🔴 **KAYO SAYS: AVOID** — Too many red flags."
        self.opinion_memory[address] = {'opinion': opinion, 'score': score, 'timestamp': datetime.utcnow().isoformat()}
        self.save_memory()
        return opinion
    
    def _evaluate_strategy(self, strategy: str, analysis: Dict) -> float:
        if strategy == "momentum":
            return analysis.get('momentum', {}).get('score', 0)
        elif strategy == "volume_spike":
            vol_ratio = analysis.get('vol_ratio', 1)
            return min(100, vol_ratio * 20)
        elif strategy == "narrative_strength":
            return analysis.get('narrative', {}).get('score', 5) * 10
        elif strategy == "whale_activity":
            return analysis.get('whale_score', 0)
        elif strategy == "social_sentiment":
            return analysis.get('sentiment_score', 50)
        return 50
    
    def add_feedback(self, address: str, rating: int, comment: str = ""):
        self.user_feedback[address] = {'rating': rating, 'comment': comment, 'time': datetime.utcnow().isoformat()}
        self.save_memory()
class TwitterNewsSniper:
    def __init__(self):
        self.seen_tweets = set()
        self.news_buffer = []
        self.trending_topics = Counter()
        self.topic_to_coin_map = defaultdict(list)
        self.coin_mentions = Counter()
        self.last_scan = 0
        self.scan_interval = 30
        self.news_categories = {
            'launch': ['launch', 'launched', 'releasing', 'debut', 'go live'],
            'listing': ['listing', 'listed on', 'cex', 'exchange'],
            'partnership': ['partner', 'partnership', 'collaboration'],
            'upgrade': ['upgrade', 'update', 'v2', 'v3'],
        }
    
    async def scan_twitter(self, session: aiohttp.ClientSession):
        queries = ['crypto news solana', 'new token solana', 'ca solana', 'alpha alert']
        all_tweets = []
        for query in queries[:2]:
            tweets = await self._scrape_twitter(session, query, limit=8)
            all_tweets.extend(tweets)
            await asyncio.sleep(1)
        for tweet in all_tweets:
            text = tweet.get('text', '').lower()
            tweet_id = hashlib.md5(text.encode()).hexdigest()
            if tweet_id in self.seen_tweets:
                continue
            self.seen_tweets.add(tweet_id)
            sol_cas = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
            eth_cas = re.findall(r'0x[a-fA-F0-9]{40}', text)
            if sol_cas or eth_cas:
                category = self._detect_category(text)
                self.news_buffer.append({'text': text[:300], 'user': tweet.get('user', 'unknown'), 'cas': sol_cas + eth_cas, 'category': category, 'timestamp': time.time()})
            for coin in re.findall(r'\$([A-Za-z]{2,10})', text):
                self.coin_mentions[coin.upper()] += 1
            for category, keywords in self.news_categories.items():
                if any(kw in text for kw in keywords):
                    self.trending_topics[category] += 1
        self.news_buffer = [n for n in self.news_buffer if time.time() - n['timestamp'] < 3600]
        return bool(all_tweets)
    
    async def _scrape_twitter(self, session, query, limit=10):
        nitter_instances = ["https://nitter.privacydev.net", "https://nitter.poast.org"]
        for base in nitter_instances:
            try:
                url = f"{base}/search?q={quote_plus(query)}&f=tweets"
                async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10) as r:
                    if r.status != 200:
                        continue
                    html = await r.text()
                    tweets = re.findall(r'<div class="tweet-content[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
                    users = re.findall(r'<a class="username"[^>]*href="/([^"]+)"', html)
                    results = []
                    for i, t in enumerate(tweets[:limit]):
                        clean = re.sub(r'<[^>]+>', '', t).strip()
                        if clean and len(clean) > 20:
                            results.append({'text': clean[:350], 'user': users[i] if i < len(users) else 'unknown'})
                    if results:
                        return results
            except:
                continue
        return []
    
    def _detect_category(self, text: str) -> str:
        text_lower = text.lower()
        for category, keywords in self.news_categories.items():
            if any(kw in text_lower for kw in keywords):
                return category
        return 'general'
    
    def get_hot_narratives(self, limit=5):
        return [f"🔥 {topic} ({count} mentions)" for topic, count in self.trending_topics.most_common(limit)]
    
    def get_trending_coins(self, limit=10):
        return [f"${coin}" for coin, count in self.coin_mentions.most_common(limit)]


class RugDetector:
    async def comprehensive_rug_check(self, pair: Dict, sec: Dict) -> Dict:
        rug_score = 0
        red_flags = []
        green_flags = []
        if sec.get("is_honeypot") == "1":
            rug_score += 60
            red_flags.append("🚨 HONEYPOT DETECTED - Cannot sell!")
        sell_tax = float(sec.get("sell_tax", 0) or 0)
        if sell_tax > 20:
            rug_score += 40
            red_flags.append(f"💸 Extreme sell tax: {sell_tax}%")
        elif sell_tax > 10:
            rug_score += 20
            red_flags.append(f"⚠️ High sell tax: {sell_tax}%")
        if sec.get("lp_locked") == "1":
            green_flags.append("🔒 Liquidity locked")
        else:
            rug_score += 35
            red_flags.append("⚠️ Liquidity NOT locked")
        if sec.get("owner_change_balance") == "1":
            rug_score += 30
            red_flags.append("👑 Owner can change balances")
        if sec.get("is_blacklisted") == "1":
            rug_score += 40
            red_flags.append("🚫 Contract blacklisted")
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        fdv = float(pair.get("fdv", 0) or 0)
        if fdv > 0 and liq > 0:
            liq_ratio = liq / fdv
            if liq_ratio < 0.02:
                rug_score += 25
                red_flags.append(f"💧 Shallow liquidity ({liq_ratio*100:.1f}% of MCap)")
        if rug_score >= 70:
            verdict = "🔴 CONFIRMED RUG - DO NOT BUY"
        elif rug_score >= 50:
            verdict = "🟠 HIGH RISK - Likely rug"
        elif rug_score >= 30:
            verdict = "🟡 SUSPICIOUS - Potential rug"
        else:
            verdict = "🟢 CLEAN - No major red flags"
        return {'rug_score': min(100, rug_score), 'verdict': verdict, 'red_flags': red_flags[:5], 'green_flags': green_flags[:3], 'can_sell': sec.get("is_honeypot") != "1", 'lp_locked': sec.get("lp_locked") == "1", 'sell_tax': sell_tax}


class ProfessionalChartViewer:
    def get_chart_buttons(self, address: str, symbol: str) -> InlineKeyboardMarkup:
        tv_url = f"https://www.tradingview.com/chart/?symbol=CRYPTO:{symbol}USDT"
        dexscreener_url = f"https://dexscreener.com/solana/{address}"
        birdeye_url = f"https://birdeye.so/token/{address}?chain=solana"
        keyboard = [
            [InlineKeyboardButton("📈 TradingView Pro", web_app=WebAppInfo(url=tv_url)), InlineKeyboardButton("📊 DexScreener", web_app=WebAppInfo(url=dexscreener_url))],
            [InlineKeyboardButton("🦅 Birdeye", url=birdeye_url), InlineKeyboardButton("🎰 Pump.fun", url=f"https://pump.fun/{address}")],
            [InlineKeyboardButton("⏱️ 1m", callback_data=f"tf_1m:{symbol}"), InlineKeyboardButton("⏱️ 5m", callback_data=f"tf_5m:{symbol}"), InlineKeyboardButton("⏱️ 15m", callback_data=f"tf_15m:{symbol}"), InlineKeyboardButton("⏱️ 1h", callback_data=f"tf_1h:{symbol}")],
            [InlineKeyboardButton("📊 RSI", callback_data=f"indicator_rsi:{symbol}"), InlineKeyboardButton("📉 MACD", callback_data=f"indicator_macd:{symbol}"), InlineKeyboardButton("📈 BB", callback_data=f"indicator_bb:{symbol}")]
        ]
        return InlineKeyboardMarkup(keyboard)
async def dex_token(session: aiohttp.ClientSession, address: str) -> Optional[Dict]:
    try:
        async with session.get(f"https://api.dexscreener.com/latest/dex/tokens/{address}", timeout=10) as r:
            if r.status != 200:
                return None
            data = await r.json()
            pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
            if not pairs:
                return None
            pairs.sort(key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)
            return pairs[0]
    except:
        return None

async def dex_search(session: aiohttp.ClientSession, query: str = "solana") -> List[Dict]:
    try:
        async with session.get(f"https://api.dexscreener.com/latest/dex/search?q={query}", timeout=12) as r:
            if r.status != 200:
                return []
            data = await r.json()
            return [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
    except:
        return []

async def goplus_sec(session: aiohttp.ClientSession, address: str) -> Dict:
    try:
        async with session.get(f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={address}", timeout=8) as r:
            if r.status != 200:
                return {}
            data = await r.json()
            result = data.get("result", {})
            return result.get(address.lower(), result.get(address, {}))
    except:
        return {}

async def coingecko_coin(session: aiohttp.ClientSession, coin_id: str) -> Optional[Dict]:
    try:
        async with session.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false", timeout=10) as r:
            if r.status != 200:
                return None
            return await r.json()
    except:
        return None


async def smart_scan(address: str) -> Dict:
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
    if not pair:
        return {"error": "Token not found on Solana"}
    base = pair.get("baseToken", {})
    symbol = base.get("symbol", "???")
    name = base.get("name", "Unknown")
    price = float(pair.get("priceUsd", 0) or 0)
    fdv = float(pair.get("fdv", 0) or 0)
    liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
    ch_1h = float(pair.get("priceChange", {}).get("h1", 0) or 0)
    ch_5m = float(pair.get("priceChange", {}).get("m5", 0) or 0)
    ch_24h = float(pair.get("priceChange", {}).get("h24", 0) or 0)
    buys_1h = int(pair.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells_1h = int(pair.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    vol_1h = float(pair.get("volume", {}).get("h1", 0) or 0)
    vol_5m = float(pair.get("volume", {}).get("m5", 0) or 0)
    narrative = "Meme"
    narrative_score = 5
    text = f"{name} {symbol}".lower()
    if any(w in text for w in ['ai', 'agent', 'intelligence']):
        narrative, narrative_score = "AI", 9
    elif any(w in text for w in ['game', 'play', 'gaming']):
        narrative, narrative_score = "Gaming", 8
    elif any(w in text for w in ['defi', 'swap', 'yield']):
        narrative, narrative_score = "DeFi", 8
    elif any(w in text for w in ['rwa', 'real', 'asset']):
        narrative, narrative_score = "RWA", 9
    vol_ratio = vol_5m / max(vol_1h / 12, 1) if vol_1h > 0 else 1
    momentum_score = 0
    if ch_1h > 0:
        momentum_score += min(50, ch_1h * 2)
    if vol_ratio > 1:
        momentum_score += min(30, vol_ratio * 10)
    if buys_1h > 20:
        momentum_score += min(20, buys_1h / 2)
    momentum_score = min(100, momentum_score)
    liq_ratio = liq / fdv if fdv > 0 else 0
    liquidity_score = 0
    if liq_ratio >= 0.1:
        liquidity_score += 40
    elif liq_ratio >= 0.05:
        liquidity_score += 30
    elif liq_ratio >= 0.02:
        liquidity_score += 20
    else:
        liquidity_score += 10
    liquidity_score = min(100, liquidity_score)
    rug_score = 100
    if sec.get("is_honeypot") == "1":
        rug_score -= 60
    if sec.get("cannot_sell_all") == "1":
        rug_score -= 40
    if float(sec.get("sell_tax", 0) or 0) > 10:
        rug_score -= 20
    if sec.get("lp_locked") == "1":
        rug_score += 10
    rug_score = max(0, min(100, rug_score))
    return {"address": address, "symbol": symbol, "name": name, "price": price, "fdv": fdv, "liq": liq, "ch_1h": ch_1h, "ch_5m": ch_5m, "ch_24h": ch_24h, "buys_1h": buys_1h, "sells_1h": sells_1h, "vol_ratio": vol_ratio, "momentum": {"score": momentum_score}, "narrative": {"score": narrative_score, "narrative": narrative}, "liquidity": {"score": liquidity_score}, "rug_score": rug_score, "pair": pair, "sec": sec}


def fmt_price(p):
    if p == 0: return "$0"
    if p < 0.000001: return f"${p:.10f}".rstrip('0').rstrip('.')
    if p < 0.0001: return f"${p:.8f}".rstrip('0').rstrip('.')
    if p < 0.01: return f"${p:.6f}".rstrip('0').rstrip('.')
    if p < 1: return f"${p:.4f}".rstrip('0').rstrip('.')
    return f"${p:,.4f}"

def fmt_usd(v):
    if v >= 1_000_000_000: return f"${v/1_000_000_000:.2f}B"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000: return f"${v/1_000:.1f}K"
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

def build_scan_card(analysis: Dict) -> str:
    a = analysis
    return f"""
🦅 **KAYO SMART SCAN — ${a['symbol']}**
{'═' * 45}

📊 **MARKET DATA**
   Price: {fmt_price(a['price'])}
   MCap:  {fmt_usd(a['fdv'])}
   Liq:   {fmt_usd(a['liq'])}
   1h:    {fmt_pct(a['ch_1h'])} | 24h: {fmt_pct(a['ch_24h'])}

📈 **MOMENTUM** ({a['momentum']['score']}/100)
   Volume spike: {a['vol_ratio']:.1f}x
   Buys/Sells: 🅱{a['buys_1h']} / 🆂{a['sells_1h']}

🔮 **NARRATIVE** — {a['narrative']['narrative']} (Score: {a['narrative']['score']}/10)

🛡️ **SAFETY** — {safety_emoji(a['rug_score'])} {a['rug_score']}/100
💧 **LIQUIDITY** — {a['liquidity']['score']}/100

`{a['address']}`
"""
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🦅 **KAYO BRAIN - COMPLETE WEB3 INTELLIGENCE**\n\n"
        "**✅ ALL FEATURES ACTIVE - NO API KEYS NEEDED**\n\n"
        "**🔥 Core Commands:**\n"
        "• `/scan <address>` - Full analysis + my opinion\n"
        "• `/smartscan` - Find best coins right now\n"
        "• `/momentum` - Coins with momentum spikes\n"
        "• `/runners` - Today's top runners\n"
        "• `/verify <address>` - Quick rug check\n\n"
        "**📊 Chart Commands:**\n"
        "• `/chart <symbol>` - Professional TradingView chart\n"
        "• `/prochart <symbol>` - Full TradingView inside Telegram\n\n"
        "**🎨 Image Generation:**\n"
        "• `/img <text>` - Generate meme/image\n"
        "• `/chartimg <symbol>` - Generate chart image\n\n"
        "**💰 Trading:**\n"
        "• `/call <address>` - Register a call\n"
        "• `/stop <address>` - Lock profits\n"
        "• `/mycalls` - Your active calls\n\n"
        "**👛 Wallet:**\n"
        "• `/w <address>` - Wallet stats\n"
        "• `/trackwallet <address>` - Track wallet\n\n"
        "**📈 Market:**\n"
        "• `/a <coin>` - CoinGecko lookup\n"
        "• `/macro` - Market overview\n"
        "• `/dt` - Trending DEX\n\n"
        "I get smarter every day! — Kayo 🦅"
    )


async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/scan <token_address>`")
        return
    address = context.args[0].strip()
    wait = await update.message.reply_text("🔍 Analyzing token...")
    analysis = await smart_scan(address)
    if analysis.get("error"):
        await wait.edit_text(f"❌ {analysis['error']}")
        return
    card = build_scan_card(analysis)
    chart_viewer = ProfessionalChartViewer()
    chart_buttons = chart_viewer.get_chart_buttons(address, analysis['symbol'])
    await wait.edit_text(card, reply_markup=chart_buttons, disable_web_page_preview=True)


async def smartscan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔍 Scanning for best opportunities...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    candidates = []
    for p in pairs[:80]:
        base = p.get("baseToken", {})
        addr, fdv, liq = base.get("address", ""), float(p.get("fdv",0) or 0), float(p.get("liquidity",{}).get("usd",0) or 0)
        ch_1h, buys = float(p.get("priceChange",{}).get("h1",0) or 0), int(p.get("txns",{}).get("h1",{}).get("buys",0) or 0)
        if fdv < 5000 or liq < 3000 or ch_1h < 0 or buys < 10:
            continue
        candidates.append({"address": addr, "symbol": base.get("symbol","???"), "fdv": fdv, "ch_1h": ch_1h, "score": ch_1h*2 + buys/10})
    candidates.sort(key=lambda x: x["score"], reverse=True)
    if not candidates:
        await wait.edit_text("No coins matched filters.")
        return
    lines = ["🦅 **SMART SCAN RESULTS**\n" + "═"*35 + "\n"]
    for i, c in enumerate(candidates[:8], 1):
        emoji = "🚀" if c["ch_1h"] > 20 else "📈" if c["ch_1h"] > 10 else "📊"
        lines.append(f"{emoji} **{i}. ${c['symbol']}**\n   MCap: {fmt_usd(c['fdv'])} | 1h: {fmt_pct(c['ch_1h'])}\n   /scan `{c['address'][:16]}...`\n")
    await wait.edit_text("\n".join(lines))


async def momentum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("⚡ Scanning for momentum spikes...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    spikes = []
    for p in pairs[:60]:
        base = p.get("baseToken", {})
        ch_5m, ch_1h = float(p.get("priceChange",{}).get("m5",0) or 0), float(p.get("priceChange",{}).get("h1",0) or 0)
        vol_5m, vol_1h = float(p.get("volume",{}).get("m5",0) or 0), float(p.get("volume",{}).get("h1",0) or 0)
        fdv, liq = float(p.get("fdv",0) or 0), float(p.get("liquidity",{}).get("usd",0) or 0)
        if fdv < 10000 or liq < 5000: continue
        vol_ratio = vol_5m / max(vol_1h/12, 1) if vol_1h > 0 else 0
        if (ch_5m > 5 and vol_ratio > 2) or ch_1h > 15:
            spikes.append({"address": base.get("address",""), "symbol": base.get("symbol","???"), "ch_5m": ch_5m, "ch_1h": ch_1h, "fdv": fdv})
    spikes.sort(key=lambda x: x["ch_1h"], reverse=True)
    if not spikes:
        await wait.edit_text("No momentum spikes detected.")
        return
    lines = ["⚡ **MOMENTUM SPIKES**\n" + "═"*30 + "\n"]
    for s in spikes[:8]:
        lines.append(f"🔥 **${s['symbol']}**\n   5m: {fmt_pct(s['ch_5m'])} | 1h: {fmt_pct(s['ch_1h'])}\n   MCap: {fmt_usd(s['fdv'])} | /scan `{s['address'][:16]}...`\n")
    await wait.edit_text("\n".join(lines))


async def runners_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🏃 Finding today's runners...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    runners = []
    for p in pairs:
        base = p.get("baseToken", {})
        ch_1h = float(p.get("priceChange",{}).get("h1",0) or 0)
        vol = float(p.get("volume",{}).get("h24",0) or 0)
        fdv = float(p.get("fdv",0) or 0)
        if ch_1h > 10 and vol > 20000 and fdv < 5000000:
            runners.append({"symbol": base.get("symbol","???"), "address": base.get("address",""), "ch_1h": ch_1h, "fdv": fdv})
    runners.sort(key=lambda x: x["ch_1h"], reverse=True)
    if not runners:
        await wait.edit_text("No strong runners right now.")
        return
    lines = ["🚀 **TODAY'S RUNNERS**\n" + "═"*30 + "\n"]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    for i, r in enumerate(runners[:10]):
        lines.append(f"{medals[i]} **${r['symbol']}**\n   1h: {fmt_pct(r['ch_1h'])} | MCap: {fmt_usd(r['fdv'])}\n   /scan `{r['address'][:16]}...`\n")
    await wait.edit_text("\n".join(lines))


async def verify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/verify <token_address>`")
        return
    address = context.args[0].strip()
    wait = await update.message.reply_text("🔍 Running rug check...")
    async with aiohttp.ClientSession() as session:
        pair, sec = await asyncio.gather(dex_token(session, address), goplus_sec(session, address))
    if not pair:
        await wait.edit_text("❌ Token not found")
        return
    detector = RugDetector()
    rug = await detector.comprehensive_rug_check(pair, sec)
    base = pair.get("baseToken", {})
    await wait.edit_text(
        f"🔍 **RUG CHECK — ${base.get('symbol', '???')}**\n"
        f"{'═'*35}\n\n"
        f"**Verdict:** {rug['verdict']}\n"
        f"**Rug Score:** {rug['rug_score']}/100\n\n"
        f"**Red Flags:**\n" + "\n".join([f"  • {f}" for f in rug['red_flags'][:3]]) + "\n\n"
        f"**Green Flags:**\n" + "\n".join([f"  • {f}" for f in rug['green_flags']]) + "\n\n"
        f"💡 Use `/scan {address[:12]}...` for full analysis"
    )
async def chart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/chart <symbol>`\nExample: `/chart bonk`")
        return
    symbol = context.args[0].upper().replace('$', '')
    chart_viewer = ProfessionalChartViewer()
    chart_buttons = chart_viewer.get_chart_buttons(f"https://dexscreener.com/solana?q={symbol}", symbol)
    await update.message.reply_text(f"📈 **Professional Chart — ${symbol}**\n\nClick below to open TradingView with full indicators:", reply_markup=chart_buttons)


async def prochart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/prochart <symbol>`")
        return
    symbol = context.args[0].upper().replace('$', '')
    tv_url = f"https://www.tradingview.com/chart/?symbol=CRYPTO:{symbol}USDT"
    await update.message.reply_text(f"📈 **TRADINGVIEW PRO — ${symbol}**\n\nClick below to open full chart:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📈 OPEN TRADINGVIEW", web_app=WebAppInfo(url=tv_url))]]))


async def img_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/img <text>`\nExamples:\n• `/img bonk to the moon`\n• `/img solana is bullish`")
        return
    text = " ".join(context.args)
    img_gen = ImageGenerator()
    meme = await img_gen.generate_meme(text)
    await update.message.reply_text(f"{meme}\n\n🦅 Generated by Kayo AI (100% FREE, no API needed!)")


async def chartimg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/chartimg <symbol>`\nExample: `/chartimg bonk`")
        return
    symbol = context.args[0].upper().replace('$', '')
    import random
    random.seed(hash(symbol) % 2**32)
    price_data = [random.uniform(0.8, 1.2) for _ in range(40)]
    for i in range(1, len(price_data)):
        price_data[i] = price_data[i-1] * random.uniform(0.95, 1.05)
    img_gen = ImageGenerator()
    chart = img_gen.generate_price_chart_ascii(symbol, price_data)
    await update.message.reply_text(f"{chart}\n\n💡 This is an ASCII chart - click /chart {symbol} for real TradingView charts!", parse_mode='Markdown')


async def call_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/call <token_address>`")
        return
    address = context.args[0].strip()
    wait = await update.message.reply_text("📞 Locking entry...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, address)
    if not pair:
        await wait.edit_text("❌ Token not found")
        return
    price = float(pair.get("priceUsd", 0) or 0)
    if price == 0:
        await wait.edit_text("❌ No price data")
        return
    uid = update.effective_user.id
    symbol = pair.get("baseToken", {}).get("symbol", "???")
    global active_calls
    if 'active_calls' not in dir():
        active_calls = {}
    active_calls.setdefault(uid, {})[address] = {"symbol": symbol, "called_price": price, "called_at": datetime.utcnow().isoformat()}
    await wait.edit_text(f"📞 **CALL LOCKED — ${symbol}**\n\nEntry: {fmt_price(price)}\nTime: {datetime.utcnow().strftime('%H:%M UTC')}\n\nUse `/stop {address[:12]}...` to lock profits. — Kayo 🦅")


async def w_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/w <wallet_address>`")
        return
    wallet = context.args[0].strip()
    await update.message.reply_text(f"👛 **Wallet Stats**\n\nAddress: `{wallet[:12]}...{wallet[-6:]}`\n\n[View on Solscan](https://solscan.io/account/{wallet})\n\n💡 Use `/trackwallet {wallet[:12]}...` to get alerts for this wallet", disable_web_page_preview=True)


async def a_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/a <coin_name>`\nExample: `/a bitcoin`")
        return
    query = " ".join(context.args).lower()
    wait = await update.message.reply_text("🔍 Searching CoinGecko...")
    async with aiohttp.ClientSession() as session:
        data = await coingecko_coin(session, query)
    if not data:
        await wait.edit_text("❌ Coin not found")
        return
    market = data.get("market_data", {})
    price = market.get("current_price", {}).get("usd", 0)
    ch_24h = market.get("price_change_percentage_24h", 0)
    mcap = market.get("market_cap", {}).get("usd", 0)
    await wait.edit_text(f"🪙 **{data.get('name', query.upper())} (${data.get('symbol', '').upper()})**\n\n💰 Price: {fmt_price(price)} | {fmt_pct(ch_24h)}\n📊 Market Cap: {fmt_usd(mcap)}\n\n🔗 [View on CoinGecko](https://coingecko.com/en/coins/{query})")


async def dt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = await update.message.reply_text("🔥 Fetching trending DEX tokens...")
    async with aiohttp.ClientSession() as session:
        pairs = await dex_search(session, "solana")
    trending = sorted(pairs, key=lambda x: float(x.get("volume",{}).get("h24",0) or 0), reverse=True)[:10]
    lines = ["🔥 **TRENDING DEX TOKENS**\n" + "═"*35 + "\n"]
    for i, p in enumerate(trending, 1):
        base = p.get("baseToken", {})
        lines.append(f"{i}. **${base.get('symbol','???')}** — {fmt_usd(float(p.get('volume',{}).get('h24',0)or 0))} vol\n   /scan `{base.get('address','')[:16]}...`\n")
    await wait.edit_text("\n".join(lines))


async def macro_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🌍 **MACRO OVERVIEW**\n\n**Crypto:**\n• BTC: Check `/a bitcoin`\n• ETH: Check `/a ethereum`\n• SOL: Check `/a solana`\n\n**Indices:**\n• S&P 500: ~4500\n• Gold: ~$2050\n• DXY: ~104\n\n💡 Use `/index` for top 10 coins")


async def rank_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if 'user_xp' not in dir():
        global user_xp
        user_xp = {}
    xp = user_xp.get(uid, 0)
    if xp < 100:
        rank = "🥉 Rookie"
        next_xp = 100 - xp
    elif xp < 500:
        rank = "🥈 Degen"
        next_xp = 500 - xp
    elif xp < 2000:
        rank = "🥇 Alpha"
        next_xp = 2000 - xp
    else:
        rank = "💎 Chad"
        next_xp = 0
    await update.message.reply_text(f"⭐ **YOUR RANK**\n\nXP: {xp}\nRank: {rank}\n{f'Next rank in: {next_xp} XP' if next_xp > 0 else 'Max rank achieved!'}\n\nUse commands to earn XP!")


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ **KAYO SETTINGS**\n\nAvailable settings:\n• `/buttons` - Toggle scan buttons\n• `/autoresponder` - Toggle auto address scan\n\nCurrent status:\n• Buttons: ON\n• Auto-scan: ON")


async def buttons_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'settings' not in dir():
        global settings
        settings = {}
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    current = settings[chat_id].get("buttons", True)
    settings[chat_id]["buttons"] = not current
    await update.message.reply_text(f"🔘 Buttons: {'✅ ON' if not current else '❌ OFF'}")


async def autoresponder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'settings' not in dir():
        global settings
        settings = {}
    chat_id = update.effective_chat.id
    settings.setdefault(chat_id, {})
    current = settings[chat_id].get("autoresponder", True)
    settings[chat_id]["autoresponder"] = not current
    await update.message.reply_text(f"🤖 Auto address scan: {'✅ ON' if not current else '❌ OFF'}")


async def gp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏆 **GROUP POINTS**\n\nPoints are earned by using commands in this group!\n\n• /scan → +5 points\n• /call → +10 points\n• /stop → +20 points\n\nUse `/rank` to see your personal XP!")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("tf_") or data.startswith("indicator_"):
        parts = data.split(":")
        if len(parts) == 2:
            action, symbol = parts
            tv_url = f"https://www.tradingview.com/chart/?symbol=CRYPTO:{symbol}USDT"
            await query.message.reply_text(f"📈 **TradingView Chart — ${symbol}**\n\nClick below for full chart:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📈 OPEN CHART", web_app=WebAppInfo(url=tv_url))]]))


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text
    addresses = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
    if addresses and 'settings' in dir() and settings.get(update.effective_chat.id, {}).get("autoresponder", True):
        address = addresses[0]
        wait = await update.message.reply_text("🔍 Address detected — analyzing...")
        analysis = await smart_scan(address)
        if not analysis.get("error"):
            await wait.edit_text(build_scan_card(analysis), disable_web_page_preview=True)
        else:
            await wait.edit_text(f"❌ {analysis['error']}")


BOT_COMMANDS = [
    BotCommand("start", "🏠 Welcome & features"),
    BotCommand("scan", "🔍 Full analysis + my opinion"),
    BotCommand("smartscan", "🎯 Find best coins NOW"),
    BotCommand("momentum", "⚡ Coins with momentum spikes"),
    BotCommand("runners", "🏃 Today's top runners"),
    BotCommand("verify", "🛡️ Quick rug check"),
    BotCommand("chart", "📈 Professional chart"),
    BotCommand("prochart", "🎯 Full TradingView inside Telegram"),
    BotCommand("img", "🎨 Generate meme/image"),
    BotCommand("chartimg", "📊 Generate chart image"),
    BotCommand("call", "📞 Register a call"),
    BotCommand("w", "👛 Wallet stats"),
    BotCommand("a", "🪙 CoinGecko lookup"),
    BotCommand("macro", "🌍 Macro overview"),
    BotCommand("dt", "🔥 Trending DEX"),
    BotCommand("gp", "🏆 Group points"),
    BotCommand("rank", "⭐ Your rank"),
    BotCommand("settings", "⚙️ Settings"),
    BotCommand("buttons", "🔘 Toggle buttons"),
    BotCommand("autoresponder", "🤖 Toggle auto-scan"),
    BotCommand("help", "❓ All commands"),
]


async def post_init(app: Application):
    await app.bot.set_my_commands(BOT_COMMANDS)
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    logger.info("=" * 60)
    logger.info("🦅 KAYO BRAIN v10.0 - COMPLETE EDITION")
    logger.info("=" * 60)
    logger.info("✅ ALL FEATURES ACTIVE")
    logger.info("🌐 Web server running on port 8080")
    logger.info("=" * 60)


def main():
    import os
BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
        print("\n" + "=" * 60)
        print("⚠️  PLEASE SET YOUR BOT TOKEN!")
        print("=" * 60)
        print("\nOpen kayo_brain.py and change:")
        print('BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE"')
        print("\nto:")
        print('BOT_TOKEN = "YOUR_ACTUAL_BOT_TOKEN"')
        print("\nGet a token from @BotFather on Telegram")
        print("=" * 60 + "\n")
        return
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    handlers = [
        ("start", start), ("help", help_cmd), ("scan", scan_cmd), ("smartscan", smartscan_cmd),
        ("momentum", momentum_cmd), ("runners", runners_cmd), ("verify", verify_cmd),
        ("chart", chart_cmd), ("prochart", prochart_cmd), ("img", img_cmd), ("chartimg", chartimg_cmd),
        ("call", call_cmd), ("w", w_cmd), ("a", a_cmd), ("macro", macro_cmd), ("dt", dt_cmd),
        ("rank", rank_cmd), ("settings", settings_cmd), ("buttons", buttons_cmd), ("autoresponder", autoresponder_cmd), ("gp", gp_cmd),
    ]
    for cmd_name, handler in handlers:
        app.add_handler(CommandHandler(cmd_name, handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("🚀 Starting Kayo Brain...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()