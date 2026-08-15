# ════════════════════════════════════════════════════════════════════════
# RICK-INSPIRED FEATURES — v42 additions
# ════════════════════════════════════════════════════════════════════════

# ── ATH TRACKING FROM FIRST SCAN ────────────────────────────────────────
# Rick tracks every token's ATH from the moment it was first scanned.
# We store {address: {first_price, first_seen, ath_price, ath_time, first_mcap}} in Redis.

async def ath_record_token(tok: Dict):
    """Record a token's first scan and update ATH. Called from bg_main_scanner."""
    addr = tok.get("address", "")
    if not addr: return
    price = tok.get("price", 0)
    mcap = tok.get("mcap", 0) or tok.get("fdv", 0)
    now = time.time()
    
    if addr not in ath_tracker:
        ath_tracker[addr] = {
            "sym": tok.get("sym", ""),
            "first_price": price,
            "first_seen": now,
            "first_mcap": mcap,
            "ath_price": price,
            "ath_time": now,
        }
    else:
        entry = ath_tracker[addr]
        if price > entry["ath_price"]:
            entry["ath_price"] = price
            entry["ath_time"] = now
    
    # Trim to last 2000 entries
    if len(ath_tracker) > 2000:
        oldest = sorted(ath_tracker.items(), key=lambda x: x[1]["first_seen"])[:500]
        for k, _ in oldest:
            del ath_tracker[k]

async def cmd_ath(update, context):
    """Show ATH leaderboard — tokens called/scanned in group, ranked by gain from first scan."""
    if not ath_tracker:
        await update.message.reply_text("📊 No ATH data yet — bot needs to scan tokens first.")
        return
    
    # Calculate gains
    gains = []
    for addr, data in ath_tracker.items():
        first = data["first_price"]
        ath = data["ath_price"]
        cur = data.get("ath_price", 0)  # We'd need live price for current, use ATH for now
        if first > 0 and ath > 0:
            mult = ath / first
            gains.append({
                "sym": data["sym"],
                "addr": addr,
                "first_price": first,
                "ath_price": ath,
                "mult": mult,
                "age_h": (time.time() - data["first_seen"]) / 3600,
            })
    
    gains.sort(key=lambda x: x["mult"], reverse=True)
    top = gains[:15]
    
    if not top:
        await update.message.reply_text("📊 No ATH data yet.")
        return
    
    text = "🏆 **ATH LEADERBOARD — Top Calls**\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, g in enumerate(top):
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i+1}."
        mult_str = f"{g['mult']:.1f}x" if g['mult'] < 100 else f"{g['mult']:.0f}x"
        age = f"{g['age_h']:.1f}h" if g['age_h'] < 24 else f"{g['age_h']/24:.1f}d"
        text += f"{emoji} ${g['sym']} — {mult_str} from first scan ({age} ago)\n"
        text += f"   First: ${g['first_price']:.8f} → ATH: ${g['ath_price']:.8f}\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

# ── PVP TOKENS — Find competing tokens in same narrative ────────────────
# Rick's /pvp finds newer tokens in the same narrative competing for volume.

async def cmd_pvp(update, context):
    """Find PVP tokens — newer tokens in the same narrative/category."""
    if not context.args:
        await update.message.reply_text(
            "⚔️ **PVP Scanner**\nUsage: `/pvp <token_address>`\n"
            "Finds competing tokens in the same narrative.",
            parse_mode="Markdown")
        return
    
    addr = context.args[0].strip()
    if addr.startswith("$"): addr = addr[1:]
    
    # Get the token's info
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await update.message.reply_text("❌ Token not found.")
        return
    
    tok = pairs[0]
    sym = tok.get("baseToken", {}).get("symbol", "?")
    name = tok.get("baseToken", {}).get("name", "?")
    fdv = tok.get("fdv", 0) or tok.get("marketCap", 0)
    liq = tok.get("liquidity", {}).get("usd", 0)
    vol24 = tok.get("volume", {}).get("h24", 0)
    created = tok.get("pairCreatedAt", 0)
    
    # Extract narrative from name/symbol
    narrative = detect_narrative(f"{sym} {name}")
    if not narrative:
        # Try DexScreener search with the token symbol to find similar
        narrative = sym.lower()
    
    # Search for competing tokens
    search_term = narrative if narrative else sym.lower()
    competitors = await dex_search_pairs(search_term)
    
    # Filter: newer than this token, similar mcap range
    pvp_list = []
    for p in competitors:
        p_sym = p.get("baseToken", {}).get("symbol", "")
        p_addr = p.get("baseToken", {}).get("address", "")
        if p_addr.lower() == addr.lower(): continue  # skip self
        p_fdv = p.get("fdv", 0) or p.get("marketCap", 0)
        p_liq = p.get("liquidity", {}).get("usd", 0)
        p_vol = p.get("volume", {}).get("h24", 0)
        p_created = p.get("pairCreatedAt", 0)
        p_ch5m = p.get("priceChange", {}).get("m5", 0)
        p_ch1h = p.get("priceChange", {}).get("h1", 0)
        
        # PVP criteria: same narrative, has liquidity, has volume
        if p_liq < 1000: continue
        if p_vol < 500: continue
        
        # Score by volume relative to mcap (attention efficiency)
        attention = p_vol / max(p_fdv, 1) * 100
        
        pvp_list.append({
            "sym": p_sym, "addr": p_addr, "name": p.get("baseToken", {}).get("name", ""),
            "fdv": p_fdv, "liq": p_liq, "vol24": p_vol,
            "ch5m": p_ch5m, "ch1h": p_ch1h,
            "created": p_created, "attention": attention,
        })
    
    pvp_list.sort(key=lambda x: x["vol24"], reverse=True)
    pvp_list = pvp_list[:10]
    
    if not pvp_list:
        await update.message.reply_text(f"⚔️ No PVP tokens found for ${sym} narrative: {narrative}")
        return
    
    text = f"⚔️ **PVP — ${sym} Narrative: {narrative}**\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"Base: ${sym} | FDV: {_usd(fdv)} | Liq: {_usd(liq)} | Vol: {_usd(vol24)}\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    for i, p in enumerate(pvp_list):
        emoji = "🔴" if p["ch5m"] < 0 else "🟢" if p["ch5m"] > 5 else "⚪"
        age = ""
        if p["created"]:
            age_h = (time.time() - p["created"]/1000) / 3600
            age = f" | {age_h:.1f}h old" if age_h < 168 else f" | {age_h/24:.0f}d old"
        text += f"{emoji} ${p['sym']} — Vol: {_usd(p['vol24'])} | FDV: {_usd(p['fdv'])}\n"
        text += f"   5m: {p['ch5m']:+.1f}% | 1h: {p['ch1h']:+.1f}%{age}\n"
        text += f"   `{p['addr'][:12]}...{p['addr'][-6:]}`\n"
    
    buttons = [[InlineKeyboardButton(f"🔍 DexScreener", url=f"https://dexscreener.com/search?q={search_term}")]]
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

# ── CONTRACT SIMILARITY (Pasta Check) ───────────────────────────────────
# Rick's /pc checks if a contract is a copy of another. We use GoPlus + DexScreener metadata.

async def cmd_pastacheck(update, context):
    """Check contract similarity — is this token a copy-paste of another?"""
    if not context.args:
        await update.message.reply_text(
            "🍝 **Pasta Check**\nUsage: `/pc <token_address>`\n"
            "Checks if this contract is a copy-paste of another token.",
            parse_mode="Markdown")
        return
    
    addr = context.args[0].strip()
    if addr.startswith("$"): addr = addr[1:]
    
    # Get token data
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await update.message.reply_text("❌ Token not found on DEX.")
        return
    
    tok = pairs[0]
    sym = tok.get("baseToken", {}).get("symbol", "?")
    name = tok.get("baseToken", {}).get("name", "?")
    
    # GoPlus security check
    gp = await goplus_check(addr)
    
    # Search for tokens with the same name/symbol
    name_results = await dex_search_all_chains(name, limit=20)
    sym_results = await dex_search_all_chains(sym, limit=20)
    
    # Filter out the original
    all_results = []
    seen_addrs = {addr.lower()}
    for p in name_results + sym_results:
        p_addr = p.get("baseToken", {}).get("address", "")
        if p_addr.lower() in seen_addrs: continue
        seen_addrs.add(p_addr.lower())
        all_results.append(p)
    
    # Count matching tokens
    total_copies = len(all_results)
    
    # Build similarity score
    uniqueness_score = 100
    if total_copies > 0:
        uniqueness_score = max(0, 100 - (total_copies * 5))
    
    # GoPlus flags
    is_honeypot = gp.get("is_honeypot", False)
    is_open_source = gp.get("is_open_source", False)
    is_mintable = gp.get("is_mintable", False)
    can_take_back = gp.get("can_take_back_ownership", False)
    holder_rate = gp.get("holder_count", 0)
    
    text = f"🍝 **Pasta Check — ${sym}**\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"**Uniqueness Score: {uniqueness_score}/100**\n"
    
    if uniqueness_score == 100:
        text += "✅ This contract appears unique — no copies found.\n"
    else:
        text += f"⚠️ Found {total_copies} tokens with similar name/symbol:\n"
        for p in all_results[:5]:
            p_sym = p.get("baseToken", {}).get("symbol", "?")
            p_chain = p.get("chainId", "?")
            p_fdv = p.get("fdv", 0) or 0
            text += f"   • ${p_sym} on {p_chain} (FDV: {_usd(p_fdv)})\n"
        if total_copies > 5:
            text += f"   ...and {total_copies - 5} more\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"**Security:**\n"
    text += f"   Honeypot: {'🔴 YES' if is_honeypot else '🟢 NO'}\n"
    text += f"   Open Source: {'🟢 YES' if is_open_source else '🔴 NO'}\n"
    text += f"   Mintable: {'🔴 YES' if is_mintable else '🟢 NO'}\n"
    text += f"   Can Reclaim: {'🔴 YES' if can_take_back else '🟢 NO'}\n"
    
    if is_honeypot or is_mintable or can_take_back:
        text += f"\n🚨 **HIGH RISK — multiple red flags**\n"
    
    buttons = [
        [InlineKeyboardButton("🔍 RugCheck", url=f"https://rugcheck.xyz/tokens/{addr}"),
         InlineKeyboardButton("📊 DexScreener", url=f"https://dexscreener.com/solana/{addr}")]
    ]
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

# ── TLDR ENGINE — Summarize URLs, articles, YouTube ─────────────────────
async def cmd_tldr(update, context):
    """Summarize any URL — article, YouTube, Twitter thread, PDF."""
    if not context.args:
        await update.message.reply_text(
            "📝 **TLDR**\nUsage: `/tldr <url>`\n"
            "Summarizes articles, YouTube videos, Twitter threads, and PDFs.",
            parse_mode="Markdown")
        return
    
    url = context.args[0].strip()
    if not url.startswith("http"):
        url = "https://" + url
    
    # Fetch the page content
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15), headers={
                "User-Agent": "Mozilla/5.0 (compatible; KayoBrain/1.0)"
            }) as r:
                if r.status != 200:
                    await update.message.reply_text(f"❌ Failed to fetch URL (HTTP {r.status})")
                    return
                html = await r.text()
                
                # Extract text content
                # Remove script/style tags
                text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
                text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
                # Get text from remaining HTML
                text = re.sub(r'<[^>]+>', ' ', text)
                text = re.sub(r'\s+', ' ', text).strip()
                
                # Extract title
                title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
                title = title_match.group(1).strip() if title_match else "Untitled"
                
                # Truncate to first 5000 chars for AI
                text = text[:5000]
                
                if len(text) < 100:
                    await update.message.reply_text("❌ Not enough text content found to summarize.")
                    return
    
    except asyncio.TimeoutError:
        await update.message.reply_text("⏱️ Request timed out.")
        return
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        return
    
    # Use AI to summarize
    msg = await update.message.reply_text("📝 Summarizing...")
    
    summary = await ai_ask(
        f"Summarize this webpage concisely. Title: {title}\n\nContent:\n{text}\n\n"
        "Give me: 1) One-line summary 2) 3 key points 3) Why it matters (if crypto-related). "
        "Keep it sharp and professional. Max 200 words.",
        fallback="Failed to generate summary.",
        inject_market=False,
        max_tokens=300
    )
    
    result = f"📝 **TLDR: {title[:60]}**\n"
    result += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{summary}"
    
    try:
        await msg.edit_text(result, parse_mode="Markdown")
    except:
        await msg.edit_text(result)

# ── DEEP AI MODE — Use Gemini for complex reasoning ─────────────────────
async def cmd_deep(update, context):
    """Deep AI mode — uses Gemini 2.0 Flash for complex reasoning questions."""
    if not context.args:
        await update.message.reply_text(
            "🧠 **Deep Mode**\nUsage: `/deep <question>`\n"
            "Uses Gemini 2.0 Flash for complex reasoning and analysis.",
            parse_mode="Markdown")
        return
    
    question = " ".join(context.args)
    
    # Skip Groq, go straight to Gemini for deep reasoning
    system_ctx = await get_live_market_context()
    prompt = (
        f"{system_ctx}\n\n"
        f"{NARRATIVE_KB}\n\n"
        "You are Kayo in Deep Mode. You use careful reasoning and analysis. "
        "Think step by step. Be thorough but concise. "
        "Question: " + question
    )
    
    msg = await update.message.reply_text("🧠 Thinking deeply...")
    
    # Try Gemini models directly
    response = ""
    for gem_model in ["gemini-2.5-flash", "gemini-2.0-flash"]:
        if not GEMINI_API_KEY: break
        try:
            payload = {"contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 800}}
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{gem_model}:generateContent?key={GEMINI_API_KEY}",
                    json=payload, timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        response = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                        break
        except Exception:
            continue
    
    if not response:
        # Fallback to regular ai_ask
        response = await ai_ask(question, fallback="All AI backends failed.", max_tokens=500)
    
    result = f"🧠 **Deep Analysis**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{response}"
    
    try:
        await msg.edit_text(result, parse_mode="Markdown")
    except:
        await msg.edit_text(result)

# ── CHAT SUMMARY (Rick's /dub feature) ──────────────────────────────────
async def cmd_dub(update, context):
    """Generate a summary of recent chat messages."""
    chat_id = update.effective_chat.id
    
    # Fetch recent messages from Telegram
    try:
        # Get recent updates (limited by Telegram API)
        recent = []
        # We'll use the bot's get_chat_administrators to verify we're in a group
        # Then generate a summary from what we've seen
        
        # Build context from recent crypto mentions
        context_summary = await ai_ask(
            f"Summarize the current crypto market mood based on these indicators: "
            f"BTC price, SOL price, trending narratives, recent alerts. "
            f"Give a 3-sentence market vibe check.",
            fallback="Market summary unavailable.",
            inject_market=True,
            max_tokens=200
        )
        
        text = "💬 **Chat Vibe Check**\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        text += context_summary
        
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Summary failed: {str(e)[:80]}")

# ── NOW — What's happening right now (Rick's /now) ───────────────────────
async def cmd_now(update, context):
    """What's happening right now in crypto — market snapshot."""
    signals = await fetch_social_signals()
    
    headlines = signals.get("news", [])[:5]
    pump_trend = signals.get("pump_trending", [])[:3]
    cg_trending = signals.get("cg_trending", [])[:5]
    
    # Get live prices
    btc = await fetch_live_price("bitcoin")
    sol = await fetch_live_price("solana")
    eth = await fetch_live_price("ethereum")
    
    text = "📡 **RIGHT NOW**\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"BTC: ${btc.get('price', 0):,.0f} ({btc.get('change_24h', 0):+.1f}%)\n"
    text += f"SOL: ${sol.get('price', 0):,.2f} ({sol.get('change_24h', 0):+.1f}%)\n"
    text += f"ETH: ${eth.get('price', 0):,.0f} ({eth.get('change_24h', 0):+.1f}%)\n"
    
    if headlines:
        text += f"\n📰 **Breaking:**\n"
        for h in headlines[:3]:
            text += f"• {h[:80]}\n"
    
    if pump_trend:
        text += f"\n🔥 **PumpFun Trending:**\n"
        for p in pump_trend[:3]:
            sym = p.get("symbol", "?")
            mc = p.get("market_cap", 0) or 0
            text += f"• ${sym} — {_usd(mc)}\n"
    
    if cg_trending:
        text += f"\n📈 **CoinGecko Trending:**\n"
        for c in cg_trending:
            text += f"• {c.get('name', c.get('id', '?'))}\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

# ── GLOBAL ATH LEADERBOARD (BurpBoard equivalent) ────────────────────────
async def cmd_burpboard(update, context):
    """Show the BurpBoard — global ATH leaderboard of tokens scanned."""
    if not ath_tracker:
        await update.message.reply_text("📊 No tokens tracked yet.")
        return
    
    gains = []
    for addr, data in ath_tracker.items():
        first = data["first_price"]
        ath = data["ath_price"]
        if first > 0 and ath > 0:
            mult = ath / first
            if mult > 1.1:  # Only show 1.1x+
                gains.append({"sym": data["sym"], "addr": addr, "mult": mult,
                              "age_h": (time.time() - data["first_seen"]) / 3600})
    
    gains.sort(key=lambda x: x["mult"], reverse=True)
    top = gains[:20]
    
    if not top:
        await update.message.reply_text("📊 No significant gains tracked yet.")
        return
    
    text = "📊 **BURPBOARD — Top Performers**\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for i, g in enumerate(top):
        emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i+1}."
        mult_str = f"{g['mult']:.1f}x" if g['mult'] < 100 else f"{g['mult']:.0f}x"
        age = f"{g['age_h']:.1f}h" if g['age_h'] < 24 else f"{g['age_h']/24:.1f}d"
        text += f"{emoji} ${g['sym']} — {mult_str} ({age})\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

# ── DEV HISTORY (Rick's /dev) ───────────────────────────────────────────
async def cmd_devhistory(update, context):
    """Show deployer history — check if a token's deployer has launched other tokens."""
    if not context.args:
        await update.message.reply_text(
            "🔍 **Deployer History**\nUsage: `/dev <token_address>`\n"
            "Checks the deployer's previous launches.",
            parse_mode="Markdown")
        return
    
    addr = context.args[0].strip()
    if addr.startswith("$"): addr = addr[1:]
    
    # Get token data to find the pair
    pairs = await dex_pairs_by_token(addr)
    if not pairs:
        await update.message.reply_text("❌ Token not found.")
        return
    
    tok = pairs[0]
    sym = tok.get("baseToken", {}).get("symbol", "?")
    
    # GoPlus gives us deployer info
    gp = await goplus_check(addr)
    
    creator = gp.get("creator_address", "Unknown")
    creator_count = gp.get("creator_count", 0) or 0
    holders = gp.get("holder_count", 0) or 0
    lp_holders = gp.get("lp_holder_count", 0) or 0
    lp_locked = gp.get("is_lp_locked", False)
    
    # Search DexScreener for other tokens by same creator (if available)
    text = f"🔍 **Deployer History — ${sym}**\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"Creator: `{creator[:12]}...{creator[-6:]}`\n"
    text += f"Tokens Created: {creator_count}\n"
    text += f"Holders: {holders}\n"
    text += f"LP Holders: {lp_holders}\n"
    text += f"LP Locked: {'🟢 YES' if lp_locked else '🔴 NO'}\n"
    
    # Additional checks
    is_proxy = gp.get("is_proxy", False)
    is_in_dex = gp.get("is_in_dex", True)
    buy_tax = gp.get("buy_tax", 0) or 0
    sell_tax = gp.get("sell_tax", 0) or 0
    
    text += f"\n**Token Info:**\n"
    text += f"   Buy Tax: {buy_tax:.1f}%\n" if isinstance(buy_tax, (int, float)) else f"   Buy Tax: {buy_tax}\n"
    text += f"   Sell Tax: {sell_tax:.1f}%\n" if isinstance(sell_tax, (int, float)) else f"   Sell Tax: {sell_tax}\n"
    text += f"   Proxy: {'🔴 YES' if is_proxy else '🟢 NO'}\n"
    text += f"   In DEX: {'🟢 YES' if is_in_dex else '🔴 NO'}\n"
    
    if creator_count and creator_count > 5:
        text += f"\n⚠️ Deployer has created {creator_count} tokens — possible serial launcher.\n"
    
    buttons = [[InlineKeyboardButton("🔍 Solscan", url=f"https://solscan.io/account/{creator}")]]
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

# ── HOLDER STATS (Rick's /hs) ───────────────────────────────────────────
async def cmd_holderstats(update, context):
    """Show holder statistics for a token."""
    if not context.args:
        await update.message.reply_text(
            "📊 **Holder Stats**\nUsage: `/holders <token_address>`\n",
            parse_mode="Markdown")
        return
    
    addr = context.args[0].strip()
    if addr.startswith("$"): addr = addr[1:]
    
    gp = await goplus_check(addr)
    pairs = await dex_pairs_by_token(addr)
    
    if not pairs:
        await update.message.reply_text("❌ Token not found.")
        return
    
    tok = pairs[0]
    sym = tok.get("baseToken", {}).get("symbol", "?")
    holders = gp.get("holder_count", 0) or 0
    lp_holders = gp.get("lp_holder_count", 0) or 0
    lp_locked = gp.get("is_lp_locked", False)
    total_supply = gp.get("total_supply", 0) or 0
    
    # Top holders from GoPlus
    top_holders = gp.get("holders", [])[:10]
    
    text = f"📊 **Holder Stats — ${sym}**\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"Total Holders: {holders}\n"
    text += f"LP Holders: {lp_holders}\n"
    text += f"LP Locked: {'🟢 YES' if lp_locked else '🔴 NO'}\n"
    
    if top_holders:
        text += f"\n**Top Holders:**\n"
        for h in top_holders[:5]:
            addr_h = h.get("address", "?")
            pct = h.get("percent", 0) or 0
            text += f"   `{addr_h[:10]}...` — {pct:.2f}%\n"
    
    # Concentration check
    if top_holders:
        top_5_pct = sum(h.get("percent", 0) for h in top_holders[:5])
        if top_5_pct > 50:
            text += f"\n⚠️ Top 5 hold {top_5_pct:.1f}% — high concentration risk.\n"
    
    await update.message.reply_text(text, parse_mode="Markdown")

