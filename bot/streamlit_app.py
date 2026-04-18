"""
MandiMax — Streamlit App for Databricks Apps
Replaces Flask backend + React frontend with one native Databricks app.
- Direct Unity Catalog SQL access (no API tokens needed)
- Sarvam AI multilingual chat
- Real APMC data from gold tables
"""

import os, json, re, math, requests
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MandiMax | FPO Intelligence Dashboard",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config ────────────────────────────────────────────────────────────────────
HTTP_PATH    = os.getenv("DATABRICKS_SQL_HTTP_PATH", "/sql/1.0/warehouses/8e4efaaf06a99681")
SARVAM_KEY   = os.getenv("SARVAM_API_KEY", "")
UC_CATALOG   = os.getenv("DATABRICKS_CATALOG", "workspace")

# Databricks SDK config — service principal auth is automatic inside Databricks Apps
_cfg = Config()

# ── CSS styling ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .stApp { background: linear-gradient(135deg, #0a0f1e 0%, #0d1b2a 100%); color: #e9edef; }
  .metric-card {
    background: linear-gradient(135deg, rgba(37,211,102,0.08), rgba(18,140,126,0.06));
    border: 1px solid rgba(37,211,102,0.2); border-radius: 12px;
    padding: 16px 20px; text-align: center; margin: 4px;
  }
  .metric-val { font-size: 2rem; font-weight: 700; color: #25d366; }
  .metric-lbl { font-size: 0.75rem; color: #8696a0; margin-top: 4px; }
  .chat-user  { background: linear-gradient(135deg,#005c4b,#128c7e); padding: 10px 14px;
                border-radius: 12px 12px 4px 12px; margin: 6px 0 6px 60px; color:#e9edef; }
  .chat-bot   { background: #1f2c34; padding: 10px 14px;
                border-radius: 12px 12px 12px 4px; margin: 6px 60px 6px 0; color:#e9edef; }
  .stButton>button { background: linear-gradient(135deg,#25d366,#128c7e) !important;
    color:white !important; border:none !important; border-radius:8px !important; font-weight:600 !important; }
  div[data-testid="stMetricValue"] { color: #25d366 !important; font-weight: 700 !important; }
  .section-header { color: #25d366; font-size: 1.1rem; font-weight: 600;
                    border-bottom: 1px solid rgba(37,211,102,0.3); padding-bottom: 6px; margin-bottom: 16px; }
</style>
""", unsafe_allow_html=True)


# ── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    """Connect to Databricks SQL — inside Databricks Apps, auth is automatic."""
    return sql.connect(
        server_hostname=_cfg.host,
        http_path=HTTP_PATH,
        credentials_provider=_cfg.authenticate,
    )

def run_sql(query: str) -> pd.DataFrame:
    """Run SQL against Unity Catalog gold tables with automatic fallback."""
    try:
        conn   = get_conn()
        cursor = conn.cursor()
        cursor.execute(f"USE CATALOG {UC_CATALOG}")
        cursor.execute("USE SCHEMA gold")
        cursor.execute(query)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.warning(f"⚠️ DB: {e}")
        return pd.DataFrame()


# ── Mock fallback data ────────────────────────────────────────────────────────
MOCK_ARB = pd.DataFrame([
    {"src_market":"AHMEDABAD","tgt_market":"SURAT","src_commodity":"Tomato","src_price":2100,"tgt_price":10000,"arbitrage_spread":7900,"distance_km":208,"tgt_lat":21.17,"tgt_lon":72.83},
    {"src_market":"LUDHIANA","tgt_market":"ROHTAK","src_commodity":"Tomato","src_price":2000,"tgt_price":9000,"arbitrage_spread":7000,"distance_km":234,"tgt_lat":28.90,"tgt_lon":76.61},
    {"src_market":"BAREILLY","tgt_market":"MATHURA","src_commodity":"Tomato","src_price":1500,"tgt_price":8200,"arbitrage_spread":6700,"distance_km":198,"tgt_lat":27.49,"tgt_lon":77.67},
    {"src_market":"GORAKHPUR","tgt_market":"SULTANPUR","src_commodity":"Tomato","src_price":1200,"tgt_price":7875,"arbitrage_spread":6675,"distance_km":141,"tgt_lat":26.26,"tgt_lon":82.07},
    {"src_market":"ALLAHABAD","tgt_market":"LUCKNOW","src_commodity":"Tomato","src_price":1600,"tgt_price":8000,"arbitrage_spread":6400,"distance_km":181,"tgt_lat":26.85,"tgt_lon":80.95},
])
MOCK_CROPS = pd.DataFrame([
    {"Commodity":"Rice","avg_price":3245,"stability":97.2,"state":"UTTAR PRADESH"},
    {"Commodity":"Potato","avg_price":1166,"stability":95.5,"state":"UTTAR PRADESH"},
    {"Commodity":"Onion","avg_price":1205,"stability":95.3,"state":"UTTAR PRADESH"},
    {"Commodity":"Wheat","avg_price":2180,"stability":94.1,"state":"MADHYA PRADESH"},
    {"Commodity":"Tomato","avg_price":4200,"stability":88.7,"state":"MAHARASHTRA"},
])


# ── Sarvam AI helpers ─────────────────────────────────────────────────────────
def sarvam_translate(text: str, src: str, tgt: str) -> str:
    if not SARVAM_KEY or src == tgt: return text
    try:
        r = requests.post("https://api.sarvam.ai/translate",
            headers={"api-subscription-key": SARVAM_KEY},
            json={"input": text, "source_language_code": src,
                  "target_language_code": tgt, "mode": "formal"},
            timeout=8)
        return r.json().get("translated_text", text)
    except: return text

def sarvam_llm(system: str, user: str, lang: str = "en-IN") -> str | None:
    if not SARVAM_KEY: return None
    try:
        r = requests.post("https://api.sarvam.ai/v1/chat/completions",
            headers={"api-subscription-key": SARVAM_KEY, "Content-Type": "application/json"},
            json={"model": "sarvam-m", "messages": [
                {"role": "system", "content": f"{system}\nRespond ONLY in language '{lang}'. Use simple farmer-friendly language."},
                {"role": "user",   "content": user}
            ], "max_tokens": 400, "temperature": 0.4}, timeout=15)
        return r.json()["choices"][0]["message"]["content"].strip()
    except: return None


# ── Recommendation engine ─────────────────────────────────────────────────────
CITY_COORDS = {
    "ujjain":(23.18,75.78),"indore":(22.72,75.86),"bhopal":(23.26,77.41),
    "lucknow":(26.85,80.95),"jaipur":(26.91,75.79),"pune":(18.52,73.86),
    "mumbai":(19.08,72.88),"delhi":(28.61,77.21),"ahmedabad":(23.02,72.57),
    "surat":(21.17,72.83),"nagpur":(21.15,79.09),"kanpur":(26.45,80.33),
    "patna":(25.59,85.14),"kolkata":(22.57,88.36),"varanasi":(25.32,82.97),
    "gorakhpur":(26.76,83.37),"allahabad":(25.44,81.85),"agra":(27.18,78.01),
}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    d = lambda x: x * math.pi / 180
    a = math.sin(d(lat2-lat1)/2)**2 + math.cos(d(lat1))*math.cos(d(lat2))*math.sin(d(lon2-lon1)/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 1)

def get_recommendations(commodity, city, rate, qty):
    df = run_sql(f"""
        SELECT tgt_market, tgt_district, tgt_state, tgt_price, tgt_lat, tgt_lon
        FROM arbitrage_opportunities_latest
        WHERE LOWER(src_commodity) = LOWER('{commodity}')
        ORDER BY tgt_price DESC LIMIT 15
    """)
    if df.empty:
        df = MOCK_ARB[MOCK_ARB["src_commodity"].str.lower() == commodity.lower()].copy()
        df["tgt_district"] = df["tgt_market"]

    coords = CITY_COORDS.get(city.lower(), (22.72, 75.86))
    results = []
    for _, r in df.iterrows():
        if not r.get("tgt_lat"): continue
        dist      = haversine(coords[0], coords[1], float(r["tgt_lat"]), float(r["tgt_lon"]))
        price     = float(r["tgt_price"])
        transport = round(dist * rate, 0)
        net       = round(price - transport, 0)
        if net > 0 and dist <= 250:
            results.append({"market": r["tgt_market"], "dist": dist,
                            "price": price, "transport": transport, "net": net,
                            "total": round(net * qty, 0)})
    return sorted(results, key=lambda x: -x["net"])[:3]


# ── Sidebar navigation ────────────────────────────────────────────────────────
st.sidebar.markdown("## 🌾 MandiMax")
st.sidebar.markdown("*FPO Intelligence Platform*")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigate", ["📊 Dashboard", "🤖 MandiMax Bot", "🌱 Crop Advisory"], label_visibility="collapsed")
st.sidebar.markdown("---")
lang_map = {"English":"en-IN","हिंदी":"hi-IN","मराठी":"mr-IN","ગુજરાતી":"gu-IN",
            "தமிழ்":"ta-IN","తెలుగు":"te-IN","ਪੰਜਾਬੀ":"pa-IN","বাংলা":"bn-IN"}
selected_lang_name = st.sidebar.selectbox("🌐 Language", list(lang_map.keys()))
lang = lang_map[selected_lang_name]
st.sidebar.markdown("---")
st.sidebar.markdown("**Powered by**")
st.sidebar.markdown("🔷 Databricks Unity Catalog")
st.sidebar.markdown("🟢 Sarvam AI LLM")
st.sidebar.markdown("📊 Real APMC Data")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.markdown("# 🌾 MandiMax FPO Intelligence Dashboard")
    st.markdown("*Real-time agricultural market intelligence powered by Databricks*")

    # ── Stats row ─────────────────────────────────────────────────────────────
    stats = run_sql("SELECT COUNT(*) AS total, COUNT(DISTINCT Market_Name) AS markets, COUNT(DISTINCT Commodity) AS commodities FROM silver.mandi_prices_clean")
    arb   = run_sql("SELECT COUNT(*) AS pairs, ROUND(AVG(arbitrage_spread),0) AS avg_spread FROM arbitrage_opportunities_latest")

    c1, c2, c3, c4 = st.columns(4)
    total  = int(stats["total"].iloc[0])  if not stats.empty  else 727656
    mkts   = int(stats["markets"].iloc[0]) if not stats.empty else 1597
    pairs  = int(arb["pairs"].iloc[0])    if not arb.empty    else 542
    spread = int(arb["avg_spread"].iloc[0]) if not arb.empty  else 3240

    c1.metric("📦 Total Records",       f"{total:,}")
    c2.metric("🏪 Unique Markets",       f"{mkts:,}")
    c3.metric("🔄 Arbitrage Pairs",     f"{pairs:,}")
    c4.metric("💰 Avg Spread",          f"₹{spread:,}")

    st.markdown("---")

    # ── Arbitrage Table ───────────────────────────────────────────────────────
    st.markdown("### 🔄 Top Arbitrage Opportunities")
    commodity_filter = st.selectbox("Filter by commodity", ["All","Tomato","Onion","Potato","Wheat","Rice"])
    where = f"WHERE LOWER(src_commodity)=LOWER('{commodity_filter}')" if commodity_filter != "All" else ""
    arb_df = run_sql(f"""
        SELECT src_market, tgt_market, src_commodity, src_price, tgt_price,
               arbitrage_spread, distance_km
        FROM arbitrage_opportunities_latest {where}
        ORDER BY arbitrage_spread DESC LIMIT 10
    """)
    if arb_df.empty: arb_df = MOCK_ARB[["src_market","tgt_market","src_commodity","src_price","tgt_price","arbitrage_spread","distance_km"]]

    arb_df.columns = [c.replace("_"," ").title() for c in arb_df.columns]
    st.dataframe(arb_df, use_container_width=True, hide_index=True)

    # ── Crop Prices Chart ─────────────────────────────────────────────────────
    st.markdown("### 📈 Crop Price Stability")
    crop_df = run_sql("""
        SELECT Commodity, ROUND(AVG(avg_30d_price),0) AS avg_price,
               ROUND(AVG(stability_score),1) AS stability
        FROM crop_recommendation
        GROUP BY Commodity ORDER BY stability DESC LIMIT 5
    """)
    if crop_df.empty: crop_df = MOCK_CROPS[["Commodity","avg_price","stability"]]

    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(crop_df.set_index("Commodity")["avg_price"], color="#25d366")
    with col2:
        st.bar_chart(crop_df.set_index("Commodity")["stability"], color="#128c7e")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2: MANDIMAX BOT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🤖 MandiMax Bot":
    st.markdown("# 🤖 MandiMax Bot")
    st.markdown("*Ask in any Indian language — powered by Sarvam AI + real APMC data*")

    # Session state
    if "messages"    not in st.session_state: st.session_state.messages    = []
    if "bot_context" not in st.session_state: st.session_state.bot_context = {}

    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"], avatar="🧑‍🌾" if msg["role"]=="user" else "🌾"):
            st.markdown(msg["content"])

    # Chat input
    user_input = st.chat_input(f"Type in {selected_lang_name}… e.g. 'sell tomato', 'कांदा', 'crop advisory UP'")

    if user_input:
        # Show user message
        st.session_state.messages.append({"role":"user","content":user_input})
        with st.chat_message("user", avatar="🧑‍🌾"):
            st.markdown(user_input)

        # Process with bot engine
        ctx = st.session_state.bot_context

        # 1. Translate to English
        msg_en = sarvam_translate(user_input, lang, "en-IN") if lang != "en-IN" else user_input
        msg_l  = msg_en.lower()

        # 2. Detect commodity
        COMMODITY_MAP = {
            "tomato": ["tomato","tamatar","टमाटर"],
            "onion":  ["onion","pyaz","प्याज","kanda","कांदा","कांडा","vengayam","eerulli"],
            "potato": ["potato","aloo","आलू","batata","बटाटा"],
            "wheat":  ["wheat","gehun","गेहूं","gahu"],
            "rice":   ["rice","chawal","चावल","tandul","तांदूळ"],
        }
        commodity = None
        for crop, keys in COMMODITY_MAP.items():
            if any(k in msg_l for k in keys):
                commodity = crop.title(); break

        FRESH = (None, "", "ask_commodity", "done")
        step  = ctx.get("step")

        raw_reply    = ""
        data_for_llm = None

        # ── Routing ────────────────────────────────────────────────────────
        if commodity and step in FRESH:
            qty = 10
            m = re.search(r"(\d+)", msg_en)
            if m: qty = int(m.group(1))
            st.session_state.bot_context = {"step":"ask_city","commodity":commodity,"qty":qty}
            raw_reply = f"Understood! You want to sell {qty} quintals of **{commodity}**.\n\n📍 **Which city are you in?**\n\nType: Indore, Lucknow, Patna, Delhi, Pune, Nagpur, Surat"

        elif msg_l in ("hi","hello","help","start") or not ctx:
            st.session_state.bot_context = {"step":"ask_commodity"}
            raw_reply = ("👋 **Welcome to MandiMax!**\n\nI help Indian farmers find the best mandi.\n\n"
                         "Try:\n- `sell tomato` — find best mandis\n- `crops UP` — crop advisory\n"
                         "- `कांदा` — works in your language!\n- `help` — this menu")

        elif "crop" in msg_l or "advisory" in msg_l:
            state_m = re.search(r'(?:crop|advisory)[s\s]+([a-zA-Z\s]+)', msg_en, re.I)
            state   = state_m.group(1).strip().upper() if state_m else "UTTAR PRADESH"
            df = run_sql(f"""
                SELECT Commodity, ROUND(AVG(avg_30d_price),0) AS avg_price,
                       ROUND(AVG(stability_score),1) AS stability
                FROM crop_recommendation
                WHERE UPPER(STATE) LIKE '%{state}%'
                GROUP BY Commodity ORDER BY stability DESC LIMIT 3
            """)
            if df.empty: df = MOCK_CROPS[MOCK_CROPS["state"]==state][["Commodity","avg_price","stability"]].head(3)
            if df.empty: df = MOCK_CROPS[["Commodity","avg_price","stability"]].head(3)
            data_for_llm = {"type":"crop_advisory","state":state,
                            "crops":[{"name":r["Commodity"],"price":int(r["avg_price"]),"stability":float(r["stability"])} for _,r in df.iterrows()]}
            raw_reply    = "\n".join(f"🌱 **{r['Commodity']}** — ₹{int(r['avg_price'])}/q (Stability {r['stability']}/100)" for _,r in df.iterrows())
            st.session_state.bot_context = {"step":"done"}
            # Show table too
            st.dataframe(df.reset_index(drop=True), use_container_width=True, hide_index=True)

        elif step == "ask_city":
            found = None
            for city, coords in CITY_COORDS.items():
                if city in msg_l: found = (city, coords); break
            if not found:
                raw_reply = "❌ City not found. Try: Indore, Lucknow, Patna, Delhi, Pune, Nagpur"
            else:
                city_name, _ = found
                st.session_state.bot_context = {**ctx, "step":"ask_transport","city":city_name}
                raw_reply = (f"📍 **{city_name.title()}** noted!\n\n"
                             "🚚 What is your transport cost per km per quintal?\n\n"
                             "Type **10** for ₹10/km or **default** for ₹8/km")

        elif step == "ask_transport":
            rate = 8.0 if "default" in msg_l else (float(re.search(r"(\d+(?:\.\d+)?)", msg_en).group(1)) if re.search(r"(\d+(?:\.\d+)?)", msg_en) else 8.0)
            commodity = ctx.get("commodity","Tomato")
            city      = ctx.get("city","indore")
            qty       = ctx.get("qty",10)
            recs      = get_recommendations(commodity, city, rate, qty)
            st.session_state.bot_context = {"step":"done"}

            if not recs:
                raw_reply = f"😔 No profitable markets within 250km of **{city.title()}** for **{commodity}** at ₹{rate}/km.\n\nConsider your local APMC."
            else:
                data_for_llm = {"type":"sell_recommendation","commodity":commodity,"city":city.title(),
                                "qty":qty,"rate":rate,"recommendations":recs}
                lines = [f"🌾 **{commodity}** near **{city.title()}** (≤250km, ₹{rate}/km)\n"]
                icons = ["🥇","🥈","🥉"]
                for i, r in enumerate(recs):
                    tag = " 💡 *Nearby!*" if r["dist"]<=80 else ""
                    lines.append(f"{icons[i]} **{r['market']}**{tag}\n"
                                 f"   💰 ₹{r['price']:,.0f}/q | 📍 {r['dist']}km | "
                                 f"🚚 ₹{r['transport']:,.0f}/q\n"
                                 f"   ✅ Net: **₹{r['net']:,.0f}/q** | Total: **₹{r['total']:,.0f}**\n")
                lines.append("*⚠️ Confirm prices before travel. Data from Databricks gold tables.*")
                raw_reply = "\n".join(lines)
                # Show table
                rec_df = pd.DataFrame(recs)
                st.dataframe(rec_df, use_container_width=True, hide_index=True)
        else:
            st.session_state.bot_context = {}
            raw_reply = "🤔 I didn't understand that.\n\nTry: `sell tomato`, `crops UP`, or say your crop name!"

        # ── Sarvam LLM for rich responses ──────────────────────────────────
        final_reply = raw_reply
        if SARVAM_KEY and data_for_llm:
            system = ("You are MandiMax, an AI for Indian farmers. "
                      "Use ONLY real APMC data provided. Give practical advice. "
                      "Warn if market >200km. Show net profit. Never invent prices.")
            prompt = f"Data: {json.dumps(data_for_llm, ensure_ascii=False)}\nFarmer said: {user_input}\nRespond helpfully."
            llm = sarvam_llm(system, prompt, lang)
            if llm: final_reply = llm
        elif lang != "en-IN" and SARVAM_KEY:
            translated = sarvam_translate(raw_reply, "en-IN", lang)
            if translated: final_reply = translated

        # Show bot response
        st.session_state.messages.append({"role":"assistant","content":final_reply})
        with st.chat_message("assistant", avatar="🌾"):
            st.markdown(final_reply)
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3: CROP ADVISORY
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🌱 Crop Advisory":
    st.markdown("# 🌱 Crop Advisory")
    st.markdown("*Best crops to grow based on 30-day APMC price stability*")

    states = ["UTTAR PRADESH","MADHYA PRADESH","MAHARASHTRA","GUJARAT",
              "PUNJAB","HARYANA","RAJASTHAN","BIHAR","WEST BENGAL","KARNATAKA"]
    state = st.selectbox("Select your state", states)

    if st.button("🔍 Get Recommendations"):
        df = run_sql(f"""
            SELECT Commodity, ROUND(AVG(avg_30d_price),0) AS avg_price,
                   ROUND(AVG(stability_score),1) AS stability,
                   COUNT(DISTINCT District_Name) AS districts_tracked
            FROM crop_recommendation
            WHERE UPPER(STATE) LIKE '%{state}%'
            GROUP BY Commodity ORDER BY stability DESC LIMIT 5
        """)
        if df.empty:
            df = MOCK_CROPS.copy()
            st.info("Showing sample data — live data loading from Databricks...")

        st.markdown(f"### Best crops for {state}")
        for i, (_, r) in enumerate(df.iterrows()):
            stability = float(r["stability"])
            color = "🟢" if stability >= 90 else "🟡" if stability >= 75 else "🔴"
            risk  = "LOW" if stability >= 90 else "MEDIUM" if stability >= 75 else "HIGH"
            with st.container():
                c1, c2, c3 = st.columns([3, 2, 2])
                c1.markdown(f"{'🥇🥈🥉🏅🎖️'[i]} **{r['Commodity']}** {color}")
                c2.metric("Avg Price", f"₹{int(r['avg_price']):,}/q")
                c3.metric("Stability", f"{stability}/100", delta=f"{risk} RISK")

        st.dataframe(df.reset_index(drop=True), use_container_width=True, hide_index=True)

        # LLM advice
        if SARVAM_KEY:
            with st.spinner("🤖 Getting Sarvam AI advice..."):
                data = {"state": state, "crops": df[["Commodity","avg_price","stability"]].to_dict("records")}
                advice = sarvam_llm(
                    "You are MandiMax agricultural advisor for Indian farmers. Give practical advice.",
                    f"Recommend which crop to grow in {state} based on: {json.dumps(data)}",
                    lang
                )
                if advice:
                    st.markdown("---")
                    st.markdown("### 🤖 Sarvam AI Advisory")
                    st.info(advice)
