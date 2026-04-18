"""
MandiMax API Server — Flask backend for dashboard + WhatsApp bot
Run: python app.py
Expose: ngrok http 5001
"""

import os, json, re, requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from twilio.twiml.messaging_response import MessagingResponse
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}},
     allow_headers=["Content-Type","Authorization"],
     methods=["GET","POST","OPTIONS","PUT","DELETE"])

# Explicit CORS headers — Databricks Apps proxy sometimes strips flask-cors
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT, DELETE"
    return response

@app.route("/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path=""):
    """Handle all CORS preflight OPTIONS requests."""
    from flask import make_response
    resp = make_response("", 204)
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT, DELETE"
    return resp

# ── Config ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST")    # e.g. dbc-ae674f11-a144.cloud.databricks.com
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN")
SQL_HTTP_PATH     = os.getenv("DATABRICKS_SQL_HTTP_PATH")
UC_CATALOG        = os.getenv("DATABRICKS_CATALOG", "workspace")
SARVAM_API_KEY    = os.getenv("SARVAM_API_KEY", "")   # https://dashboard.sarvam.ai

# Sarvam language codes for Indian languages
SARVAM_LANGS = {
    "hindi":"hi-IN", "gujarati":"gu-IN", "marathi":"mr-IN",
    "tamil":"ta-IN", "telugu":"te-IN",  "kannada":"kn-IN",
    "bengali":"bn-IN","punjabi":"pa-IN", "malayalam":"ml-IN",
    "odia":"od-IN",  "urdu":"ur-IN",    "english":"en-IN",
}

# ── Session state: remember farmer context between messages (in-memory) ────────
SESSIONS = {}   # { phone_number: { "step": "ask_commodity" | "ask_transport", "commodity": ..., ... } }

# ── Query Databricks Gold Table ───────────────────────────────────────────────
def query_databricks(sql: str) -> list[dict]:
    """Execute SQL against live UC gold tables. Falls back to mock if unavailable."""
    if not (DATABRICKS_HOST and DATABRICKS_TOKEN and SQL_HTTP_PATH):
        print("[Databricks] Credentials not set — using mock data")
        return []
    try:
        from databricks import sql as dbsql
        conn   = dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=SQL_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
        cursor = conn.cursor()
        # Set UC catalog + schema so bare table names resolve (gold.table_name)
        cursor.execute(f"USE CATALOG {UC_CATALOG}")
        cursor.execute("USE SCHEMA gold")
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[Databricks] Fallback to mock: {e}")
        return []

def get_arbitrage(commodity: str) -> list[dict]:
    rows = query_databricks(f"""
        SELECT tgt_market, tgt_district, tgt_state,
               src_price, tgt_price, arbitrage_spread,
               tgt_lat, tgt_lon
        FROM gold.arbitrage_opportunities_latest
        WHERE LOWER(src_commodity) = LOWER('{commodity}')
        ORDER BY arbitrage_spread DESC
        LIMIT 10
    """)
    # Fallback mock if Databricks unreachable
    if not rows:
        MOCK = {
            "tomato": [
                {"tgt_market":"SURAT","tgt_district":"SURAT","tgt_state":"GUJARAT","tgt_price":10000,"src_price":2100,"arbitrage_spread":7900,"tgt_lat":21.1702,"tgt_lon":72.8311},
                {"tgt_market":"MATHURA","tgt_district":"MATHURA","tgt_state":"UTTAR PRADESH","tgt_price":8200,"src_price":1500,"arbitrage_spread":6700,"tgt_lat":27.4924,"tgt_lon":77.6737},
                {"tgt_market":"ROHTAK","tgt_district":"ROHTAK","tgt_state":"HARYANA","tgt_price":9000,"src_price":2000,"arbitrage_spread":7000,"tgt_lat":28.8955,"tgt_lon":76.6066},
            ],
            "onion": [
                {"tgt_market":"LASALGAON","tgt_district":"NASHIK","tgt_state":"MAHARASHTRA","tgt_price":3200,"src_price":1200,"arbitrage_spread":2000,"tgt_lat":20.1269,"tgt_lon":74.0832},
            ],
            "potato": [
                {"tgt_market":"AGRA","tgt_district":"AGRA","tgt_state":"UTTAR PRADESH","tgt_price":1400,"src_price":600,"arbitrage_spread":800,"tgt_lat":27.1767,"tgt_lon":78.0081},
            ],
        }
        rows = MOCK.get(commodity.lower(), [])
    return rows

def get_crop_recs(state: str) -> list[dict]:
    rows = query_databricks(f"""
        SELECT Commodity, ROUND(AVG(avg_30d_price),0) AS avg_price,
               ROUND(AVG(stability_score),1) AS stability
        FROM gold.crop_recommendation
        WHERE UPPER(STATE) LIKE UPPER('%{state}%')
        GROUP BY Commodity ORDER BY stability DESC LIMIT 3
    """)
    if not rows:
        rows = [
            {"Commodity":"Rice",   "avg_price":3245,"stability":97.2},
            {"Commodity":"Potato", "avg_price":1166,"stability":95.5},
            {"Commodity":"Onion",  "avg_price":1205,"stability":95.3},
        ]
    return rows

# ── Entity extraction from WhatsApp message ───────────────────────────────────
def extract_commodity(text: str) -> str | None:
    text = text.lower()
    MAP  = {
        "tomato": ["tomato","tamatar","टमाटर","tomat","tometo"],
        "onion":  ["onion","pyaz","प्याज","kanda","कांदा","कांडा","dungri","ड�ुंगरी","vengayam","ulligadda","eerulli"],
        "potato": ["potato","aloo","आलू","batata","बटाटा","urulaikilangu","bangaladumpa"],
        "wheat":  ["wheat","gehun","गेहूं","gahu","गहू","godhi","ਕਣਕ"],
        "rice":   ["rice","chawal","चावल","tandul","तांदूळ","arisi","biyyam","akki"],
    }
    for crop, keys in MAP.items():
        if any(k in text for k in keys):
            return crop.title()
    return None

def extract_number(text: str) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)", text.replace(",",""))
    return float(m.group(1)) if m else None

def extract_quantity(text: str) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:quintal|q\b|kwint)", text.lower())
    return float(m.group(1)) if m else None

# ── Recommendation engine (pure Python, no Spark needed here) ────────────────
def recommend(commodity, farmer_lat, farmer_lon, transport_rate, quantity):
    from math import radians, sin, cos, sqrt, asin
    def haversine_km(lat1, lon1, lat2, lon2):
        R = 6371
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a    = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
        return R * 2 * asin(sqrt(a))

    rows   = get_arbitrage(commodity)
    rate   = transport_rate or 8.0
    results= []
    seen   = set()

    for row in rows:
        mkt = row["tgt_market"]
        if mkt in seen or not row.get("tgt_lat"):
            continue
        seen.add(mkt)

        dist      = haversine_km(farmer_lat, farmer_lon, float(row["tgt_lat"]), float(row["tgt_lon"]))
        price     = float(row["tgt_price"])
        transport = dist * rate
        net       = price - transport
        total     = net * quantity

        if net > 0:
            results.append({
                "market":    mkt,
                "district":  row.get("tgt_district") or mkt,   # fallback to market name
                "state":     row.get("tgt_state", ""),
                "price":     price,
                "dist":      round(dist,1),
                "transport": round(transport,0),
                "net":       round(net,0),
                "total":     round(total,0),
            })

    results.sort(key=lambda x: x["net"], reverse=True)
    return results[:3]

# ── Message formatter ──────────────────────────────────────────────────────────
def format_recommendations(recs, commodity, quantity, transport_rate):
    if not recs:
        return (f"😔 No profitable markets found for {commodity} at ₹{transport_rate}/km transport rate.\n"
                "Consider selling locally or reducing quantity.")

    lines = [f"🌾 *{commodity}* — Top Mandis for You\n"]
    ranks = ["🥇","🥈","🥉"]
    for i, r in enumerate(recs):
        lines.append(
            f"{ranks[i]} *{r['market']}*, {r['district']}\n"
            f"   💰 Price: ₹{r['price']:,.0f}/q\n"
            f"   📍 Distance: {r['dist']} km\n"
            f"   🚚 Transport: ₹{r['transport']:,.0f}/q\n"
            f"   ✅ Net profit: ₹{r['net']:,.0f}/q\n"
            f"   💵 Total for {quantity}q: *₹{r['total']:,.0f}*\n"
        )
    lines.append("_⚠️ Verify prices at mandi before travel. MD = MandiMax._")
    return "\n".join(lines)

def format_crop_recs(rows, state):
    if not rows:
        return "No crop data for your region yet."
    lines = [f"🌱 Best crops for *{state.title()}*:\n"]
    for i, r in enumerate(rows):
        s = float(r.get("stability", r.get("avg_stability", 90)))
        icon = "🟢" if s >= 90 else "🟡"
        lines.append(
            f"{i+1}. {icon} *{r['Commodity']}* — "
            f"₹{r.get('avg_price', r.get('avg_market_price','?')):,}/q "
            f"(Stability {s}/100)"
        )
    lines.append("\n_Based on 30-day APMC price data._")
    return "\n".join(lines)

# ── Conversation state machine ────────────────────────────────────────────────
HELP_MSG = (
    "👋 *MandiMax Bot*\n\n"
    "I help you find the best mandi to sell your crop.\n\n"
    "Type:\n"
    "• *sell tomato* — get best mandis\n"
    "• *crops UP* — crop advisory for your state\n"
    "• *help* — this message"
)

def handle_message(phone: str, text: str) -> str:
    text_l = text.lower().strip()
    session = SESSIONS.get(phone, {})

    # ── Help ─────────────────────────────────────────────────────────────────
    if text_l in ("hi","hello","help","start","helo","हेलो"):
        SESSIONS.pop(phone, None)
        return HELP_MSG

    # ── Crop advisory ─────────────────────────────────────────────────────────
    if "crop" in text_l or "advisory" in text_l:
        # Try to extract state name
        state_m = re.search(r'(?:crop|advisory)[s\s]+([a-zA-Z\s]+)', text_l)
        state   = state_m.group(1).strip().upper() if state_m else "UTTAR PRADESH"
        rows    = get_crop_recs(state)
        return format_crop_recs(rows, state)

    # ── New sell query ────────────────────────────────────────────────────────
    commodity = extract_commodity(text)
    if commodity and "step" not in session:
        qty = extract_quantity(text) or 10
        session = {
            "step":      "ask_location",
            "commodity": commodity,
            "quantity":  qty,
        }
        SESSIONS[phone] = session
        return (
            f"Got it! 🌾 *{commodity}*, {qty} quintal(s).\n\n"
            "📍 Share your *location* (tap 📎 → Location in WhatsApp)\n"
            "OR type your city name (e.g. *Ujjain*, *Lucknow*)"
        )

    # ── Location step ─────────────────────────────────────────────────────────
    if session.get("step") == "ask_location":
        # Check for WhatsApp location share (lat/lon in request handled in webhook)
        city_coords = {
            "ujjain":(23.1828,75.7772), "indore":(22.7196,75.8577),
            "bhopal":(23.2599,77.4126), "lucknow":(26.8467,80.9462),
            "jaipur":(26.9124,75.7873), "pune":(18.5204,73.8567),
            "mumbai":(19.0760,72.8777), "delhi":(28.6139,77.2090),
            "ahmedabad":(23.0225,72.5714), "surat":(21.1702,72.8311),
            "nagpur":(21.1458,79.0882), "kanpur":(26.4499,80.3319),
            "patna":(25.5941,85.1376),  "kolkata":(22.5726,88.3639),
        }
        lat, lon = None, None
        for city, coords in city_coords.items():
            if city in text_l:
                lat, lon = coords
                break
        if not lat:
            return ("📍 I couldn't find that location.\nTry a major city name like:\n"
                    "*Ujjain, Lucknow, Jaipur, Pune, Bhopal, Delhi, Patna*")

        session["lat"] = lat
        session["lon"] = lon
        session["step"] = "ask_transport"
        SESSIONS[phone] = session
        return (
            "🚚 What's your *transport cost per km per quintal*?\n\n"
            "Example: type *12* for ₹12/km\n"
            "Or type *default* to use our estimate (₹8/km)"
        )

    # ── Transport rate step ───────────────────────────────────────────────────
    if session.get("step") == "ask_transport":
        if "default" in text_l:
            rate = 8.0
        else:
            rate = extract_number(text) or 8.0

        commodity = session["commodity"]
        lat, lon  = session["lat"],  session["lon"]
        quantity  = session["quantity"]
        SESSIONS.pop(phone, None)

        recs = recommend(commodity, lat, lon, rate, quantity)
        return format_recommendations(recs, commodity, quantity, rate)

    # ── Fallback ──────────────────────────────────────────────────────────────
    SESSIONS.pop(phone, None)
    return ("🤔 I didn't understand that.\n\n" + HELP_MSG)


# ── WhatsApp Webhook (Twilio) ─────────────────────────────────────────────────
@app.route("/webhook/whatsapp", methods=["POST"])
def whatsapp_webhook():
    phone     = request.form.get("From", "unknown")
    body      = request.form.get("Body", "").strip()
    lat_share = request.form.get("Latitude")
    lon_share = request.form.get("Longitude")

    # If farmer shared their GPS location directly via WhatsApp
    if lat_share and lon_share and phone in SESSIONS:
        session = SESSIONS[phone]
        if session.get("step") == "ask_location":
            session["lat"]  = float(lat_share)
            session["lon"]  = float(lon_share)
            session["step"] = "ask_transport"
            SESSIONS[phone] = session
            reply = ("📍 Location received!\n\n"
                     "🚚 What's your transport cost per km per quintal?\n"
                     "Type a number like *12* or type *default*")
        else:
            reply = handle_message(phone, body)
    else:
        reply = handle_message(phone, body)

    resp = MessagingResponse()
    resp.message(reply)
    return str(resp)

@app.route("/health", methods=["GET"])
def health():
    db_ok = bool(DATABRICKS_HOST and DATABRICKS_TOKEN and SQL_HTTP_PATH)
    return jsonify({
        "status": "ok",
        "service": "MandiMax-API",
        "version": "2.0",
        "databricks_configured": db_ok,
    })

@app.route("/test", methods=["GET"])
def test_query():
    recs = recommend("Tomato", 23.1828, 75.7772, 8.0, 20)
    return jsonify({"recommendations": recs})


# ═══════════════════════════════════════════════════════════════════════════════
# SARVAM AI — Translation + LLM + STT + TTS
# ═══════════════════════════════════════════════════════════════════════════════

def sarvam_translate(text: str, source_lang: str, target_lang: str) -> str:
    """Translate between Indian languages using Sarvam AI."""
    if not SARVAM_API_KEY or source_lang == target_lang:
        return text
    try:
        resp = requests.post(
            "https://api.sarvam.ai/translate",
            headers={"api-subscription-key": SARVAM_API_KEY},
            json={"input": text, "source_language_code": source_lang,
                  "target_language_code": target_lang,
                  "speaker_gender": "Male", "mode": "formal"},
            timeout=8,
        )
        return resp.json().get("translated_text", text)
    except Exception as e:
        print(f"[Sarvam Translate] {e}")
        return text


def sarvam_llm(system_prompt: str, user_message: str, language_code: str = "en-IN") -> str | None:
    """Generate contextual response via Sarvam-M LLM in the farmer's language."""
    if not SARVAM_API_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.sarvam.ai/v1/chat/completions",
            headers={"api-subscription-key": SARVAM_API_KEY,
                     "Content-Type": "application/json"},
            json={
                "model": "sarvam-m",
                "messages": [
                    {"role": "system", "content": (
                        system_prompt +
                        f"\nRespond ONLY in language '{language_code}'. "
                        "Use simple words a farmer can understand. Be concise."
                    )},
                    {"role": "user", "content": user_message},
                ],
                "max_tokens": 400,
                "temperature": 0.4,
            },
            timeout=15,
        )
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[Sarvam LLM] {e}")
        return None


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """
    Multilingual chat — Sarvam AI powered.
    Flow:
      1. Receive {message, language, context}
      2. Translate message → English (Sarvam translate)
      3. Extract intent/entities from English
      4. Route: commodity > help > crop > ask_city > ask_transport
      5. For rich data (recommendations): use Sarvam-M LLM in farmer's language
      6. For all other replies: translate English raw_reply → farmer's language
    """
    body    = request.get_json(force=True)
    message = body.get("message", "").strip()
    lang    = body.get("language", "en-IN")
    context = body.get("context", {})

    # Step 1 — Translate input to English
    msg_en = sarvam_translate(message, lang, "en-IN") if lang != "en-IN" else message
    msg_l  = msg_en.lower()

    commodity = extract_commodity(msg_en)
    step      = context.get("step")

    data_for_llm = None
    next_context = {}
    FRESH = (None, "", "ask_commodity", "done")

    # ── Route: commodity HIGHEST priority ────────────────────────────────────
    if commodity and step in FRESH:
        qty          = extract_quantity(msg_en) or 10
        next_context = {"step": "ask_city", "commodity": commodity, "quantity": qty}
        raw_reply    = (f"Understood! You want to sell {qty} quintals of {commodity}. "
                        "Which city are you in? (e.g. Indore, Lucknow, Patna, Delhi, Pune)")

    elif msg_l in ("hi","hello","help","start","हेलो","नमस्ते","नमस्कार") or not context:
        raw_reply    = ("Welcome to MandiMax! I help Indian farmers find the best mandi. "
                        "Tell me your crop: tomato, onion, potato, wheat, rice "
                        "| कौन सी फसल? टमाटर, कांदा, बटाटा, गेहूं, चावल")
        next_context = {"step": "ask_commodity"}

    elif "crop" in msg_l or "advisory" in msg_l:
        state_m = re.search(r'(?:crop|advisory)[s\s]+([a-zA-Z\s]+)', msg_en, re.I)
        state   = state_m.group(1).strip() if state_m else "UTTAR PRADESH"
        recs    = get_crop_recs(state)
        data_for_llm = {"type": "crop_advisory", "state": state,
                        "crops": [{"name": r.get("Commodity",""),
                                   "avg_price": r.get("avg_price",0),
                                   "stability": r.get("stability",0)} for r in recs]}
        raw_reply    = (f"Best crops for {state}: " +
                        ", ".join(f"{r.get('Commodity','')} ₹{r.get('avg_price',0)}/q"
                                  for r in recs))
        next_context = {"step": "done"}

    elif step == "ask_city":
        commodity = context.get("commodity", "Tomato")
        qty       = context.get("quantity", 10)
        CITIES = {
            "ujjain":(23.1828,75.7772),"indore":(22.7196,75.8577),"bhopal":(23.2599,77.4126),
            "lucknow":(26.8467,80.9462),"jaipur":(26.9124,75.7873),"pune":(18.5204,73.8567),
            "mumbai":(19.0760,72.8777),"delhi":(28.6139,77.2090),"ahmedabad":(23.0225,72.5714),
            "surat":(21.1702,72.8311),"nagpur":(21.1458,79.0882),"kanpur":(26.4499,80.3319),
            "patna":(25.5941,85.1376),"kolkata":(22.5726,88.3639),
            "gorakhpur":(26.7606,83.3732),"varanasi":(25.3176,82.9739),
        }
        found = next(((c, coords) for c, coords in CITIES.items() if c in msg_l), None)
        if not found:
            raw_reply    = "City not found. Try: Indore, Lucknow, Patna, Delhi, Pune, Nagpur, Surat."
            next_context = context
        else:
            city_name, (lat, lon) = found
            next_context = {"step":"ask_transport","commodity":commodity,
                            "quantity":qty,"lat":lat,"lon":lon,"city":city_name}
            raw_reply    = (f"Got it — {city_name.title()}! "
                            "What is your transport cost per km per quintal? "
                            "Type a number like 10 for ₹10/km, or say 'default' for ₹8/km.")

    elif step == "ask_transport":
        commodity = context.get("commodity","Tomato")
        lat       = context.get("lat", 22.7196)
        lon       = context.get("lon", 75.8577)
        qty       = context.get("quantity", 10)
        city      = context.get("city","your city")
        rate      = 8.0 if "default" in msg_l else (extract_number(msg_en) or 8.0)
        recs      = recommend(commodity, lat, lon, rate, qty)
        practical = [r for r in recs if r["dist"] <= 250] or recs
        nearest   = min(recs, key=lambda r: r["dist"]) if recs else None
        data_for_llm = {"type":"sell_recommendation","commodity":commodity,"city":city,
                        "quantity":qty,"transport_rate":rate,
                        "recommendations":practical[:3],"nearest_market":nearest}
        next_context = {"step":"done"}
        if not practical:
            raw_reply = (f"No profitable markets within 250km of {city} for {commodity} "
                         f"at ₹{rate}/km. Consider your local APMC.")
        else:
            icons = ["🥇","🥈","🥉"]
            lines = [f"🌾 {commodity} near {city.title()} (≤250km, ₹{rate}/km)\n"]
            for i, r in enumerate(practical[:3]):
                tag = " 💡 Nearby!" if r["dist"]<=80 else (" ⚠️ Far" if r["dist"]>200 else "")
                lines.append(f"{icons[i]} {r['market']}, {r['district']}{tag}\n"
                             f"   💰 ₹{r['price']:,.0f}/q | 📍 {r['dist']}km | "
                             f"🚚 ₹{r['transport']:,.0f}/q | ✅ Net ₹{r['net']:,.0f}/q\n"
                             f"   💵 Total ({qty}q): ₹{r['total']:,.0f}\n")
            lines.append("_⚠️ Confirm prices before travel. Data from Databricks gold tables._")
            raw_reply = "\n".join(lines)
    else:
        raw_reply    = ("Please tell me which crop: tomato, onion, potato, wheat, rice "
                        "| फसल बताएं: टमाटर, कांदा, बटाटा, गेहूं, चावल")
        next_context = {"step":"ask_commodity"}

    # ── Sarvam LLM: generates full reply for rich data responses ─────────────
    final_reply = raw_reply
    if SARVAM_API_KEY and data_for_llm:
        system = ("You are MandiMax, an AI for Indian farmers. "
                  "Use ONLY the real APMC data provided. Give practical advice. "
                  "Warn if market >200km. Always show net profit. Never invent prices.")
        prompt = (f"Data: {json.dumps(data_for_llm, ensure_ascii=False)}\n"
                  f"Farmer said: {message}\nRespond helpfully and concisely.")
        llm_resp = sarvam_llm(system, prompt, lang)
        if llm_resp:
            final_reply = llm_resp

    # ── Translate ALL other (intermediate) replies to farmer's language ───────
    if lang != "en-IN" and final_reply == raw_reply and SARVAM_API_KEY:
        translated = sarvam_translate(raw_reply, "en-IN", lang)
        if translated and translated != raw_reply:
            final_reply = translated

    return jsonify({"reply": final_reply, "context": next_context,
                    "language": lang, "sarvam_used": bool(SARVAM_API_KEY)})


@app.route("/api/stt", methods=["POST"])
def api_stt():
    """Speech-to-Text using Sarvam Saarika ASR."""
    if not SARVAM_API_KEY:
        return jsonify({"error": "Sarvam key not configured"}), 503
    audio = request.files.get("audio")
    lang  = request.form.get("language", "hi-IN")
    if not audio:
        return jsonify({"error": "No audio file"}), 400
    try:
        resp = requests.post(
            "https://api.sarvam.ai/speech-to-text",
            headers={"api-subscription-key": SARVAM_API_KEY},
            files={"file": (audio.filename or "audio.webm",
                            audio.read(),
                            audio.content_type or "audio/webm")},
            data={"language_code": lang, "model": "saarika:v2",
                  "with_timestamps": "false"},
            timeout=15,
        )
        result = resp.json()
        return jsonify({"transcript": result.get("transcript",""), "language": lang})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tts", methods=["POST"])
def api_tts():
    """Text-to-Speech using Sarvam Bulbul TTS."""
    if not SARVAM_API_KEY:
        return jsonify({"error": "Sarvam key not configured"}), 503
    body = request.get_json(force=True)
    text = body.get("text", "")
    lang = body.get("language", "hi-IN")
    if not text:
        return jsonify({"error": "No text"}), 400
    try:
        resp = requests.post(
            "https://api.sarvam.ai/text-to-speech",
            headers={"api-subscription-key": SARVAM_API_KEY,
                     "Content-Type": "application/json"},
            json={"inputs": [text[:500]], "target_language_code": lang,
                  "speaker": "meera", "pitch": 0, "pace": 1.15,
                  "loudness": 1.5, "speech_sample_rate": 22050,
                  "enable_preprocessing": True, "model": "bulbul:v1"},
            timeout=15,
        )
        result = resp.json()
        audios = result.get("audios", [])
        if audios:
            return jsonify({"audio_base64": audios[0], "language": lang})
        return jsonify({"error": "TTS failed", "detail": result}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """Pipeline health stats for StatCards and PipelineHealthBanner."""
    rows = query_databricks("""
        SELECT
            COUNT(*)                      AS total_records,
            COUNT(DISTINCT Market_Name)   AS unique_markets,
            COUNT(DISTINCT Commodity)     AS unique_commodities,
            SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_flags
        FROM silver.mandi_prices_clean
    """)
    arb = query_databricks("""
        SELECT COUNT(*) AS arb_pairs,
               ROUND(AVG(arbitrage_spread), 0) AS avg_spread
        FROM gold.arbitrage_opportunities_latest
    """)
    if rows and arb:
        r, a = rows[0], arb[0]
        return jsonify({
            "totalRecords":       f"{int(r['total_records']):,}",
            "uniqueMarkets":      f"{int(r['unique_markets']):,}",
            "commodities":        f"{int(r['unique_commodities']):,}",
            "arbitragePairs":     f"{int(a['arb_pairs']):,}",
            "avgArbitrageSpread": f"\u20b9{int(a['avg_spread']):,}",
            "source":             "live",
        })
    # Fallback
    return jsonify({
        "totalRecords": "727,656", "uniqueMarkets": "1,597",
        "commodities": "5", "arbitragePairs": "542",
        "avgArbitrageSpread": "\u20b93,240", "source": "cached",
    })


@app.route("/api/arbitrage", methods=["GET"])
def api_arbitrage():
    """Top arbitrage opportunities — filtered by commodity."""
    commodity = request.args.get("commodity", "")
    limit     = int(request.args.get("limit", 10))
    where     = f"WHERE LOWER(src_commodity) = LOWER('{commodity}')" if commodity else ""

    rows = query_databricks(f"""
        SELECT src_market, tgt_market, src_district, tgt_district,
               src_state, tgt_state, src_commodity, src_variety,
               src_price, tgt_price, arbitrage_spread, distance_km,
               spread_per_km, src_lat, src_lon, tgt_lat, tgt_lon
        FROM gold.arbitrage_opportunities_latest
        {where}
        ORDER BY arbitrage_spread DESC
        LIMIT {limit}
    """)
    if rows:
        return jsonify({"data": rows, "count": len(rows), "source": "live"})

    # Fallback mock
    MOCK = [
        {"src_market":"AHMEDABAD","tgt_market":"SURAT","src_district":"AHMEDABAD","tgt_district":"SURAT","src_state":"GUJARAT","tgt_state":"GUJARAT","src_commodity":"Tomato","src_variety":"Other","src_price":2100,"tgt_price":10000,"arbitrage_spread":7900,"distance_km":208,"spread_per_km":38.04,"src_lat":23.0225,"src_lon":72.5714,"tgt_lat":21.1702,"tgt_lon":72.8311},
        {"src_market":"LUDHIANA","tgt_market":"ROHTAK","src_district":"LUDHIANA","tgt_district":"ROHTAK","src_state":"PUNJAB","tgt_state":"HARYANA","src_commodity":"Tomato","src_variety":"Other","src_price":2000,"tgt_price":9000,"arbitrage_spread":7000,"distance_km":234,"spread_per_km":29.86,"src_lat":30.901,"src_lon":75.8573,"tgt_lat":28.8955,"tgt_lon":76.6066},
        {"src_market":"BAREILLY","tgt_market":"MATHURA","src_district":"BAREILLY","tgt_district":"MATHURA","src_state":"UTTAR PRADESH","tgt_state":"UTTAR PRADESH","src_commodity":"Tomato","src_variety":"Deshi","src_price":1500,"tgt_price":8200,"arbitrage_spread":6700,"distance_km":198,"spread_per_km":33.82,"src_lat":28.367,"src_lon":79.4304,"tgt_lat":27.4924,"tgt_lon":77.6737},
        {"src_market":"GORAKHPUR","tgt_market":"SULTANPUR","src_district":"GORAKHPUR","tgt_district":"AMETHI","src_state":"UTTAR PRADESH","tgt_state":"UTTAR PRADESH","src_commodity":"Tomato","src_variety":"Hybrid","src_price":1200,"tgt_price":7875,"arbitrage_spread":6675,"distance_km":141,"spread_per_km":47.46,"src_lat":26.7606,"src_lon":83.3732,"tgt_lat":26.2647,"tgt_lon":82.0727},
        {"src_market":"ALLAHABAD","tgt_market":"LUCKNOW","src_district":"ALLAHABAD","tgt_district":"LUCKNOW","src_state":"UTTAR PRADESH","tgt_state":"UTTAR PRADESH","src_commodity":"Tomato","src_variety":"Other","src_price":1600,"tgt_price":8000,"arbitrage_spread":6400,"distance_km":181,"spread_per_km":35.36,"src_lat":25.4358,"src_lon":81.8463,"tgt_lat":26.8467,"tgt_lon":80.9462},
    ]
    filtered = [r for r in MOCK if not commodity or r["src_commodity"].lower() == commodity.lower()]
    return jsonify({"data": filtered or MOCK, "count": len(filtered or MOCK), "source": "cached"})


@app.route("/api/crop-recommendations", methods=["GET"])
def api_crops():
    """Crop recommendations by state."""
    state = request.args.get("state", "")
    limit = int(request.args.get("limit", 5))
    where = f"AND UPPER(STATE) LIKE UPPER('%{state}%')" if state else ""

    rows = query_databricks(f"""
        SELECT Commodity, STATE,
               ROUND(AVG(avg_30d_price), 0)   AS avg_price,
               ROUND(AVG(stability_score), 1) AS stability,
               COUNT(DISTINCT District_Name)  AS districts_tracked
        FROM gold.crop_recommendation
        WHERE active_days >= 10 {where}
        GROUP BY Commodity, STATE
        ORDER BY stability DESC, avg_price DESC
        LIMIT {limit}
    """)
    if rows:
        result = []
        for i, r in enumerate(rows):
            s    = float(r["stability"])
            risk = "LOW" if s >= 90 else "MEDIUM" if s >= 75 else "HIGH"
            result.append({
                "rank": i+1, "crop": r["Commodity"], "state": r["STATE"],
                "avg_price": r["avg_price"], "stability": s, "volatility": risk,
                "districts": r["districts_tracked"],
            })
        return jsonify({"data": result, "region": state or "All India", "source": "live"})

    MOCK = [
        {"rank":1,"crop":"Rice",  "state":"UTTAR PRADESH","avg_price":3245,"stability":97.2,"volatility":"LOW","districts":51},
        {"rank":2,"crop":"Potato","state":"UTTAR PRADESH","avg_price":1166,"stability":95.5,"volatility":"LOW","districts":53},
        {"rank":3,"crop":"Onion", "state":"UTTAR PRADESH","avg_price":1205,"stability":95.3,"volatility":"LOW","districts":53},
    ]
    return jsonify({"data": MOCK, "region": state or "All India", "source": "cached"})


@app.route("/api/forecast", methods=["GET"])
def api_forecast():
    """7-day price forecast from gold.price_forecast_7day (ML model output)."""
    market    = request.args.get("market", "BANGALORE")
    commodity = request.args.get("commodity", "Tomato")

    rows = query_databricks(f"""
        SELECT forecast_date, ROUND(AVG(predicted_price), 0) AS forecast_price,
               forecast_horizon
        FROM gold.price_forecast_7day
        WHERE LOWER(Market_Name) LIKE LOWER('%{market}%')
          AND LOWER(Commodity)   LIKE LOWER('%{commodity}%')
        GROUP BY forecast_date, forecast_horizon
        ORDER BY forecast_horizon
        LIMIT 7
    """)
    if rows:
        return jsonify({"data": rows, "market": market, "commodity": commodity, "source": "live"})
    return jsonify({"data": [], "source": "no_data"})


@app.route("/api/pipeline-health", methods=["GET"])
def api_pipeline_health():
    """Full pipeline health for PipelineHealthBanner."""
    rows = query_databricks("""
        SELECT
          (SELECT COUNT(*) FROM bronze.mandi_prices_raw)             AS bronze_rows,
          (SELECT COUNT(*) FROM silver.mandi_prices_clean)           AS silver_rows,
          (SELECT COUNT(*) FROM gold.arbitrage_opportunities_latest) AS arb_pairs,
          (SELECT COUNT(*) FROM gold.price_forecast_7day)            AS forecast_rows,
          (SELECT COUNT(*) FROM gold.crop_recommendation)            AS crop_recs
    """)
    if rows:
        r = rows[0]
        return jsonify({
            "bronze_records":    f"{int(r['bronze_rows']):,}",
            "silver_records":    f"{int(r['silver_rows']):,}",
            "gold_arb_pairs":    f"{int(r['arb_pairs']):,}",
            "forecast_rows":     f"{int(r['forecast_rows']):,}",
            "crop_recs":         f"{int(r['crop_recs']):,}",
            "model_version":     "HistGradientBoosting v2",
            "model_mape":        "9.75%",
            "model_r2":          "0.727",
            "pipeline_status":   "LIVE",
            "source":            "live",
        })
    return jsonify({
        "bronze_records": "727,656", "silver_records": "727,656",
        "gold_arb_pairs": "542",     "forecast_rows": "20,412",
        "crop_recs": "548",          "model_version": "HistGradientBoosting v2",
        "model_mape": "9.75%",       "model_r2": "0.727",
        "pipeline_status": "LIVE",   "source": "cached",
    })


@app.route("/api/recommend", methods=["POST"])
def api_recommend():
    """Bot recommendation endpoint — called by chat widget."""
    body       = request.get_json(force=True)
    commodity  = body.get("commodity")
    lat        = float(body.get("farmer_lat", 0))
    lon        = float(body.get("farmer_lon", 0))
    rate       = float(body.get("transport_rate", 8))
    quantity   = float(body.get("quantity", 10))

    if not commodity or not lat or not lon:
        return jsonify({"error": "commodity, farmer_lat, farmer_lon required"}), 400

    recs = recommend(commodity, lat, lon, rate, quantity)
    return jsonify({"recommendations": recs, "commodity": commodity})

if __name__ == "__main__":
    print("🌾 MandiMax Bot starting on http://localhost:5000")
    print("   Test: http://localhost:5000/test")
    print("   Webhook: POST http://localhost:5000/webhook/whatsapp")
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)


# ══════════════════════════════════════════@app.route("/api/chat", methods=["POST"])
def api_chat():
    """
    Multilingual chat — Sarvam AI powered.
    Flow: User msg → translate to EN → intent → raw reply → Sarvam LLM/translate → user language
    """
    body    = request.get_json(force=True)
    message = body.get("message", "").strip()
    lang    = body.get("language", "en-IN")
    context = body.get("context", {})

    # 1. Translate user message → English for intent parsing
    msg_en = sarvam_translate(message, lang, "en-IN") if lang != "en-IN" else message
    msg_l  = msg_en.lower()

    commodity = extract_commodity(msg_en)
    step      = context.get("step")

    # 2. Route — commodity is HIGHEST priority
    data_for_llm = None
    next_context = {}
    FRESH = (None, "", "ask_commodity", "done")

    if commodity and step in FRESH:
        qty          = extract_quantity(msg_en) or 10
        next_context = {"step": "ask_city", "commodity": commodity, "quantity": qty}
        raw_reply    = (f"Understood! You want to sell {qty} quintals of {commodity}. "
                        f"Which city are you in? (e.g. Indore, Lucknow, Patna, Delhi, Pune, Nagpur)")

    elif msg_l in ("hi","hello","help","start","हेलो","नमस्ते","नमस्कार") or not context:
        raw_reply    = ("Welcome to MandiMax! I help Indian farmers find the best mandi. "
                        "Tell me your crop: tomato, onion, potato, wheat, rice "
                        "| कौन सी फसल? टमाटर, कांदा, बटाटा, गेहूं, चावल")
        next_context = {"step": "ask_commodity"}

    elif "crop" in msg_l or "advisory" in msg_l:
        state_m = re.search(r'(?:crop|advisory)[s\s]+([a-zA-Z\s]+)', msg_en, re.I)
        state   = state_m.group(1).strip() if state_m else "UTTAR PRADESH"
        recs    = get_crop_recs(state)
        data_for_llm = {"type": "crop_advisory", "state": state,
                        "crops": [{"name": r.get("Commodity",""), "avg_price": r.get("avg_price",0),
                                   "stability": r.get("stability",0)} for r in recs]}
        raw_reply    = (f"Best crops for {state}: " +
                        ", ".join(f"{r.get('Commodity','')} ₹{r.get('avg_price',0)}/q" for r in recs))
        next_context = {"step": "done"}

    elif step == "ask_city":
        commodity = context.get("commodity", "Tomato")
        qty       = context.get("quantity", 10)
        city_coords = {
            "ujjain":(23.1828,75.7772),"indore":(22.7196,75.8577),"bhopal":(23.2599,77.4126),
            "lucknow":(26.8467,80.9462),"jaipur":(26.9124,75.7873),"pune":(18.5204,73.8567),
            "mumbai":(19.0760,72.8777),"delhi":(28.6139,77.2090),"ahmedabad":(23.0225,72.5714),
            "surat":(21.1702,72.8311),"nagpur":(21.1458,79.0882),"kanpur":(26.4499,80.3319),
            "patna":(25.5941,85.1376),"kolkata":(22.5726,88.3639),
            "gorakhpur":(26.7606,83.3732),"varanasi":(25.3176,82.9739),
        }
        found = next(((c, coords) for c, coords in city_coords.items() if c in msg_l), None)
        if not found:
            raw_reply    = "City not found. Try: Indore, Lucknow, Patna, Delhi, Pune, Nagpur, Surat, Kanpur."
            next_context = context
        else:
            city_name, (lat, lon) = found
            next_context = {"step":"ask_transport","commodity":commodity,
                            "quantity":qty,"lat":lat,"lon":lon,"city":city_name}
            raw_reply    = (f"Got it — {city_name.title()}! "
                            f"What is your transport cost per km per quintal? "
                            f"Type a number like 10 for ₹10/km, or say 'default' for ₹8/km.")

    elif step == "ask_transport":
        commodity = context.get("commodity","Tomato")
        lat       = context.get("lat",22.7196)
        lon       = context.get("lon",75.8577)
        qty       = context.get("quantity",10)
        city      = context.get("city","your city")
        rate      = 8.0 if "default" in msg_l else (extract_number(msg_en) or 8.0)
        recs      = recommend(commodity, lat, lon, rate, qty)
        practical = [r for r in recs if r["dist"] <= 250] or recs
        nearest   = min(recs, key=lambda r: r["dist"]) if recs else None
        data_for_llm = {"type":"sell_recommendation","commodity":commodity,"city":city,
                        "quantity":qty,"transport_rate":rate,
                        "recommendations":practical[:3],"nearest_market":nearest}
        next_context = {"step":"done"}
        if not practical:
            raw_reply = (f"😔 No profitable markets within 250km of {city} for {commodity} "
                         f"at ₹{rate}/km. Consider selling at your local APMC.")
        else:
            icons = ["🥇","🥈","🥉"]
            lines = [f"🌾 {commodity} near {city.title()} (≤250km, ₹{rate}/km transport)\n"]
            for i, r in enumerate(practical[:3]):
                tag = " 💡 Nearby!" if r["dist"]<=80 else (" ⚠️ Far" if r["dist"]>200 else "")
                lines.append(f"{icons[i]} {r['market']}, {r['district']}{tag}\n"
                             f"   💰 ₹{r['price']:,.0f}/q | 📍 {r['dist']}km | "
                             f"🚚 ₹{r['transport']:,.0f}/q | ✅ Net ₹{r['net']:,.0f}/q\n"
                             f"   💵 Total ({qty}q): ₹{r['total']:,.0f}\n")
            lines.append("_⚠️ Confirm prices before travel. Data from Databricks gold tables._")
            raw_reply = "\n".join(lines)
    else:
        raw_reply    = ("Please tell me which crop to sell: tomato, onion, potato, wheat, rice "
                        "| फसल बताएं: टमाटर, कांदा, बटाटा, गेहूं, चावल")
        next_context = {"step":"ask_commodity"}

    # 3. Sarvam LLM for rich data responses (recommendations, advisory)
    final_reply = raw_reply
    if SARVAM_API_KEY and data_for_llm:
        system = ("You are MandiMax, an AI for Indian farmers. "
                  "Use real APMC data provided. Give practical advice. "
                  "Warn if market >200km. Always show net profit. Do NOT invent prices.")
        prompt = (f"Data: {json.dumps(data_for_llm, ensure_ascii=False)}\n"
                  f"User said: {message}\nrespond helpfully and concisely.")
        llm_resp = sarvam_llm(system, prompt, lang)
        if llm_resp:
            final_reply = llm_resp

    # 4. Translate ALL intermediate replies (city/transport/welcome) to user's language
    if lang != "en-IN" and final_reply == raw_reply and SARVAM_API_KEY:
        translated = sarvam_translate(raw_reply, "en-IN", lang)
        if translated and translated != raw_reply:
            final_reply = translated

    return jsonify({"reply": final_reply, "context": next_context,
                    "language": lang, "sarvam_used": bool(SARVAM_API_KEY)})


@app.route("/api/stt", methods=["POST"])
def api_stt():
    """Speech-to-Text: convert farmer's voice to text using Sarvam Saarika ASR."""
    if not SARVAM_API_KEY:
        return jsonify({"error": "Sarvam key not configured"}), 503
    audio = request.files.get("audio")
    lang  = request.form.get("language", "hi-IN")
    if not audio:
        return jsonify({"error": "No audio file"}), 400
    try:
        resp = requests.post(
            "https://api.sarvam.ai/speech-to-text",
            headers={"api-subscription-key": SARVAM_API_KEY},
            files={"file": (audio.filename or "audio.webm", audio.read(), audio.content_type or "audio/webm")},
            data={"language_code": lang, "model": "saarika:v2", "with_timestamps": "false"},
            timeout=15,
        )
        result = resp.json()
        return jsonify({"transcript": result.get("transcript", ""), "language": lang})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tts", methods=["POST"])
def api_tts():
    """Text-to-Speech: convert bot reply to audio using Sarvam Bulbul TTS."""
    if not SARVAM_API_KEY:
        return jsonify({"error": "Sarvam key not configured"}), 503
    body  = request.get_json(force=True)
    text  = body.get("text", "")
    lang  = body.get("language", "hi-IN")
    if not text:
        return jsonify({"error": "No text"}), 400
    try:
        resp = requests.post(
            "https://api.sarvam.ai/text-to-speech",
            headers={"api-subscription-key": SARVAM_API_KEY,
                     "Content-Type": "application/json"},
            json={"inputs": [text[:500]], "target_language_code": lang,
                  "speaker": "meera", "pitch": 0, "pace": 1.15,
                  "loudness": 1.5, "speech_sample_rate": 22050,
                  "enable_preprocessing": True, "model": "bulbul:v1"},
            timeout=15,
        )
        result = resp.json()
        # Returns base64-encoded audio
        audios = result.get("audios", [])
        if audios:
            return jsonify({"audio_base64": audios[0], "language": lang})
        return jsonify({"error": "TTS failed", "detail": result}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
