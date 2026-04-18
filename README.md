# 🌾 MandiMax — AI-Powered APMC Market Intelligence for Indian Farmers

> **Bharat Bricks Hackathon 2026 | IIT Indore | Track: Swatantra (Open)**

MandiMax is an end-to-end agricultural intelligence platform that ingests live APMC mandi price data into Databricks Unity Catalog, runs ML-based price forecasting and arbitrage detection, and delivers actionable crop-selling recommendations to farmers through a multilingual AI chatbot powered by **Sarvam-M LLM** — all in their native Indian language.

---

## 🏆 Hackathon Submission Summary

| Field | Details |
|---|---|
| **Track** | Swatantra — Open / Indic AI Use Case |
| **Team** | CSE, IIT Indore |
| **Problem** | Indian farmers lose 30–40% of potential income by selling at the wrong APMC mandi at the wrong time |
| **Solution** | Real-time arbitrage detection + multilingual AI bot that tells farmers exactly where to sell, at what price, after transport costs |
| **Indian AI Model** | ✅ Sarvam-M LLM + Sarvam Saarika ASR + Sarvam Bulbul TTS |
| **Databricks App** | ✅ Streamlit deployed as Databricks App |
| **Live Demo** | https://mandimax-bot-7474653808917244.aws.databricksapps.com |
| **React Dashboard** | https://mandimax.netlify.app |

---

## 🎯 What It Does

MandiMax ingests raw APMC price data from 1,597 markets across India, processes it through a bronze→silver→gold Lakehouse pipeline on Databricks, trains a price forecasting ML model, and surfaces the results through:
1. A **Streamlit dashboard** (deployed as Databricks App) with live arbitrage opportunities, crop recommendations, and an AI chatbot.
2. A **React dashboard** (Netlify) with heatmaps, charts, and the multilingual MandiMax Bot.
3. A **WhatsApp bot** (Twilio + Flask) for farmers without smartphones.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   data.gov.in APMC API  │  Agmarknet CSV  │  Geocode Lookup     │
└──────────────┬──────────────────────────────────────────────────┘
               │ Notebook 01: Bronze Ingestion
               ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS UNITY CATALOG (workspace)               │
│                                                                 │
│  Bronze Layer (workspace.bronze)                                │
│  └── mandi_prices_raw  [727,656 rows, Delta Lake]              │
│                                                                 │
│  Silver Layer (workspace.silver)         Notebook 02            │
│  └── mandi_prices_clean  [deduped, geo-enriched, outliers]     │
│                                                                 │
│  Gold Layer (workspace.gold)             Notebook 03            │
│  ├── arbitrage_opportunities_latest  [542 pairs]               │
│  ├── crop_recommendation             [548 rows, by state]      │
│  └── price_forecast_7day             [20,412 rows, 7-day ML]   │
└──────────────┬──────────────────────────────────────────────────┘
               │
     ┌─────────┴──────────┐
     │                    │
     ▼                    ▼
┌─────────────┐    ┌──────────────────────────────────────┐
│  Notebook   │    │     DATABRICKS APP (Streamlit)        │
│  04: ML     │    │  • Dashboard (live SQL from gold)     │
│  Training   │    │  • MandiMax Bot (Sarvam-M LLM)        │
│  HistGBM    │    │  • Crop Advisory (by state)           │
│  MAPE:9.75% │    │  Auth: Service Principal (auto)       │
└─────────────┘    └──────────────┬───────────────────────┘
                                  │
                   ┌──────────────┘
                   ▼
           ┌──────────────────────────────────┐
           │    SARVAM AI (Indian LLM)        │
           │  • Sarvam-M: Multilingual LLM    │
           │  • Saarika: ASR (voice input)    │
           │  • Bulbul: TTS (voice output)    │
           │  • Translate: 10 Indian langs    │
           └──────────────────────────────────┘
                   │
     ┌─────────────┴──────────────┐
     ▼                            ▼
┌──────────────┐          ┌───────────────────┐
│ React (Netlify│          │  WhatsApp Bot     │
│ mandimax.netlify.app)   │  Flask + Twilio   │
│ Netlify Proxy│          │  (bot/app.py)     │
└──────────────┘          └───────────────────┘
```

---

## 🔧 Databricks Technologies Used

| Technology | How We Use It |
|---|---|
| **Delta Lake** | Bronze/Silver/Gold tables with ACID transactions, schema evolution |
| **Apache Spark / PySpark** | Data cleaning, geo-enrichment, 30-day rolling window aggregations |
| **Unity Catalog** | Three-level namespace (`workspace.gold.*`), fine-grained ACLs |
| **Databricks Apps** | Hosts the Streamlit ML dashboard with service principal auth |
| **MLflow** | Experiment tracking for HistGradientBoosting price forecast model |
| **Databricks SQL Warehouse** | Low-latency queries from Streamlit app at runtime |
| **Databricks Workflows** | Scheduled pipeline: ingest → clean → aggregate → forecast |
| **Databricks Secrets** | Sarvam API key stored securely |

---

## 🤖 Indian AI Models Used

| Model | Provider | Usage |
|---|---|---|
| **Sarvam-M** | Sarvam AI | Core LLM — generates multilingual farmer recommendations |
| **Saarika v2** | Sarvam AI | ASR — voice-to-text for mic input in chatbot |
| **Bulbul v1** | Sarvam AI | TTS — reads bot replies aloud to farmers |
| **Sarvam Translate** | Sarvam AI | Translates between 10 Indian languages |

Supported languages: Hindi, Marathi, Gujarati, Tamil, Telugu, Kannada, Bengali, Punjabi, Malayalam, Odia

---

## 📁 Repository Structure

```
BharatBricks/
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Ingest raw APMC data → Delta Lake
│   ├── 02_silver_transform.py      # Clean, deduplicate, geo-enrich
│   ├── 03_gold_aggregations.py     # Arbitrage detection, crop recs
│   ├── 04_ml_training.py           # HistGBM price forecasting (MLflow)
│   └── 05_bot_api.py               # Notebook for API testing
│
├── bot/
│   ├── streamlit_app.py            # ⭐ Main Databricks App (Streamlit)
│   ├── app.py                      # Flask WhatsApp bot (Twilio)
│   ├── app.yaml                    # Databricks Apps deployment config
│   └── requirements.txt
│
├── dashboard/
│   ├── src/
│   │   ├── components/
│   │   │   ├── ChatBot.js          # Multilingual AI chatbot widget
│   │   │   ├── ArbitrageTable.js   # Live arbitrage opportunities
│   │   │   ├── HeatmapPanel.js     # Price heatmap (India map)
│   │   │   └── StatCards.js        # Pipeline health stats
│   │   └── api.js                  # Live-or-cached API client
│   ├── netlify.toml                # Netlify proxy config (CORS bypass)
│   └── netlify/functions/
│       └── api-proxy.js            # Serverless proxy with auth
│
├── .env.example                    # Environment variable template
└── README.md
```

---

## 🚀 How to Run

### Prerequisites

- Databricks Free Edition workspace (sign up at [databricks.com](https://databricks.com))
- Python 3.10+
- Node.js 18+ (for React dashboard only)
- Sarvam AI API key ([dashboard.sarvam.ai](https://dashboard.sarvam.ai))
- Databricks CLI installed: `pip install databricks-cli`

---

### Step 1: Configure Databricks CLI

```bash
databricks configure --host https://<your-workspace>.cloud.databricks.com
# Enter your Personal Access Token when prompted
```

---

### Step 2: Run the Data Pipeline (Notebooks)

Import all notebooks into your Databricks workspace and run in order:

```
Workspace → Import → Upload → select notebooks/01_bronze_ingestion.py
```

Run order:
```
01_bronze_ingestion.py    →  Creates workspace.bronze.mandi_prices_raw
02_silver_transform.py    →  Creates workspace.silver.mandi_prices_clean
03_gold_aggregations.py   →  Creates 3 gold tables (arbitrage, crops, forecast)
04_ml_training.py         →  Trains HistGBM, logs to MLflow, writes price_forecast_7day
```

Each notebook uses Unity Catalog — no manual table creation needed.

---

### Step 3: Deploy the Streamlit App to Databricks

```bash
# 1. Configure app.yaml with your SQL warehouse HTTP path
#    (find it: Databricks → SQL Warehouses → your warehouse → Connection Details)
# Edit bot/app.yaml: set DATABRICKS_SQL_HTTP_PATH

# 2. Set your Sarvam API key in Databricks App environment
#    (DO NOT put real key in app.yaml — use Databricks App environment tab)

# 3. Sync and deploy
cd bot/
databricks sync . /Workspace/Users/<your-email>/mandimax-bot
databricks apps deploy mandimax-bot \
  --source-code-path /Workspace/Users/<your-email>/mandimax-bot
```

Wait ~2 minutes, then open:
```
https://<your-app-id>.databricksapps.com
```

---

### Step 4: Grant Service Principal Access to Gold Tables

In Databricks SQL Editor, run:

```sql
-- Find your app's service principal applicationId first:
-- databricks service-principals list --output json | grep -A3 "mandimax"

USE CATALOG workspace;

GRANT USE CATALOG ON CATALOG workspace
  TO `<app-service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA workspace.gold
  TO `<app-service-principal-application-id>`;

GRANT USE SCHEMA ON SCHEMA workspace.silver
  TO `<app-service-principal-application-id>`;

GRANT SELECT ON TABLE workspace.gold.arbitrage_opportunities_latest
  TO `<app-service-principal-application-id>`;

GRANT SELECT ON TABLE workspace.gold.crop_recommendation
  TO `<app-service-principal-application-id>`;

GRANT SELECT ON TABLE workspace.gold.price_forecast_7day
  TO `<app-service-principal-application-id>`;

GRANT SELECT ON TABLE workspace.silver.mandi_prices_clean
  TO `<app-service-principal-application-id>`;
```

---

### Step 5: Run React Dashboard Locally (Optional)

```bash
cd dashboard/
npm install
REACT_APP_API_BASE_URL=http://localhost:8000 npm start
```

To run Flask backend locally:
```bash
cd bot/
pip install -r requirements.txt
cp ../.env.example .env
# Fill in .env with your credentials
python app.py
```

---

### Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
# Databricks
DATABRICKS_HOST=https://dbc-ae674f11-a144.cloud.databricks.com
DATABRICKS_TOKEN=your_pat_token
DATABRICKS_SQL_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
DATABRICKS_CATALOG=workspace

# Sarvam AI
SARVAM_API_KEY=your_sarvam_api_key

# Twilio (WhatsApp bot — optional)
TWILIO_ACCOUNT_SID=your_sid
TWILIO_AUTH_TOKEN=your_token
```

---

## 🎮 Demo Steps

### Demo 1: Streamlit App (Databricks App)

1. Open `https://mandimax-bot-7474653808917244.aws.databricksapps.com`
2. **Dashboard page**: See live arbitrage opportunities from real APMC data
3. **MandiMax Bot page**:
   - Select language: **मराठी** (Marathi)
   - Type: `कांदा` → bot responds in Marathi asking for city
   - Type: `पुणे` → asks for transport cost
   - Type: `12` → shows top 3 mandis with net profit after transport
4. **Crop Advisory page**: Select `MAHARASHTRA` → Sarvam AI generates advice in Marathi

### Demo 2: React Dashboard (Netlify)

1. Open `https://mandimax.netlify.app`
2. Click the chat bubble (bottom right) → MandiMax Bot opens
3. Try: `sell 50 quintal tomato`
4. Type city: `Indore`
5. Type transport rate: `8`
6. See top 3 mandis with 🥇🥈🥉 rankings and net profit calculation

### Demo 3: Multilingual Voice Input

1. Click the 🎤 mic button in the chatbot
2. Say: *"मुझे टमाटर बेचना है"* (Hindi: "I want to sell tomato")
3. Sarvam Saarika transcribes → bot responds in Hindi → Sarvam Bulbul reads reply aloud

---

## 📊 Model Performance

| Metric | Value |
|---|---|
| Model | HistGradientBoostingRegressor |
| MAPE | **9.75%** |
| R² Score | **0.727** |
| Training data | 727,656 APMC price records |
| Forecast horizon | 7 days |
| Markets covered | 1,597 APMC mandis |
| Commodities | Tomato, Onion, Potato, Wheat, Rice |
| MLflow run | Logged in Databricks workspace |

---

## 🌐 Live Deployments

| Component | URL | Status |
|---|---|---|
| Streamlit (Databricks App) | https://mandimax-bot-7474653808917244.aws.databricksapps.com | ✅ Live |
| React Dashboard (Netlify) | https://mandimax.netlify.app | ✅ Live |
| API Health Check | https://mandimax-bot-7474653808917244.aws.databricksapps.com/health | ✅ Live |

---

## 💡 Key Innovation

1. **Real APMC Data Pipeline**: Not mock data — we ingest and process actual government APMC prices through a production Lakehouse pipeline
2. **Practical Distance Filter**: 250km radius filter ensures only reachable markets are recommended — a farmer in Indore won't be sent to Kolkata
3. **Net Profit After Transport**: Shows `Net = Price − (distance × rate)` and `Total = Net × quantity` — the number that actually matters to a farmer
4. **Full Sarvam AI Stack**: Translate input → LLM in native language → TTS output — complete voice-to-voice multilingual loop
5. **Zero CORS Architecture**: Streamlit on Databricks = same-origin, no proxy needed

---

## 🔗 Submission Links

- **GitHub**: https://github.com/abhishekmane6122/BharatBricks
- **Demo Video**: *(2-min demo video link)*
- **Live App**: https://mandimax-bot-7474653808917244.aws.databricksapps.com
- **Dashboard**: https://mandimax.netlify.app

---

## 👨‍💻 Team

Built at **Bharat Bricks Hackathon 2026, IIT Indore**
Track: Swatantra | April 17–18, 2026

---

*Built with ❤️ on Databricks Free Edition | Powered by Sarvam AI | For Indian Farmers*
