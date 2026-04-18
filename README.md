# 🌾 Mandi-Max: Spatial Arbitrage & Predictive Engine for Indian Agriculture
**Bharat Bricks Hackathon 2026: IIT Indore** | **Track:** Swatantra — Open Indic AI

## 📖 Overview
Mandi-Max solves **farmer information asymmetry** by leveraging Databricks Lakehouse architecture and Agentic AI. It provides a WhatsApp Voice Bot that understands rural farmers in their native Indian languages and a dashboard for FPO (Farmer Producer Organization) admins to track localized commodity price predictions and spatial arbitrage opportunities.

## 🏆 Hackathon Core Requirements Fulfilled
- **Databricks as Core:** Uses **Delta Lake** for medallion architecture, **PySpark** for data engineering, and **MLflow** for experiment tracking.
- **AI as Central:** Core value driven by **HistGradientBoostingRegressor** for time-series price forecasting and **Sarvam AI** for multilingual native voice understanding.
- **Constraints Aware:** Designed specifically for **Databricks Free Edition (15GB, CPU-only)** by using `HistGradientBoostingRegressor` which natively handles NaNs without data distortion and scales on CPUs fast, and spatial bucketing algorithms for memory-safe arbitrage computation.
- **UI Included:** React + Leaflet Dashboard + Databricks Apps Flask Backend serving WhatsApp bot.

## 🏗️ Architecture & Pipeline View
```text
Raw CSV (DBFS) 
     ↓ [Bronze Layer (PySpark)]
mandi_prices_raw │ geocode_cache
     ↓ [Silver Layer (Data Cleaning & Spatial Arbitrage)]
mandi_prices_clean │ arbitrage_base (OOM-Safe Geohash algorithm)
     ↓ [Gold Layer (Aggregations)]
ml_feature_store │ daily_commodity_summary │ arbitrage_opportunities_latest
     ↓ [ML Training (Scikit-Learn on Pandas + MLflow)]
HistGradientBoostingRegressor (Optimized for Tabular Scale)
     ↓ [Inference]
gold.price_forecast_7day
     ↓
Bot API (Databricks Apps / Flask) ↔ Sarvam AI (STT/TTS) ↔ WhatsApp Farmers
FPO Dashboard (React)
```

## 🛠️ Technology Stack
- **Compute & Storage:** Databricks Free Edition (CPU-only), DBFS, Delta Lake.
- **Data Engineering:** PySpark (Medallion Architecture 🥉🥈🥇)
- **Machine Learning Engine:** `scikit-learn`'s `HistGradientBoostingRegressor` (Serverless-compatible, handles NaNs natively, extremely fast on tabular CPU data). This replaces previous RandomForest approaches.
- **MLOps:** Databricks MLflow with `infer_signature` for rigorous tracking of experiments, parameters, and evaluation metrics (MAE, RMSE, R², MAPE).
- **Indic AI Voice Layer:** Sarvam AI models (Indic STT and TTS) enabling seamless multi-lingual processing.
- **Front-end / Integration:** React.js, Recharts, Leaflet (Dashboard); Flask (Databricks App / WhatsApp endpoint).

---

## 🚀 Step-by-Step Setup Guidelines

### 1. Prerequisite: Databricks Free Edition Workspace
Sign up for [Databricks Free Edition](https://community.cloud.databricks.com/login.html) and launch a CPU compute cluster.

### 2. Import Project Notebooks
Clone the repository and import the `notebooks/` directory into your Workspace.
```bash
databricks workspace import_dir ./notebooks /Workspace/MandiMax/notebooks
```

### 3. Data Ingestion (DBFS)
Download the `Agriculture_price_dataset.csv` from data.gov.in and upload it to DBFS:
```text
dbfs:/FileStore/mandi_max/raw/Agriculture_price_dataset.csv
```

### 4. Execute the Data & ML Pipeline (Notebooks)
Run the notebooks sequentially on your Databricks cluster:
1. **`00_geocode_seeder.py`**: Pre-caches market lat/long (requires `OPENCAGE_API_KEY`).
2. **`01_bronze_ingestion.py`**: Moves raw CSV into a structured Delta Bronze table.
3. **`02_silver_transform.py`**: Cleanses data and utilizes a precision-4 Geohash bucketing framework to compute nearby Mandis without out-of-memory overhead (`O(9N)` comparison vs `O(N²)`).
4. **`03_gold_aggregations.py`**: Curates the feature store (`gold.ml_feature_store`) with lag/rolling/cyclical date features.
5. **`04_ml_training.py`**: Model training phase using `HistGradientBoostingRegressor`. Evaluates model, registers signature, and logs to **MLflow** registry. It recursively auto-generates 7-day dynamic forecasts and dumps into `gold.price_forecast_7day`.
6. **`05_bot_api.py`**: Deploys the Flask Backend integrating the generated ML inferences with Sarvam AI.

### 5. Start the Web Dashboard
```bash
cd dashboard
cp .env.example .env    # Add your backend API and tokens
npm install
npm start               # App starts at http://localhost:3000
```

---

## 🎯 Important Design Decisions
- **OOM-Safe Spatial Arbitrage:** Uses Geohash precision-4 bucketing. Each market only checks 9 nearby geohash cells (~40km radius).
- **Time-Based Train/Test Split:** Uses a hard cutoff date (`2025-01-01`) rather than random splitting to prevent data leakage in time-series data.
- **Dynamic 7-Day Recursive Forecast:** The ML pipeline dynamically advances the date, updates cyclical (sine/cosine) timestamps, and predicts forward to simulate an accurate curve.
- **Resource Aware (Hackathon Constraints):** Using `HistGradientBoostingRegressor` handles NaNs implicitly avoiding standard scaling data distortion. Data is processed distributedly on PySpark and safely aggregated `toPandas()` for the final modeling step.
