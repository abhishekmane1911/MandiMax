# Databricks notebook source
# MAGIC %md
# MAGIC # 🌍 Notebook 00 — Geocode Seeder
# MAGIC **Mandi-Max | Bharat Bricks Hackathon**
# MAGIC
# MAGIC **Purpose:** Call OpenCage Geocoding API for the top N most-active markets
# MAGIC from the bronze table and persist results to `bronze.geocode_cache`.
# MAGIC Run this ONCE before any Silver processing.
# MAGIC
# MAGIC **Free tier:** OpenCage gives 2,500 calls/day. We only need ~200-300 unique markets.

# COMMAND ----------
# MAGIC %pip install requests -q

# COMMAND ----------
# MAGIC %md ### 🔑 Load API Keys

# COMMAND ----------
# Load keys from the shared 00_config notebook.
# If that fails, paste your key directly in OPENCAGE_API_KEY below.
try:
    %run ./00_config
    print("✅ Keys loaded from 00_config")
except:
    OPENCAGE_API_KEY = "YOUR_OPENCAGE_KEY_HERE"   # ← paste key here
    RAW_DATA_PATH    = "dbfs:/FileStore/tables/Agriculture_price_dataset.csv"
    print("⚠️  Fallback config — hardcoded key used")

# COMMAND ----------
import requests
import time
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

# ── API Config ─────────────────────────────────────────────────────────────────
# Set your key in Databricks Secrets or replace directly for the hackathon
OPENCAGE_API_KEY = dbutils.secrets.get(scope="mandi-max", key="opencage_api_key") \
                   if False else "YOUR_OPENCAGE_KEY_HERE"   # ← Replace for hackathon

OPENCAGE_URL   = "https://api.opencagedata.com/geocode/v1/json"
RATE_LIMIT_SEC = 0.5    # 2 requests/sec — well under free tier limit
TOP_N_MARKETS  = 250    # Geocode top 250 markets by record count

# COMMAND ----------
# MAGIC %md ## Step 1 — Get Top N Markets from Bronze

# COMMAND ----------
top_markets = (
    spark.sql("""
        SELECT
            Market_Name,
            District_Name,
            STATE,
            COUNT(*) AS record_count
        FROM bronze.mandi_prices_raw
        WHERE _is_quarantine = false
          AND Market_Name IS NOT NULL
        GROUP BY Market_Name, District_Name, STATE
        ORDER BY record_count DESC
        LIMIT {n}
    """.format(n=TOP_N_MARKETS))
    .toPandas()
)

# Skip markets already in cache
existing_cache = spark.sql("""
    SELECT LOWER(TRIM(market_name)) AS mk, LOWER(TRIM(district_name)) AS dk
    FROM bronze.geocode_cache
""").toPandas()

existing_set = set(zip(
    existing_cache["mk"].str.lower().str.strip(),
    existing_cache["dk"].str.lower().str.strip()
))

to_geocode = top_markets[
    ~top_markets.apply(
        lambda r: (r["Market_Name"].lower().strip(), r["District_Name"].lower().strip()) in existing_set,
        axis=1
    )
]

print(f"📍 Total top markets : {len(top_markets)}")
print(f"✅ Already cached    : {len(top_markets) - len(to_geocode)}")
print(f"🔄 To geocode now    : {len(to_geocode)}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Call OpenCage API

# COMMAND ----------
results = []
failed  = []

for idx, row in to_geocode.iterrows():
    query = f"{row['Market_Name']}, {row['District_Name']}, {row['STATE']}, India"
    try:
        resp = requests.get(OPENCAGE_URL, params={
            "q":          query,
            "key":        OPENCAGE_API_KEY,
            "countrycode": "in",
            "limit":      1,
            "no_annotations": 1
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data["results"]:
            geo  = data["results"][0]["geometry"]
            results.append({
                "market_name":   row["Market_Name"],
                "district_name": row["District_Name"],
                "state":         row["STATE"],
                "lat":           float(geo["lat"]),
                "lon":           float(geo["lng"]),
                "source":        "opencage",
                "fetched_at":    datetime.utcnow().isoformat()
            })
        else:
            failed.append({"market": row["Market_Name"], "reason": "no_results"})

    except Exception as e:
        failed.append({"market": row["Market_Name"], "reason": str(e)})

    time.sleep(RATE_LIMIT_SEC)

print(f"✅ Geocoded successfully : {len(results)}")
print(f"❌ Failed               : {len(failed)}")
if failed:
    print("Failed markets:", [f["market"] for f in failed[:10]])

# COMMAND ----------
# MAGIC %md ## Step 3 — Write Results to bronze.geocode_cache

# COMMAND ----------
if results:
    import pandas as pd
    df_geo = spark.createDataFrame(pd.DataFrame(results))
    df_geo = df_geo.withColumn("fetched_at", F.to_timestamp(F.col("fetched_at")))

    df_geo.write.format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .saveAsTable("bronze.geocode_cache")

    print(f"✅ Wrote {len(results)} geocodes to bronze.geocode_cache")

# Verify
spark.sql("SELECT COUNT(*) AS cached_markets FROM bronze.geocode_cache").show()
spark.sql("SELECT * FROM bronze.geocode_cache LIMIT 5").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Geocode Seeding Complete
# MAGIC Re-run this notebook anytime to enrich new markets. Already-cached markets are skipped.
