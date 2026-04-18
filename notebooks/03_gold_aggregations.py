# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Notebook 03 — Gold Layer: Business-Ready Serving Tables
# MAGIC **Mandi-Max | Bharat Bricks Hackathon**
# MAGIC
# MAGIC **Purpose:** Aggregate Silver data into production-ready Gold tables consumed
# MAGIC by the FPO Dashboard and WhatsApp Bot API.
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `gold.daily_commodity_summary`      — Dashboard heatmap
# MAGIC - `gold.arbitrage_opportunities_latest` — Bot serving table
# MAGIC - `gold.market_geocodes_lookup`        — Low-latency geo lookup
# MAGIC - `gold.crop_recommendation`           — Seasonal advisory

# COMMAND ----------
# MAGIC %md ## Step 1 — Setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

# UC FIX: Don't USE a schema globally — qualify all table names with schema prefix
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("✅ Gold database ready")

# COMMAND ----------
# MAGIC %md ## Step 2 — gold.daily_commodity_summary (Dashboard Heatmap)

# COMMAND ----------
# Aggregated daily price statistics per (State, District, Commodity)
# Outliers excluded from aggregations — they skew heatmap colors

df_summary = spark.sql("""
    SELECT
        STATE,
        District_Name,
        Market_Name,
        Commodity,
        Variety,
        Price_Date,
        ROUND(AVG(Modal_Price), 2)  AS avg_modal_price,
        ROUND(MIN(Min_Price),   2)  AS day_min_price,
        ROUND(MAX(Max_Price),   2)  AS day_max_price,
        ROUND(STDDEV(Modal_Price), 2) AS price_std,
        COUNT(*)                     AS record_count,
        lat,
        lon,
        geohash4
    FROM silver.mandi_prices_clean
    WHERE is_outlier = false
    GROUP BY
        STATE, District_Name, Market_Name, Commodity, Variety,
        Price_Date, lat, lon, geohash4
""")

df_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Commodity", "STATE") \
    .saveAsTable("gold.daily_commodity_summary")

# UC FIX: OPTIMIZE + ZORDER not supported on Serverless — skipped gracefully
try:
    spark.sql("OPTIMIZE gold.daily_commodity_summary ZORDER BY (Price_Date, District_Name)")
except Exception as e:
    print(f"⚠️  OPTIMIZE skipped: {e.__class__.__name__}")

print(f"✅ gold.daily_commodity_summary: {spark.table('gold.daily_commodity_summary').count():,} rows")

# COMMAND ----------
# MAGIC %md ## Step 3 — gold.arbitrage_opportunities_latest (Bot Serving Table)

# COMMAND ----------
# Top 5 arbitrage opportunities per source market per commodity
# This is what the WhatsApp bot queries — must be fast (≤200ms)

rank_window = Window.partitionBy("src_market", "src_commodity") \
                    .orderBy(F.col("arbitrage_spread").desc())

df_arb_latest = (
    spark.table("silver.arbitrage_base")
    .withColumn("_rank", F.row_number().over(rank_window))
    .filter(F.col("_rank") <= 5)    # Top 5 target mandis per source market
    .drop("_rank")
    # Add a "nearest" flag for the bot — rank 1 = nearest by spread, not distance
    .withColumn("is_best_option", F.lit(False))   # Will be set per query in bot logic
)

df_arb_latest.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.arbitrage_opportunities_latest")

# UC FIX: OPTIMIZE + ZORDER not supported on Serverless — skipped gracefully
try:
    spark.sql("OPTIMIZE gold.arbitrage_opportunities_latest ZORDER BY (src_commodity, src_market)")
except Exception as e:
    print(f"⚠️  OPTIMIZE skipped: {e.__class__.__name__}")

print(f"✅ gold.arbitrage_opportunities_latest: {spark.table('gold.arbitrage_opportunities_latest').count():,} rows")

# COMMAND ----------
# MAGIC %md ## Step 4 — gold.market_geocodes_lookup (Low-Latency Geo)

# COMMAND ----------
# Clean, deduplicated market → (lat, lon) lookup table
# Used by the bot to geocode the farmer's stated location → nearest market

df_geo_lookup = spark.sql("""
    SELECT DISTINCT
        UPPER(TRIM(market_name))   AS market_name,
        UPPER(TRIM(district_name)) AS district_name,
        UPPER(TRIM(state))         AS state,
        lat,
        lon,
        source,
        fetched_at
    FROM bronze.geocode_cache
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").dropDuplicates(["market_name", "district_name"])

df_geo_lookup.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.market_geocodes_lookup")

print(f"✅ gold.market_geocodes_lookup: {df_geo_lookup.count():,} unique markets with coords")

# COMMAND ----------
# MAGIC %md ## Step 5 — gold.crop_recommendation (Seasonal Advisory)

# COMMAND ----------
# UC FIX: DATE_SUB(MAX(col) OVER()) is a window aggregate inside WHERE — not supported.
# Replaced with a two-step approach: compute max date first, then filter.

max_price_date = spark.sql("SELECT MAX(Price_Date) FROM silver.mandi_prices_clean").collect()[0][0]

df_crop_rec = spark.sql(f"""
    WITH recent AS (
        SELECT
            STATE,
            District_Name,
            Commodity,
            Price_Date,
            AVG(Modal_Price) AS avg_price
        FROM silver.mandi_prices_clean
        WHERE is_outlier = false
          AND Price_Date >= DATE_SUB(DATE('{max_price_date}'), 30)
        GROUP BY STATE, District_Name, Commodity, Price_Date
    ),
    stats AS (
        SELECT
            STATE,
            District_Name,
            Commodity,
            ROUND(AVG(avg_price), 2)    AS avg_30d_price,
            ROUND(STDDEV(avg_price), 2) AS std_30d_price,
            ROUND(
                STDDEV(avg_price) / NULLIF(AVG(avg_price), 0) * 100, 2
            )                           AS coeff_variation_pct,
            COUNT(DISTINCT Price_Date)  AS active_days,
            MAX(avg_price)              AS recent_high,
            MIN(avg_price)              AS recent_low
        FROM recent
        GROUP BY STATE, District_Name, Commodity
    )
    SELECT
        STATE,
        District_Name,
        Commodity,
        avg_30d_price,
        std_30d_price,
        ROUND(100 - LEAST(coeff_variation_pct, 100), 2) AS stability_score,
        active_days,
        recent_high,
        recent_low,
        ROUND(recent_high - recent_low, 2) AS price_range_30d,
        CASE
            WHEN coeff_variation_pct < 10 THEN 'LOW'
            WHEN coeff_variation_pct < 25 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS volatility_label,
        RANK() OVER (
            PARTITION BY STATE, District_Name
            ORDER BY (100 - LEAST(coeff_variation_pct, 100)) DESC, avg_30d_price DESC
        ) AS crop_rank
    FROM stats
    WHERE active_days >= 10
""")

df_crop_rec.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("STATE") \
    .saveAsTable("gold.crop_recommendation")

print(f"✅ gold.crop_recommendation: {df_crop_rec.count():,} district-commodity combinations ranked")

# COMMAND ----------
# MAGIC %md ## Step 6 — Gold Layer Summary

# COMMAND ----------
print("=" * 60)
print("GOLD LAYER — ALL TABLES READY")
print("=" * 60)

tables = [
    "gold.daily_commodity_summary",
    "gold.arbitrage_opportunities_latest",
    "gold.market_geocodes_lookup",
    "gold.crop_recommendation",
]

for t in tables:
    count = spark.table(t).count()
    print(f"  ✅ {t:<45} {count:>10,} rows")

print("\n📊 Sample: Top Arbitrage Opportunities Right Now:")
spark.sql("""
    SELECT
        src_market       AS From_Mandi,
        tgt_market       AS To_Mandi,
        src_commodity    AS Crop,
        ROUND(src_price) AS From_Price,
        ROUND(tgt_price) AS To_Price,
        ROUND(arbitrage_spread) AS Spread_INR,
        ROUND(distance_km)      AS Distance_KM
    FROM gold.arbitrage_opportunities_latest
    ORDER BY arbitrage_spread DESC
    LIMIT 10
""").show(truncate=False)

print("\n🌾 Top Crop Recommendations (Least Volatile, Highest Price):")
spark.sql("""
    SELECT District_Name, STATE, Commodity, avg_30d_price,
           volatility_label, stability_score, crop_rank
    FROM gold.crop_recommendation
    WHERE crop_rank <= 3
    ORDER BY STATE, District_Name, crop_rank
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Gold Layer Complete
# MAGIC
# MAGIC All serving tables are ready. The ML Forecast table (`gold.price_forecast_7day`)
# MAGIC will be created by **Notebook 04** after model training.
# MAGIC
# MAGIC **Next Step → Run Notebook 04: ML Training & Forecasting**
