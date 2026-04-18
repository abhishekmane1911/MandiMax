# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Notebook 02 — Silver Layer: Clean, Enrich & Arbitrage
# MAGIC **Mandi-Max | Bharat Bricks Hackathon**
# MAGIC
# MAGIC **Purpose:** Transform raw bronze data into trusted, analysis-ready Silver tables.
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `silver.mandi_prices_clean`  — deduplicated, typed, outlier-flagged, geo-enriched
# MAGIC - `silver.commodity_name_map`  — auditable commodity normalization dictionary
# MAGIC - `silver.arbitrage_base`      — OOM-safe geohash-bounded spatial arbitrage pairs

# COMMAND ----------
# MAGIC %pip install geohash2 haversine -q

# COMMAND ----------
# MAGIC %md ## Step 1 — Imports & Config

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DoubleType, DateType, BooleanType, StringType, ArrayType
)
import geohash2
from haversine import haversine, Unit

# UC FIX: Don't USE a schema globally — qualify all table names instead
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# UC FIX: No dbfs: checkpoint paths on Serverless
GEOHASH_PRECISION   = 4
MAX_ARBITRAGE_KM    = 150
OUTLIER_Z_THRESHOLD = 3.0

print("✅ Config loaded")

# COMMAND ----------
# MAGIC %md ## Step 2 — Commodity Normalization Map

# COMMAND ----------
# Canonical commodity name mapping — handles Hindi names, spelling variants, abbreviations
COMMODITY_MAP = {
    # Soybean variants
    "soya bean": "Soybean", "soya": "Soybean", "soyabean": "Soybean",
    "soybeans": "Soybean",
    # Wheat
    "gehun": "Wheat", "गेहूं": "Wheat",
    # Onion
    "pyaz": "Onion", "प्याज": "Onion",
    # Tomato
    "tamatar": "Tomato", "टमाटर": "Tomato",
    # Potato
    "aloo": "Potato", "आलू": "Potato",
    # Maize
    "corn": "Maize", "makka": "Maize", "मक्का": "Maize",
    # Cotton
    "kapas": "Cotton", "कपास": "Cotton",
    # Mustard
    "sarson": "Mustard", "सरसों": "Mustard", "rapeseed": "Mustard",
    # Gram / Chickpea
    "chana": "Gram", "chickpea": "Gram", "bengal gram": "Gram",
    # Add more as you discover from your dataset
}

# Materialize as a Delta table for audit trail
if COMMODITY_MAP:
    import pandas as pd
    df_map = spark.createDataFrame(
        pd.DataFrame([{"raw_name": k, "canonical_name": v} for k, v in COMMODITY_MAP.items()])
    )
    df_map.write.format("delta").mode("overwrite").saveAsTable("silver.commodity_name_map")
    print(f"✅ Wrote {len(COMMODITY_MAP)} commodity mappings to silver.commodity_name_map")

# COMMAND ----------
# MAGIC %md ## Step 3 — Clean & Standardize Bronze Data

# COMMAND ----------
df_bronze = spark.table("bronze.mandi_prices_raw").filter(
    F.col("_is_quarantine") == False
)

# ── 3a: Type Casting & Basic Cleaning ─────────────────────────────────────────
df_typed = (
    df_bronze
    .withColumn("STATE",         F.upper(F.trim(F.col("STATE"))))
    .withColumn("District_Name", F.upper(F.trim(F.col("District_Name"))))
    .withColumn("Market_Name",   F.upper(F.trim(F.col("Market_Name"))))
    .withColumn("Commodity",     F.initcap(F.trim(F.col("Commodity"))))
    .withColumn("Variety",       F.initcap(F.trim(F.col("Variety"))))
    .withColumn("Grade",         F.upper(F.trim(F.col("Grade"))))
    .withColumn("Min_Price",     F.col("Min_Price").cast(DoubleType()))
    .withColumn("Max_Price",     F.col("Max_Price").cast(DoubleType()))
    .withColumn("Modal_Price",   F.col("Modal_Price").cast(DoubleType()))
    .withColumn("Price_Date",    F.to_date(F.col("Price_Date"), "M/d/yyyy"))
    # Null out impossible prices
    .withColumn("Min_Price",   F.when(F.col("Min_Price")   <= 0, None).otherwise(F.col("Min_Price")))
    .withColumn("Max_Price",   F.when(F.col("Max_Price")   <= 0, None).otherwise(F.col("Max_Price")))
    .withColumn("Modal_Price", F.when(F.col("Modal_Price") <= 0, None).otherwise(F.col("Modal_Price")))
    .filter(F.col("Modal_Price").isNotNull())   # Drop rows with no modal price
    .filter(F.col("Price_Date").isNotNull())    # Drop rows with unparseable dates
)

# ── 3b: Commodity Name Normalization ──────────────────────────────────────────
# Build a Spark map from the commodity_map dict for vectorized lookup
from pyspark.sql.functions import create_map, lit
from itertools import chain

mapping_expr = create_map([lit(x) for x in chain(*COMMODITY_MAP.items())])

df_normalized = df_typed.withColumn(
    "Commodity",
    F.coalesce(
        mapping_expr[F.lower(F.col("Commodity"))],   # Lookup normalized name
        F.col("Commodity")                            # Fallback to original if not found
    )
)

print(f"✅ Typed + normalized rows: {df_normalized.count():,}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Deduplication

# COMMAND ----------
# Keep the most-recently-ingested record for each unique fact
dedup_window = Window.partitionBy(
    "Market_Name", "Commodity", "Variety", "Price_Date"
).orderBy(F.col("_ingest_ts").desc())

df_deduped = (
    df_normalized
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_ingest_ts", "_source_file", "_is_quarantine")
)

dupes_removed = df_normalized.count() - df_deduped.count()
print(f"✅ Deduplicated: removed {dupes_removed:,} duplicate rows")

# COMMAND ----------
# MAGIC %md ## Step 5 — Outlier Detection (Rolling Z-Score)

# COMMAND ----------
# UC FIX: DATE cannot be cast to "long"/BIGINT in UC Serverless.
# Use F.unix_date() which returns days since epoch (integer).
# rangeBetween unit must match: unix_date() → days → use -28, not -28*86400.
rolling_window = (
    Window.partitionBy("Market_Name", "Commodity", "Variety")
          .orderBy(F.unix_date(F.col("Price_Date")))
          .rangeBetween(-28, 0)   # 28 days (unix_date is in DAYS, not seconds)
)

df_outliers = (
    df_deduped
    .withColumn("rolling_28d_mean", F.avg("Modal_Price").over(rolling_window))
    .withColumn("rolling_28d_std",  F.stddev("Modal_Price").over(rolling_window))
    .withColumn(
        "price_z_score",
        F.when(
            F.col("rolling_28d_std") > 0,
            (F.col("Modal_Price") - F.col("rolling_28d_mean")) / F.col("rolling_28d_std")
        ).otherwise(0.0)
    )
    .withColumn("is_outlier", F.abs(F.col("price_z_score")) > OUTLIER_Z_THRESHOLD)
)

outlier_count = df_outliers.filter(F.col("is_outlier") == True).count()
print(f"⚠️  Flagged {outlier_count:,} outlier rows (z-score > {OUTLIER_Z_THRESHOLD})")

# COMMAND ----------
# MAGIC %md ## Step 6 — Geo Enrichment (Join Geocode Cache)

# COMMAND ----------
df_geo = spark.table("bronze.geocode_cache").select(
    F.upper(F.trim(F.col("market_name"))).alias("geo_market"),
    F.upper(F.trim(F.col("district_name"))).alias("geo_district"),
    "lat", "lon"
).dropDuplicates(["geo_market", "geo_district"])

df_enriched = df_outliers.join(
    df_geo,
    on=[(F.col("Market_Name") == F.col("geo_market")) &
        (F.col("District_Name") == F.col("geo_district"))],
    how="left"
).drop("geo_market", "geo_district")

geocode_hit_rate = df_enriched.filter(F.col("lat").isNotNull()).count() / df_enriched.count() * 100
print(f"📍 Geocode hit rate: {geocode_hit_rate:.1f}%")
print(f"   (Markets without coords are kept but won't appear in arbitrage maps)")

# COMMAND ----------
# MAGIC %md ## Step 7 — Geohash Bucketing

# COMMAND ----------
# UDF to compute geohash for a (lat, lon) pair
@F.udf(StringType())
def compute_geohash(lat, lon):
    if lat is None or lon is None:
        return None
    try:
        return geohash2.encode(lat, lon, precision=GEOHASH_PRECISION)
    except Exception:
        return None

# UDF to get 8 neighboring geohashes + self (9 cells total)
@F.udf(ArrayType(StringType()))
def get_neighbor_geohashes(gh):
    if gh is None:
        return None
    try:
        neighbors = list(geohash2.expand(gh))   # Returns self + 8 neighbors
        return neighbors
    except Exception:
        return [gh]

df_silver = (
    df_enriched
    .withColumn("geohash4",          compute_geohash(F.col("lat"), F.col("lon")))
    .withColumn("neighbor_hashes",   get_neighbor_geohashes(F.col("geohash4")))
)

# COMMAND ----------
# MAGIC %md ## Step 8 — Write silver.mandi_prices_clean

# COMMAND ----------
(
    df_silver.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("STATE", "Commodity")
             .saveAsTable("silver.mandi_prices_clean")
)

# UC FIX: OPTIMIZE + ZORDER not supported on Serverless — wrap in try/except
try:
    spark.sql("OPTIMIZE silver.mandi_prices_clean ZORDER BY (Market_Name, Price_Date)")
    print("✅ ZORDER optimization applied")
except Exception as e:
    print(f"⚠️  OPTIMIZE skipped (Serverless limitation): {e.__class__.__name__}")

total_silver = spark.table("silver.mandi_prices_clean").count()
print(f"✅ silver.mandi_prices_clean: {total_silver:,} rows")

# COMMAND ----------
# MAGIC %md ## Step 9 — OOM-Safe Spatial Arbitrage Computation

# COMMAND ----------
# ── 9a: Get latest date's data for arbitrage (reduces cardinality significantly) ──
latest_date = spark.sql("SELECT MAX(Price_Date) FROM silver.mandi_prices_clean").collect()[0][0]

df_latest = spark.sql(f"""
    SELECT
        Market_Name, District_Name, STATE, Commodity, Variety,
        Modal_Price, Price_Date, lat, lon, geohash4, neighbor_hashes
    FROM silver.mandi_prices_clean
    WHERE Price_Date = '{latest_date}'
      AND is_outlier = false
      AND lat IS NOT NULL
      AND lon IS NOT NULL
""").cache()    # Cache — reused in the self-join below

print(f"📅 Computing arbitrage for: {latest_date}")
print(f"   Records in scope: {df_latest.count():,}")

# COMMAND ----------
# ── 9b: Explode neighbor hashes (each market now has 9 rows) ──────────────────
df_exploded = df_latest.withColumn("lookup_hash", F.explode(F.col("neighbor_hashes")))

# ── 9c: Self-join: source market → all target markets in same geohash cell ────
df_source = df_latest.select(
    F.col("Market_Name").alias("src_market"),
    F.col("District_Name").alias("src_district"),
    F.col("STATE").alias("src_state"),
    F.col("Commodity").alias("src_commodity"),
    F.col("Variety").alias("src_variety"),
    F.col("Modal_Price").alias("src_price"),
    F.col("lat").alias("src_lat"),
    F.col("lon").alias("src_lon"),
    F.col("Price_Date").alias("price_date"),
    F.col("geohash4").alias("src_geohash"),
)

df_target = df_exploded.select(
    F.col("Market_Name").alias("tgt_market"),
    F.col("District_Name").alias("tgt_district"),
    F.col("STATE").alias("tgt_state"),
    F.col("Modal_Price").alias("tgt_price"),
    F.col("lat").alias("tgt_lat"),
    F.col("lon").alias("tgt_lon"),
    F.col("Commodity").alias("tgt_commodity"),
    F.col("Variety").alias("tgt_variety"),
    F.col("lookup_hash"),
)

# Join on: same commodity+variety, target's geohash is in source's neighbor list, different market
df_pairs = df_source.join(
    df_target,
    on=(
        (df_source["src_commodity"] == df_target["tgt_commodity"]) &
        (df_source["src_variety"]   == df_target["tgt_variety"])   &
        (df_source["src_geohash"]   == df_target["lookup_hash"])   &
        (df_source["src_market"]    != df_target["tgt_market"])
    ),
    how="inner"
)

print(f"   Candidate pairs after geohash join: {df_pairs.count():,}")

# COMMAND ----------
# ── 9d: Haversine Distance Filter ─────────────────────────────────────────────
@F.udf(DoubleType())
def haversine_km(src_lat, src_lon, tgt_lat, tgt_lon):
    if None in (src_lat, src_lon, tgt_lat, tgt_lon):
        return None
    try:
        return haversine((src_lat, src_lon), (tgt_lat, tgt_lon), unit=Unit.KILOMETERS)
    except Exception:
        return None

df_arbitrage = (
    df_pairs
    .withColumn("distance_km",
                haversine_km(F.col("src_lat"), F.col("src_lon"),
                             F.col("tgt_lat"), F.col("tgt_lon")))
    .filter(F.col("distance_km") <= MAX_ARBITRAGE_KM)
    .filter(F.col("distance_km") > 0)     # Exclude self-pairs that slipped through
    .withColumn("arbitrage_spread",
                F.col("tgt_price") - F.col("src_price"))
    .filter(F.col("arbitrage_spread") > 0)     # Only positive (profitable) spreads
    .withColumn("spread_per_km",
                F.col("arbitrage_spread") / F.col("distance_km"))
    .select(
        "src_market", "src_district", "src_state",
        "tgt_market", "tgt_district", "tgt_state",
        "src_commodity", "src_variety",
        "src_price", "tgt_price",
        "arbitrage_spread", "distance_km", "spread_per_km",
        "src_lat", "src_lon", "tgt_lat", "tgt_lon",
        "price_date"
    )
)

print(f"   Pairs within {MAX_ARBITRAGE_KM}km with positive spread: {df_arbitrage.count():,}")

# COMMAND ----------
# MAGIC %md ## Step 10 — Write silver.arbitrage_base

# COMMAND ----------
(
    df_arbitrage.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable("silver.arbitrage_base")
)

# UC FIX: OPTIMIZE + ZORDER not supported on Serverless — wrap in try/except
try:
    spark.sql("OPTIMIZE silver.arbitrage_base ZORDER BY (src_commodity, price_date)")
    print("✅ ZORDER optimization applied to arbitrage_base")
except Exception as e:
    print(f"⚠️  OPTIMIZE skipped: {e.__class__.__name__}")

print(f"✅ silver.arbitrage_base written with {df_arbitrage.count():,} arbitrage pairs")

# COMMAND ----------
# MAGIC %md ## Step 11 — Silver Layer Summary

# COMMAND ----------
print("=" * 55)
print("SILVER LAYER SUMMARY")
print("=" * 55)
spark.sql("""
    SELECT
        COUNT(*)                    AS total_records,
        COUNT(DISTINCT Market_Name) AS unique_markets,
        COUNT(DISTINCT Commodity)   AS unique_commodities,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_flags,
        SUM(CASE WHEN lat IS NULL THEN 1 ELSE 0 END) AS no_geocode
    FROM silver.mandi_prices_clean
""").show()

print("\nTop 5 Highest Arbitrage Opportunities (Today):")
spark.sql("""
    SELECT src_market, tgt_market, src_commodity, src_variety,
           ROUND(src_price,0) AS from_price,
           ROUND(tgt_price,0) AS to_price,
           ROUND(arbitrage_spread,0) AS spread_inr,
           ROUND(distance_km,0) AS km,
           ROUND(spread_per_km,2) AS inr_per_km
    FROM silver.arbitrage_base
    ORDER BY arbitrage_spread DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Silver Layer Complete
# MAGIC
# MAGIC | Table | Records | Key Feature |
# MAGIC |---|---|---|
# MAGIC | `silver.mandi_prices_clean` | See above | Typed, deduped, outlier-flagged, geo-enriched |
# MAGIC | `silver.commodity_name_map` | N mappings | Auditable normalization dictionary |
# MAGIC | `silver.arbitrage_base`     | See above | OOM-safe geohash spatial arbitrage pairs |
# MAGIC
# MAGIC **Next Step → Run Notebook 03: Gold Layer**
