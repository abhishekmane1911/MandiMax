# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Notebook 01 — Bronze Layer: Raw Ingestion
# MAGIC **Mandi-Max | Bharat Bricks Hackathon**
# MAGIC
# MAGIC **Purpose:** Ingest raw CSV → Delta Lake bronze tables.
# MAGIC **UC-Safe:** Fully compatible with Unity Catalog + Serverless compute.
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `bronze.mandi_prices_raw`
# MAGIC - `bronze.geocode_cache`

# COMMAND ----------
# MAGIC %md ## Step 0 — Imports & DB Setup

# COMMAND ----------
import re
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
print("✅ bronze database ready")

# COMMAND ----------
# MAGIC %md ## Step 1 — Find the Data File

# COMMAND ----------
# Searches every known upload location. First match wins.
RAW_DATA_PATH   = None
USE_DELTA_TABLE = None

CANDIDATE_PATHS = [
    # Workspace Drafts (confirmed location from last run)
    "/Workspace/Users/cse240001043@iiti.ac.in/Drafts/Agriculture_price_dataset.csv",
    "/Workspace/Shared/Agriculture_price_dataset.csv",
    # Unity Catalog Volumes
    "/Volumes/workspace/default/uploads/Agriculture_price_dataset.csv",
    "/Volumes/workspace/default/mandi_max/Agriculture_price_dataset.csv",
    "/Volumes/workspace/default/Agriculture_price_dataset.csv",
    # DBFS FileStore
    "dbfs:/FileStore/tables/Agriculture_price_dataset.csv",
    "dbfs:/FileStore/tables/agriculture_price_dataset.csv",
    "dbfs:/FileStore/Agriculture_price_dataset.csv",
    "dbfs:/FileStore/mandi_max/raw/Agriculture_price_dataset.csv",
]

for path in CANDIDATE_PATHS:
    try:
        folder  = path.rsplit("/", 1)[0]
        files   = dbutils.fs.ls(folder)
        matches = [f for f in files if "agriculture" in f.name.lower()]
        if matches:
            found = folder + "/" + matches[0].name
            # UC FIX: strip "dbfs:" prefix for /Workspace and /Volumes paths
            # spark.read.csv can't use these, but pandas can read /Workspace directly
            if found.startswith("dbfs:/Workspace") or found.startswith("dbfs:/Volumes"):
                RAW_DATA_PATH = found[5:]   # → /Workspace/... or /Volumes/...
            else:
                RAW_DATA_PATH = found       # → dbfs:/FileStore/... (spark.read ok)
            print(f"✅ Found at: {RAW_DATA_PATH}")
            break
    except Exception:
        continue

# Fallback: deep scan of DBFS
if RAW_DATA_PATH is None:
    try:
        for d in dbutils.fs.ls("dbfs:/FileStore/"):
            for f in dbutils.fs.ls(d.path):
                if "agriculture" in f.name.lower():
                    RAW_DATA_PATH = f.path
                    print(f"✅ Found via scan: {RAW_DATA_PATH}")
                    break
            if RAW_DATA_PATH: break
    except Exception:
        pass

# Fallback: check if uploaded as Delta table
if RAW_DATA_PATH is None:
    try:
        rows = spark.sql("SHOW TABLES IN default").collect()
        hits = [r.tableName for r in rows
                if any(k in r.tableName.lower() for k in ["agri","mandi","price"])]
        if hits:
            USE_DELTA_TABLE = hits[0]
            print(f"✅ Found Delta table: default.{USE_DELTA_TABLE}")
        else:
            raise RuntimeError(
                "❌ CSV not found anywhere!\n"
                "   → Go to Catalog → Add Data → Upload files to a volume\n"
                "   → Or put the CSV in your Workspace Drafts folder"
            )
    except RuntimeError as ex:
        raise ex
    except Exception as ex:
        raise RuntimeError(f"❌ Could not find data: {ex}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Create Bronze Tables (UC-native, no LOCATION)

# COMMAND ----------
# UC FIX: No LOCATION 'dbfs:/...' clause — Unity Catalog manages storage.
# No TBLPROPERTIES autoOptimize — not supported on Serverless.

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.mandi_prices_raw (
        STATE           STRING,
        District_Name   STRING,
        Market_Name     STRING,
        Commodity       STRING,
        Variety         STRING,
        Grade           STRING,
        Min_Price       STRING,
        Max_Price       STRING,
        Modal_Price     STRING,
        Price_Date      STRING,
        _ingest_ts      TIMESTAMP,
        _source_file    STRING,
        _is_quarantine  BOOLEAN
    )
    USING DELTA
""")
print("✅ bronze.mandi_prices_raw table ready")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.geocode_cache (
        market_name   STRING,
        district_name STRING,
        state         STRING,
        lat           DOUBLE,
        lon           DOUBLE,
        source        STRING,
        fetched_at    TIMESTAMP
    )
    USING DELTA
""")
print("✅ bronze.geocode_cache table ready")

# COMMAND ----------
# MAGIC %md ## Step 3 — Helper: Sanitize Column Names

# COMMAND ----------
def sanitize_columns(df_pandas):
    """
    Rename DataFrame columns to be Delta Lake compatible.
    Rules: strip whitespace, replace spaces/special chars with underscore.
    Delta banned chars: space , ; { } ( ) \\n \\t =
    """
    banned = re.compile(r'[ ,;{}\(\)\n\t=]+')
    new_cols = {}
    for col in df_pandas.columns:
        clean = banned.sub("_", col.strip())
        clean = re.sub(r'_+', '_', clean).strip('_')  # collapse multiple underscores
        new_cols[col] = clean
    df_pandas = df_pandas.rename(columns=new_cols)
    print(f"   Column mapping: {new_cols}")
    return df_pandas

# COMMAND ----------
# MAGIC %md ## Step 4 — Read Data

# COMMAND ----------
if USE_DELTA_TABLE:
    # ── Mode A: Data uploaded as managed Delta table ──────────────────────────
    print(f"📂 Reading from Delta table: default.{USE_DELTA_TABLE}")
    df_spark = spark.table(f"default.{USE_DELTA_TABLE}")
    # Sanitize any bad column names
    for old_col in df_spark.columns:
        clean = re.sub(r'[ ,;{}\(\)\n\t=]+', '_', old_col.strip())
        clean = re.sub(r'_+', '_', clean).strip('_')
        if clean != old_col:
            df_spark = df_spark.withColumnRenamed(old_col, clean)
    df_with_meta = (
        df_spark
        .withColumn("_ingest_ts",     F.current_timestamp())
        .withColumn("_source_file",   F.lit(f"delta:default.{USE_DELTA_TABLE}"))
        .withColumn("_is_quarantine", F.lit(False))
    )

elif RAW_DATA_PATH.startswith("/Workspace") or RAW_DATA_PATH.startswith("/Volumes"):
    # ── Mode B: Workspace or Volume path → pandas → Spark ────────────────────
    # UC FIX: spark.read.csv() CANNOT read /Workspace or /Volumes paths on
    # UC Serverless. pandas.read_csv() bypasses the Spark file-scheme restriction.
    print(f"📂 Reading via pandas (Workspace/Volume path): {RAW_DATA_PATH}")
    pdf = pd.read_csv(RAW_DATA_PATH, dtype=str, keep_default_na=False)

    # CRITICAL FIX: sanitize column names BEFORE createDataFrame
    # "District Name" → "District_Name", "Price Date" → "Price_Date", etc.
    pdf = sanitize_columns(pdf)

    # Add audit columns
    pdf["_ingest_ts"]     = datetime.utcnow().isoformat()
    pdf["_source_file"]   = RAW_DATA_PATH
    pdf["_is_quarantine"] = False

    print(f"   Rows loaded: {len(pdf):,}")
    print(f"   Final columns ({len(pdf.columns)}): {list(pdf.columns)}")

    # Convert to Spark DataFrame (in-memory, no file I/O, no UC restriction)
    df_with_meta = spark.createDataFrame(pdf)
    df_with_meta = df_with_meta.withColumn("_ingest_ts", F.to_timestamp("_ingest_ts"))

else:
    # ── Mode C: DBFS path → standard spark.read.csv ───────────────────────────
    print(f"📂 Reading via spark.read.csv: {RAW_DATA_PATH}")
    df_raw = (
        spark.read
             .option("header", "true")
             .option("mode", "PERMISSIVE")
             .csv(RAW_DATA_PATH)
    )
    # Sanitize column names at Spark level
    for old_col in df_raw.columns:
        clean = re.sub(r'[ ,;{}\(\)\n\t=]+', '_', old_col.strip())
        clean = re.sub(r'_+', '_', clean).strip('_')
        if clean != old_col:
            df_raw = df_raw.withColumnRenamed(old_col, clean)
    df_with_meta = (
        df_raw
        .withColumn("_ingest_ts",     F.current_timestamp())
        .withColumn("_source_file",   F.lit(RAW_DATA_PATH))
        .withColumn("_is_quarantine", F.lit(False))
    )

# COMMAND ----------
# MAGIC %md ## Step 5 — Validate & Count

# COMMAND ----------
total_rows      = df_with_meta.count()
quarantine_rows = df_with_meta.filter(F.col("_is_quarantine") == True).count()
clean_rows      = total_rows - quarantine_rows

print(f"📊 Total rows ingested : {total_rows:,}")
print(f"✅ Clean rows          : {clean_rows:,}")
print(f"⚠️  Quarantined rows   : {quarantine_rows:,}")

# COMMAND ----------
# MAGIC %md ## Step 6 — Write to bronze.mandi_prices_raw

# COMMAND ----------
# Select only columns that exist in our target table schema
TARGET_COLS = [
    "STATE", "District_Name", "Market_Name", "Commodity", "Variety",
    "Grade", "Min_Price", "Max_Price", "Modal_Price", "Price_Date",
    "_ingest_ts", "_source_file", "_is_quarantine"
]

# Only keep columns that exist (handles CSVs with different sets of columns)
available_cols = [c for c in TARGET_COLS if c in df_with_meta.columns]
missing_cols   = [c for c in TARGET_COLS if c not in df_with_meta.columns]

if missing_cols:
    print(f"⚠️  Missing columns (will be written as NULL): {missing_cols}")
    # Add missing columns as null so schema matches
    for mc in missing_cols:
        df_with_meta = df_with_meta.withColumn(mc, F.lit(None).cast("string"))

df_clean = df_with_meta.select(TARGET_COLS).filter(F.col("_is_quarantine") == False)

df_clean.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("bronze.mandi_prices_raw")

print(f"✅ Wrote {clean_rows:,} rows to bronze.mandi_prices_raw")

# COMMAND ----------
# MAGIC %md ## Step 7 — Verify

# COMMAND ----------
print("=" * 55)
print("BRONZE LAYER INGESTION SUMMARY")
print("=" * 55)

spark.sql("""
    SELECT COUNT(*) AS total_records,
           MIN(Price_Date) AS earliest_date,
           MAX(Price_Date) AS latest_date
    FROM bronze.mandi_prices_raw
    WHERE _is_quarantine = false
""").show()

spark.sql("""
    SELECT STATE, COUNT(*) AS records
    FROM bronze.mandi_prices_raw
    WHERE _is_quarantine = false
    GROUP BY STATE ORDER BY records DESC
    LIMIT 10
""").show()

spark.sql("""
    SELECT COUNT(DISTINCT Commodity) AS unique_commodities,
           COUNT(DISTINCT Market_Name) AS unique_markets
    FROM bronze.mandi_prices_raw
    WHERE _is_quarantine = false
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Bronze Complete → Run Notebook 00 (geocode), then 02, 03, 04
