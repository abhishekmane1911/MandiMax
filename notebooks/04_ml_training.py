# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Notebook 04 — ML Training: GBT Price Forecaster
# MAGIC **Mandi-Max | Bharat Bricks Hackathon**
# MAGIC
# MAGIC **Purpose:** Train a Gradient Boosted Tree (GBT) Regressor on engineered lag features
# MAGIC to produce 7-day forward price forecasts. Full MLflow experiment tracking.
# MAGIC Model is registered in the MLflow Model Registry as `Production`.
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `gold.ml_feature_store`    — Engineered features (auditable)
# MAGIC - `gold.price_forecast_7day` — 7-day predictions for all (market, commodity) pairs

# COMMAND ----------
# MAGIC %md ## Step 0 — Cluster Check & Library Install

# COMMAND ----------
# Auto-install required libraries (works on both Classic and Serverless compute)
%pip install geohash2 haversine -q

# COMMAND ----------
# MAGIC %md ## Step 1 — Imports & MLflow Setup (Unity Catalog Compatible)

# COMMAND ----------
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import numpy as np
import time

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# ── MLflow URI Setup (CRITICAL FIX for full Databricks workspaces) ─────────────
# On full Databricks workspaces (non-Community), you must explicitly set these.
# This resolves: [CONFIG_NOT_AVAILABLE] spark.mlflow.modelRegistryUri
mlflow.set_tracking_uri("databricks")

# ── Detect Unity Catalog vs classic Workspace registry ─────────────────────────
# Unity Catalog workspaces use 3-level model names and aliases (not stages).
# Classic workspaces use simple names and stage transitions.
try:
    # Check if Unity Catalog is enabled
    spark.sql("USE CATALOG main")
    UNITY_CATALOG = True
    # Unity Catalog model name: catalog.schema.model_name
    MODEL_NAME = "main.default.MandiMax_PriceForecast"
    mlflow.set_registry_uri("databricks-uc")
    print("✅ Unity Catalog detected → using 3-level model name + aliases")
except Exception:
    UNITY_CATALOG = False
    MODEL_NAME = "MandiMax-PriceForecast"
    mlflow.set_registry_uri("databricks")
    print("✅ Classic Workspace registry detected → using stage-based promotion")

EXPERIMENT_NAME = "/MandiMax/PriceForecasting"
TRAIN_CUTOFF    = "2025-01-01"   # Everything before = train, from = test

mlflow.set_experiment(EXPERIMENT_NAME)
client = MlflowClient()

print(f"✅ MLflow experiment : {EXPERIMENT_NAME}")
print(f"✅ Model name        : {MODEL_NAME}")
print(f"   Train/test split at: {TRAIN_CUTOFF}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Feature Engineering (gold.ml_feature_store)

# COMMAND ----------
# Window for lag features: per (Market, Commodity, Variety), ordered by date
lag_window = Window.partitionBy("Market_Name", "Commodity", "Variety") \
                   .orderBy("Price_Date")

df_clean = spark.sql("""
    SELECT Market_Name, District_Name, STATE, Commodity, Variety,
           Modal_Price, Price_Date, lat, lon
    FROM silver.mandi_prices_clean
    WHERE is_outlier = false
      AND Modal_Price IS NOT NULL
""")

df_features = (
    df_clean
    # ── Lag Features ──────────────────────────────────────────────────────────
    .withColumn("lag_1d",  F.lag("Modal_Price", 1).over(lag_window))
    .withColumn("lag_3d",  F.lag("Modal_Price", 3).over(lag_window))
    .withColumn("lag_7d",  F.lag("Modal_Price", 7).over(lag_window))
    .withColumn("lag_14d", F.lag("Modal_Price", 14).over(lag_window))
    .withColumn("lag_28d", F.lag("Modal_Price", 28).over(lag_window))

    # ── Rolling Statistics ─────────────────────────────────────────────────────
    .withColumn("rolling_7d_mean",
                F.avg("Modal_Price").over(
                    lag_window.rowsBetween(-7, -1)
                ))
    .withColumn("rolling_7d_std",
                F.stddev("Modal_Price").over(
                    lag_window.rowsBetween(-7, -1)
                ))
    .withColumn("rolling_28d_mean",
                F.avg("Modal_Price").over(
                    lag_window.rowsBetween(-28, -1)
                ))

    # ── Price Momentum ─────────────────────────────────────────────────────────
    .withColumn("price_momentum_7d",    F.col("lag_1d") - F.col("lag_7d"))
    .withColumn("price_momentum_14d",   F.col("lag_1d") - F.col("lag_14d"))

    # ── Ratio Features ─────────────────────────────────────────────────────────
    .withColumn("price_vs_28d_mean",
                F.col("lag_1d") / F.nullif(F.col("rolling_28d_mean"), F.lit(0)))

    # ── Calendar Features (Cyclical Encoding) ─────────────────────────────────
    .withColumn("day_of_week",     F.dayofweek("Price_Date"))
    .withColumn("week_of_year",    F.weekofyear("Price_Date"))
    .withColumn("month",           F.month("Price_Date"))
    .withColumn("day_of_month",    F.dayofmonth("Price_Date"))
    # UC FIX: DATE.cast("long") banned → use F.unix_date() (days since epoch)
    .withColumn("days_since_epoch", F.unix_date(F.col("Price_Date")))

    # Cyclical sin/cos encoding (avoids ordinal bias)
    .withColumn("dow_sin",  F.sin(2 * 3.14159 * F.col("day_of_week")  / 7))
    .withColumn("dow_cos",  F.cos(2 * 3.14159 * F.col("day_of_week")  / 7))
    .withColumn("woy_sin",  F.sin(2 * 3.14159 * F.col("week_of_year") / 52))
    .withColumn("woy_cos",  F.cos(2 * 3.14159 * F.col("week_of_year") / 52))
    .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month")       / 12))
    .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month")       / 12))

    # ── Target variable: price 7 days in the future ───────────────────────────
    .withColumn("target_price_7d",
                F.lead("Modal_Price", 7).over(lag_window))

    # Drop rows with nulls in any feature (caused by lag at start of series)
    .dropna(subset=[
        "lag_1d", "lag_3d", "lag_7d", "lag_14d", "lag_28d",
        "rolling_7d_mean", "rolling_28d_mean", "target_price_7d"
    ])
)

# Persist feature store
df_features.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Commodity") \
    .saveAsTable("gold.ml_feature_store")

print(f"✅ gold.ml_feature_store: {df_features.count():,} rows with {len(df_features.columns)} columns")

# COMMAND ----------
# MAGIC %md ## Step 3 — Train / Test Split (Time-Based — Never Random)

# COMMAND ----------
df_fs = spark.table("gold.ml_feature_store")

df_train = df_fs.filter(F.col("Price_Date") <  TRAIN_CUTOFF)
df_test  = df_fs.filter(F.col("Price_Date") >= TRAIN_CUTOFF)

n_train = df_train.count()
n_test  = df_test.count()
print(f"✅ Train: {n_train:,} rows (before {TRAIN_CUTOFF})")
print(f"✅ Test : {n_test:,}  rows (from  {TRAIN_CUTOFF})")
print(f"   Split ratio: {n_train/(n_train+n_test)*100:.1f}% / {n_test/(n_train+n_test)*100:.1f}%")

# COMMAND ----------
# MAGIC %md ## Step 4 — ML Pipeline Definition

# COMMAND ----------
FEATURE_COLS = [
    "lag_1d", "lag_3d", "lag_7d", "lag_14d", "lag_28d",
    "rolling_7d_mean", "rolling_7d_std", "rolling_28d_mean",
    "price_momentum_7d", "price_momentum_14d", "price_vs_28d_mean",
    "dow_sin", "dow_cos", "woy_sin", "woy_cos", "month_sin", "month_cos",
    "days_since_epoch",
    "commodity_idx",   # Added after StringIndexer
    "market_idx",      # Added after StringIndexer
]

commodity_indexer = StringIndexer(inputCol="Commodity",   outputCol="commodity_idx", handleInvalid="keep")
market_indexer    = StringIndexer(inputCol="Market_Name", outputCol="market_idx",    handleInvalid="keep")
assembler         = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")

# ── Gradient Boosted Trees (Primary Model) ────────────────────────────────────
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="target_price_7d",
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,          # Learning rate
    subsamplingRate=0.8,   # Row sampling — prevents overfitting
    featureSubsetStrategy="sqrt",
    seed=42
)

pipeline_gbt = Pipeline(stages=[commodity_indexer, market_indexer, assembler, gbt])

# ── Random Forest (Baseline for MLflow comparison) ────────────────────────────
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="target_price_7d",
    numTrees=50,
    maxDepth=5,
    featureSubsetStrategy="sqrt",
    seed=42
)
pipeline_rf = Pipeline(stages=[commodity_indexer, market_indexer, assembler, rf])

# COMMAND ----------
# MAGIC %md ## Step 5 — Train GBT + Log to MLflow (Primary Run)

# COMMAND ----------
evaluator_rmse = RegressionEvaluator(labelCol="target_price_7d", predictionCol="prediction", metricName="rmse")
evaluator_mae  = RegressionEvaluator(labelCol="target_price_7d", predictionCol="prediction", metricName="mae")
evaluator_r2   = RegressionEvaluator(labelCol="target_price_7d", predictionCol="prediction", metricName="r2")

def compute_mape(df_pred):
    """Mean Absolute Percentage Error"""
    return df_pred.withColumn(
        "ape", F.abs((F.col("target_price_7d") - F.col("prediction")) / F.col("target_price_7d")) * 100
    ).agg(F.avg("ape")).collect()[0][0]

def compute_directional_accuracy(df_pred, lag_col="lag_1d"):
    """% of predictions where direction (up/down) matches actual direction"""
    df_dir = df_pred.withColumn(
        "actual_dir", F.signum(F.col("target_price_7d") - F.col(lag_col))
    ).withColumn(
        "pred_dir",   F.signum(F.col("prediction") - F.col(lag_col))
    )
    correct = df_dir.filter(F.col("actual_dir") == F.col("pred_dir")).count()
    total   = df_dir.count()
    return correct / total * 100 if total > 0 else 0.0

# ── Train GBT ─────────────────────────────────────────────────────────────────
with mlflow.start_run(run_name="GBT_PriceForecaster_v1") as gbt_run:
    mlflow.set_tag("model_type",       "GBTRegressor")
    mlflow.set_tag("train_date_cutoff", TRAIN_CUTOFF)
    mlflow.set_tag("feature_version",  "v1")
    mlflow.set_tag("team",             "MandiMax")

    mlflow.log_param("maxIter",           gbt.getMaxIter())
    mlflow.log_param("maxDepth",          gbt.getMaxDepth())
    mlflow.log_param("stepSize",          gbt.getStepSize())
    mlflow.log_param("subsamplingRate",   gbt.getSubsamplingRate())
    mlflow.log_param("featureSubsetStrategy", gbt.getFeatureSubsetStrategy())
    mlflow.log_param("n_features",        len(FEATURE_COLS))
    mlflow.log_param("n_training_records", n_train)
    mlflow.log_param("n_test_records",    n_test)

    start_time = time.time()
    model_gbt = pipeline_gbt.fit(df_train)
    train_time = time.time() - start_time

    df_pred_gbt = model_gbt.transform(df_test)

    rmse  = evaluator_rmse.evaluate(df_pred_gbt)
    mae   = evaluator_mae.evaluate(df_pred_gbt)
    r2    = evaluator_r2.evaluate(df_pred_gbt)
    mape  = compute_mape(df_pred_gbt)
    dir_acc = compute_directional_accuracy(df_pred_gbt)
    mean_price = df_test.agg(F.avg("target_price_7d")).collect()[0][0]

    mlflow.log_metric("rmse",                  round(rmse, 4))
    mlflow.log_metric("mae",                   round(mae, 4))
    mlflow.log_metric("mape_pct",              round(mape, 4))
    mlflow.log_metric("r2_score",              round(r2, 4))
    mlflow.log_metric("directional_accuracy",  round(dir_acc, 4))
    mlflow.log_metric("mean_price_test_set",   round(mean_price, 2))
    mlflow.log_metric("wall_time_seconds",     round(train_time, 2))

    # ── Feature Importance ────────────────────────────────────────────────────
    gbt_model            = model_gbt.stages[-1]
    feature_importances  = gbt_model.featureImportances.toArray()
    fi_df = pd.DataFrame({
        "feature":    FEATURE_COLS,
        "importance": feature_importances
    }).sort_values("importance", ascending=False)

    # Log as CSV artifact
    fi_path = "/tmp/feature_importance_gbt.csv"
    fi_df.to_csv(fi_path, index=False)
    mlflow.log_artifact(fi_path, "feature_importance")

    # ── Save model ────────────────────────────────────────────────────────────
    mlflow.spark.log_model(model_gbt, "gbt_price_model")

    gbt_run_id = gbt_run.info.run_id
    print(f"✅ GBT Run logged. Run ID: {gbt_run_id}")
    print(f"   RMSE: ₹{rmse:.2f} | MAE: ₹{mae:.2f} | MAPE: {mape:.2f}% | R²: {r2:.4f}")
    print(f"   Directional Accuracy: {dir_acc:.1f}%")
    print(f"   Training time: {train_time:.1f}s")

# COMMAND ----------
# MAGIC %md ## Step 6 — Train Random Forest Baseline (Comparison)

# COMMAND ----------
with mlflow.start_run(run_name="RF_Baseline_v1") as rf_run:
    mlflow.set_tag("model_type",        "RandomForestRegressor")
    mlflow.set_tag("train_date_cutoff", TRAIN_CUTOFF)
    mlflow.log_param("numTrees",        rf.getNumTrees())
    mlflow.log_param("maxDepth",        rf.getMaxDepth())
    mlflow.log_param("n_training_records", n_train)

    start_time = time.time()
    model_rf   = pipeline_rf.fit(df_train)
    train_time = time.time() - start_time

    df_pred_rf = model_rf.transform(df_test)
    rmse_rf    = evaluator_rmse.evaluate(df_pred_rf)
    mae_rf     = evaluator_mae.evaluate(df_pred_rf)
    r2_rf      = evaluator_r2.evaluate(df_pred_rf)
    mape_rf    = compute_mape(df_pred_rf)

    mlflow.log_metric("rmse",            round(rmse_rf, 4))
    mlflow.log_metric("mae",             round(mae_rf, 4))
    mlflow.log_metric("mape_pct",        round(mape_rf, 4))
    mlflow.log_metric("r2_score",        round(r2_rf, 4))
    mlflow.log_metric("wall_time_seconds", round(train_time, 2))

    rf_run_id = rf_run.info.run_id
    print(f"✅ RF Baseline Run logged. Run ID: {rf_run_id}")
    print(f"   RMSE: ₹{rmse_rf:.2f} | MAE: ₹{mae_rf:.2f} | R²: {r2_rf:.4f}")

print("\n📊 Model Comparison:")
print(f"   GBT  → RMSE: ₹{rmse:.2f} | MAPE: {mape:.2f}% | Dir Acc: {dir_acc:.1f}%")
print(f"   RF   → RMSE: ₹{rmse_rf:.2f} | MAPE: {mape_rf:.2f}%")

# COMMAND ----------
# MAGIC %md ## Step 7 — Register Best Model in MLflow Model Registry

# COMMAND ----------
# Register GBT model (best performer) to Model Registry
model_uri = f"runs:/{gbt_run_id}/gbt_price_model"

# Create or get the registered model
try:
    client.create_registered_model(
        MODEL_NAME,
        description="GBT 7-day price forecaster. Trained on 2-year Indian mandi data. Time-based split."
    )
    print(f"✅ Created new registered model: {MODEL_NAME}")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"ℹ️  Model '{MODEL_NAME}' already exists — adding new version")
    else:
        raise e

# Create model version
model_version = client.create_model_version(
    name=MODEL_NAME,
    source=model_uri,
    run_id=gbt_run_id,
    description=f"GBT v1 | RMSE=₹{rmse:.2f} | MAPE={mape:.2f}% | R²={r2:.4f} | Train cutoff={TRAIN_CUTOFF}"
)

# Wait for version to be ready
import time as _time
for _ in range(30):
    mv = client.get_model_version(MODEL_NAME, model_version.version)
    if mv.status == "READY":
        break
    _time.sleep(2)
print(f"✅ Model version {model_version.version} is READY")

# ── Promote model: Unity Catalog uses ALIASES, Classic uses STAGES ─────────────
if UNITY_CATALOG:
    # Unity Catalog: assign 'champion' alias (replaces deprecated 'Production' stage)
    client.set_registered_model_alias(
        name=MODEL_NAME,
        alias="champion",
        version=model_version.version
    )
    LOAD_URI = f"models:/{MODEL_NAME}@champion"
    print(f"✅ Model '{MODEL_NAME}' v{model_version.version} → alias='champion' (Unity Catalog)")
else:
    # Classic Workspace: use traditional stage promotion
    try:
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=model_version.version,
            stage="Production",
            archive_existing_versions=True
        )
        LOAD_URI = f"models:/{MODEL_NAME}/Production"
        print(f"✅ Model '{MODEL_NAME}' v{model_version.version} → Production stage")
    except Exception as e:
        # Fallback: use run URI directly if stage transition fails
        LOAD_URI = model_uri
        print(f"⚠️  Stage transition not available — using run URI: {LOAD_URI}")
        print(f"   (This is fine for the hackathon demo)")

print(f"   Load URI for serving: {LOAD_URI}")

# COMMAND ----------
# MAGIC %md ## Step 8 — Generate 7-Day Forecasts → gold.price_forecast_7day

# COMMAND ----------
# Load the registered production model using the URI determined in Step 7
print(f"🔄 Loading model from: {LOAD_URI}")
try:
    production_model = mlflow.spark.load_model(LOAD_URI)
    print("✅ Model loaded from registry")
except Exception as e:
    # Ultimate fallback — use the in-memory model we just trained (same session)
    print(f"⚠️  Registry load failed ({e.__class__.__name__}), using in-memory model")
    production_model = model_gbt

# Use the latest available data as the "current" state for forecasting
latest_date = spark.sql("SELECT MAX(Price_Date) FROM silver.mandi_prices_clean").collect()[0][0]
print(f"📅 Generating forecasts for base date: {latest_date}")

df_current = spark.table("gold.ml_feature_store").filter(
    F.col("Price_Date") == latest_date
)

# Generate predictions
df_forecast = production_model.transform(df_current) \
    .select(
        "Market_Name", "District_Name", "STATE",
        "Commodity", "Variety",
        "Modal_Price",
        F.col("Price_Date").alias("forecast_base_date"),
        F.col("prediction").alias("forecast_price_7d"),
        F.lit(7).alias("forecast_horizon_days"),
        F.date_add(F.col("Price_Date"), 7).alias("forecast_target_date"),
        F.round(
            ((F.col("prediction") - F.col("Modal_Price")) / F.col("Modal_Price")) * 100, 2
        ).alias("predicted_change_pct"),
        F.when(F.col("prediction") > F.col("Modal_Price"), "UP")
         .when(F.col("prediction") < F.col("Modal_Price"), "DOWN")
         .otherwise("FLAT").alias("price_direction"),
        F.lit(MODEL_NAME).alias("model_name"),
        F.current_timestamp().alias("generated_at")
    )

df_forecast.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Commodity") \
    .saveAsTable("gold.price_forecast_7day")

n_forecasts = spark.table("gold.price_forecast_7day").count()
print(f"✅ gold.price_forecast_7day: {n_forecasts:,} predictions generated")
print(f"   Forecast base: {latest_date} → target date: +7 days")

# COMMAND ----------
# MAGIC %md ## Step 9 — ML Summary

# COMMAND ----------
print("=" * 60)
print("ML TRAINING COMPLETE — MLFLOW EXPERIMENT SUMMARY")
print("=" * 60)
print(f"  Experiment    : {EXPERIMENT_NAME}")
print(f"  Best Model    : GBT | RMSE=₹{rmse:.2f} | MAPE={mape:.2f}% | R²={r2:.4f}")
print(f"  Dir Accuracy  : {dir_acc:.1f}%")
print(f"  Registry      : {MODEL_NAME} v{model_version.version}")
print(f"  Load URI      : {LOAD_URI}")
print(f"  UC Mode       : {UNITY_CATALOG}")
print()

print("📈 Sample 7-Day Forecasts (UP predictions):")
spark.sql("""
    SELECT Market_Name, Commodity,
           ROUND(Modal_Price)      AS current_price_inr,
           ROUND(forecast_price_7d) AS forecast_7d_inr,
           predicted_change_pct,
           price_direction
    FROM gold.price_forecast_7day
    WHERE price_direction = 'UP'
    ORDER BY predicted_change_pct DESC
    LIMIT 10
""").show(truncate=False)

print("📉 Sample 7-Day Forecasts (DOWN predictions):")
spark.sql("""
    SELECT Market_Name, Commodity,
           ROUND(Modal_Price)      AS current_price_inr,
           ROUND(forecast_price_7d) AS forecast_7d_inr,
           predicted_change_pct,
           price_direction
    FROM gold.price_forecast_7day
    WHERE price_direction = 'DOWN'
    ORDER BY predicted_change_pct ASC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ ML Pipeline Complete
# MAGIC
# MAGIC | Artifact | Details |
# MAGIC |---|---|
# MAGIC | MLflow Experiment | `/MandiMax/PriceForecasting` — 2 runs logged |
# MAGIC | Best Model | GBT Regressor |
# MAGIC | Registry | `MandiMax-PriceForecast` → Production |
# MAGIC | Forecast Table | `gold.price_forecast_7day` |
# MAGIC
# MAGIC **Next Step → Run Notebook 05: API Endpoint & Bot**
