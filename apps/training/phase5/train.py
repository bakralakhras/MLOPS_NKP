import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from botocore.config import Config
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split


RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
WORKDIR = Path("/tmp/aegis-phase5")
WORKDIR.mkdir(parents=True, exist_ok=True)


def getenv(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID", required=True)
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY", required=True)
AWS_DEFAULT_REGION = getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ENDPOINT_URL = getenv("MLFLOW_S3_ENDPOINT_URL", required=True)
MLFLOW_TRACKING_URI = getenv("MLFLOW_TRACKING_URI", required=True)

RAW_BUCKET = getenv("AEGIS_RAW_BUCKET", "aegis-raw")
BRONZE_BUCKET = getenv("AEGIS_BRONZE_BUCKET", "aegis-bronze")
SILVER_BUCKET = getenv("AEGIS_SILVER_BUCKET", "aegis-silver")
GOLD_BUCKET = getenv("AEGIS_GOLD_BUCKET", "aegis-gold")

EXPERIMENT_NAME = getenv("MLFLOW_EXPERIMENT_NAME", "aegis-phase-5-training-pipeline")
REGISTERED_MODEL_NAME = getenv("REGISTERED_MODEL_NAME", "aegis-fraud-baseline")
REGISTER_MODEL = getenv("REGISTER_MODEL", "true").lower() == "true"

PHASE = getenv("AEGIS_PHASE", "5")
ORCHESTRATOR = getenv("AEGIS_ORCHESTRATOR", "airflow")
DATASET_ROWS = int(getenv("AEGIS_DATASET_ROWS", "5000"))
RANDOM_SEED = int(getenv("AEGIS_RANDOM_SEED", "42"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
        config=Config(signature_version="s3v4"),
    )


def upload_file(client, local_path: Path, bucket: str, key: str):
    client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def generate_synthetic_transactions(rows: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    customer_id = rng.integers(1000, 1999, size=rows)
    merchant_id = rng.integers(500, 799, size=rows)
    device_id = rng.integers(10000, 19999, size=rows)

    amount = np.round(rng.lognormal(mean=3.5, sigma=0.9, size=rows), 2)
    hour_of_day = rng.integers(0, 24, size=rows)
    customer_tx_count_1h = rng.poisson(lam=2.0, size=rows)
    customer_spend_1h = np.round(amount * rng.uniform(1.0, 6.0, size=rows), 2)
    customer_avg_amount_30d = np.round(rng.lognormal(mean=3.3, sigma=0.5, size=rows), 2)
    merchant_fraud_rate_30d = np.round(rng.beta(a=1.5, b=18.0, size=rows), 4)
    device_seen_before = rng.choice([0, 1], size=rows, p=[0.18, 0.82])
    is_cross_border = rng.choice([0, 1], size=rows, p=[0.88, 0.12])
    channel = rng.choice(["mobile_app", "web", "pos"], size=rows, p=[0.55, 0.25, 0.20])

    amount_vs_customer_average = np.round(
        amount / np.maximum(customer_avg_amount_30d, 1.0), 4
    )

    risk_score = (
        (amount_vs_customer_average > 3.0).astype(int) * 1.2
        + (hour_of_day < 5).astype(int) * 0.7
        + (customer_tx_count_1h > 5).astype(int) * 1.0
        + (merchant_fraud_rate_30d > 0.15).astype(int) * 1.1
        + (device_seen_before == 0).astype(int) * 0.8
        + (is_cross_border == 1).astype(int) * 0.6
        + rng.normal(0, 0.4, size=rows)
    )

    fraud_probability = 1 / (1 + np.exp(-(risk_score - 2.4)))
    is_fraud = rng.binomial(1, fraud_probability)

    timestamps = pd.date_range(
        "2026-07-01T00:00:00Z",
        periods=rows,
        freq="min",
    )

    df = pd.DataFrame(
        {
            "transaction_id": [f"txn_{RUN_TS}_{i:06d}" for i in range(rows)],
            "customer_id": [f"cust_{x}" for x in customer_id],
            "merchant_id": [f"merch_{x}" for x in merchant_id],
            "device_id": [f"device_{x}" for x in device_id],
            "amount": amount,
            "currency": "JOD",
            "country": rng.choice(["JO", "SA", "AE", "QA"], size=rows, p=[0.78, 0.09, 0.08, 0.05]),
            "channel": channel,
            "event_timestamp": timestamps.astype(str),
            "hour_of_day": hour_of_day,
            "customer_tx_count_1h": customer_tx_count_1h,
            "customer_spend_1h": customer_spend_1h,
            "customer_avg_amount_30d": customer_avg_amount_30d,
            "merchant_fraud_rate_30d": merchant_fraud_rate_30d,
            "device_seen_before": device_seen_before,
            "is_cross_border": is_cross_border,
            "amount_vs_customer_average": amount_vs_customer_average,
            "is_fraud": is_fraud,
        }
    )

    return df


def validate_raw_dataset(df: pd.DataFrame) -> dict:
    required_columns = {
        "transaction_id",
        "customer_id",
        "merchant_id",
        "device_id",
        "amount",
        "currency",
        "country",
        "channel",
        "event_timestamp",
        "is_fraud",
    }

    missing_columns = sorted(required_columns - set(df.columns))
    null_counts = df[list(required_columns & set(df.columns))].isnull().sum().to_dict()
    duplicate_transactions = int(df["transaction_id"].duplicated().sum()) if "transaction_id" in df else -1
    invalid_amounts = int((df["amount"] <= 0).sum()) if "amount" in df else -1

    fraud_rate = float(df["is_fraud"].mean()) if "is_fraud" in df else -1.0

    result = {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "missing_columns": missing_columns,
        "null_counts": null_counts,
        "duplicate_transactions": duplicate_transactions,
        "invalid_amounts": invalid_amounts,
        "fraud_rate": fraud_rate,
        "passed": (
            len(missing_columns) == 0
            and duplicate_transactions == 0
            and invalid_amounts == 0
            and len(df) > 100
            and 0.01 <= fraud_rate <= 0.80
        ),
    }

    if not result["passed"]:
        raise RuntimeError(f"Dataset validation failed: {json.dumps(result, indent=2)}")

    return result


def build_bronze(df: pd.DataFrame) -> pd.DataFrame:
    bronze = df.copy()
    bronze["event_timestamp"] = pd.to_datetime(bronze["event_timestamp"], utc=True)
    bronze["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
    bronze["source_system"] = "aegis-phase5-synthetic-generator"
    bronze["raw_dataset_version"] = RUN_TS
    return bronze


def build_silver(bronze: pd.DataFrame) -> pd.DataFrame:
    silver = bronze.copy()
    silver = silver.drop_duplicates(subset=["transaction_id"])
    silver = silver[silver["amount"] > 0].copy()
    silver["is_night_transaction"] = silver["hour_of_day"].between(0, 5).astype(int)
    silver["is_high_velocity_customer"] = (silver["customer_tx_count_1h"] > 5).astype(int)
    silver["is_high_merchant_risk"] = (silver["merchant_fraud_rate_30d"] > 0.15).astype(int)
    silver["validation_status"] = "valid"
    return silver


def build_gold(silver: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    feature_columns = [
        "amount",
        "hour_of_day",
        "customer_tx_count_1h",
        "customer_spend_1h",
        "customer_avg_amount_30d",
        "merchant_fraud_rate_30d",
        "device_seen_before",
        "is_cross_border",
        "amount_vs_customer_average",
        "is_night_transaction",
        "is_high_velocity_customer",
        "is_high_merchant_risk",
    ]

    gold_columns = [
        "transaction_id",
        "customer_id",
        "merchant_id",
        "device_id",
        *feature_columns,
        "is_fraud",
    ]

    gold = silver[gold_columns].copy()
    return gold, feature_columns


def train_and_evaluate(gold: pd.DataFrame, feature_columns: list[str]):
    X = gold[feature_columns]
    y = gold["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.25,
        random_state=RANDOM_SEED,
        stratify=y,
    )

    model = RandomForestClassifier(
        n_estimators=150,
        max_depth=8,
        min_samples_leaf=5,
        random_state=RANDOM_SEED,
        class_weight="balanced",
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, predictions)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
    }

    cm = confusion_matrix(y_test, predictions).tolist()

    split_info = {
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "features": feature_columns,
    }

    return model, metrics, cm, split_info


def write_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def main():
    print("=== Aegis Phase 5 training pipeline ===")
    print(f"run_ts={RUN_TS}")
    print(f"mlflow_tracking_uri={MLFLOW_TRACKING_URI}")
    print(f"s3_endpoint_url={S3_ENDPOINT_URL}")

    client = s3_client()

    raw_df = generate_synthetic_transactions(DATASET_ROWS, RANDOM_SEED)
    validation_result = validate_raw_dataset(raw_df)

    bronze_df = build_bronze(raw_df)
    silver_df = build_silver(bronze_df)
    gold_df, feature_columns = build_gold(silver_df)

    raw_csv = WORKDIR / "transactions.csv"
    raw_metadata = WORKDIR / "metadata.json"
    bronze_parquet = WORKDIR / "transactions_bronze.parquet"
    silver_parquet = WORKDIR / "transactions_silver.parquet"
    gold_parquet = WORKDIR / "fraud_training_dataset.parquet"

    raw_df.to_csv(raw_csv, index=False)
    write_json(
        raw_metadata,
        {
            "run_ts": RUN_TS,
            "dataset_rows": int(len(raw_df)),
            "fraud_rate": float(raw_df["is_fraud"].mean()),
            "source": "synthetic_phase5_generator",
            "validation": validation_result,
        },
    )

    bronze_df.to_parquet(bronze_parquet, index=False)
    silver_df.to_parquet(silver_parquet, index=False)
    gold_df.to_parquet(gold_parquet, index=False)

    raw_csv_uri = upload_file(client, raw_csv, RAW_BUCKET, f"phase5/run_ts={RUN_TS}/transactions.csv")
    raw_metadata_uri = upload_file(client, raw_metadata, RAW_BUCKET, f"phase5/run_ts={RUN_TS}/metadata.json")
    bronze_uri = upload_file(client, bronze_parquet, BRONZE_BUCKET, f"phase5/run_ts={RUN_TS}/transactions_bronze.parquet")
    silver_uri = upload_file(client, silver_parquet, SILVER_BUCKET, f"phase5/run_ts={RUN_TS}/transactions_silver.parquet")
    gold_uri = upload_file(client, gold_parquet, GOLD_BUCKET, f"phase5/run_ts={RUN_TS}/fraud_training_dataset.parquet")

    model, metrics, cm, split_info = train_and_evaluate(gold_df, feature_columns)

    metrics_path = WORKDIR / "metrics.json"
    confusion_path = WORKDIR / "confusion_matrix.json"
    summary_path = WORKDIR / "training_summary.txt"
    lineage_path = WORKDIR / "lineage.json"

    write_json(metrics_path, metrics)
    write_json(confusion_path, {"labels": ["not_fraud", "fraud"], "matrix": cm})
    write_json(
        lineage_path,
        {
            "run_ts": RUN_TS,
            "raw_csv_uri": raw_csv_uri,
            "raw_metadata_uri": raw_metadata_uri,
            "bronze_uri": bronze_uri,
            "silver_uri": silver_uri,
            "gold_uri": gold_uri,
            "feature_columns": feature_columns,
            "registered_model_name": REGISTERED_MODEL_NAME,
        },
    )

    summary_path.write_text(
        "\n".join(
            [
                "Aegis Phase 5 Training Summary",
                f"run_ts={RUN_TS}",
                f"raw_csv_uri={raw_csv_uri}",
                f"bronze_uri={bronze_uri}",
                f"silver_uri={silver_uri}",
                f"gold_uri={gold_uri}",
                f"train_rows={split_info['train_rows']}",
                f"test_rows={split_info['test_rows']}",
                f"accuracy={metrics['accuracy']}",
                f"precision={metrics['precision']}",
                f"recall={metrics['recall']}",
                f"f1={metrics['f1']}",
                f"roc_auc={metrics['roc_auc']}",
            ]
        )
        + "\n"
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"aegis-phase5-training-{RUN_TS}") as run:
        mlflow.log_params(
            {
                "phase": PHASE,
                "orchestrator": ORCHESTRATOR,
                "model_type": "RandomForestClassifier",
                "dataset_rows": int(len(gold_df)),
                "train_rows": split_info["train_rows"],
                "test_rows": split_info["test_rows"],
                "raw_csv_uri": raw_csv_uri,
                "bronze_uri": bronze_uri,
                "silver_uri": silver_uri,
                "gold_uri": gold_uri,
                "random_seed": RANDOM_SEED,
            }
        )

        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        mlflow.log_artifact(str(metrics_path), artifact_path="evaluation")
        mlflow.log_artifact(str(confusion_path), artifact_path="evaluation")
        mlflow.log_artifact(str(summary_path), artifact_path="summary")
        mlflow.log_artifact(str(lineage_path), artifact_path="lineage")

        if REGISTER_MODEL:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=REGISTERED_MODEL_NAME,
                input_example=gold_df[feature_columns].head(5),
            )
        else:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                input_example=gold_df[feature_columns].head(5),
            )

        print("=== MLflow run complete ===")
        print(f"experiment={EXPERIMENT_NAME}")
        print(f"run_id={run.info.run_id}")
        print(f"registered_model_name={REGISTERED_MODEL_NAME if REGISTER_MODEL else 'not_registered'}")

    print("=== Phase 5 training completed successfully ===")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
