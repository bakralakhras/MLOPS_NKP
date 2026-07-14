import io
import json
import os
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config


BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
WORKDIR = Path("/tmp/aegis-phase6")
WORKDIR.mkdir(parents=True, exist_ok=True)


def getenv(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)

    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID", required=True)
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY", required=True)
AWS_DEFAULT_REGION = getenv("AWS_DEFAULT_REGION", "us-east-1")

S3_ENDPOINT_URL = getenv("S3_ENDPOINT_URL", required=True)

SILVER_BUCKET = getenv("AEGIS_SILVER_BUCKET", "aegis-silver")
FEATURE_BUCKET = getenv("AEGIS_FEATURE_BUCKET", "aegis-features")

SILVER_PREFIX = getenv("AEGIS_SILVER_PREFIX", "phase5/run_ts=")
OUTPUT_PREFIX = getenv("AEGIS_FEATURE_PREFIX", "phase6/offline")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def find_latest_silver_key(client) -> str:
    keys: list[str] = []

    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=SILVER_BUCKET,
        Prefix=SILVER_PREFIX,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]

            if key.endswith("/transactions_silver.parquet"):
                keys.append(key)

    if not keys:
        raise RuntimeError(
            f"No silver Parquet files found in "
            f"s3://{SILVER_BUCKET}/{SILVER_PREFIX}"
        )

    return sorted(keys)[-1]


def read_parquet(client, bucket: str, key: str) -> pd.DataFrame:
    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )

    return pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )


def prune_window(window: deque, cutoff: pd.Timestamp) -> None:
    while window and window[0][0] < cutoff:
        window.popleft()


def build_feature_tables(source: pd.DataFrame):
    required_columns = {
        "transaction_id",
        "customer_id",
        "merchant_id",
        "device_id",
        "amount",
        "hour_of_day",
        "is_cross_border",
        "event_timestamp",
        "is_fraud",
    }

    missing = sorted(required_columns - set(source.columns))

    if missing:
        raise RuntimeError(
            f"Silver source is missing columns: {missing}"
        )

    transactions = source[list(required_columns)].copy()

    transactions["event_timestamp"] = pd.to_datetime(
        transactions["event_timestamp"],
        utc=True,
    )

    transactions = transactions.sort_values(
        ["event_timestamp", "transaction_id"],
        kind="stable",
    ).reset_index(drop=True)

    if transactions["transaction_id"].duplicated().any():
        raise RuntimeError("Duplicate transaction IDs detected")

    if transactions["event_timestamp"].isna().any():
        raise RuntimeError("Invalid event timestamps detected")

    if (transactions["amount"] <= 0).any():
        raise RuntimeError("Non-positive transaction amounts detected")

    customer_1h_history = defaultdict(deque)
    customer_30d_history = defaultdict(deque)
    merchant_30d_history = defaultdict(deque)
    devices_seen: set[str] = set()

    customer_rows: list[dict] = []
    merchant_rows: list[dict] = []
    device_rows: list[dict] = []
    observation_rows: list[dict] = []

    created_timestamp = pd.Timestamp.now(tz="UTC")

    # Transactions sharing an identical event timestamp are processed as one
    # batch. They cannot see each other, which avoids introducing an arbitrary
    # order between simultaneous events.
    for event_timestamp, timestamp_batch in transactions.groupby(
        "event_timestamp",
        sort=True,
    ):
        batch_rows = list(
            timestamp_batch.itertuples(index=False)
        )

        for row in batch_rows:
            customer_id = str(row.customer_id)
            merchant_id = str(row.merchant_id)
            device_id = str(row.device_id)
            amount = float(row.amount)

            customer_1h = customer_1h_history[customer_id]
            customer_30d = customer_30d_history[customer_id]
            merchant_30d = merchant_30d_history[merchant_id]

            prune_window(
                customer_1h,
                event_timestamp - timedelta(hours=1),
            )
            prune_window(
                customer_30d,
                event_timestamp - timedelta(days=30),
            )
            prune_window(
                merchant_30d,
                event_timestamp - timedelta(days=30),
            )

            customer_tx_count_1h = len(customer_1h)
            customer_spend_1h = sum(
                prior_amount
                for _, prior_amount in customer_1h
            )

            if customer_30d:
                customer_avg_amount_30d = (
                    sum(
                        prior_amount
                        for _, prior_amount in customer_30d
                    )
                    / len(customer_30d)
                )
            else:
                customer_avg_amount_30d = 0.0

            if merchant_30d:
                merchant_fraud_rate_30d = (
                    sum(
                        prior_label
                        for _, prior_label in merchant_30d
                    )
                    / len(merchant_30d)
                )
            else:
                merchant_fraud_rate_30d = 0.0

            device_seen_before = int(device_id in devices_seen)

            customer_rows.append(
                {
                    "customer_id": customer_id,
                    "event_timestamp": event_timestamp,
                    "created_timestamp": created_timestamp,
                    "customer_tx_count_1h": int(
                        customer_tx_count_1h
                    ),
                    "customer_spend_1h": round(
                        float(customer_spend_1h),
                        2,
                    ),
                    "customer_avg_amount_30d": round(
                        float(customer_avg_amount_30d),
                        4,
                    ),
                }
            )

            merchant_rows.append(
                {
                    "merchant_id": merchant_id,
                    "event_timestamp": event_timestamp,
                    "created_timestamp": created_timestamp,
                    "merchant_fraud_rate_30d": round(
                        float(merchant_fraud_rate_30d),
                        6,
                    ),
                }
            )

            device_rows.append(
                {
                    "device_id": device_id,
                    "event_timestamp": event_timestamp,
                    "created_timestamp": created_timestamp,
                    "device_seen_before": device_seen_before,
                }
            )

            observation_rows.append(
                {
                    "transaction_id": str(row.transaction_id),
                    "customer_id": customer_id,
                    "merchant_id": merchant_id,
                    "device_id": device_id,
                    "event_timestamp": event_timestamp,
                    "amount": amount,
                    "hour_of_day": int(row.hour_of_day),
                    "is_cross_border": int(row.is_cross_border),
                    "is_fraud": int(row.is_fraud),
                }
            )

        # Add the current timestamp batch only after every feature row for this
        # timestamp has been calculated.
        for row in batch_rows:
            customer_id = str(row.customer_id)
            merchant_id = str(row.merchant_id)
            device_id = str(row.device_id)

            customer_1h_history[customer_id].append(
                (event_timestamp, float(row.amount))
            )
            customer_30d_history[customer_id].append(
                (event_timestamp, float(row.amount))
            )
            merchant_30d_history[merchant_id].append(
                (event_timestamp, int(row.is_fraud))
            )
            devices_seen.add(device_id)

    customer_df = pd.DataFrame(customer_rows)
    merchant_df = pd.DataFrame(merchant_rows)
    device_df = pd.DataFrame(device_rows)
    observations_df = pd.DataFrame(observation_rows)

    for name, dataframe in {
        "customer": customer_df,
        "merchant": merchant_df,
        "device": device_df,
        "observations": observations_df,
    }.items():
        if dataframe.empty:
            raise RuntimeError(f"{name} table is empty")

        if dataframe.isnull().any().any():
            raise RuntimeError(
                f"{name} table contains null values"
            )

    return (
        customer_df,
        merchant_df,
        device_df,
        observations_df,
    )


def upload_dataframe(
    client,
    dataframe: pd.DataFrame,
    filename: str,
) -> dict:
    local_path = WORKDIR / filename
    dataframe.to_parquet(local_path, index=False)

    versioned_key = (
        f"{OUTPUT_PREFIX}/run_ts={BUILD_TS}/{filename}"
    )
    current_key = (
        f"{OUTPUT_PREFIX}/current/{filename}"
    )

    client.upload_file(
        str(local_path),
        FEATURE_BUCKET,
        versioned_key,
    )

    client.upload_file(
        str(local_path),
        FEATURE_BUCKET,
        current_key,
    )

    return {
        "versioned_uri": (
            f"s3://{FEATURE_BUCKET}/{versioned_key}"
        ),
        "current_uri": (
            f"s3://{FEATURE_BUCKET}/{current_key}"
        ),
        "rows": int(len(dataframe)),
    }


def main():
    print("=== Aegis Phase 6 feature builder ===")
    print(f"build_ts={BUILD_TS}")
    print(f"s3_endpoint={S3_ENDPOINT_URL}")

    client = s3_client()

    silver_key = find_latest_silver_key(client)

    print(
        f"source=s3://{SILVER_BUCKET}/{silver_key}"
    )

    silver_df = read_parquet(
        client,
        SILVER_BUCKET,
        silver_key,
    )

    print(f"source_rows={len(silver_df)}")

    (
        customer_df,
        merchant_df,
        device_df,
        observations_df,
    ) = build_feature_tables(silver_df)

    outputs = {
        "customer_features": upload_dataframe(
            client,
            customer_df,
            "customer_features.parquet",
        ),
        "merchant_features": upload_dataframe(
            client,
            merchant_df,
            "merchant_features.parquet",
        ),
        "device_features": upload_dataframe(
            client,
            device_df,
            "device_features.parquet",
        ),
        "training_observations": upload_dataframe(
            client,
            observations_df,
            "training_observations.parquet",
        ),
    }

    manifest = {
        "phase": "6",
        "build_ts": BUILD_TS,
        "source_uri": (
            f"s3://{SILVER_BUCKET}/{silver_key}"
        ),
        "feature_logic": (
            "Features use only transactions with an event timestamp "
            "strictly earlier than the observation timestamp."
        ),
        "outputs": outputs,
    }

    manifest_path = WORKDIR / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
    )

    for key in [
        f"{OUTPUT_PREFIX}/run_ts={BUILD_TS}/manifest.json",
        f"{OUTPUT_PREFIX}/current/manifest.json",
    ]:
        client.upload_file(
            str(manifest_path),
            FEATURE_BUCKET,
            key,
        )

    print("=== Feature build output ===")
    print(json.dumps(manifest, indent=2))
    print("=== Phase 6 feature build completed successfully ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
