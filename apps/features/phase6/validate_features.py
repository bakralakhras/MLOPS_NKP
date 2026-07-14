import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config
from feast import FeatureStore

from definitions import (
    customer,
    merchant,
    device,
    customer_activity_v1,
    merchant_risk_v1,
    device_history_v1,
    fraud_detection_v1,
)


RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
WORKDIR = Path("/tmp/aegis-phase6-validation")
WORKDIR.mkdir(parents=True, exist_ok=True)

ENDPOINT = os.environ["S3_ENDPOINT_URL"]
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

BUCKET = "aegis-features"
SOURCE_PREFIX = "phase6/offline/current"
REPO_PATH = "/opt/aegis/phase6/feature_repo"

FEATURE_COLUMNS = [
    "customer_tx_count_1h",
    "customer_spend_1h",
    "customer_avg_amount_30d",
    "merchant_fraud_rate_30d",
    "device_seen_before",
]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def read_parquet(client, key: str) -> pd.DataFrame:
    response = client.get_object(
        Bucket=BUCKET,
        Key=key,
    )

    dataframe = pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )

    if "event_timestamp" in dataframe.columns:
        dataframe["event_timestamp"] = pd.to_datetime(
            dataframe["event_timestamp"],
            utc=True,
        )

    return dataframe


def main():
    print("=== Aegis Phase 6 Feast validation ===")
    print(f"run_ts={RUN_TS}")

    client = s3_client()

    customer_df = read_parquet(
        client,
        f"{SOURCE_PREFIX}/customer_features.parquet",
    )
    merchant_df = read_parquet(
        client,
        f"{SOURCE_PREFIX}/merchant_features.parquet",
    )
    device_df = read_parquet(
        client,
        f"{SOURCE_PREFIX}/device_features.parquet",
    )
    observations_df = read_parquet(
        client,
        f"{SOURCE_PREFIX}/training_observations.parquet",
    )

    print("=== Input rows ===")
    print(f"customer_features={len(customer_df)}")
    print(f"merchant_features={len(merchant_df)}")
    print(f"device_features={len(device_df)}")
    print(f"training_observations={len(observations_df)}")

    if len(observations_df) != 5000:
        raise RuntimeError(
            f"Expected 5000 observations, got {len(observations_df)}"
        )

    print("=== Applying permanent Feast definitions ===")

    writer = FeatureStore(repo_path=REPO_PATH)

    writer.apply(
        [
            customer,
            merchant,
            device,
            customer_activity_v1,
            merchant_risk_v1,
            device_history_v1,
            fraud_detection_v1,
        ]
    )

    # Create a new client so registry persistence is genuinely tested.
    store = FeatureStore(repo_path=REPO_PATH)

    entities = sorted(
        entity.name
        for entity in store.list_entities()
    )
    feature_views = sorted(
        feature_view.name
        for feature_view in store.list_feature_views()
    )
    feature_services = sorted(
        feature_service.name
        for feature_service in store.list_feature_services()
    )

    print(f"entities={entities}")
    print(f"feature_views={feature_views}")
    print(f"feature_services={feature_services}")

    expected_entities = {
        "customer",
        "merchant",
        "device",
    }

    expected_views = {
        "customer_activity_v1",
        "merchant_risk_v1",
        "device_history_v1",
    }

    if not expected_entities.issubset(set(entities)):
        raise RuntimeError("Feast registry is missing entities")

    if not expected_views.issubset(set(feature_views)):
        raise RuntimeError("Feast registry is missing feature views")

    if "fraud_detection_v1" not in feature_services:
        raise RuntimeError("Feast registry is missing feature service")

    entity_df = observations_df[
        [
            "transaction_id",
            "customer_id",
            "merchant_id",
            "device_id",
            "event_timestamp",
            "amount",
            "hour_of_day",
            "is_cross_border",
            "is_fraud",
        ]
    ].copy()

    print("=== Running Feast historical retrieval ===")

    feature_service = store.get_feature_service(
        "fraud_detection_v1"
    )

    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=feature_service,
    ).to_df()

    training_df["event_timestamp"] = pd.to_datetime(
        training_df["event_timestamp"],
        utc=True,
    )

    if len(training_df) != len(entity_df):
        raise RuntimeError(
            f"Feast returned {len(training_df)} rows; "
            f"expected {len(entity_df)}"
        )

    if training_df["transaction_id"].duplicated().any():
        raise RuntimeError(
            "Feast returned duplicate transaction rows"
        )

    missing_columns = sorted(
        set(FEATURE_COLUMNS) - set(training_df.columns)
    )

    if missing_columns:
        raise RuntimeError(
            f"Missing retrieved features: {missing_columns}"
        )

    null_counts = (
        training_df[FEATURE_COLUMNS]
        .isnull()
        .sum()
        .to_dict()
    )

    if any(null_counts.values()):
        raise RuntimeError(
            f"Feature values contain nulls: {null_counts}"
        )

    print("=== Comparing Feast output with source tables ===")

    expected_df = entity_df.merge(
        customer_df[
            [
                "customer_id",
                "event_timestamp",
                "customer_tx_count_1h",
                "customer_spend_1h",
                "customer_avg_amount_30d",
            ]
        ],
        on=["customer_id", "event_timestamp"],
        how="left",
        validate="one_to_one",
    )

    expected_df = expected_df.merge(
        merchant_df[
            [
                "merchant_id",
                "event_timestamp",
                "merchant_fraud_rate_30d",
            ]
        ],
        on=["merchant_id", "event_timestamp"],
        how="left",
        validate="one_to_one",
    )

    expected_df = expected_df.merge(
        device_df[
            [
                "device_id",
                "event_timestamp",
                "device_seen_before",
            ]
        ],
        on=["device_id", "event_timestamp"],
        how="left",
        validate="one_to_one",
    )

    training_df = training_df.sort_values(
        "transaction_id"
    ).reset_index(drop=True)

    expected_df = expected_df.sort_values(
        "transaction_id"
    ).reset_index(drop=True)

    if not training_df["transaction_id"].equals(
        expected_df["transaction_id"]
    ):
        raise RuntimeError(
            "Feast output transaction IDs do not match observations"
        )

    for feature in FEATURE_COLUMNS:
        actual = training_df[feature].astype(float)
        expected = expected_df[feature].astype(float)

        if not np.allclose(
            actual,
            expected,
            rtol=1e-9,
            atol=1e-9,
        ):
            mismatch_count = int(
                (
                    ~np.isclose(
                        actual,
                        expected,
                        rtol=1e-9,
                        atol=1e-9,
                    )
                ).sum()
            )

            raise RuntimeError(
                f"{feature} has {mismatch_count} mismatched values"
            )

    output_path = WORKDIR / "fraud_training_dataset.parquet"

    training_df.to_parquet(
        output_path,
        index=False,
    )

    output_keys = [
        (
            f"phase6/training/run_ts={RUN_TS}/"
            "fraud_training_dataset.parquet"
        ),
        (
            "phase6/training/current/"
            "fraud_training_dataset.parquet"
        ),
    ]

    for key in output_keys:
        client.upload_file(
            str(output_path),
            BUCKET,
            key,
        )

    validation = {
        "phase": "6",
        "run_ts": RUN_TS,
        "status": "PASS",
        "rows": int(len(training_df)),
        "entities": entities,
        "feature_views": feature_views,
        "feature_service": "fraud_detection_v1",
        "feature_columns": FEATURE_COLUMNS,
        "null_counts": null_counts,
        "registry_uri": (
            "s3://aegis-features/"
            "phase6/registry/registry.pb"
        ),
        "training_dataset_uri": (
            "s3://aegis-features/"
            "phase6/training/current/"
            "fraud_training_dataset.parquet"
        ),
    }

    validation_path = WORKDIR / "validation.json"
    validation_path.write_text(
        json.dumps(validation, indent=2, sort_keys=True)
    )

    client.upload_file(
        str(validation_path),
        BUCKET,
        "phase6/validation/current/validation.json",
    )

    print("=== Retrieved sample ===")
    print(
        training_df[
            [
                "transaction_id",
                "customer_id",
                "merchant_id",
                "device_id",
                *FEATURE_COLUMNS,
                "is_fraud",
            ]
        ].head(5).to_string(index=False)
    )

    print("=== Validation result ===")
    print(json.dumps(validation, indent=2))

    print(
        "PASS: permanent Feast registry and "
        "multi-entity historical retrieval validated"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
