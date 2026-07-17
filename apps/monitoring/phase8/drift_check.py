from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import s3fs


FEATURE_COLUMNS = [
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

ENDPOINT = os.getenv(
    "S3_ENDPOINT_URL",
    "http://minio.aegis-data.svc.cluster.local:9000",
)
GOLD_BUCKET = os.getenv("AEGIS_GOLD_BUCKET", "aegis-gold")
FEATURE_BUCKET = os.getenv("AEGIS_FEATURE_BUCKET", "aegis-features")

BASELINE_GLOB = (
    f"{GOLD_BUCKET}/phase5/run_ts=*/"
    "fraud_training_dataset.parquet"
)
CURRENT_KEY = (
    f"{FEATURE_BUCKET}/phase6/training/current/"
    "fraud_training_dataset.parquet"
)
REPORT_PREFIX = f"{FEATURE_BUCKET}/phase8/drift"

PSI_WARNING = float(os.getenv("PSI_WARNING_THRESHOLD", "0.10"))
PSI_FAILURE = float(os.getenv("PSI_FAILURE_THRESHOLD", "0.25"))
MISSING_RATE_FAILURE = float(
    os.getenv("MISSING_RATE_FAILURE_THRESHOLD", "0.05")
)
LABEL_SHIFT_FAILURE = float(
    os.getenv("LABEL_SHIFT_FAILURE_THRESHOLD", "0.10")
)


def get_filesystem() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"endpoint_url": ENDPOINT},
        config_kwargs={"s3": {"addressing_style": "path"}},
    )


def calculate_psi(
    baseline: pd.Series,
    current: pd.Series,
    bins: int = 10,
) -> float:
    baseline_values = pd.to_numeric(
        baseline, errors="coerce"
    ).dropna().to_numpy(dtype=float)

    current_values = pd.to_numeric(
        current, errors="coerce"
    ).dropna().to_numpy(dtype=float)

    if baseline_values.size == 0 or current_values.size == 0:
        return 0.0

    quantiles = np.linspace(0, 1, bins + 1)
    boundaries = np.unique(
        np.quantile(baseline_values, quantiles)
    )

    if boundaries.size < 2:
        return 0.0

    boundaries[0] = -np.inf
    boundaries[-1] = np.inf

    baseline_counts, _ = np.histogram(
        baseline_values,
        bins=boundaries,
    )
    current_counts, _ = np.histogram(
        current_values,
        bins=boundaries,
    )

    epsilon = 1e-6
    baseline_ratio = baseline_counts / max(
        baseline_counts.sum(), 1
    )
    current_ratio = current_counts / max(
        current_counts.sum(), 1
    )

    baseline_ratio = np.clip(
        baseline_ratio, epsilon, None
    )
    current_ratio = np.clip(
        current_ratio, epsilon, None
    )

    psi = np.sum(
        (current_ratio - baseline_ratio)
        * np.log(current_ratio / baseline_ratio)
    )

    return float(psi)


def load_parquet(
    filesystem: s3fs.S3FileSystem,
    key: str,
) -> pd.DataFrame:
    with filesystem.open(key, "rb") as stream:
        return pd.read_parquet(stream)


def write_json(
    filesystem: s3fs.S3FileSystem,
    key: str,
    payload: dict[str, Any],
) -> None:
    encoded = json.dumps(
        payload,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")

    with filesystem.open(key, "wb") as stream:
        stream.write(encoded)


def main() -> int:
    filesystem = get_filesystem()

    baseline_keys = sorted(filesystem.glob(BASELINE_GLOB))
    if not baseline_keys:
        raise RuntimeError(
            f"No baseline datasets matched {BASELINE_GLOB}"
        )

    baseline_key = baseline_keys[-1]

    if not filesystem.exists(CURRENT_KEY):
        raise RuntimeError(
            f"Current dataset not found: {CURRENT_KEY}"
        )

    baseline = load_parquet(filesystem, baseline_key)
    current = load_parquet(filesystem, CURRENT_KEY)

    # Reconstruct derived features when the current Phase 6 dataset
    # contains only their source columns.
    if "amount_vs_customer_average" not in current.columns:
        denominator = current["customer_avg_amount_30d"].clip(lower=1.0)
        current["amount_vs_customer_average"] = (
            current["amount"] / denominator
        )

    if "is_night_transaction" not in current.columns:
        current["is_night_transaction"] = (
            current["hour_of_day"].between(0, 5)
        ).astype("int64")

    if "is_high_velocity_customer" not in current.columns:
        current["is_high_velocity_customer"] = (
            current["customer_tx_count_1h"] > 5
        ).astype("int64")

    if "is_high_merchant_risk" not in current.columns:
        current["is_high_merchant_risk"] = (
            current["merchant_fraud_rate_30d"] > 0.15
        ).astype("int64")

    missing_baseline = sorted(
        set(FEATURE_COLUMNS) - set(baseline.columns)
    )
    missing_current = sorted(
        set(FEATURE_COLUMNS) - set(current.columns)
    )

    if missing_baseline or missing_current:
        raise RuntimeError(
            "Feature schema mismatch: "
            f"baseline_missing={missing_baseline}, "
            f"current_missing={missing_current}"
        )

    feature_results: dict[str, Any] = {}
    failed_features: list[str] = []
    warning_features: list[str] = []

    for feature in FEATURE_COLUMNS:
        psi = calculate_psi(
            baseline[feature],
            current[feature],
        )

        baseline_missing_rate = float(
            baseline[feature].isna().mean()
        )
        current_missing_rate = float(
            current[feature].isna().mean()
        )
        missing_rate_increase = (
            current_missing_rate - baseline_missing_rate
        )

        status = "pass"

        if (
            psi >= PSI_FAILURE
            or missing_rate_increase
            > MISSING_RATE_FAILURE
        ):
            status = "fail"
            failed_features.append(feature)
        elif psi >= PSI_WARNING:
            status = "warning"
            warning_features.append(feature)

        feature_results[feature] = {
            "psi": round(psi, 6),
            "baseline_missing_rate": round(
                baseline_missing_rate, 6
            ),
            "current_missing_rate": round(
                current_missing_rate, 6
            ),
            "missing_rate_increase": round(
                missing_rate_increase, 6
            ),
            "status": status,
        }

    label_shift = None
    label_status = "not_evaluated"

    if (
        "is_fraud" in baseline.columns
        and "is_fraud" in current.columns
    ):
        baseline_fraud_rate = float(
            baseline["is_fraud"].mean()
        )
        current_fraud_rate = float(
            current["is_fraud"].mean()
        )
        label_shift = abs(
            current_fraud_rate - baseline_fraud_rate
        )
        label_status = (
            "fail"
            if label_shift > LABEL_SHIFT_FAILURE
            else "pass"
        )

    overall_status = "pass"

    if failed_features or label_status == "fail":
        overall_status = "fail"
    elif warning_features:
        overall_status = "warning"

    run_timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    report = {
        "run_timestamp": run_timestamp,
        "overall_status": overall_status,
        "baseline": f"s3://{baseline_key}",
        "current": f"s3://{CURRENT_KEY}",
        "baseline_rows": int(len(baseline)),
        "current_rows": int(len(current)),
        "thresholds": {
            "psi_warning": PSI_WARNING,
            "psi_failure": PSI_FAILURE,
            "missing_rate_failure": (
                MISSING_RATE_FAILURE
            ),
            "label_shift_failure": (
                LABEL_SHIFT_FAILURE
            ),
        },
        "failed_features": failed_features,
        "warning_features": warning_features,
        "label_shift": (
            None
            if label_shift is None
            else round(label_shift, 6)
        ),
        "label_status": label_status,
        "features": feature_results,
    }

    versioned_key = (
        f"{REPORT_PREFIX}/run_ts={run_timestamp}/report.json"
    )
    latest_key = f"{REPORT_PREFIX}/latest.json"

    write_json(filesystem, versioned_key, report)
    write_json(filesystem, latest_key, report)

    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"versioned_report=s3://{versioned_key}")
    print(f"latest_report=s3://{latest_key}")

    return 2 if overall_status == "fail" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            f"Drift monitoring failed: {exc}",
            file=sys.stderr,
        )
        raise
