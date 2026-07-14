import os
from datetime import timedelta

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    ValueType,
)
from feast.data_format import ParquetFormat
from feast.types import Float64, Int64


S3_ENDPOINT_URL = os.getenv(
    "S3_ENDPOINT_URL",
    "http://minio.aegis-data.svc.cluster.local:9000",
)


customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
    description="Customer associated with a transaction",
)

merchant = Entity(
    name="merchant",
    join_keys=["merchant_id"],
    value_type=ValueType.STRING,
    description="Merchant receiving a transaction",
)

device = Entity(
    name="device",
    join_keys=["device_id"],
    value_type=ValueType.STRING,
    description="Device used to initiate a transaction",
)


customer_source = FileSource(
    name="customer_features_source_v1",
    path=(
        "s3://aegis-features/"
        "phase6/offline/current/customer_features.parquet"
    ),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    s3_endpoint_override=S3_ENDPOINT_URL,
)

merchant_source = FileSource(
    name="merchant_features_source_v1",
    path=(
        "s3://aegis-features/"
        "phase6/offline/current/merchant_features.parquet"
    ),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    s3_endpoint_override=S3_ENDPOINT_URL,
)

device_source = FileSource(
    name="device_features_source_v1",
    path=(
        "s3://aegis-features/"
        "phase6/offline/current/device_features.parquet"
    ),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    s3_endpoint_override=S3_ENDPOINT_URL,
)


customer_activity_v1 = FeatureView(
    name="customer_activity_v1",
    entities=[customer],
    ttl=timedelta(days=30),
    schema=[
        Field(
            name="customer_tx_count_1h",
            dtype=Int64,
        ),
        Field(
            name="customer_spend_1h",
            dtype=Float64,
        ),
        Field(
            name="customer_avg_amount_30d",
            dtype=Float64,
        ),
    ],
    source=customer_source,
    online=False,
    tags={
        "owner": "aegis-platform",
        "phase": "6",
        "version": "v1",
    },
)

merchant_risk_v1 = FeatureView(
    name="merchant_risk_v1",
    entities=[merchant],
    ttl=timedelta(days=30),
    schema=[
        Field(
            name="merchant_fraud_rate_30d",
            dtype=Float64,
        ),
    ],
    source=merchant_source,
    online=False,
    tags={
        "owner": "aegis-platform",
        "phase": "6",
        "version": "v1",
    },
)

device_history_v1 = FeatureView(
    name="device_history_v1",
    entities=[device],
    ttl=timedelta(days=365),
    schema=[
        Field(
            name="device_seen_before",
            dtype=Int64,
        ),
    ],
    source=device_source,
    online=False,
    tags={
        "owner": "aegis-platform",
        "phase": "6",
        "version": "v1",
    },
)


fraud_detection_v1 = FeatureService(
    name="fraud_detection_v1",
    features=[
        customer_activity_v1,
        merchant_risk_v1,
        device_history_v1,
    ],
)
