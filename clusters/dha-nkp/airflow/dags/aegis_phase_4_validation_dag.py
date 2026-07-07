from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


with DAG(
    dag_id="aegis_phase_4_validation",
    description="Validate Airflow orchestration against MinIO and MLflow for Aegis Phase 4",
    start_date=datetime(2026, 7, 7),
    schedule=None,
    catchup=False,
    tags=["aegis", "phase-4", "validation"],
) as dag:

    validate_minio_and_mlflow = KubernetesPodOperator(
        task_id="validate_minio_and_mlflow",
        name="aegis-phase-4-validation",
        namespace="aegis-airflow",
        image="ghcr.io/bakralakhras/aegis-mlflow:v2.16.2-pg-s3",
        image_pull_policy="IfNotPresent",
        cmds=["python", "-c"],
        arguments=[
            r'''
import os
import tempfile
from datetime import datetime, timezone

import boto3
import mlflow

tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
s3_endpoint = os.environ["MLFLOW_S3_ENDPOINT_URL"]
bucket = os.environ["AEGIS_VALIDATION_BUCKET"]

mlflow.set_tracking_uri(tracking_uri)

ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
object_key = f"airflow/phase4/validation-{ts}.txt"
content = f"aegis phase 4 airflow validation ok at {ts}\n"

s3 = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)

s3.put_object(Bucket=bucket, Key=object_key, Body=content.encode("utf-8"))
read_back = s3.get_object(Bucket=bucket, Key=object_key)["Body"].read().decode("utf-8")

if read_back != content:
    raise RuntimeError("MinIO read-back validation failed")

mlflow.set_experiment("aegis-phase-4-airflow-validation")

with mlflow.start_run(run_name="airflow-phase-4-validation") as run:
    mlflow.log_param("orchestrator", "airflow")
    mlflow.log_param("phase", "4")
    mlflow.log_param("minio_object_key", object_key)
    mlflow.log_metric("validation_success", 1.0)

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt") as f:
        f.write(content)
        artifact_path = f.name

    mlflow.log_artifact(artifact_path, artifact_path="airflow-validation")

    print("Aegis Phase 4 Airflow validation OK")
    print(f"run_id={run.info.run_id}")
    print(f"minio_object=s3://{bucket}/{object_key}")
'''
        ],
        env_vars=[
            k8s.V1EnvVar(
                name="MLFLOW_TRACKING_URI",
                value_from=k8s.V1EnvVarSource(
                    config_map_key_ref=k8s.V1ConfigMapKeySelector(
                        name="airflow-aegis-validation-config",
                        key="MLFLOW_TRACKING_URI",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="MLFLOW_S3_ENDPOINT_URL",
                value_from=k8s.V1EnvVarSource(
                    config_map_key_ref=k8s.V1ConfigMapKeySelector(
                        name="airflow-aegis-validation-config",
                        key="MLFLOW_S3_ENDPOINT_URL",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_DEFAULT_REGION",
                value_from=k8s.V1EnvVarSource(
                    config_map_key_ref=k8s.V1ConfigMapKeySelector(
                        name="airflow-aegis-validation-config",
                        key="AWS_DEFAULT_REGION",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AEGIS_VALIDATION_BUCKET",
                value_from=k8s.V1EnvVarSource(
                    config_map_key_ref=k8s.V1ConfigMapKeySelector(
                        name="airflow-aegis-validation-config",
                        key="AEGIS_VALIDATION_BUCKET",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-s3-credentials",
                        key="AWS_ACCESS_KEY_ID",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-s3-credentials",
                        key="AWS_SECRET_ACCESS_KEY",
                    )
                ),
            ),
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )
