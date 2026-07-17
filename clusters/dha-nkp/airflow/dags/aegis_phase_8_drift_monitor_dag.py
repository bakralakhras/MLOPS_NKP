from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s


IMAGE = (
    "ghcr.io/bakralakhras/aegis-features@"
    "sha256:e1b77befb2f409b191af25aa93981e66"
    "ea7936a766f3bc2099db560313a78e37"
)

TARGET_NAMESPACE = "aegis-features"
SERVICE_ACCOUNT = "aegis-feast"


def config_env(name: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            config_map_key_ref=k8s.V1ConfigMapKeySelector(
                name="aegis-feature-config",
                key=name,
            )
        ),
    )


def secret_env(name: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="aegis-feast-s3-credentials",
                key=name,
            )
        ),
    )


COMMON_ENV = [
    secret_env("AWS_ACCESS_KEY_ID"),
    secret_env("AWS_SECRET_ACCESS_KEY"),
    config_env("AWS_DEFAULT_REGION"),
    config_env("AWS_REGION"),
    config_env("AWS_EC2_METADATA_DISABLED"),
    config_env("AWS_ENDPOINT_URL"),
    config_env("AWS_ENDPOINT_URL_S3"),
    config_env("AWS_S3_ADDRESSING_STYLE"),
    config_env("S3_ENDPOINT_URL"),
    config_env("AEGIS_GOLD_BUCKET"),
    config_env("AEGIS_FEATURE_BUCKET"),
    k8s.V1EnvVar(
        name="PSI_WARNING_THRESHOLD",
        value="0.10",
    ),
    k8s.V1EnvVar(
        name="PSI_FAILURE_THRESHOLD",
        value="0.25",
    ),
    k8s.V1EnvVar(
        name="MISSING_RATE_FAILURE_THRESHOLD",
        value="0.05",
    ),
    k8s.V1EnvVar(
        name="LABEL_SHIFT_FAILURE_THRESHOLD",
        value="0.10",
    ),
    k8s.V1EnvVar(name="HOME", value="/tmp"),
    k8s.V1EnvVar(
        name="XDG_CACHE_HOME",
        value="/tmp/.cache",
    ),
]

SCRIPT_VOLUME = k8s.V1Volume(
    name="drift-script",
    config_map=k8s.V1ConfigMapVolumeSource(
        name="aegis-phase8-drift-script",
        default_mode=0o444,
    ),
)

SCRIPT_MOUNT = k8s.V1VolumeMount(
    name="drift-script",
    mount_path="/opt/aegis-phase8",
    read_only=True,
)

TEMP_VOLUME = k8s.V1Volume(
    name="temporary-data",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)

TEMP_MOUNT = k8s.V1VolumeMount(
    name="temporary-data",
    mount_path="/tmp",
)

POD_SECURITY_CONTEXT = k8s.V1PodSecurityContext(
    run_as_non_root=True,
    run_as_user=10001,
    run_as_group=10001,
    fs_group=10001,
    seccomp_profile=k8s.V1SeccompProfile(
        type="RuntimeDefault"
    ),
)

CONTAINER_SECURITY_CONTEXT = k8s.V1SecurityContext(
    allow_privilege_escalation=False,
    read_only_root_filesystem=True,
    capabilities=k8s.V1Capabilities(
        drop=["ALL"]
    ),
)

with DAG(
    dag_id="aegis_phase_8_drift_monitor",
    description=(
        "Compare current fraud features against the Phase 5 "
        "training baseline and publish a drift report to MinIO"
    ),
    start_date=datetime(2026, 7, 17),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "aegis-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=[
        "aegis",
        "phase-8",
        "monitoring",
        "drift",
    ],
) as dag:
    calculate_drift = KubernetesPodOperator(
        task_id="calculate_drift",
        name="aegis-phase8-drift-monitor",
        namespace=TARGET_NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        automount_service_account_token=False,
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name="ghcr-bakralakhras"
            )
        ],
        cmds=["python"],
        arguments=[
            "/opt/aegis-phase8/drift_check.py"
        ],
        env_vars=COMMON_ENV,
        labels={
            "app.kubernetes.io/name": (
                "aegis-phase8-drift-monitor"
            ),
            "app.kubernetes.io/part-of": "aegis",
            "aegis.io/phase": "8",
            "aegis.io/minio-client": "true",
            "aegis.io/workload": "drift-monitoring",
        },
        volumes=[
            SCRIPT_VOLUME,
            TEMP_VOLUME,
        ],
        volume_mounts=[
            SCRIPT_MOUNT,
            TEMP_MOUNT,
        ],
        container_resources=(
            k8s.V1ResourceRequirements(
                requests={
                    "cpu": "250m",
                    "memory": "512Mi",
                },
                limits={
                    "cpu": "1",
                    "memory": "2Gi",
                },
            )
        ),
        security_context=POD_SECURITY_CONTEXT,
        container_security_context=(
            CONTAINER_SECURITY_CONTEXT
        ),
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=False,
        reattach_on_restart=True,
        startup_timeout_seconds=300,
        execution_timeout=timedelta(minutes=20),
        do_xcom_push=False,
    )
