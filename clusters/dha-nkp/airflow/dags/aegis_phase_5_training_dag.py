from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


with DAG(
    dag_id="aegis_phase_5_training",
    description="Run the Aegis Phase 5 training pipeline and register the model in MLflow",
    start_date=datetime(2026, 7, 12),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "aegis-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["aegis", "phase-5", "training", "mlflow"],
) as dag:

    train_and_register_model = KubernetesPodOperator(
        task_id="train_and_register_model",
        name="aegis-phase-5-training",
        namespace="aegis-airflow",

        image="ghcr.io/bakralakhras/aegis-training:phase5",
        image_pull_policy="Always",
        image_pull_secrets=[
            k8s.V1LocalObjectReference(name="ghcr-bakralakhras")
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

            k8s.V1EnvVar(name="AEGIS_RAW_BUCKET", value="aegis-raw"),
            k8s.V1EnvVar(name="AEGIS_BRONZE_BUCKET", value="aegis-bronze"),
            k8s.V1EnvVar(name="AEGIS_SILVER_BUCKET", value="aegis-silver"),
            k8s.V1EnvVar(name="AEGIS_GOLD_BUCKET", value="aegis-gold"),

            k8s.V1EnvVar(
                name="MLFLOW_EXPERIMENT_NAME",
                value="aegis-phase-5-training-pipeline",
            ),
            k8s.V1EnvVar(
                name="REGISTERED_MODEL_NAME",
                value="aegis-fraud-baseline",
            ),
            k8s.V1EnvVar(name="REGISTER_MODEL", value="true"),

            k8s.V1EnvVar(name="AEGIS_PHASE", value="5"),
            k8s.V1EnvVar(
                name="AEGIS_ORCHESTRATOR",
                value="airflow-kubernetes-pod-operator",
            ),
            k8s.V1EnvVar(name="AEGIS_DATASET_ROWS", value="5000"),
            k8s.V1EnvVar(name="AEGIS_RANDOM_SEED", value="42"),
        ],

        labels={
            "app.kubernetes.io/name": "aegis-phase5-training",
            "app.kubernetes.io/part-of": "aegis",
            "aegis.io/phase": "5",
            "aegis.io/workload": "training",
        },

        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "500m",
                "memory": "1Gi",
            },
            limits={
                "cpu": "2",
                "memory": "3Gi",
            },
        ),

        security_context=k8s.V1PodSecurityContext(
            run_as_non_root=True,
            run_as_user=10001,
            run_as_group=10001,
            seccomp_profile=k8s.V1SeccompProfile(
                type="RuntimeDefault"
            ),
        ),

        container_security_context=k8s.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=k8s.V1Capabilities(
                drop=["ALL"]
            ),
        ),

        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=False,
        reattach_on_restart=True,
        startup_timeout_seconds=300,
        execution_timeout=timedelta(minutes=30),
        do_xcom_push=False,
    )
