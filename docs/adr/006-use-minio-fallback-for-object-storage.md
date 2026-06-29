# ADR 006 — Use MinIO Fallback for Aegis Object Storage

## Status

Accepted

## Context

Aegis requires S3-compatible object storage for:

- raw transaction data
- bronze datasets
- silver datasets
- gold datasets
- feature offline store
- MLflow artifacts
- model artifacts
- prediction logs
- Airflow logs
- validation reports
- training datasets
- model evaluation outputs

The preferred enterprise option was Nutanix Objects because Aegis runs on NKP.

Cluster discovery found that the Kubernetes Container Object Storage Interface APIs are installed:

- bucketclaims.objectstorage.k8s.io
- buckets.objectstorage.k8s.io
- bucketclasses.objectstorage.k8s.io
- bucketaccesses.objectstorage.k8s.io
- bucketaccessclasses.objectstorage.k8s.io

However, no usable object storage backend is configured:

- BucketClass: none
- BucketAccessClass: none
- Bucket: none
- BucketClaim: none
- BucketAccess: none

Prism Central is reachable from the cluster, but Aegis does not currently have Nutanix Objects endpoint details or dedicated Objects credentials.

## Decision

Aegis will deploy MinIO inside the `aegis-data` namespace as the Phase 2 object storage implementation.

MinIO will use Nutanix CSI-backed PVCs through the default `nutanix-volume` StorageClass.

## Rationale

This keeps Phase 2 moving while still following the enterprise architecture decision tree:

1. Prefer Nutanix Objects if available.
2. Prefer an existing enterprise S3-compatible object store if available.
3. Use MinIO inside Aegis if no external object storage is currently available.

MinIO provides S3-compatible object storage for later Aegis components including MLflow, Airflow, KServe, Feast, and data lake pipelines.

## Rejected Alternatives

### Nutanix Objects

Rejected for current implementation because Aegis does not have the required endpoint, credentials, BucketClass, or BucketAccessClass.

Nutanix Objects remains the preferred production option if the platform team later provides proper access.

### Rook-Ceph

Rejected because NKP already provides Nutanix CSI block storage. Adding Rook-Ceph would duplicate the storage layer and introduce unnecessary operational complexity for this MLOps platform.

## Consequences

Aegis owns and operates MinIO as a tenant workload.

This adds some operational responsibility, but keeps storage self-contained and available for the rest of the MLOps platform.

## Future Migration

If Nutanix Objects becomes available later, Aegis can migrate from MinIO to Nutanix Objects by updating S3 endpoint configuration, credentials, and bucket locations for dependent services.
