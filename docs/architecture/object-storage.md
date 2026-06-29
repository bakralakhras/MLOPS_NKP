# Aegis Object Storage Architecture

## Phase

Phase 2 — Object Storage Decision and Deployment

## Selected Implementation

Aegis will use MinIO deployed inside the `aegis-data` namespace.

## Reason

Nutanix Objects was the preferred enterprise option, but it is not currently accessible to Aegis.

Discovery showed:

- COSI controller exists
- BucketClass does not exist
- BucketAccessClass does not exist
- Nutanix Objects endpoint is not available
- dedicated Aegis Objects credentials are not available

Therefore, MinIO is selected as the Phase 2 fallback.

## Architecture

```text
Nutanix Volumes
  ↓
nutanix-volume StorageClass
  ↓
MinIO PVCs
  ↓
MinIO StatefulSet in aegis-data
  ↓
S3 API service
  ↓
Aegis buckets
  ↓
MLflow / Airflow / KServe / Feast / data lake
