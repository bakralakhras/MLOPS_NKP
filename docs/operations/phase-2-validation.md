# Phase 2 Validation — Object Storage

## Phase

Phase 2 — Object Storage Decision and Deployment

## Final Decision

Aegis uses MinIO as the Phase 2 S3-compatible object storage backend.

Nutanix Objects was investigated first, but was not accessible to Aegis because:

- COSI APIs existed, but no BucketClass existed
- no BucketAccessClass existed
- no Nutanix Objects S3 endpoint was available
- no dedicated Objects credentials were available

MinIO was selected as the fallback implementation.

## Deployment Summary

Namespace:

```text
aegis-data
