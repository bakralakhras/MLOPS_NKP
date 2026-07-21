#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="${1:?Usage: $0 <model-version>}"
MODEL_NAME="aegis-fraud-baseline"
MANIFEST="clusters/dha-nkp/serving/dev/aegis-fraud-baseline.yaml"

echo "[1/6] Resolving MLflow model version ${VERSION}"

SOURCE="$(
kubectl -n aegis-ml exec -i deploy/mlflow -c mlflow -- \
  python - "$VERSION" <<'PY'
import json
import sys
import urllib.parse
import urllib.request

version = sys.argv[1]
params = urllib.parse.urlencode({
    "name": "aegis-fraud-baseline",
    "version": version,
})

url = "http://127.0.0.1:5000/api/2.0/mlflow/model-versions/get?" + params

with urllib.request.urlopen(url) as response:
    model = json.load(response)["model_version"]

print(model["source"])
PY
)"

SOURCE="$(printf '%s\n' "$SOURCE" | tail -1 | tr -d '\r')"

if [[ "$SOURCE" != s3://* ]]; then
  echo "ERROR: Invalid MLflow artifact source: $SOURCE" >&2
  exit 1
fi

echo "Model version: $VERSION"
echo "Artifact:      $SOURCE"

echo "[2/6] Updating KServe manifest"

python3 - "$MANIFEST" "$SOURCE" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
source = sys.argv[2]
text = path.read_text()

updated, count = re.subn(
    r"(?m)^(\s*storageUri:\s*).+$",
    rf"\1{source}",
    text,
    count=1,
)

if count != 1:
    raise RuntimeError("Could not find exactly one storageUri")

path.write_text(updated)
PY

grep -n "storageUri:" "$MANIFEST"

echo "[3/6] Committing controlled model promotion"

git add "$MANIFEST"
git commit -m "Promote fraud model version ${VERSION}" -- "$MANIFEST"
git push origin HEAD:main

echo "[4/6] Reconciling Flux"

if command -v flux >/dev/null 2>&1; then
  flux reconcile kustomization aegis-dha-nkp \
    -n aegis-system \
    --with-source
else
  kubectl -n aegis-system annotate \
    kustomization aegis-dha-nkp \
    reconcile.fluxcd.io/requestedAt="$(date +%s)" \
    --overwrite

  sleep 10

  kubectl -n aegis-system wait \
    kustomization/aegis-dha-nkp \
    --for=condition=Ready \
    --timeout=180s
fi

echo "[5/6] Waiting for KServe to receive the new artifact"

for attempt in $(seq 1 60); do
  APPLIED="$(
    kubectl -n aegis-serving-dev get \
      inferenceservice "$MODEL_NAME" \
      -o jsonpath='{.spec.predictor.model.storageUri}'
  )"

  if [[ "$APPLIED" == "$SOURCE" ]]; then
    break
  fi

  sleep 5
done

if [[ "${APPLIED:-}" != "$SOURCE" ]]; then
  echo "ERROR: KServe did not receive the new artifact URI" >&2
  exit 1
fi

kubectl -n aegis-serving-dev wait \
  inferenceservice/"$MODEL_NAME" \
  --for=condition=Ready \
  --timeout=300s

echo "[6/6] Promotion completed"

echo "Serving version: $VERSION"
echo "Serving artifact: $(
  kubectl -n aegis-serving-dev get \
    inferenceservice "$MODEL_NAME" \
    -o jsonpath='{.spec.predictor.model.storageUri}'
)"

kubectl -n aegis-serving-dev get \
  inferenceservice "$MODEL_NAME"

kubectl -n aegis-serving-dev get pods \
  -l app=isvc.aegis-fraud-baseline-predictor \
  -o wide
