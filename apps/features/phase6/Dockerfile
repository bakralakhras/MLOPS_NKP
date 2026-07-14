FROM quay.io/feastdev/feature-server:0.64.0@sha256:00229275866eccd0a04d5d28f9e03d5896e1e9c0a74a7c9fc5b13b8b85449fa7

USER 0

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install \
      --no-cache-dir \
      -r /tmp/requirements.txt \
    && python -m pip check \
    && rm -f /tmp/requirements.txt

WORKDIR /opt/aegis/phase6

COPY --chown=10001:10001 \
  build_features.py \
  /opt/aegis/phase6/build_features.py

COPY --chown=10001:10001 \
  validate_features.py \
  /opt/aegis/phase6/validate_features.py

COPY --chown=10001:10001 \
  feature_repo \
  /opt/aegis/phase6/feature_repo

ENV PYTHONUNBUFFERED=1
ENV HOME=/tmp
ENV XDG_CACHE_HOME=/tmp/.cache
ENV PYTHONPATH=/opt/aegis/phase6/feature_repo

USER 10001

ENTRYPOINT ["python", "/opt/aegis/phase6/build_features.py"]
