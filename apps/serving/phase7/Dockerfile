FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MLSERVER_HTTP_PORT=8080 \
    MLSERVER_GRPC_PORT=9000 \
    MODELS_DIR=/mnt/models

RUN groupadd --gid 1000 mlserver \
    && useradd \
         --uid 1000 \
         --gid 1000 \
         --create-home \
         --shell /usr/sbin/nologin \
         mlserver

RUN python -m pip install --upgrade pip \
    && python -m pip install \
         "mlserver==1.5.0" \
         "mlserver-mlflow==1.5.0" \
         "mlflow==2.16.2" \
         "scikit-learn==1.5.2" \
         "cloudpickle==3.0.0" \
         "protobuf==4.25.3" \
         "uvloop==0.21.0" \
    && python -m pip check

RUN mkdir -p /mnt/models \
    && chown -R 1000:1000 /mnt/models

USER 1000:1000

EXPOSE 8080 9000

CMD ["mlserver", "start", "/mnt/models"]
