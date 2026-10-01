FROM tabulario/spark-iceberg:latest

USER root

RUN python -m pip install \
    --no-cache-dir \
    --timeout 30 \
    --retries 2 \
    boto3 \
    requests