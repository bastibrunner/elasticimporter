FROM python:3.12-alpine

WORKDIR /app

COPY scripts/importer.py /app/importer.py

USER 65532:65532

ENTRYPOINT ["python", "/app/importer.py"]
