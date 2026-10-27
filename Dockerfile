FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV TZ=Asia/Jakarta
ENV SQL_FILE_PATH=/app/src/query/REALIZATION.sql
ENV TRANSFORM_SQL_FILE_PATH=/app/src/query/TRANSFORM.sql

ENTRYPOINT ["python", "-m", "src.main"]
