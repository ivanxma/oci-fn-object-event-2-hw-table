FROM python:3.13-slim
WORKDIR /consumer
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer/stream_consumer.py consumer/message_store.py consumer/vault_config.py consumer/database.py consumer/stream_client.py consumer/loader.py consumer/verify_capture_store.py ./
COPY consumer/sql ./sql/
COPY function/func.py function/partition_loader.py ./function_loader/
RUN useradd --create-home --uid 10001 consumer && chown -R consumer:consumer /consumer
USER consumer
ENV PYTHONUNBUFFERED=1
CMD ["python", "stream_consumer.py"]
