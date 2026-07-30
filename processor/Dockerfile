FROM python:3.13-slim
WORKDIR /processor
COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor/stream_processor.py processor/message_store.py processor/vault_config.py processor/database.py processor/stream_client.py processor/loader.py ./
COPY processor/sql ./sql/
COPY loader_core/event_processor.py loader_core/partition_loader.py ./loader_core/
COPY loader_core/sql ./loader_core/sql/
RUN useradd --create-home --uid 10001 processor && chown -R processor:processor /processor
USER processor
ENV PYTHONUNBUFFERED=1
CMD ["python", "stream_processor.py"]
