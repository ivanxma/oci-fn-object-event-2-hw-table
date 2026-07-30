FROM python:3.13-slim
WORKDIR /processor
ARG RELEASE_VERSION=dev
ARG GIT_SHA=unknown
ARG SOURCE_BRANCH=unknown
ARG BUILD_UTC=unknown
ARG PROCESSOR_IMAGE_NAME=object-storage-stream-processor
ARG PROCESSOR_IMAGE_TAG=dev
ARG CONFIG_SCHEMA_VERSION=2
ENV RELEASE_VERSION=$RELEASE_VERSION GIT_SHA=$GIT_SHA SOURCE_BRANCH=$SOURCE_BRANCH BUILD_UTC=$BUILD_UTC PROCESSOR_IMAGE_NAME=$PROCESSOR_IMAGE_NAME PROCESSOR_IMAGE_TAG=$PROCESSOR_IMAGE_TAG CONFIG_SCHEMA_VERSION=$CONFIG_SCHEMA_VERSION
COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor/stream_processor.py processor/message_store.py processor/vault_config.py processor/database.py processor/stream_client.py processor/loader.py processor/release.py ./
COPY processor/sql ./sql/
COPY loader_core/event_processor.py loader_core/partition_loader.py ./loader_core/
COPY loader_core/sql ./loader_core/sql/
RUN useradd --create-home --uid 10001 processor && chown -R processor:processor /processor
USER processor
ENV PYTHONUNBUFFERED=1
CMD ["python", "stream_processor.py"]
