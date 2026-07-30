FROM python:3.13-slim

WORKDIR /app
ARG RELEASE_VERSION=dev
ARG GIT_SHA=unknown
ARG SOURCE_BRANCH=unknown
ARG BUILD_UTC=unknown
ARG UI_IMAGE_NAME=object-storage-heatwave-ui
ARG UI_IMAGE_TAG=dev
ARG CONFIG_SCHEMA_VERSION=2
ENV RELEASE_VERSION=$RELEASE_VERSION GIT_SHA=$GIT_SHA SOURCE_BRANCH=$SOURCE_BRANCH BUILD_UTC=$BUILD_UTC UI_IMAGE_NAME=$UI_IMAGE_NAME UI_IMAGE_TAG=$UI_IMAGE_TAG CONFIG_SCHEMA_VERSION=$CONFIG_SCHEMA_VERSION
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt
COPY myapp ./myapp
RUN useradd --create-home --uid 10001 appuser && mkdir -p /app/instance && chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
CMD ["python", "-m", "flask", "--app", "myapp.app", "run", "--host", "0.0.0.0", "--port", "8080"]
