# doc_api/Dockerfile
FROM python:3.10-slim-bookworm

# this is for GitHub Actions to inject build metadata
ARG VERSION
ARG COMMIT_SHA
ARG BUILD_DATE

ARG DEBIAN_FRONTEND=noninteractive

LABEL org.opencontainers.image.title="DocAPI"
LABEL org.opencontainers.image.description="API for document processing."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.url="https://github.com/DCGM/DocAPI"
LABEL org.opencontainers.image.source="https://github.com/DCGM/DocAPI.git"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.revision=$COMMIT_SHA
LABEL org.opencontainers.image.created=$BUILD_DATE
LABEL org.opencontainers.image.authors="Jan Kohút"

ENV APP_VERSION=${VERSION:-custom}
ENV APP_GIT_COMMIT=${COMMIT_SHA}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Copy and install dependencies
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy your app source code
COPY . /app/doc_api

ENV PYTHONPATH=/app

# Default runtime envs
ENV APP_HOST=0.0.0.0
ENV APP_PORT=9999
EXPOSE 9999

ENV PRODUCTION=true

# bind this to a volume in your docker-compose or docker run command
ENV BASE_DIR=/app/doc_api_data

# if the database does not exist, it will be created on startup
# replace with the actual user, password, host and port of postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_HOST=localhost
ENV POSTGRES_PORT=5432
ENV DATABASE_URL=postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/doc_api_db
# Startup guards take precedence over automatic updates.
ENV DATABASE_REQUIRE_MATCHING_SCHEMA=true
ENV DATABASE_ALLOW_SCHEMA_UPDATE=false
ENV DATABASE_REQUIRE_MATCHING_COMMIT=false
ENV DATABASE_ALLOW_COMMIT_UPDATE=true

WORKDIR /app/doc_api

CMD ["python", "-m", "doc_api.run"]
