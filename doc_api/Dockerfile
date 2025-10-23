# doc_api/Dockerfile
FROM python:3.10-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy your app source code
COPY . /app

# Create a non-root user and fix permissions
RUN useradd -m -u 10001 appuser && chown -R appuser:appuser /app
USER appuser

# Default runtime envs
ENV APP_HOST=0.0.0.0 \
    APP_PORT=8888 \
    PRODUCTION=true

EXPOSE 8888

CMD ["python", "-m", "doc_api.run"]
