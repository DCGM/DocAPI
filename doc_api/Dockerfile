# doc_api/Dockerfile
FROM python:3.10-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Copy and install dependencies
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy your app source code
COPY . /app/doc_api

# Default runtime envs
ENV APP_HOST=0.0.0.0
ENV APP_PORT=8888
ENV PRODUCTION=true
ENV BASE_DIR=/app/doc_api_data
ENV PRODUCTION=true
ENV PYTHONPATH=/app

EXPOSE 8888

WORKDIR /app/doc_api

CMD ["python", "-m", "doc_api.run"]
