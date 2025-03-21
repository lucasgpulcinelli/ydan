FROM python:3.12.8-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /requirements.txt

RUN pip install --upgrade pip \
    && pip install wheel \
    && pip install -r /requirements.txt --no-cache-dir

RUN useradd -M scraper

COPY --chmod=755 ./ /app/
RUN mv /app/.pyiceberg.docker.yaml /app/.pyiceberg.yaml

WORKDIR /app

USER scraper

ENTRYPOINT ["python3", "-u", "/app/main.py"]
