# Remo API — Dockerfile
# pyquotex requires Python >=3.12, so we pin Python explicitly rather
# than relying on a Playwright base image's bundled interpreter version
# (those track Ubuntu codenames, not Python versions — jammy ships 3.10,
# noble ships 3.12 — and that mismatch is what broke the last build).

FROM python:3.12-slim

WORKDIR /app

# System deps Chromium needs to run headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installs Chromium itself plus its OS-level dependencies
RUN playwright install --with-deps chromium

# Flat layout: main.py, auth.py, quotex_client.py sit at repo root
COPY *.py ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
