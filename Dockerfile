FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# pyquotex library (copy the whole package folder into the image)
COPY pyquotex ./pyquotex

COPY remo_api.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["uvicorn", "remo_api:app", "--host", "0.0.0.0", "--port", "8000"]
