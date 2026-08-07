FROM python:3.13-slim

# Install system dependencies including tesseract-ocr
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "--worker-class", "gevent", "--workers", "4", "--bind", "0.0.0.0:5000", "--timeout", "120", "sms_service:app"]
