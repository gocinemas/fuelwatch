web: python -m gunicorn sms_service:app --bind 0.0.0.0:8080 --workers 1 --worker-class sync --timeout 30
worker: python brand_worker.py
