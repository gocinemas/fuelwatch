web: python -m gunicorn sms_service:app --bind 0.0.0.0:8080 --worker-class gevent --workers 1 --timeout 30 --keep-alive 5
worker: python brand_worker.py
