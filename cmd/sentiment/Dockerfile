FROM python:3.11-slim

WORKDIR /app

COPY cmd/sentiment/requirements-mac.txt /app/
RUN pip install --no-cache-dir -r requirements-mac.txt

COPY internal/utils/queue.py /app/internal/utils/
COPY internal/sentiment/sentiment_worker.py /app/internal/sentiment/
COPY cmd/sentiment/sentiment_worker.py /app/

ENV PYTHONPATH="/app"

CMD ["python", "/app/sentiment_worker.py"]