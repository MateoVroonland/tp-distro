FROM python:3.11-slim

WORKDIR /app

# COPY cmd/client/requirements.txt /app/
# RUN pip install --no-cache-dir -r requirements.txt

COPY internal/utils/queue.py /app/internal/utils/
COPY internal/utils/communication.py /app/internal/utils/
COPY cmd/client/client.py /app/

ENV PYTHONPATH="/app"

CMD ["python", "/app/client.py"]