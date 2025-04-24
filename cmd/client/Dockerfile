FROM python:3.11-slim

WORKDIR /app

COPY internal/utils/queue.py /app/internal/utils/
COPY internal/utils/communication.py /app/internal/utils/
COPY cmd/client/client.py /app/

ENV PYTHONPATH="/app"

CMD ["python", "/app/client.py"]