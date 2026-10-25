FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor.py keywords.txt chats.txt ./
COPY miniapp ./miniapp/

# Bothost keeps /app/data between deployments on persistent-storage plans.
RUN mkdir -p /app/data
ENV WEBAPP_HOST=0.0.0.0 \
    WEBAPP_PORT=8080 \
    STATE_FILE=/app/data/state.json \
    REGISTRY_FILE=/app/data/lead_registry.json \
    SESSION_NAME=/app/data/monitor_session

EXPOSE 8080

CMD ["python", "monitor.py"]
