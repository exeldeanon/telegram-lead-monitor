FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor.py keywords.txt chats.txt ./

CMD ["python", "monitor.py"]
