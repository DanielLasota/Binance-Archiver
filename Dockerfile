FROM python:3.11-slim

#RUN apt-get update && apt-get install -y curl

RUN apt-get update && apt-get install -y \
    curl \
    python3-tk \
    tk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs

VOLUME [ "/app/data", "/app/logs" ]

ENV PYTHONPATH=/app

#EXPOSE 5000

ENV PYTHONUNBUFFERED=1

CMD ["python", "binance_archiver/main_data_sink.py"]
