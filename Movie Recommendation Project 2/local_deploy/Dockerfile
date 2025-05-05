# first do "ssh -L 9092:localhost:9092 tunnel@128.2.204.215 -NT" on localhost
# docker build -t interstellar-service .
# docker run --rm --network host interstellar-service

FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    bash \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x startService.sh \
    && chmod +x data_processing/startKafkaFetch.sh \
    && chmod +x model_training/Server/startMLServer.sh \
    && chmod +x inference_service/startServer.sh

CMD ["./startService.sh"]
