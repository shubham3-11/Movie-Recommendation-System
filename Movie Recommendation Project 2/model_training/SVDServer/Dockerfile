FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install flask pandas numpy scikit-learn

RUN chmod +x startMLServer.sh
CMD ["./startMLServer.sh"]
