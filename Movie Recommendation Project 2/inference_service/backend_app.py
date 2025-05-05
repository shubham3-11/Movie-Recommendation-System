import itertools
import threading
import time
from flask import Flask, request, make_response
import pandas as pd
import requests
import json
app = Flask(__name__)
# Load config from config.py
app.config.from_object("config.Config")
host = app.config.get("HOST", "128.2.205.110")
port = app.config.get("PORT", 8082)
ML_SERVERS = [
    # comment this line when local testing!
    # “http://backend-service-1:8083”,
    # “http://backend-service-2:8084”,
    # uncomment this line when local testing!
    "http://128.2.205.110:8083",
    "http://128.2.205.110:8084",
]
# Round-robin iterator for distributing requests
server_pool = itertools.cycle(ML_SERVERS)

# start server for collecting prometheus metrics for model accuracy
from prometheus_client import Gauge, start_http_server
threading.Thread(target=lambda: start_http_server(8766), daemon=True).start()
MODEL_ACCURACY = Gauge("model_accuracy", "Accuracy of the model")
MODEL_ACCURACY.set(0.0)

# TODO: should track client ip in real setting 
def record_request_ip(user_id):
    current_time = time.time()
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    with open("user_requests_log.csv", "a") as f:
        f.write(f"{user_id},{current_time},{ip_address}\n")

@app.route("/")
def home():
    return "Movie Recommendation Service is Running!"
@app.route("/recommend/<int:userid>")
def recommend(userid):
    record_request_ip(userid)
    # find ml_port with probabilities
    ml_url = next(server_pool)
    print(f"redirecting request to ML service: {ml_url}")
    # redirect to ML services
    ml_url = f'{ml_url}/recommend/{userid}'
    try:
        response = requests.get(ml_url, timeout=50)
        if response.status_code == 200:
            try:
                print(f"getting response from ML service {ml_url}")
                responseBody = response.json()
                accuracy = responseBody["accuracy"]
                MODEL_ACCURACY.set(accuracy)
                print(f"Model accuracy: {accuracy}")
                movie_list = responseBody["recommendation_results"]
                movie_string = ",".join(str(m) for m in movie_list)
                return make_response(movie_string, 200)
            except json.JSONDecodeError:
                return make_response({"error": "Invalid JSON from ML service"}, 500)
        else:
            return make_response({"error": f"ML service error: {response.status_code}"}, response.status_code)
    except requests.exceptions.Timeout:
        return make_response({"error": "ML service timed out"}, 504)
    except Exception as e:
        return make_response({"error": f"Request failed: {str(e)}"}, 500)
if __name__ == "__main__":
    app.run(host, port)