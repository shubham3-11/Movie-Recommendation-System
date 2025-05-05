import sys
import os
from flask import Flask, jsonify, make_response
from dotenv import load_dotenv
import threading
import time
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Models.SVD import SVDPipeline
from ABTESTING.svd_accuracy import evaluate_svd_rmse_all_users
app = Flask(__name__)
model_lock = threading.RLock()
# ---------- RETRAINING CONFIGURATION ----------
RETRAINING_INTERVAL_DAYS = 3  # Retrain every 3 days
test_interval = 2  # Test interval in minutes
MODEL_HISTORY_FILE = "model_history.json"
# ---------- CONFIG ----------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
LOG_DB = os.getenv("LOG_DB", "log_database")
MOVIE_DB = os.getenv("MOVIE_DB", "movie_database")
REQUEST_PROVENANCE_LOG_COLL = os.getenv("REQUEST_PROVENANCE_LOG", "request_provenance_log")
USER_RATE_COLLECTION = os.getenv("USER_RATE_COLLECTION", "user_rate_data")
client = MongoClient(MONGO_URI)
request_provenance_log_coll = client[LOG_DB][REQUEST_PROVENANCE_LOG_COLL]
ratings_coll = client[MOVIE_DB][USER_RATE_COLLECTION]
# ---------- MODEL PROVENANCE TRACKING ----------
# for any past recommendation, log the model version, the used pipeline version, and the used training data version
def log_request_provenance_to_mongo(request_start_time, user_id, pipeline_type, model_version, training_data_version, recommendations):
    request_provenance_json = {
        "timestamp": request_start_time,
        "user_id": user_id,
        "pipeline_type": pipeline_type,
        "model_version": model_version,
        "training_data_version": training_data_version,
        "recommendation_results": recommendations,
        "status_code": 200
    }
    print(f"logging to mongo {MONGO_URI}: {request_provenance_json}")
    request_provenance_log_coll.insert_one(request_provenance_json)
    print("Inserted to mongo successfully")
# TODO: replace this with save model log to DB instead of json file
def save_model_history(model_info):
    """Save model version information to history file"""
    try:
        # Load existing history if available
        if os.path.exists(MODEL_HISTORY_FILE):
            with open(MODEL_HISTORY_FILE, 'r') as f:
                history = json.load(f)
        else:
            history = {"models": []}
        # Add new model info
        history["models"].append({
            "model_version": model_info["model_version"],
            "data_version": model_info["data_version"],
            "last_trained": model_info["last_trained"],
            "timestamp": datetime.now().isoformat()
        })
        # Save updated history
        with open(MODEL_HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
        print(f"{datetime.now()} - Saved model version {model_info['model_version']} to history")
    except Exception as e:
        print(f"{datetime.now()} - Error saving model history: {e}")
def initialize_model():
    """Initialize the model when the server starts"""
    global svd_pipeline
    with model_lock:
        print(f"{datetime.now()} - Initializing SVD model")
        svd_pipeline = SVDPipeline()
        # Save initial model info to history
        model_info = svd_pipeline.get_model_info()
        save_model_history(model_info)
        print(f"{datetime.now()} - Model initialization complete - Version: {model_info['model_version']}")
# Initialize model on startup
initialize_model()
def periodic_model_retraining():
    """Background thread to periodically retrain the model"""
    global svd_pipeline
    while True:
        try:
            # Sleep until next scheduled retraining
            # Initially sleep for a short time to test
            time.sleep(60)  # 1 minute initial delay for testing
            print("Checking if model retraining is due")
            with model_lock:
                # Check if it’s time to retrain (last_trained is None or older than RETRAINING_INTERVAL_DAYS)
                if (svd_pipeline.last_trained is None or
                    datetime.now() - svd_pipeline.last_trained > timedelta(minutes=test_interval)):
                    print("Starting model retraining")
                    try:
                        # Save old model version for comparison
                        old_version = svd_pipeline.model_version
                        # Retrain model
                        svd_pipeline.refresh_model()
                        # Save new model info to history
                        model_info = svd_pipeline.get_model_info()
                        save_model_history(model_info)
                        print(f"Model successfully retrained at {svd_pipeline.last_trained}")
                        print(f"{datetime.now()} - Old version: {old_version} -> New version: {model_info['model_version']}")
                    except Exception as e:
                        print(f"Error retraining model: {e}")
            # Sleep for the retraining interval (converted to seconds)
            # For production, use this sleep instead of the 1-minute test delay
            time.sleep(60)
            # time.sleep(RETRAINING_INTERVAL_DAYS * 24 * 60 * 60)
        except Exception as e:
            print(f"Error in model retraining thread: {e}")
            # Sleep for an hour before retrying after an error
            time.sleep(60)
@app.route("/")
def home():
    return "ML Service is Running!"
@app.route("/recommend/<int:userid>")
def recommend(userid):
    # TODO call SVD Model here and return result
    global svd_pipeline
    try:
        with model_lock:
            request_start_time = datetime.now(timezone.utc)
            recommendations = svd_pipeline.get_recommendations(userid)
            # TODO: calculate model_accuracy
            # model_accuracy = evaluate_svd_rmse_all_users(request_provenance_log_coll, ratings_coll)
            # print(f”Model accuracy: {model_accuracy}“)
            # Log the request provenance to MongoDB
            log_request_provenance_to_mongo(request_start_time, userid, "SVD", svd_pipeline.model_version, svd_pipeline.data_version, recommendations)
            print(f"{datetime.now()} - Generated recommendations for user {userid} - Model version {svd_pipeline.model_version}, Data version {svd_pipeline.data_version}")
        response_body = {
            "recommendation_results": recommendations,
            "accuracy": 0.1
        }
        response_obj = make_response(jsonify(response_body))
        response_obj.status_code = 200
        return response_obj
    except Exception as e:
        print(f"{datetime.now()} - Error generating recommendations: {e}")
        fail_response_obj = make_response(
            {"error": f"Request failed: {str(e)}"}
        )
        fail_response_obj.status_code = 500
        return fail_response_obj
if __name__ == "__main__":
    # Load config from config.py
    retraining_thread = threading.Thread(target=periodic_model_retraining, daemon=True)
    retraining_thread.start()
    app.config.from_object("config.Config")
    host = app.config.get("HOST", "128.2.205.110")
    port = app.config.get("PORT", 8083)
    debug = app.config.get("DEBUG", False)
    app.run(host, port, debug, threaded=True)
