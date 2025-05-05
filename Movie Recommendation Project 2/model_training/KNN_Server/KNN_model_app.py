from flask import Flask, make_response
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Models.KNN import KNNPipeline
from ABTESTING.knn_accuracy import evaluate_knn_rmse_all_users
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import threading
import json
import time

app = Flask(__name__)

model_lock = threading.RLock()

# ---------- CONFIG ----------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("LOG_DB", "log_database")
REQUEST_PROVENANCE_LOG_COLL = os.getenv("REQUEST_PROVENANCE_LOG", "request_provenance_log")
client = MongoClient(MONGO_URI)
request_provenance_log_coll = client[DB_NAME][REQUEST_PROVENANCE_LOG_COLL]
ratings_coll = client["movie_database"]["user_rate_data"]

# ---------- RETRAINING CONFIGURATION ----------
RETRAINING_INTERVAL_DAYS = 3  # Retrain every 3 days
test_interval = 2  # Test interval in minutes
MODEL_HISTORY_FILE = "knn_model_history.json"

# ---------- MODEL PROVENANCE TRACKING ----------
# TODO: add model version, pipeline version, training data version
def log_request_provenance_to_mongo(request_start_time, user_id, pipeline_type, model_version, training_data_version, model_accuracy, recommendations):
    request_provenance_json = {
        "timestamp": request_start_time,
        "user_id": user_id,
        "pipeline_type": pipeline_type,
        "model_version": model_version,
        "training_data_version": training_data_version,
        "model_accuracy": model_accuracy,
        "recommendation_results": recommendations,
        "status_code": 200
    }
    print(f"logging to mongo {MONGO_URI}: {request_provenance_json}")
    request_provenance_log_coll.insert_one(request_provenance_json)
    
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
            
        print(f"{datetime.now()} - Saved KNN model version {model_info['model_version']} to history")
    except Exception as e:
        print(f"{datetime.now()} - Error saving KNN model history: {e}")

def initialize_model():
    """Initialize the model when the server starts"""
    global knn_pipeline
    with model_lock:
        print(f"{datetime.now()} - Initializing KNN model")
        knn_pipeline = KNNPipeline()
        
        # Save initial model info to history
        model_info = knn_pipeline.get_model_info()
        save_model_history(model_info)
        
        print(f"{datetime.now()} - KNN model initialization complete - Version: {model_info['model_version']}")

# Initialize model on startup
initialize_model()

def periodic_model_retraining():
    """Background thread to periodically retrain the model"""
    global knn_pipeline
    
    while True:
        try:
            # Sleep until next scheduled retraining
            # Initially sleep for a short time to test
            time.sleep(60)  # 1 minute initial delay for testing
            
            print(f"{datetime.now()} - Checking if KNN model retraining is due")
            
            with model_lock:
                # Check if it's time to retrain (last_trained is None or older than RETRAINING_INTERVAL_DAYS)
                if (knn_pipeline.last_trained is None or 
                    datetime.now() - knn_pipeline.last_trained > timedelta(minutes=test_interval)):
                    
                    print(f"{datetime.now()} - Starting KNN model retraining")
                    try:
                        # Save old model version for comparison
                        old_version = knn_pipeline.model_version
                        # Retrain model
                        knn_pipeline.refresh_model()
                        # Save new model info to history
                        model_info = knn_pipeline.get_model_info()
                        save_model_history(model_info)
                        print(f"{datetime.now()} - KNN model successfully retrained at {knn_pipeline.last_trained}")
                        print(f"{datetime.now()} - Old version: {old_version} -> New version: {model_info['model_version']}")
                    except Exception as e:
                        print(f"{datetime.now()} - Error retraining KNN model: {e}")
            
            # Sleep for the retraining interval (converted to seconds)
            # For production, use this sleep instead of the 1-minute test delay
            time.sleep(60)
            # time.sleep(RETRAINING_INTERVAL_DAYS * 24 * 60 * 60)
            
        except Exception as e:
            print(f"{datetime.now()} - Error in KNN model retraining thread: {e}")
            # Sleep for an hour before retrying after an error
            time.sleep(60)

@app.route("/")
def home():
    return "KNN Model Server Running"

@app.route("/recommend/<int:userid>")
def recommend(userid):
    
    try:
        request_start_time = datetime.now(timezone.utc)
        # TODO: implement model update logics for KNN
        print(f"Received recommendation request for user {userid}")
        recommendations = knn_pipeline.get_recommendations(str(userid))
        # TODO: record model_accuracy, model version, data info to the response as well
        model_accuracy = None #evaluate_knn_rmse_all_users(request_provenance_log_coll, ratings_coll)
        # Log the request provenance to MongoDB
        log_request_provenance_to_mongo(request_start_time, userid, "KNN", "knn version constant 1", "knn training data constant 1", model_accuracy, recommendations)
        
        # TODO: calculate model_accuracy
        return make_response(recommendations, 200)
    except Exception as e:
        return make_response({"error": str(e)}, 500)

if __name__ == "__main__":
    # Load config from config.py
    retraining_thread = threading.Thread(target=periodic_model_retraining, daemon=True)
    retraining_thread.start()

    app.config.from_object("config.Config")
    
    host = app.config.get("HOST", "128.2.205.110")
    port = app.config.get("PORT", 8084)
    debug = app.config.get("DEBUG", False)
    app.run(host, port, debug)