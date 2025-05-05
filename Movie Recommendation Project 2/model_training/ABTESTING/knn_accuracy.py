# knn_accuracy_service.py

import numpy as np  
import pandas as pd
from surprise import Dataset, Reader, KNNBasic, accuracy
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

np.random.seed(42)
load_dotenv()

# ---------- MongoDB Configuration ----------
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, server_api=ServerApi("1"))

provenance_coll = client["log_database"]["request_provenance_log"]
ratings_coll = client["movie_database"]["user_rate_data"]

# ---------- Accuracy Evaluation ----------
def evaluate_knn_rmse_all_users(provenance_coll, ratings_coll):
    all_logs = provenance_coll.find({"pipeline_type": "KNN"})
    user_logs = {}

    for log in all_logs:
        uid = str(log["user_id"])
        if uid not in user_logs or log["timestamp"] > user_logs[uid]["timestamp"]:
            user_logs[uid] = log

    ratings = pd.DataFrame(list(ratings_coll.find({}, {"_id": 0})))
    if ratings.empty:
        print("[ERROR] No ratings data found.")
        return

    ratings["user_id"] = ratings["user_id"].astype(str)
    ratings["movie_id"] = ratings["movie_id"].astype(str)
    ratings.rename(columns={"score": "rating"}, inplace=True)

    reader = Reader(rating_scale=(0, 5))
    data = Dataset.load_from_df(ratings[["user_id", "movie_id", "rating"]], reader)
    trainset = data.build_full_trainset()

    sim_options = {
        "name": "cosine",
        "user_based": True
    }
    model = KNNBasic(sim_options=sim_options)
    model.fit(trainset)

    all_predictions = []

    for uid, log in user_logs.items():
        recommended = log.get("recommendation_results", [])
        ground_truth = ratings[(ratings["user_id"] == uid) & (ratings["movie_id"].isin(recommended))]
        if ground_truth.empty:
            continue

        testset = [(row["user_id"], row["movie_id"], row["rating"]) for _, row in ground_truth.iterrows()]
        preds = [model.predict(u, i, r) for u, i, r in testset]
        all_predictions.extend(preds)

    if not all_predictions:
        print("[INFO] No matching predictions for any user.")
        return

    print("\n✅ Overall KNN RMSE across users:")
    accuracy.rmse(all_predictions, verbose=True)

if __name__ == "__main__":
    evaluate_knn_rmse_all_users(provenance_coll, ratings_coll)

