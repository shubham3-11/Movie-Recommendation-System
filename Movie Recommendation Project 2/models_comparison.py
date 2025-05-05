import os


from dotenv import load_dotenv
from pymongo import MongoClient
from scipy.stats import ttest_ind, chi2_contingency
import pandas as pd
from datetime import datetime

# ---------- INIT ----------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("LOG_DB", "log_database")
REQUEST_PROVENANCE_LOG_COLL = os.getenv("REQUEST_PROVENANCE_LOG", "request_provenance_log")
MODEL_COMPARISON_LOG_COLL = os.getenv("MODEL_COMPARISON_LOG", "model_comparison_log")
USE_LOGGING = True

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
request_provenance_log_coll = client[DB_NAME][REQUEST_PROVENANCE_LOG_COLL]
model_comparison_log_coll = client[DB_NAME][MODEL_COMPARISON_LOG_COLL]

# ---------- STATISTICAL EVALUATION ----------
def generate_compare_models_get_statistics():
    logs = pd.DataFrame(list(request_provenance_log_coll.find().limit(3000)))
    watch_coll = client["movie_database"]["user_watch_data"]
   
    watch_data = pd.DataFrame(list(
    watch_coll.find({}, {"user_id": 1, "movie_id": 1, "time": 1, "minute_mpg": 1, "_id": 0}).limit(3000)
))
    # Inject matching test watch data for testing
    test_watch_logs = pd.DataFrame([
    {
        "user_id": "204078", 
        "movie_id": "how+to+train+your+dragon+2+2014",
        "time": pd.Timestamp("2025-04-14T02:30:00"),
        "minute_mpg": "42.mpg"
    },
    {
        "user_id": "2427",
        "movie_id": "gladiator+2000",
        "time": pd.Timestamp("2025-04-14T02:20:00"),
        "minute_mpg": "50.mpg"
    },
    {
        "user_id": "194802",
        "movie_id": "tangled+2010",
        "time": pd.Timestamp("2025-04-14T02:40:00"),
        "minute_mpg": "35.mpg"
    }
    ])


   
    watch_data = pd.concat([watch_data, test_watch_logs], ignore_index=True)


    logs['timestamp'] = pd.to_datetime(logs['timestamp'])
    watch_data['time'] = pd.to_datetime(watch_data['time'])
    logs['recommendation_results'] = logs['recommendation_results'].apply(
        lambda x: list(map(str, x)) if isinstance(x, list) else []
    )

    records = []
    for _, log in logs.iterrows():
        # user = log['user_id']
        user = str(log['user_id']) 
        variant = log['pipeline_type']  # "SVD" or "KNN"
        recs = set(log['recommendation_results'])
        ts = log['timestamp']


        watched = watch_data[
            (watch_data['user_id'] == user) &
            (watch_data['movie_id'].isin(recs))&
            (watch_data['time'] >= ts)
        ]

        if not watched.empty:
           
            for _, w in watched.iterrows():
                try:
                    watch_sec = int(w['minute_mpg'].split('.')[0]) * 60
                except:
                    watch_sec = 0
                records.append((user, variant, 1, watch_sec))
        else:
            records.append((user, variant, 0, 0))

    

    df = pd.DataFrame(records, columns=["user_id", "variant", "watched", "watch_time_sec"])

    summary = df.groupby("variant").agg(
        total_users=("user_id", "count"),
        conversions=("watched", "sum"),
        avg_watch_time=("watch_time_sec", "mean")
    )
    summary["conversion_rate"] = summary["conversions"] / summary["total_users"]

    svd_times = df[df["variant"] == "SVD"]["watch_time_sec"]
    knn_times = df[df["variant"] == "KNN"]["watch_time_sec"]
    t_stat, t_pval = ttest_ind(svd_times, knn_times, equal_var=False)

    contingency = pd.crosstab(df["variant"], df["watched"])
    chi2, chi_pval, _, _ = chi2_contingency(contingency)

    result = {
        "timestamp": datetime.utcnow().isoformat(),
        "summary": summary.reset_index().to_dict(orient="records"),
        "t_test": {"t_statistic": round(t_stat, 4), "p_value": round(t_pval, 4)},
        "chi_square": {"chi2_statistic": round(chi2, 4), "p_value": round(chi_pval, 4)}
    }

    model_comparison_log_coll.insert_one(result)

    # ---------- DISPLAY RESULTS ----------
    print("=== Model Comparison Summary ===\n")
    print(summary)

    print("\n\n=== T-Test (Average Watch Time) ===")
    print(f"t-statistic:  {round(t_stat, 4)}")
    print(f"p-value:      {round(t_pval, 4)}")

    print("\n\n=== Chi-Square Test (Conversion Rate) ===")
    print(f"chi2-statistic: {round(chi2, 4)}")
    print(f"p-value:        {round(chi_pval, 4)}")

# ---------- MAIN ----------
if __name__ == "__main__":
    generate_compare_models_get_statistics()
