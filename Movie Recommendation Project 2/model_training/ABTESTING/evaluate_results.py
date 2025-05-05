# ------------------------------------------------------------------------------
# evaluate_results.py
#
# How to Run:
#     Run this script directly using: python3 evaluate_results.py
#
# Purpose:
#     This script connects to the running A/B testing server at http://localhost:8090
#     and calls the `/evaluate` endpoint.
#
#     It fetches online evaluation results, including:
#       - A comparison table of SVD vs KNN model performance
#       - Conversion rate (% of users who watched a recommended movie)
#       - Average watch time per model
#       - Statistical significance via:
#           - Independent t-test (for watch time)
#           - Chi-squared test (for conversion counts)
#
# Requirements:
#     - A/B testing server must be running on port 8090
#     - SVD and KNN model servers must have been called with recommendation requests
#     - MongoDB should have both `request_provenance_log` and `user_watch_data` populated
# ------------------------------------------------------------------------------
import requests
import pandas as pd

# URL to the evaluate endpoint
EVALUATE_URL = "http://localhost:8090/evaluate"

def fetch_and_display_evaluation():
    try:
        response = requests.get(EVALUATE_URL)
        response.raise_for_status()
        data = response.json()

        # Convert summary dictionary to DataFrame
        summary = pd.DataFrame(data["summary"]).T

        # Format columns
        summary["conversion_rate"] = (summary["conversion_rate"] * 100).round(2).astype(str) + "%"
        summary["avg_watch_time"] = summary["avg_watch_time"].astype(int).astype(str) + "s"

        # Reorder columns for display
        display_df = summary[["total_users", "conversions", "conversion_rate", "avg_watch_time"]]
        display_df.index.name = "Model"

        print("\nModel Comparison Table:")
        print(display_df.to_markdown())

        # Print statistical test results
        print("\nT-test Results:")
        print(data["t_test"])

        print("\nChi-Squared Test Results:")
        print(data["chi_square"])

    except requests.exceptions.RequestException as e:
        print("Failed to fetch evaluation results:", e)

if __name__ == "__main__":
    fetch_and_display_evaluation()
