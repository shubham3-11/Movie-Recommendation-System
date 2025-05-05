# Required Libraries
# Install with: !pip install pymongo apscheduler
from pymongo import MongoClient
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import time

class OnlineEvaluator:
    def __init__(self, CONNECTION_STRING):
        """
        Initializes MongoDB connection and sets up collections.
        """
        self.client = MongoClient(CONNECTION_STRING)
        self.log_db = self.client["log_database"]
        self.movie_db = self.client["movie_database"]

        # Collections for recommendation logs, user watch data, and telemetry output
        self.recommendation_collection = self.log_db["recommendation_log"]
        self.watch_time_collection = self.movie_db["user_watch_data"]
        self.telemetry_collection = self.log_db["online_evaluation_telemetry"]

    def extract_watch_duration(self, minute_mpg):
        """
        Extracts watch time from 'minute_mpg' (e.g. "12.mpg") and converts to seconds.
        """
        try:
            minutes = int(minute_mpg.split(".")[0])
            return minutes * 60
        except:
            return 0  # Fallback to 0 if parsing fails

    def compute_avg_watch_time(self):
        """
        Computes the average watch time (in seconds) for recommended movies
        watched after the recommendation was made.
        """
        total_watch_time = 0
        total_users = 0

        # Limit to 1000 recent recommendation logs to reduce load
        recommendation_logs = self.recommendation_collection.find().limit(1000)

        for rec in recommendation_logs:
            user_id = rec.get("user_id")
            recommended_movies = rec.get("recommendation_results", [])
            rec_timestamp = rec.get("time")

            # Convert string timestamps to datetime
            if isinstance(rec_timestamp, str):
                try:
                    rec_timestamp = datetime.fromisoformat(rec_timestamp.replace("Z", "+00:00"))
                except ValueError:
                    continue  # Skip invalid timestamps

            # Find movies watched by the user after receiving the recommendation
            watch_logs = list(self.watch_time_collection.find(
                {
                    "user_id": user_id,
                    "movie_id": {"$in": recommended_movies},
                    "time": {"$gte": rec_timestamp}
                },
                {"minute_mpg": 1}
            ))

            for watch in watch_logs:
                watch_duration = self.extract_watch_duration(watch["minute_mpg"])
                total_watch_time += watch_duration
                total_users += 1

        # Avoid division by zero
        avg_watch_time = total_watch_time / total_users if total_users > 0 else 0
        print(f"Average Watch Time AFTER Recommendation: {avg_watch_time:.2f} seconds")
        return avg_watch_time

    def compute_conversion_rate(self):
        """
        Calculates what percentage of users watched at least one
        recommended movie after receiving the recommendation.
        """
        total_recommendations = self.recommendation_collection.count_documents({})
        users_who_watched = set()  # Store unique user IDs

        for rec in self.recommendation_collection.find().limit(1000):
            user_id = rec.get("user_id")
            recommended_movies = rec.get("recommendation_results", [])
            rec_timestamp = rec.get("time")

            # Convert timestamp if stored as string
            if isinstance(rec_timestamp, str):
                try:
                    rec_timestamp = datetime.fromisoformat(rec_timestamp.replace("Z", "+00:00"))
                except Exception as e:
                    print(f"Skipping user {user_id} due to invalid time format: {e}")
                    continue

            # Check if user watched any recommended movie after the rec
            watch_logs = list(self.watch_time_collection.find({
                "user_id": user_id,
                "movie_id": {"$in": recommended_movies},
                "time": {"$gte": rec_timestamp}
            }))

            if len(watch_logs) > 0:
                users_who_watched.add(user_id)

        # Calculate conversion rate as a percentage
        conversion_rate = len(users_who_watched) / total_recommendations if total_recommendations > 0 else 0
        print(f"📈 Watch Time Conversion Rate AFTER Recommendation: {conversion_rate:.2%}")
        return conversion_rate

    def log_online_telemetry(self):
        """
        Calculates both metrics and stores them as a telemetry record in MongoDB.
        """
        avg_watch_time = self.compute_avg_watch_time()
        conversion_rate = self.compute_conversion_rate()

        telemetry_record = {
            "timestamp": datetime.utcnow(),
            "average_watch_time_sec": avg_watch_time,
            "conversion_rate_percent": conversion_rate * 100,
            "note": "Online telemetry snapshot"
        }

        self.telemetry_collection.insert_one(telemetry_record)
        print("✅ Telemetry logged:", telemetry_record)
        return telemetry_record

    def start_scheduler(self, interval_hours=1):
        """
        Starts a background scheduler to automatically log telemetry
        every `interval_hours` (default = 1 hour).
        """
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.log_online_telemetry, 'interval', hours=interval_hours)
        scheduler.start()
        print(f"🕒 Scheduler started: logging telemetry every {interval_hours} hour(s)")

        # Keep script running to allow scheduler to run in background
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            print("🛑 Scheduler stopped.")
