import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

import sys
import os

# Add model_training/ to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'model_training')))

from online_model_evaluation import OnlineEvaluator

@pytest.fixture
def evaluator():
    # Create evaluator with mocked MongoClient
    evaluator = OnlineEvaluator(CONNECTION_STRING="mongodb://fake_uri")
    evaluator.recommendation_collection = MagicMock()
    evaluator.watch_time_collection = MagicMock()
    evaluator.telemetry_collection = MagicMock()
    return evaluator


def test_extract_watch_duration_valid(evaluator):
    assert evaluator.extract_watch_duration("12.mpg") == 720


def test_extract_watch_duration_invalid(evaluator):
    assert evaluator.extract_watch_duration("invalid.mpg") == 0


def test_compute_avg_watch_time(evaluator):
    # Mock recommendation logs
    fake_rec_time = datetime.utcnow() - timedelta(minutes=10)
    evaluator.recommendation_collection.find.return_value.limit.return_value = [
        {
            "user_id": "user1",
            "recommendation_results": ["m1", "m2"],
            "time": fake_rec_time.isoformat()
        }
    ]

    # Mock watch logs
    evaluator.watch_time_collection.find.return_value = [
        {"minute_mpg": "10.mpg"},
        {"minute_mpg": "5.mpg"}
    ]

    avg = evaluator.compute_avg_watch_time()
    assert avg == 450  # 10*60 + 5*60 = 900 / 2


def test_compute_conversion_rate(evaluator):
    fake_rec_time = datetime.utcnow() - timedelta(minutes=10)
    evaluator.recommendation_collection.count_documents.return_value = 2
    evaluator.recommendation_collection.find.return_value.limit.return_value = [
        {
            "user_id": "user1",
            "recommendation_results": ["m1"],
            "time": fake_rec_time.isoformat()
        },
        {
            "user_id": "user2",
            "recommendation_results": ["m2"],
            "time": fake_rec_time.isoformat()
        }
    ]

    # First user watched something, second did not
    def watch_time_side_effect(query, *args, **kwargs):
        if query["user_id"] == "user1":
            return [{"minute_mpg": "5.mpg"}]
        else:
            return []

    evaluator.watch_time_collection.find.side_effect = watch_time_side_effect

    rate = evaluator.compute_conversion_rate()
    assert rate == 0.5


def test_log_online_telemetry(evaluator):
    evaluator.compute_avg_watch_time = MagicMock(return_value=300)
    evaluator.compute_conversion_rate = MagicMock(return_value=0.25)
    evaluator.telemetry_collection.insert_one = MagicMock()

    record = evaluator.log_online_telemetry()
    assert record["average_watch_time_sec"] == 300
    assert record["conversion_rate_percent"] == 25.0
    assert "timestamp" in record
