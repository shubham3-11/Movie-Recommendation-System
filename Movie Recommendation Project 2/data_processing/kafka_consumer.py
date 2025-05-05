import os
import logging
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaConsumer
import requests
from dotenv import load_dotenv
import sys
from prometheus_client import Counter, Histogram, start_http_server

# Load environment variables from a .env file if available
load_dotenv()

# Method to load configuration
def load_config():
    IP = os.getenv('API_IP', 'localhost')

    USER_API_URL = os.getenv('USER_API_URL', f'http://{IP}:8080/user')
    MOVIE_API_URL = os.getenv('MOVIE_API_URL', f'http://{IP}:8080/movie')

    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    USER_DB = os.getenv('USER_DB', 'user_database')
    MOVIE_DB = os.getenv('MOVIE_DB', 'movie_database')
    LOG_DB = os.getenv('LOG_DB', 'log_database')

    USER_RATE_COLLECTION = os.getenv('USER_RATE_COLLECTION', 'user_rate_data')
    USER_WATCH_COLLECTION = os.getenv('USER_WATCH_COLLECTION', 'user_watch_data')
    USER_DATA_COLLECTION = os.getenv('USER_DATA_COLLECTION', 'user_info')
    MOVIE_DATA_COLLECTION = os.getenv('MOVIE_DATA_COLLECTION', 'movie_info')
    RECOMMENDATION_LOG = os.getenv('RECOMMENDATION_LOG', 'recommendation_log')

    return {
        "USER_API_URL": USER_API_URL,
        "MOVIE_API_URL": MOVIE_API_URL,
        "MONGO_URI": MONGO_URI,
        "USER_DB": USER_DB,
        "MOVIE_DB": MOVIE_DB,
        "LOG_DB": LOG_DB,
        "USER_RATE_COLLECTION": USER_RATE_COLLECTION,
        "USER_WATCH_COLLECTION": USER_WATCH_COLLECTION,
        "USER_DATA_COLLECTION": USER_DATA_COLLECTION,
        "MOVIE_DATA_COLLECTION": MOVIE_DATA_COLLECTION,
        "RECOMMENDATION_LOG": RECOMMENDATION_LOG
    }

# Load config
config = load_config()

# start server for collecting prometheus metrics
start_http_server(8765)

# Metrics to show requests count and request latency
REQUEST_COUNT = Counter(
    'request_count', 'Recommendation Request Count',
    ['http_status']
)

REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency')

class DB:
    def __init__(self):
        # mongo DB database and collections setup
        self.client = MongoClient(config['MONGO_URI'])
        self.user_db = self.client[config['USER_DB']]  # user_info
        self.movie_db = self.client[config['MOVIE_DB']]  # movie_info, user_watch_data, user_rate_data
        self.log_db = self.client[config['LOG_DB']]

        # MongoDB collections
        self.user_info = self.user_db[config['USER_DATA_COLLECTION']]
        self.movie_info = self.movie_db[config['MOVIE_DATA_COLLECTION']]
        self.user_rate = self.movie_db[config['USER_RATE_COLLECTION']]
        self.user_watch = self.movie_db[config['USER_WATCH_COLLECTION']]
        self.recommendation_log = self.log_db[config['RECOMMENDATION_LOG']]

# Kafka consumer class
class KafkaConsumerApp:
    def __init__(self, topic, bootstrap_servers):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',  # Start from the latest message
            enable_auto_commit=True,     # Enable auto-commit of offsets
            auto_commit_interval_ms=1000  # How often to tell Kafka that an offset has been read
        )
        
        self.DB = DB()
        print(f'Kafka Consumer initialized for topic: {topic}')

    def process_user_info(self, user_id):
        try:
            response = requests.get(f'{config["USER_API_URL"]}/{user_id}')
            if response.status_code == 200:
                api_data = response.json()
                age = int(api_data["age"])
                occupation = api_data["occupation"]
                gender = api_data["gender"]
                
                # Add both the event time and a collection timestamp (current date)
                collection_date = datetime.now()
                
                user_data = {
                    "timestamp": collection_date,
                    "user_id": user_id,
                    "age": age,
                    "occupation": occupation,
                    "gender": gender
                }
                self.DB.user_info.update_one(
                    {"user_id": user_id},  # Find the user by user_id
                    {"$set": user_data},  # Update the user data if found
                    upsert=True  # If no match is found, insert the document
                )
            else:
                logging.error(f'API fetch failed: {response.status_code}')
        except Exception as e:
            logging.error(f'Error inserting user info to DB: {e}')

    def process_movie_info(self, movie_id):
        try:
            response = requests.get(f'{config["MOVIE_API_URL"]}/{movie_id}')
            if response.status_code == 200:
                api_data = response.json()
                adult = api_data["adult"]
                genres = str(api_data["genres"])
                release_date = api_data["release_date"]
                release_date = datetime.strptime(release_date, "%Y-%m-%d") if release_date else "unknown"
                original_language = api_data["original_language"]
                
                # Add both the event time and a collection timestamp (current date)
                collection_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                
                movie_data = {
                    "timestamp": collection_date,
                    "movie_id": movie_id,
                    "adult": adult,
                    "genres": genres,
                    "release_date": release_date,
                    "original_language": original_language
                }
                self.DB.movie_info.update_one(
                    {"movie_id": movie_id},  # Find the movie by movie_id
                    {"$set": movie_data},  # Update the movie data if found
                    upsert=True  # If no match is found, insert the document
                )
                
            else:
                logging.error(f'API fetch failed: {response.status_code}')
        except Exception as e:
            logging.error(f'Error inserting movie info to DB: {e}')

    def process_user_rate(self, rate_msg):
        try:
            time, user_id, other = rate_msg.split(',')
            movie_id, score = other[10:].split('=')
            
            self.process_user_info(user_id)
            self.process_movie_info(movie_id)

            cleaned_time = ''.join(c if c.isnumeric() or c in "-T:" else '' for c in time)
            if len(cleaned_time) == 16:  # 'YYYY-MM-DDTHH:MM' format
                cleaned_time += ":00"  # Automatically add ":00" for seconds
                
            # Add both the event time and a collection timestamp (current date)
            collection_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            user_rate_data = {
                "timestamp": collection_date,
                "time": datetime.strptime(cleaned_time, '%Y-%m-%dT%H:%M:%S'),
                "user_id": user_id,
                "movie_id": movie_id,
                "score": float(score)
            }
            self.DB.user_rate.insert_one(user_rate_data)
        except Exception as e:
            logging.error(f'Error inserting to user rate DB: {e}')
    
    def process_user_watch_history(self, watch_msg):
        try:
            time, user_id, other = watch_msg.split(',')
            movie_id, minute_mpg = other[12:].split('/')
            
            self.process_user_info(user_id)
            self.process_movie_info(movie_id)
                
            cleaned_time = ''.join(c if c.isnumeric() or c in "-T:" else '' for c in time)
            if len(cleaned_time) == 16:  # 'YYYY-MM-DDTHH:MM' format
                cleaned_time += ":00"  # Automatically add ":00" for seconds
            
            # Add both the event time and a collection timestamp (current date)
            collection_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            user_watch_data = {
                "timestamp": collection_date,
                "time": datetime.strptime(cleaned_time, '%Y-%m-%dT%H:%M:%S'),
                "user_id": user_id,
                "movie_id": movie_id,
                "minute_mpg": minute_mpg
            }
            self.DB.user_watch.insert_one(user_watch_data)
        except Exception as e:
            logging.error(f'Error inserting to user watch DB: {e}')
    
    def process_recommendation_result(self, recommendation_msg):
        try:
            recommendationList = recommendation_msg.split(',')
            time = recommendationList[0].strip()
            user_id = recommendationList[1].strip()
            status_code = int(recommendationList[3].replace('status', '').strip())
            
            # Collect the request count and request latency metrics
            REQUEST_COUNT.labels(http_status=status_code).inc()
            
            if status_code != 200:
                logging.error(f'Recommendation response failed: {recommendationList}')
                return
            
            recommendation_results = recommendationList[4:24]
            recommendation_results[0] = recommendation_results[0].replace('result:', '').strip()
            response_time = recommendationList[-1].replace('ms', '').strip()
            
            # Collect the request latency metrics
            REQUEST_LATENCY.observe(int(response_time) / 1000)

            cleaned_time = ''.join(c if c.isnumeric() or c in "-T:" else '' for c in time)
            if len(cleaned_time) >= 19:
                cleaned_time = cleaned_time[:19]
                
            recommendation_log = {
                "time": datetime.strptime(cleaned_time, '%Y-%m-%dT%H:%M:%S'),
                "user_id": user_id,
                "status_code": status_code,
                "recommendation_results": recommendation_results,
                "response_time": int(response_time)
            }
            
            self.DB.recommendation_log.insert_one(recommendation_log)
        except Exception as e:
            logging.error(f'Error inserting to recommendation log DB: {e}')

    def consume_messages(self):
        try:
            for message in self.consumer:
                message_str = message.value.decode("utf-8")
                if 'GET /data/m/' in message_str:
                    self.process_user_watch_history(message_str)
                elif 'GET /rate/' in message_str:
                    self.process_user_rate(message_str)
                elif 'recommendation request' in message_str:
                    self.process_recommendation_result(message_str)
        except ConnectionError as e:
            logging.error(f"Kafka connection failed: {e}")
            self.consumer.close()  # Close the Kafka consumer connection
            sys.exit(1)

if __name__ == "__main__":
    topic = 'movielog8'  # Kafka topic
    bootstrap_servers = ['localhost:9092']

    consumer_app = KafkaConsumerApp(topic, bootstrap_servers)
    consumer_app.consume_messages()