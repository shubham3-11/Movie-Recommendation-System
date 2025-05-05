from dotenv import load_dotenv
from pymongo import MongoClient
from surprise import KNNBasic, Dataset, Reader, accuracy
from surprise.model_selection import train_test_split
import pandas as pd
import numpy as np
import datetime
import os

# Load environment variables from a .env file if available
load_dotenv()

# Load configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'localhost')
USER_DB = os.getenv('USER_DB', 'user_database')
MOVIE_DB = os.getenv('MOVIE_DB', 'movie_database')

class DB:
    """ 
        This class establishes a connection to MongoDB and provides access to user and movie databases. 
    """
    def __init__(self):
        # mongo DB database and collections setup
        self.client = MongoClient(MONGO_URI)
        self.user_db = self.client[USER_DB]   # user_info
        self.movie_db = self.client[MOVIE_DB] # movie_info, user_watch_data, user_rate_data
class KNNPipeline:
    def __init__(self):
        self.DB = DB()
        self.model = self.train_model()
        self.last_trained = datetime.datetime.now()
        self.model_version = f"knn_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Track model version
        self.data_version = f"data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Track data version

    def get_model_info(self):
        """Return information about the current model"""
        return {
            "model_version": self.model_version,
            "data_version": self.data_version,
            "last_trained": self.last_trained.isoformat() if self.last_trained else None,

        }
        
    def load_clean_data(self):
        df_ratings = pd.DataFrame(list(self.DB.movie_db["user_rate_data"].find())).dropna()
        df_ratings = df_ratings[['user_id', 'movie_id', 'score']]
        df_ratings.columns = ['user_id', 'movie_id', 'rating']
        return df_ratings

    def train_model(self):
        print("Training KNN model...")
        df = self.load_clean_data()
        self.data_version = f"data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(df[['user_id', 'movie_id', 'rating']], reader)
        trainset, test_set = train_test_split(data, test_size=0.2)
        model = KNNBasic()
        model.fit(trainset)
        # prediction = model.predict(test_set)
        # accuracy.rmse(prediction)
        # print(f"Model trained with RMSE: {accuracy.rmse(prediction)}")
        
        print("KNN model trained.")
        return model
    
    def refresh_model(self):
        """Reload data and retrain the model"""
        print("Refreshing SVD model...")
        self.svd_model = self.train_model()
        self.last_trained = datetime.datetime.now()
        self.model_version = f"knn_{self.last_trained.strftime('%Y%m%d_%H%M%S')}"

        print(f"{datetime.datetime.now()} - Model refreshed at {self.last_trained}")
        print(f"{datetime.datetime.now()} - New model version: {self.model_version}")
        print(f"{datetime.datetime.now()} - Data version: {self.data_version}")
        
        return self.svd_model


    def get_recommendations(self, user_id, num_recommendations=20):
        df = self.load_clean_data()
        all_movies = set(df['movie_id'].unique())
        seen_movies = set(df[df['user_id'] == user_id]['movie_id'].unique())
        unseen_movies = list(all_movies - seen_movies)

        predictions = [self.model.predict(user_id, m) for m in unseen_movies]
        top = sorted(predictions, key=lambda x: x.est, reverse=True)[:num_recommendations]
        return [p.iid for p in top]
