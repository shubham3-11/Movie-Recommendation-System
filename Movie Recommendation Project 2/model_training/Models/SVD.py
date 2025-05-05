from dotenv import load_dotenv
from pymongo import MongoClient
import numpy as np
import pandas as pd
import ast
import datetime
import os

from surprise import SVD, Dataset, Reader
from surprise.model_selection import train_test_split

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
        
class SVDPipeline:
    """ 
        This class handles data processing and movie recommendation using the SVD algorithm.
        
        It connects to MongoDB, loads and cleans data, and trains a recommendation model 
        that predicts user ratings for unseen movies.
    """
    def __init__(self):
        self.DB = DB()
        self.movies_df = None
        self.users_df = None
        self.ratings_df = None
        self.watch_df = None
        self.combined_ratings_df = None
        self.training_size = 5000
        self.svd_model = self.train_and_save_model()
        self.last_trained = datetime.datetime.now()
        self.model_version = f"svd_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Track model version
        self.data_version = f"data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Track data version
        
    def get_model_info(self):
        """Return information about the current model"""
        return {
            "model_version": self.model_version,
            "data_version": self.data_version,
            "last_trained": self.last_trained.isoformat() if self.last_trained else None,
            "training_size": self.training_size
        }
    
    def fetch_collection_data(self, db, collection_name, days_back=None, filter_field="timestamp"):
        """
        Fetches data from a specified MongoDB collection while keeping '_id' as 'movie_id'.
        """
        collection = db[collection_name]
        
        if days_back is not None and filter_field is not None:
            # Calculate cutoff date
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
            
            # Create query filter
            query = {filter_field: {"$gte": cutoff_date}}
            
            data = list(collection.find(query).limit(self.training_size))
        else:
            data = list(collection.find({}).limit(self.training_size))
                
        df = pd.DataFrame(data)    
        return df

    def clean_user_data(self, user_db):
        """
        Loads and cleans user data from MongoDB (user_database.user_info).
        """
        df = self.fetch_collection_data(user_db, "user_info")
        return df[['age', 'occupation', 'gender']]

    def clean_ratings_data(self, movie_db):
        """
        Loads and cleans ratings data from MongoDB (movie_database.user_rate_data).
        """
        df = self.fetch_collection_data(movie_db, "user_rate_data", 7)

        df['movie_id'] = df['movie_id'].astype(str)

        return df[['user_id', 'movie_id', 'score']]

    def clean_movie_data(self, movie_db):
        """
        Loads and cleans movie data from MongoDB (movie_database.movie_info).
        """
        df = self.fetch_collection_data(movie_db, "movie_info")

        # Ensure movie_id exists; MongoDB might not store it explicitly
        if 'movie_id' not in df.columns:
            df.reset_index(inplace=True)  # Reset index in case movie_id is missing
            df.rename(columns={'index': 'movie_id'}, inplace=True)  # Assign unique ID if missing

        # Convert genres from list of dictionaries to comma-separated string
        if 'genres' in df.columns:
            def extract_genre_names(genre_data):
                """Helper function to extract genre names correctly."""
                if isinstance(genre_data, str):  # Handle stringified JSON
                    try:
                        genre_data = ast.literal_eval(genre_data)
                    except (ValueError, SyntaxError):
                        return ""
                if isinstance(genre_data, list):
                    return ', '.join([genre.get('name', '') for genre in genre_data])
                return ""

            df['genres'] = df['genres'].apply(extract_genre_names)

        # Ensure movie_id is in string format for consistency
        df['movie_id'] = df['movie_id'].astype(str)

        return df[['movie_id', 'adult', 'genres', 'release_date', 'original_language']]

    def clean_watch_data(self, movie_db):
        """
        Loads and cleans watch history data from MongoDB (movie_database.user_watch_data).
        """
        df = self.fetch_collection_data(movie_db, "user_watch_data", 7)

        df['movie_id'] = df['movie_id'].astype(str)

        # Extract numeric watch time from "minute_mpg"
        if 'minute_mpg' in df.columns:
            df['watch_time'] = df['minute_mpg'].str.extract(r'(\d+)').astype(float)
        else:
            raise KeyError("Column 'minute_mpg' is missing from the dataset!")

        # Normalize watch time to a rating scale (1-5) instead of (0-5)
        min_watch = df['watch_time'].min()
        max_watch = df['watch_time'].max()
        df['rating'] = 1.0 + (df['watch_time'] - min_watch) / (max_watch - min_watch) * 4  # Scale between 1 and 5

        return df[['user_id', 'movie_id', 'rating']]
    
    def load_clean_data(self):
        """
        Load the cleaned data from database.
        """
        self.movies_df = self.clean_movie_data(self.DB.movie_db)
        self.users_df = self.clean_user_data(self.DB.user_db)
        self.ratings_df = self.clean_ratings_data(self.DB.movie_db)
        self.watch_df = self.clean_watch_data(self.DB.movie_db)
    
    def train_and_save_model(self):
        """
        Train the SVD model on user rating data and save it to a file.
        """
        print("Start Training...")
        self.load_clean_data()
        self.data_version = f"data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print("Finish Data Collection...")
        # Merge explicit and implicit ratings
        self.ratings_df.rename(columns={'score': 'rating'}, inplace=True)
        self.combined_ratings_df = pd.concat([self.ratings_df, self.watch_df], ignore_index=True)
        self.combined_ratings_df['rating'] = self.combined_ratings_df['rating'].fillna(2.5)

        # Ensure compatibility
        self.combined_ratings_df['user_id'] = self.combined_ratings_df['user_id'].astype(str)
        self.combined_ratings_df['movie_id'] = self.combined_ratings_df['movie_id'].astype(str)

        # Load data into Surprise
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(self.combined_ratings_df[['user_id', 'movie_id', 'rating']], reader)
        trainset, testset = train_test_split(data, test_size=0.2)

        # Train SVD model
        svd_model = SVD()
        svd_model.fit(trainset)

        print("SVD model trained successfully.")
        return svd_model
    
    def refresh_model(self):
        """Reload data and retrain the model"""
        print("Refreshing SVD model...")
        self.svd_model = self.train_and_save_model()
        self.last_trained = datetime.datetime.now()
        self.model_version = f"svd_{self.last_trained.strftime('%Y%m%d_%H%M%S')}"

        print(f"{datetime.datetime.now()} - Model refreshed at {self.last_trained}")
        print(f"{datetime.datetime.now()} - New model version: {self.model_version}")
        print(f"{datetime.datetime.now()} - Data version: {self.data_version}")
        
        return self.svd_model

    def get_recommendations(self, user_id=None, num_recommendations=20):
        """
        Generate movie recommendations for a given user.
        If no user_id is provided, a random user from the dataset is selected.
        """
        # If user id not found, using random user
        if user_id is None or str(user_id) not in self.combined_ratings_df['user_id'].unique():
            user_id = str(np.random.choice(self.combined_ratings_df['user_id'].unique()))

        # Get movies the user has already rated
        rated_movies = set(self.combined_ratings_df[self.combined_ratings_df['user_id'] == user_id]['movie_id'].unique())

        # Get all movie IDs available
        all_movies = set(self.combined_ratings_df['movie_id'].unique())

        # Get movies not rated by user
        movies_to_predict = list(all_movies - rated_movies)

        # if no new movies to recommend for user, show random popular movies
        if len(movies_to_predict) == 0:
            return self.movies_df.sample(num_recommendations)[['movie_id', 'genres']]

        # Predict ratings for unseen movies
        predictions = [self.svd_model.predict(user_id, movie_id) for movie_id in movies_to_predict]
        predictions.sort(key=lambda x: x.est, reverse=True)

        # Get top recommended movies
        top_movies = [pred.iid for pred in predictions[:num_recommendations]]
        recommended_movies = self.movies_df[self.movies_df['movie_id'].isin(top_movies)][['movie_id', 'genres']]

        return recommended_movies['movie_id'].tolist()