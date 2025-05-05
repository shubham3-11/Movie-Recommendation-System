# group-project-s25-interstellars

group-project-s25-interstellars created by GitHub Classroom

# Instruction to start Kafka Server For Data/Result Fetching:

Remember to first establish SSH channel with the target Kafka server.

Then run the command:
`./startKafkaFetch.sh`

Check the data collected from the Kafka stream and API on target Mongo DB.

# Instruction to start backend server:

To start the server, first go to the `server` folder, then run the command:
`python3 app.py`

# SVD MODEL:
Located in thr **SVD.ipynb**<br>
The following lines need to be changed to the location path of the four files as input:-<br>

**Load and clean data**<br>
movie_data_path = "movie_data.csv"<br>
user_data_path = "user_data.csv"<br>
ratings_data_path = "user_rate_short_data.csv"<br>
watch_data_path = "user_watch_short_data.csv"<br>

(1) movie_data.csv (2) user_data.csv (3)user_rate_short_data (4) user_watch_short_data.csv
They are located in **DATA** in this branch i.e. Shubham and from the Data processing branch<br>

The model trains on three fields 'user_id', 'movie_id', 'rating'<br>
We get this from cleaning **user_rate_short data** and **user_watch_short_data.csv** and combining them into **combined_ratings_df**<br>


![image](https://github.com/user-attachments/assets/1ef257d3-c176-42e5-9436-91a6f5e48001)
![image](https://github.com/user-attachments/assets/d85f3fe8-5933-4813-be84-ab776d8f85ef)

The model entry point is from #step 2 Train SVD Model<br>

The recommendations is given in Step4 by function **get_recommendations**<br>
