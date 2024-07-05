import random
import pymongo
from datetime import datetime, timedelta

# Establish a connection to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["tweets-db"]  # Replace "mydatabase" with your database name
collection = db["job_results"]  # Replace "Top10" with your collection name

# Read lines from the file and extract top 10 lines
file_path_users = "users_interactions.txt"  # Update with your file path
file_path_hashtags = "hashtags.txt"  # Update with your file path
users_lines = []
hashtags_lines = []

with open(file_path_users, "r") as file:
    users_lines = [line.strip().split("\t") for line in file.readlines()]


with open(file_path_hashtags, "r", encoding="utf-8") as file:
    hashtags_lines = [line.strip().split("\t") for line in file.readlines()]



# Create a document to save in MongoDB
for i in range(1, 100):

    # Get 10 lines starting from 10*i in users_lines
    top10_users_lines = users_lines[10*i:10*i+10]

    # Get 10 lines just before 10*i in hashtags_lines
    latest10_hashtags_lines = hashtags_lines[max(0, -10*(i+1)):-10*i]

    users_json_format = []
    hashtags_json_format = []
    counter = 10
    for line in top10_users_lines:
        number = random.randint(1, 100)
        users_json_format.append({"username":line[0], "interactions": int(line[1])+ counter * 5636 + number})
        counter -= 1
        
    counter = 1
    for line in latest10_hashtags_lines:
        number = random.randint(1, 100)
        hashtags_json_format.append({"hashtag":line[0], "occurrence": int(line[1])+ counter * 6554 + number})
        counter += 1

    document = {
        "timestamp": datetime.now() + timedelta(hours=i),
        "top_10_trending_users": users_json_format,
        "total_number_of_users": len(users_lines),  
        "top_10_trending_hashtags": hashtags_json_format,
        "total_number_of_hashtags": len(hashtags_lines)
    }

    # Insert the document into the MongoDB collection
    collection.insert_one(document)

print("Job results saved to MongoDB with timestamp.")
