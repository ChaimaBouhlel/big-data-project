import random
import pymongo
from datetime import datetime, timedelta

# Establish a connection to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["tweets-db"]  # Replace "mydatabase" with your database name
collection = db["top_hashtags"]  # Replace "Top10" with your collection name

# Read lines from the file and extract top 10 lines
file_path_hashtags = "hashtags.txt"  # Update with your file path
hashtags_lines = []


with open(file_path_hashtags, "r", encoding="utf-8") as file:
    hashtags_lines = [line.strip().split("\t") for line in file.readlines()]

documents = []

# Create a document to save in MongoDB
for i in range(0, 100):

    # Get 10 lines just before 10*i in hashtags_lines
    latest10_hashtags_lines = hashtags_lines[-10*(i+1):-10*i]

    hashtags_json_format = []
    counter = 1

    for line in latest10_hashtags_lines:
        number = random.randint(1, 100)
        counter += 1
        document = {
           "timestamp": datetime.now() + timedelta(days=i),
            "total_number_of_hashtags": len(hashtags_lines),
            "hashtag":line[0], 
            "occurrence": int(line[1])+ counter * 6554 + number
        }
        print(counter)
        print(int(line[1])+ counter * 6554 + number)
        collection.insert_one(document)


    # Insert the document into the MongoDB collection
print("Job results saved to MongoDB with timestamp.")
