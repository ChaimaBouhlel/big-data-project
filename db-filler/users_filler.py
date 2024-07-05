import random
import pymongo
from datetime import datetime, timedelta

# Establish a connection to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["tweets-db"]  # Replace "mydatabase" with your database name
collection = db["top_users"]  # Replace "Top10" with your collection name

# Read lines from the file and extract top 10 lines
file_path_users = "users_interactions.txt"  # Update with your file path
users_lines = []


with open(file_path_users, "r", encoding="utf-8") as file:
    users_lines = [line.strip().split("\t") for line in file.readlines()]

documents = []

# Create a document to save in MongoDB
for i in range(0, 100):

    # Get 10 lines just before 10*i in users_lines
    latest10_users_lines = users_lines[10*i: 10*i+10]

    users_json_format = []
    counter = 1

    for line in latest10_users_lines:
        number = random.randint(1, 100)
        counter += 1
        document = {
           "timestamp": datetime.now() + timedelta(days=i),
            "total_number_of_users": len(users_lines),
            "hashtag":line[0], 
            "occurrence": int(line[1])+ counter * 6554 + number
        }
        print(counter)
        print(int(line[1])+ counter * 6554 + number)
        collection.insert_one(document)


    # Insert the document into the MongoDB collection
print("Job results saved to MongoDB with timestamp.")
