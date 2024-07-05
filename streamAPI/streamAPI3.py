from flask import Flask, json, request
from confluent_kafka import Producer

app = Flask(__name__)

p = Producer({'bootstrap.servers': 'localhost:9092'})

@app.route('/tweet', methods=['POST'])
def tweet():
    tweet = request.json
    p.produce('tweets', json.dumps(tweet))  # convert dictionary to string
    p.flush()
    
    print("pushed to kafka successfully")
    return 'Tweet sent to Kafka', 200

if __name__ == '__main__':
    app.run(port=5000)
