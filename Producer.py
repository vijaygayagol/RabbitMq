import pika
import json
from datetime import datetime
from pymongo import MongoClient
from flask import Flask, request, jsonify

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/') # mongodb localhost
db = client['mqtt_db'] # database name(mongodb)
collection = db['mqtt_messages'] # collection(mongodb)

# Flask app setup
app = Flask(__name__)

def callback(ch, method, properties, body):
    message = json.loads(body)
    message['timestamp'] = datetime.utcnow()
    collection.insert_one(message)
    print(f"Received message: {message}")

def start_consumer():
    try:
        connect = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        connection_params = pika.ConnectionParameters(
            host='localhost',  # RabbitMQ server hostname or IP address
            port=5672,  # RabbitMQ default port
            virtual_host='/',  # Virtual host (default is '/')
            credentials=pika.PlainCredentials('Guest', 'Guest')  # Username and password
        )
        channel = connect.channel()

    # Declare the exchange and queue
        channel.exchange_declare(exchange='mqtt_exchange', exchange_type='topic')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        # Bind the queue to the exchange with a routing key
        channel.queue_bind(exchange='mqtt_exchange', queue=queue_name, routing_key='mqtt.status')

        # Start consuming messages
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        print('Waiting for messages...')
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError:
        print("RabbitMQ its not connected.")

@app.route('/count_status', methods=['GET'])
def status_count():
    #for example
    # http://127.0.0.1:5000/count_status?start_time=2024-07-01T00:00:00&end_time=2024-07-31T23:59:59
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    if not start_time or not end_time:
        return jsonify({"error": "Please provide both start_time and end_time"}), 400

    try:
        start_time = datetime.fromisoformat(start_time)
        end_time = datetime.fromisoformat(end_time)
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use ISO format."}), 400

    pipeline = [
        {"$match": {"timestamp": {"$gte": start_time, "$lt": end_time}}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]

    results = list(collection.aggregate(pipeline))
    return jsonify(results)

if __name__ == "__main__":
    import threading
    # Start the MQTT consumer in a separate thread
    threading.Thread(target=start_consumer, daemon=True).start()
    app.run(debug=True)
