import pika
import json
import random
import time

def send_mqtt_message():
    # Establish a connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare an exchange of type 'topic'
    channel.exchange_declare(exchange='mqtt_exchange', exchange_type='topic')

    try:
        while True:
            # Generate a random status
            status = random.randint(0, 6)
            message = json.dumps({"status": status})

            # Publish the message to the exchange with a routing key
            channel.basic_publish(exchange='mqtt_exchange', routing_key='mqtt.status', body=message)

            print(f"Sent message: {message}")

            # Wait for a second before sending the next message
            time.sleep(1*60)

    finally:
        # Close the connection
        connection.close()

if __name__ == "__main__":
    send_mqtt_message()
