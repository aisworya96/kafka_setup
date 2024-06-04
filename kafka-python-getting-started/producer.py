from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages():
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(**conf)

    for i in range(10):
        first_name = input(f"Enter first name for message {i}: ")
        last_name = input(f"Enter last name for message {i}: ")

        message_data = {
            'first_name': first_name,
            'last_name': last_name

        }

        json_message = json.dumps(message_data)

        producer.produce('topic.order.confirm', json_message.encode('utf-8'), callback=delivery_report)
        producer.poll(0.5)


    producer.flush()

if __name__ == "__main__":
    produce_messages()
