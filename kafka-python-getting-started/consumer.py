from confluent_kafka import Consumer, KafkaError
import json

def consume_messages():
    conf = {
        'bootstrap.servers': "localhost:9092",
        'group.id': "my-consumer-group",
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(**conf)
    consumer.subscribe(['topic.order.confirm'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break


            message_json = json.loads(msg.value().decode('utf-8'))
            first_name = message_json.get('first_name', '')
            last_name = message_json.get('last_name', '')
            email_message = f"Received message:\nFirst Name: {first_name}\nLast Name: {last_name}\n"
            email_id = first_name+'.'+last_name+'@gmail.com'
            print(email_id)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()

