from confluent_kafka import Consumer

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['codegymtopic'])
print("Consumer is listening on topic 'codegymtopic'...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
    else:
        print(f"Received message: {msg.value().decode('utf-8')}")