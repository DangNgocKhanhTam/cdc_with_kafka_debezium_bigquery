from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer

app = FastAPI()
class MessageRequest(BaseModel):
    msg: str

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.post("/api/test")
def publish(req: MessageRequest):
    producer.produce('codegymtopic', req.msg, callback=delivery_report)
    producer.flush()
    return {"message": "Message sent to Kafka"}