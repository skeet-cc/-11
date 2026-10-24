import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

app = FastAPI(title="Kafka REST API", version="1.0.0")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "messages-topic-kochetov"

class MessageSchema(BaseModel):
    message: str
    topic: str = DEFAULT_TOPIC

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except NoBrokersAvailable:
        raise HTTPException(status_code=503, detail="Kafka broker is not available")

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Kafka API"}

@app.post("/produce")
def produce_message(payload: MessageSchema):
    producer = get_producer()
    try:
        producer.send(payload.topic, {"text": payload.message})
        producer.flush()
        return {"status": "sent", "topic": payload.topic, "message": payload.message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        producer.close()

@app.get("/consume")
def consume_messages(topic: str = DEFAULT_TOPIC, max_messages: int = 5):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        
        messages = []
        for msg in consumer:
            messages.append({
                "partition": msg.partition,
                "offset": msg.offset,
                "value": msg.value
            })
            if len(messages) >= max_messages:
                break
        
        consumer.close()
        return {"topic": topic, "count": len(messages), "messages": messages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
