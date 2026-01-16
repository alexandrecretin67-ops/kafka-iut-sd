from kafka import KafkaProducer
import time
import json

# Define the Kafka broker and topic
broker = 'my-kafka.e4951u-dev.svc.cluster.local:9092'
topic = 'my-first-topic'

# Create a Kafka producer
producer = KafkaProducer(
     bootstrap_servers=[broker],
     sasl_mechanism='SCRAM-SHA-256',
     security_protocol='SASL_PLAINTEXT',
     sasl_plain_username='user1',
     sasl_plain_password='mmJtDlUI4c',
     value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
#producer = KafkaProducer(
#    bootstrap_servers=[broker],
#    value_serializer=lambda v: json.dumps(v).encode('utf-8')
#)

i = 0
while True:
    # Define the message to send
    message = {
        'key': 'id',
        'value': i,
        'timestamp' : time.time()
    }
    # Send the message to the Kafka topic
    producer.send(topic, value=message)
    i += 1
    time.sleep(3)


# Ensure all messages are sent before closing the producer
producer.flush()

print(f"Message sent to topic {topic}")