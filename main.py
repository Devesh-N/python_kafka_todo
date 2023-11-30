from flask import Flask, request
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

@app.route('/send_email', methods=['POST'])
def send_email():
    # Extract email from POST request
    data = request.json
    email = data.get("email")

    if not email:
        return "No email provided", 400

    # Send email to Kafka topic 'UserEmail'
    producer.send('UserEmail', email)
    producer.flush()

    return f"Email {email} sent to Kafka", 200

if __name__ == '__main__':
    app.run(debug=True)