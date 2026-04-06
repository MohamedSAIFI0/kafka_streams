from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
import threading

# Configurer l'application Kafka Streams
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka-streams-app',
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(config)
producer = Producer(producer_config)

# Construire le flux
consumer.subscribe(['input-topic'])

stop_event = threading.Event()

def process_stream():
    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Erreur: {msg.error()}")
                continue

            # Transformation : convertir en majuscules
            value = msg.value().decode('utf-8')
            upper_case_value = value.upper() + "-TEST"

            # Envoyer vers output-topic
            producer.produce('output-topic', value=upper_case_value.encode('utf-8'))
            producer.flush()

    finally:
        consumer.close()

# Démarrer l'application
stream_thread = threading.Thread(target=process_stream)
stream_thread.start()

# Ajouter un hook pour arrêter proprement
try:
    stream_thread.join()
except KeyboardInterrupt:
    print("Arrêt en cours...")
    stop_event.set()
    stream_thread.join()
    print("Application arrêtée.")