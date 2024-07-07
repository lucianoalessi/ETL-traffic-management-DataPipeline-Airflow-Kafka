from confluent_kafka import Producer
from datetime import datetime
from random import randint, choice
from time import sleep

# Configuración del productor
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)

# Función para enviar mensajes
def produce_messages(topic, num_messages):
    vehicle_types = ("car", "truck", "van")
    for _ in range(num_messages):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        vehicle_id = randint(10000, 10000000)
        vehicle_type = choice(vehicle_types)
        plaza_id = randint(4000, 4010)
        amount = randint(5, 100)
        tag = f"tag_{randint(1, 100)}"
        tollplaza_type = choice(['type1', 'type2'])
        tag_type = choice(['typeA', 'typeB'])
        message = f"{now},{vehicle_id},{vehicle_type},{plaza_id},{amount},{tag},{tollplaza_type},{tag_type}"
        producer.produce(topic, message.encode('utf-8'))
        print(f"Produced message: {message}")
        sleep(1)
    producer.flush()

if __name__ == "__main__":
    produce_messages('toll', 100)