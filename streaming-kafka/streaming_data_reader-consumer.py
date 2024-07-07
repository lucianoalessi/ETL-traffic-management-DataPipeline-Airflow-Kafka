from confluent_kafka import Consumer, KafkaError
from datetime import datetime
from sqlalchemy import text
from etl_utils import connect_to_db


# Configuración del consumidor
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# Función para consumir mensajes y almacenarlos en Postgres
def consume_messages(topic):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            message = msg.value().decode('utf-8')
            (timestamp, vehicle_id, vehicle_type, plaza_id, amount, tag, tollplaza_type, tag_type) = message.split(',')
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

            # Conexion a base de datos
            engine = connect_to_db(
                "conf/pipeline.conf",
                "postgres",
                "postgresql+psycopg2"
            )

            if engine:
                print('Conexión exitosa!')
                with engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, toll_plaza_id, amount, tag, tollplaza_type, tag_type)
                        VALUES (:timestamp, :vehicle_id, :vehicle_type, :toll_plaza_id, :amount, :tag, :tollplaza_type, :tag_type)
                    """), {
                        'timestamp': timestamp,
                        'vehicle_id': vehicle_id,
                        'vehicle_type': vehicle_type,
                        'toll_plaza_id': plaza_id,
                        'amount': amount,
                        'tag': tag,
                        'tollplaza_type': tollplaza_type,
                        'tag_type': tag_type
                    })
            else:
                print('Error al conectar con la base de datos')

            print(f"Consumed and stored message: {message}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages('toll')