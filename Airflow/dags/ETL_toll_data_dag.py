# Importar librerías necesarias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from etl_utils import *
import os

#Definir argumentos del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Definir el DAG
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# Definiendo el destino base del proyecto
BASE_DIR = "/proyectoluciano/"

# Definiendo rutas específicas para el archivo comprimido y el directorio de extracción
source = os.path.join(BASE_DIR, "data/input/tolldata.tgz")
extract_dir = os.path.join(BASE_DIR, "data/extracted")

# Rutas de archivos extraídos
vehicle_data = os.path.join(extract_dir, "vehicle-data.csv")
tollplaza_data = os.path.join(extract_dir, "tollplaza-data.tsv")
payment_data = os.path.join(extract_dir, "payment-data.txt")

# Rutas para los archivos procesados y de staging
csv_data = os.path.join(BASE_DIR, "data/staging/csv_data.csv")
tsv_data = os.path.join(BASE_DIR, "data/staging/tsv_data.csv")
fixed_width_data = os.path.join(BASE_DIR, "data/staging/fixed_width_data.csv")
extracted_data = os.path.join(BASE_DIR, "data/staging/extracted_data.csv")
transformed_data = os.path.join(BASE_DIR, "data/output/transformed_data.csv")

# Crear directorios si no existen
os.makedirs(extract_dir, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data/staging"), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data/output"), exist_ok=True)


# Definir tareas
# Task 1 - Crear una tarea para descomprimir los datos
unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_tolldata,
    op_args=[source, BASE_DIR],
    dag=dag
)

# Task 2 - Crear una tarea para extraer datos del archivo CSV
extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_csv_data,
    op_args=[vehicle_data, csv_data],
    dag=dag
)

# Task 3 - Crear una tarea para extraer datos del archivo TSV
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_tsv_data,
    op_args=[tollplaza_data, tsv_data],
    dag=dag
)

# Task 4 - Crear una tarea para extraer datos del archivo de ancho fijo
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_fixed_width_data,
    op_args=[payment_data, fixed_width_data],
    dag=dag
)

# Task 5 - Crear una tarea para consolidar datos extraídos de tareas anteriores
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data_extracted,
    op_args=[[csv_data, tsv_data, fixed_width_data], extracted_data],
    dag=dag
)

# Task 6 - Transformar y cargar los datos
transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_load_data,
    op_args=[extracted_data, transformed_data],
    dag=dag
)

# Task 7 - Transformaciones adicionales y carga de datos en Postgresql
def load_data_to_db():
    # Leer los datos del CSV sin encabezado. header=None indica que las columnas no tienen nombre, osea esta sin encabezado.
    data = pd.read_csv(transformed_data, header=None)
    # Asignar nombres de columnas manualmente
    data.columns = ['id', 'timestamp', 'vehicle_id', 'vehicle_type', 'toll_plaza_id', 'amount', 'tag', 'tollplaza_type', 'tag_type']
    # Eliminación de duplicados
    data = data.drop_duplicates()
    # Conversiones de tipo de columna
    conversion_mapping = {
        "vehicle_id": "int",
        "vehicle_type": "category",
        "toll_plaza_id": "int",
        "amount": "int",
        "tag": "str",
        "tollplaza_type": "category",
        "tag_type": "str"
        }
    data = data.astype(conversion_mapping)
    # Conversiones de tipo time
    data['timestamp'] = pd.to_datetime(data['timestamp'], format='%a %b %d %H:%M:%S %Y')

    # Conexion a base de datos
    engine = connect_to_db(
        "conf/pipeline.conf",
        "postgres",
        "postgresql+psycopg2"
        )

    if engine:
        print('Conexión exitosa!')
        # Crear la tabla 'livetolldata'
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS livetolldata(
                timestamp TIMESTAMP,
                vehicle_id INT,
                vehicle_type VARCHAR(15),
                toll_plaza_id INT,
                amount INTEGER,
                tag VARCHAR(50),
                tollplaza_type VARCHAR(10),
                tag_type VARCHAR(10)
            );
            """))
        # Cargar los datos en la tabla
        data.to_sql('livetolldata', con=engine, if_exists='append', index=False, method='multi')
    else:
        print('Error al conectar con la base de datos')


load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data_to_db,
    dag=dag
)

# Definir la secuencia de tareas
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data >> load_data

