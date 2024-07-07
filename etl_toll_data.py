import os
import pandas as pd
from etl_utils import *

# Definiendo el destino base del proyecto
BASE_DIR = ""

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

if __name__ == "__main__":
    # Ejecutar tareas secuencialmente
    unzip_tolldata(source, extract_dir)
    extract_csv_data(vehicle_data, csv_data)
    extract_tsv_data(tollplaza_data, tsv_data)
    extract_fixed_width_data(payment_data, fixed_width_data)
    consolidate_data_extracted([csv_data, tsv_data, fixed_width_data], extracted_data)
    transform_load_data(extracted_data, transformed_data)


# Otras transformaciones:

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


# Carga en base datos PostgreSQL:

# Conexion a base de datos
engine = connect_to_db(
    "conf/pipeline.conf",
    "postgres",
    "postgresql+psycopg2"
    )

if engine:
    print('Conexión exitosa!')
else:
    print('Error al conectar con la base de datos')

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


# # Carga de datos en mySql
# # Establecemos conexion con la base de datos
# engine = connect_to_db(
#     "pipeline.conf",
#     "mysql",
#     "mysql+pymysql"
#     )

# # Crear la base de datos 'tolldata' y la tabla 'livetolldata'
# with engine.connect() as conn:
#     conn.execute(text("CREATE DATABASE IF NOT EXISTS tolldata;"))
#     conn.execute(text("USE tolldata;"))
#     conn.execute(text("""
#     CREATE TABLE IF NOT EXISTS livetolldata(
#         timestamp DATETIME,
#         vehicle_id INT,
#         vehicle_type CHAR(15),
#         toll_plaza_id SMALLINT
#     );
#     """))

# # Cargar los datos en la tabla
# data = pd.read_csv(transformed_data)
# data.to_sql('livetolldata', con=engine, if_exists='append', index=False, method='multi')

# # Cerrar la conexión
# conn.close()
