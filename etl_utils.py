import tarfile
import csv
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from configparser import ConfigParser



# Extraccion de datos 
def unzip_tolldata(source, destination):
    """
    Extrae el contenido del archivo .tgz fuente al directorio de destino especificado.
    
    Args:
        source (str): Ruta al archivo .tgz fuente.
        destination (str): Directorio donde se extraerá el contenido.
    """
    try:
        with tarfile.open(source, "r:gz") as tgz:
            tgz.extractall(destination)
    except Exception as e:
        print(f"Error extracting {source}: {e}")

def extract_csv_data(infile, outfile):
    """
    Extrae las columnas especificadas de un archivo CSV de entrada y guarda el resultado
    en un archivo CSV de salida.

    Args:
        infile (str): Ruta al archivo CSV de entrada.
        outfile (str): Ruta al archivo CSV de salida.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            # Itera sobre cada línea del archivo de entrada
            for line in readfile:
                # Divide la línea por comas y selecciona las columnas 1 a 4 (índice basado en 0)
                selected_columns = ",".join(line.strip().split(",")[:4])
                # Escribe las columnas seleccionadas en el archivo de salida, añadiendo una nueva línea
                writefile.write(selected_columns + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")

'''
Código explicativo línea por línea:
1. Abre un archivo CSV de entrada para lectura y un archivo CSV de salida para escritura.
   with open(infile, "r") as readfile, open(outfile, "w") as writefile:
   - `open(infile, "r")`: Abre el archivo especificado por `infile` en modo de lectura (`"r"`).
   - `open(outfile, "w")`: Abre el archivo especificado por `outfile` en modo de escritura (`"w"`). Si el archivo no existe, se creará. Si ya existe, se sobrescribirá.
   - `as readfile` y `as writefile`: Asigna los manejadores de archivo a las variables `readfile` y `writefile`, respectivamente.
   - `with` statement: Asegura que ambos archivos se cierren automáticamente cuando se salga del bloque `with`, incluso si ocurre una excepción.

2. Itera sobre cada línea del archivo de entrada.
   for line in readfile:
   - `for line in readfile`: Itera sobre cada línea en el archivo `readfile`.

3. Procesa cada línea.
   selected_columns = ",".join(line.strip().split(",")[:4])
   - `line.strip()`: Elimina cualquier espacio en blanco (incluyendo nuevos saltos de línea) al principio y al final de la línea.
   - `line.strip().split(",")`: Divide la línea en una lista de elementos, utilizando la coma (`,`) como separador.
   - `[:4]`: Selecciona los primeros cuatro elementos de la lista (índices 0 a 3).
   - `",".join(...)`: Une los elementos seleccionados en una cadena, separándolos con comas.

4. Escribe la línea procesada en el archivo de salida.
   writefile.write(selected_columns + "\n")
   - `writefile.write(...)`: Escribe la cadena `selected_columns` en el archivo `writefile`.
   - `selected_columns + "\n"`: Añade un carácter de nueva línea (`"\n"`) al final de la cadena `selected_columns` para asegurar que cada entrada se escriba en una nueva línea del archivo de salida.

'''

def extract_tsv_data(infile, outfile):
    """
    Extrae las columnas especificadas de un archivo TSV de entrada y guarda el resultado
    en un archivo CSV de salida.

    Args:
        infile (str): Ruta al archivo TSV de entrada.
        outfile (str): Ruta al archivo CSV de salida.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Divide la línea por tabulaciones y selecciona las columnas 5 a 7 (índice basado en 0)
                selected_columns = ",".join(line.strip().split("\t")[4:7])
                writefile.write(selected_columns + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")

def extract_fixed_width_data(infile, outfile):
    """
    Extrae las columnas especificadas de un archivo de ancho fijo de entrada y guarda el resultado
    en un archivo CSV de salida.

    Args:
        infile (str): Ruta al archivo de ancho fijo de entrada.
        outfile (str): Ruta al archivo CSV de salida.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Elimina espacios adicionales y divide por espacios
                cleaned_line = " ".join(line.split())

                # Selecciona las columnas 10 y 11 (índice basado en 0) directamente
                selected_columns = cleaned_line.split(" ")[9:11]
                writefile.write(",".join(selected_columns) + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")


# Transformacion de datos

def consolidate_data_extracted(infile, outfile):
    """
    Combina datos de los archivos especificados en un solo archivo CSV.

    Args:
        infile (list): Lista de rutas de archivos CSV de entrada.
        outfile (str): Ruta al archivo CSV de salida.
    """
    try:
        # Leer cada archivo CSV en la lista 'infile' y combinarlos horizontalmente
        combined_csv = pd.concat([pd.read_csv(f) for f in infile], axis=1)
        
        # Guardar el DataFrame combinado en un archivo CSV sin incluir el índice
        combined_csv.to_csv(outfile, index=False)
    except Exception as e:
        # Imprimir un mensaje de error si ocurre alguna excepción
        print(f"Error processing {infile}: {e}")

'''
Explicación del código:
1. Definición de la función y docstring
   def consolidate_data_extracted(infile, outfile):
   """
   Combina datos de los archivos especificados en un solo archivo CSV.
   
   Args:
       infile (list): Lista de rutas de archivos CSV de entrada.
       outfile (str): Ruta al archivo CSV de salida.
   """
   - La función `consolidate_data_extracted` toma dos argumentos:
     - `infile`: una lista de rutas de archivos CSV de entrada.
     - `outfile`: la ruta del archivo CSV de salida.
   - La docstring proporciona una descripción de la función y sus argumentos.

2. Bloque try-except
   try:
       ...
   except Exception as e:
       print(f"Error processing {infile}: {e}")
   - `try`: Intenta ejecutar el bloque de código que combina y guarda los archivos CSV.
   - `except Exception as e`: Captura cualquier excepción que ocurra durante la ejecución del bloque `try`.
   - `print(f"Error processing {infile}: {e}")`: Imprime un mensaje de error si ocurre una excepción.

3. Combinar archivos CSV
   combined_csv = pd.concat([pd.read_csv(f) for f in infile], axis=1)
   - `[pd.read_csv(f) for f in infile]`: Usa una lista por comprensión para leer cada archivo CSV en la lista `infile` y los convierte en DataFrames de pandas.
   - `pd.concat(..., axis=1)`: Combina los DataFrames leídos horizontalmente (columna por columna) en un solo DataFrame `combined_csv`.

Explicación del parámetro 'axis=1':
 - `pd.concat(..., axis=1)`: Combina los DataFrames leídos horizontalmente, es decir,
   agrega las columnas de cada DataFrame en lugar de las filas. Cada DataFrame en la
   lista se agrega como un nuevo conjunto de columnas al DataFrame resultante.

4. Guardar el DataFrame combinado en un archivo CSV
   combined_csv.to_csv(outfile, index=False)
   - `combined_csv.to_csv(outfile, index=False)`: Guarda el DataFrame combinado en el archivo especificado por `outfile`. El parámetro `index=False` asegura que no se escriba el índice del DataFrame en el archivo CSV de salida.
'''

def transform_load_data(infile, outfile):
    """
    Transforma la cuarta columna en un archivo CSV a mayúsculas.

    Args:
        infile (str): Ruta al archivo CSV de entrada.
        outfile (str): Ruta al archivo CSV de salida.
    """
    try:
        # Abre el archivo de entrada para lectura y el archivo de salida para escritura
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            # Crea un lector CSV para leer el archivo de entrada
            reader = csv.reader(readfile)
            # Crea un escritor CSV para escribir en el archivo de salida
            writer = csv.writer(writefile)

            # Itera sobre cada fila en el archivo de entrada
            for row in reader:
                # Modifica el cuarto campo (índice 3) y lo convierte a mayúsculas
                row[3] = row[3].upper()
                # Escribe la fila modificada en el archivo de salida
                writer.writerow(row)
    except Exception as e:
        # Imprime un mensaje de error si ocurre alguna excepción
        print(f"Error processing {infile}: {e}")


# Carga de datos

def connect_to_db(config_file, section, driverdb):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.
    driverdb (str): El driver de la base de datos a la que se conectará.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)

        # Creación de un diccionario
        # donde cargaremos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos
            engine = create_engine(
                f"{driverdb}://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
            )
            return engine

        else:
            print(
                f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None