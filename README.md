# ETL Traffic Management data pipeline with Airflow and Kafka

![alt text](image.png)

## Summary:

This project focuses on building an ETL (Extract, Transform, Load) pipeline to manage and analyze road traffic data from various toll plazas with the aim of alleviating congestion on national highways. It is my solution for the final project of the IBM Coursera course "ETL and Data Pipelines with Shell, Airflow, and Kafka."

## Objectives

* Collect and consolidate data available in different formats (.csv, .tsv, .txt) into a single file.
* Create an ETL pipeline using Apache Airflow.
* Build a streaming ETL pipeline using Apache Kafka.

## Project Scenario

As a data engineer, the task is to harmonize road traffic data from multiple toll operators, each using different file formats, to derive insights that contribute to smoother traffic flow on national highways. The project involves:

- Collecting and consolidating data from different formats.
- Creating a data pipeline to collect streaming data from Kafka and load it into a PostgreSQL database.

## Tools and Technologies Used:

* Apache Airflow
* Kafka
* PostgreSQL
* Python

## Project Structure

```bash
proyecto-peajes/
├── conf/
│   └──pipeline.conf
├── data/
│   ├── input/
│   │   └── tolldata.tgz 
│   ├── output/
│   ├── staging/
│   └── extracted/ 
├── airflow/
│   └── dags/
│       └── etl_toll_data_dag.py
├── kafka/
│   ├── streaming_data_reader-consumer.py
│   └── toll_traffic_generator-producer.py
├── .gitignore
├── etl_toll_data.py
├── etl_utils.py
├── LICENSE
├── README.md
└── requirements.txt
```

- `conf/`: 
  - `pipeline.conf`: Configuration file that contains the necessary settings for the ETL pipeline, including database credentials.

- `data/`:
  - `input/`:
    - `tolldata.tgz`: Compressed file containing toll plaza traffic data in various formats (.csv, .tsv, .txt) and additional information about it.
  - `output/`: Directory where processed and transformed data ready for analysis or database loading will be stored.
  - `staging/`: Temporary directory used to store intermediate data during the ETL process.
  - `extracted/`: Directory where data extracted and decompressed from the input file is stored.

- `airflow/`:
  - `dags/`:
    - `etl_toll_data_dag.py`: Script that defines the Airflow DAG (Directed Acyclic Graph) for the toll traffic data ETL pipeline. This DAG orchestrates the extraction, transformation, and loading tasks.

- `kafka/`:
  - `streaming_data_reader-consumer.py`: Kafka consumer script that reads real-time transmitted traffic data, processes it, and loads it into a PostgreSQL database.
  - `toll_traffic_generator-producer.py`: Kafka producer script that simulates traffic data generation and sends it to the appropriate Kafka topic.

- `.gitignore`: File that specifies which files or directories should be ignored by Git, generally to exclude temporary files, local configuration files, or sensitive data.

- `etl_toll_data.py`: Main ETL pipeline script that contains the logic for extracting, transforming, and loading traffic data.

- `etl_utils.py`: Utility file that contains reusable functions that support the ETL process.

- `LICENSE`: MIT license.

- `README.md`: Documentation file that provides an overview of the project, setup instructions, and usage guidelines.

- `requirements.txt`: File that lists the Python dependencies needed for the project, which can be installed using pip.p


## Requirements

- Python 3.8 o superior
- Apache Airflow
- Apache Kafka
- PostgreSQL
- Docker and Docker Compose (optional, but recommended)

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/lucianoalessi/ETL-traffic-management-dataPipeline-airflow-kafka.git
cd ETL-traffic-management-dataPipeline-airflow-kafka
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 3. Install dependencies
```
pip install -r requirements.txt

```

# Configuration

### PostgreSQL

1. Create a PostgreSQL database (if you don't have one, you can create one for free at aiven.com).

2. Set up the PostgreSQL database and configure the connection details:

    Create a configuration file named pipeline.conf in the project directory (/toll-etl-airflow-kafka-dataPipeline/conf/pipeline.conf). This file should contain the connection details and PostgreSQL credentials. These details will be used by the connect_to_db function to establish the connection with the database.

    Template for creating the file and entering connection data:

    ```
    [postgres]
    host=****.aivencloud.com
    port=15191
    user=avnadmin
    pwd=****
    dbname=defaultdb
    ```

### Apache Kafka

1. Download Apache Kafka:

    ```
    curl -O https://downloads.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
    tar -xzf kafka_2.13-3.0.0.tgz
    cd kafka_2.13-3.0.0
    ```
2. Start Zookeeper:

    ```
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```
3. Start Kafka:

    ```
    bin/kafka-server-start.sh config/server.properties
    ```
4. Create the necessary topics:

    ```
    bin/kafka-topics.sh --create --topic toll_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```

### Apache Airflow

Note: It's recommended to install and run Airflow in Docker or on a cloud-based virtual machine instance.

1. Copy the etl_toll_data_dag.py file to the Airflow DAGs directory.
2. Start the Airflow web server:

    ```
    airflow webserver -p 8080
    ```
3. Start the Airflow scheduler:
    ```
    airflow scheduler
    ```
4. Go to the Airflow UI:
    - Open Airflow in your web browser (http://localhost:8080), activate and run the ETL_toll_data DAG.

# Running the Project

### 1. ETL

```
python etl_toll_data.py
```
This will do the following:
1. Unzip the traffic data
2. Extract and process data from the extracted files
3. Consolidate the processed data
4. Transform and load the data


### 2. Kafka Producer & Consumer
1. change directory

    ```
    cd streaming-kafka
    ```

2. Run the producer to send messages to Kafka:

    - In a terminal, run:

        ```
        python toll_traffic_generator-producer.py
        ```
3. Run the consumer to read messages from Kafka and store them in PostgreSQL:

    - In another terminal, run

        ``` 
        python streaming_data_reader-consumer.py
        ```

### 3. Airflow DAG

1. Start the Airflow web server:

    ```
    airflow webserver -p 8080
    ```

2. Start the Airflow scheduler:
    ```
    airflow scheduler
    ```
3. Go to the Airflow UI:
    - Open Airflow in your web browser (http://localhost:8080), activate and run the ETL_toll_data DAG.



## Contributing

Contributions are welcome. Please open an issue or submit a pull request for any improvements or corrections.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.