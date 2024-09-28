# Data Processing and Cleaning Project

This project contains a set of Python scripts for processing and cleaning data using ClickHouse as the database backend. It includes functionality for data preprocessing, deduplication, and various data cleaning operations.

## Project Structure

```
.
├── .env
├── .gitignore
├── Dockerfile
├── Pandas/
├── README.md
├── birthday_cleaninig_conditions.json
├── clickhouse_client.py
├── clickhouse_preprocessing.py
├── data_processor.py
├── docker-compose.yml
├── main.py
├── record_db.py
├── requirements.txt
└── stop_words.txt
```

## Files

1. `data_processor.py`: Main data processing logic
2. `clickhouse_client.py`: ClickHouse database client wrapper
3. `clickhouse_preprocessing.py`: Data cleaning and preprocessing operations
4. `main.py`: Entry point for the application
5. `record_db.py`: Database record operations
6. `Dockerfile`: Instructions for building the Docker image
7. `docker-compose.yml`: Docker Compose configuration
8. `.env`: Environment variables (make sure to keep sensitive information secure)
9. `requirements.txt`: Python dependencies
10. `birthday_cleaninig_conditions.json`: Conditions for cleaning birthday data
11. `stop_words.txt`: List of stop words to be removed during processing

## Dependencies

- Python 3.x
- ClickHouse
- Docker (optional, for containerized deployment)

See `requirements.txt` for a full list of Python dependencies.

## Setup

1. Clone the repository:
   ```
   git clone <repository-url>
   cd <project-directory>
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up your environment variables in the `.env` file.

4. Ensure you have a ClickHouse server running and accessible, or use the provided Docker setup.

## Usage

### Running with Python

1. Make sure your ClickHouse server is running.
2. Run the main script:
   ```
   python main.py
   ```

### Running with Docker

1. Build and start the containers:
   ```
   docker-compose up --build
   ```

This will start both the ClickHouse server and the Python application.

## Main Components

### DataProcessor

The `DataProcessor` class in `data_processor.py` is the main entry point for data processing. It uses the `ClickHouseDataCleaner` for preprocessing and handles the overall data processing flow.

### ClickhouseClient

The `ClickhouseClient` class in `clickhouse_client.py` provides a wrapper for ClickHouse database operations.

### ClickHouseDataCleaner

The `ClickHouseDataCleaner` class in `clickhouse_preprocessing.py` provides various data cleaning and preprocessing operations.

## Features

- Data preprocessing and cleaning
- Stop word removal
- Birthday data cleaning
- Phone number formatting
- Transliteration (Russian to English and vice versa)
- Deduplication
- Case normalization

 

## Note

- Make sure to customize the connection details, file paths, and table names according to your specific setup and requirements.
- Keep your `.env` file secure and do not commit it to version control.
- The `birthday_cleaninig_conditions.json` file contains conditions for cleaning birthday data. Make sure it's properly configured for your use case.
- The `stop_words.txt` file contains words to be removed during text processing. Update this file as needed for your specific requirements.
 
 
## Clickhouse connection
Для подулючения к Clickhouse использовали ClickhouseClient(host='localhost', port=9000, user='default', password='')
