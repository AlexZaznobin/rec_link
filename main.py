from clickhouse_client import ClickhouseClient
from data_processor import DataProcessor


def main () :
    # Используем параметры подключения, соответствующие вашему Docker-окружению
    client = ClickhouseClient(host='localhost', port=9000, user='default', password='')
    processor = DataProcessor(client)

    processor.process_data()

    client.close()

if __name__ == "__main__" :
    main()