from clickhouse_client import ClickhouseClient
from data_processor import DataProcessor


def main () :
    client = ClickhouseClient(host='clickhouse', user='default', password='')
    processor = DataProcessor(client)

    processor.process_data()

    client.close()


if __name__ == "__main__" :
    main()