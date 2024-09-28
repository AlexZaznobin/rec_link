from clickhouse_driver import Client

class ClickHouseUploader:
    def __init__(self, host='localhost', port=9000, user='default', password=''):
        # Инициализация подключения к ClickHouse
        self.client = Client(host=host, port=port, user=user, password=password)

    def execute(self, query, params=None):
        """
        Выполняет произвольный SQL-запрос.
        :param query: Строка SQL-запроса
        :param params: Параметры для параметризованного запроса (опционально)
        :return: Результат выполнения запроса
        """
        return self.client.execute(query, params)

    def create_table(self, table_name, df):
        """
        Создает таблицу в ClickHouse на основе DataFrame.
        :param table_name: Имя таблицы
        :param df: pandas DataFrame для анализа схемы
        """
        # Определение схемы на основе типов данных в DataFrame
        schema = self.infer_schema(df)
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        '''
        self.execute(create_table_query)
        print(f"Таблица {table_name} успешно создана с автоматически определенной схемой: {schema}")

    def infer_schema(self, df):
        """
        Автоматически определяет схему таблицы на основе типов данных DataFrame.
        :param df: pandas DataFrame
        :return: строка схемы таблицы для ClickHouse
        """
        type_mapping = {
            'int64': 'Int64',
            'int32': 'Int32',
            'float64': 'Float64',
            'float32': 'Float32',
            'object': 'String',
            'bool': 'UInt8',
            'datetime64[ns]': 'DateTime',
            'timedelta[ns]': 'Int64'
        }

        schema = []
        for column, dtype in df.dtypes.items():
            ch_type = type_mapping.get(str(dtype), 'String')  # По умолчанию String
            schema.append(f"{column} {ch_type}")

        return ', '.join(schema)

    def insert_data(self, table_name, df):
        """
        Вставляет данные из DataFrame в таблицу ClickHouse.
        :param table_name: Имя таблицы
        :param df: pandas DataFrame с данными для вставки
        """
        # Подготовка данных для вставки (список кортежей)
        data_tuples = [tuple(x) for x in df.to_numpy()]
        columns = ', '.join(df.columns)

        # SQL-запрос для вставки данных
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES"

        # Вставка данных
        self.client.execute(insert_query, data_tuples)
        print(f"Данные успешно загружены в таблицу {table_name}.")

    def close(self):
        """
        Закрытие соединения с ClickHouse.
        """
        self.client.disconnect()
        print("Соединение с ClickHouse закрыто.")