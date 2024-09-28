from fuzzywuzzy import fuzz
from collections import defaultdict
from clickhouse_preprocessing import ClickHouseDataCleaner
import uuid


class DataProcessor :
    def __init__ (self, client) :
        self.client = client
        self.preprocessor=ClickHouseDataCleaner(client=self.client,
                                                stop_words_file='stop_words.txt',
                                                birthday_conditions_file='birthday_cleaning_conditions.json')

    def process_data (self) :

        self.preprocessor.run(source_table="table_dataset1", target_table="table_dataset1_clean")
        # Получаем данные из всех таблиц
        data1 = self.get_data_from_table("table_dataset1")



        data2 = self.get_data_from_table("table_dataset2")
        data3 = self.get_data_from_table("table_dataset3")

        # Создаем словарь для хранения групп записей
        groups = defaultdict(lambda : {"id_is1" : [], "id_is2" : [], "id_is3" : []})

        # Обрабатываем каждый датасет
        self.process_dataset(data1, groups, "id_is1")
        self.process_dataset(data2, groups, "id_is2")
        self.process_dataset(data3, groups, "id_is3")

        # Записываем результаты
        self.write_results(groups)

    def get_data_from_table (self, table_name) :
        query = f"SELECT * FROM {table_name} LIMIT 1000"
        return self.client.execute(query)

    def process_dataset (self, data, groups, id_field) :
       ''' to be done '''

    def write_results (self, groups) :
        results = [(group["id_is1"], group["id_is2"], group["id_is3"]) for group in groups.values()]
        query = "INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES"
        self.client.execute(query, results)