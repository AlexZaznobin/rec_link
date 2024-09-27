from fuzzywuzzy import fuzz
from collections import defaultdict
import uuid


class DataProcessor :
    def __init__ (self, client) :
        self.client = client

    def process_data (self) :
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
        query = f"SELECT * FROM {table_name}"
        return self.client.execute(query)

    def process_dataset (self, data, groups, id_field) :
        for row in data :
            uid = row[0]
            key = self.generate_key(row)

            # Ищем подходящую группу
            best_match = None
            best_score = 0
            for group_key in groups :
                score = fuzz.ratio(key, group_key)
                if score > best_score and score > 80 :  # Порог схожести
                    best_match = group_key
                    best_score = score

            if best_match :
                groups[best_match][id_field].append(uid)
            else :
                groups[key][id_field].append(uid)

    def generate_key (self, row) :
        # Генерируем ключ для сравнения записей
        # Адаптируйте эту функцию в зависимости от структуры ваших данных
        return " ".join(str(field) for field in row[1 :])  # Исключаем UUID

    def write_results (self, groups) :
        results = [(group["id_is1"], group["id_is2"], group["id_is3"]) for group in groups.values()]
        query = "INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES"
        self.client.execute(query, results)