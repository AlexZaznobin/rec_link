import json
from clickhouse_driver import Client


class ClickHouseDataCleaner :
    def __init__ (self, client, source_table, target_table, stop_words_file, birthday_conditions_file) :
        self.client = client
        self.source_table = source_table
        self.target_table = target_table
        self.stop_words_file = stop_words_file
        self.birthday_conditions_file = birthday_conditions_file
        self.stop_words = self.load_stop_words()
        self.birthday_conditions = self.load_birthday_conditions()

    def load_stop_words (self) :
        with open(self.stop_words_file, 'r', encoding='utf-8') as file :
            return [word.strip() for word in file.readlines()]

    def load_birthday_conditions (self) :
        with open(self.birthday_conditions_file, 'r', encoding='utf-8') as file :
            return json.load(file)

    def create_target_table (self) :
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.target_table} (
            full_name String,
            phone String,
            email String,
            birthdate String
        ) ENGINE = MergeTree() ORDER BY full_name
        """
        self.client.execute(create_table_query)

    def remove_stop_words (self, column_name) :
        stop_words_condition = ", ".join(
            [f"replaceRegexpAll({column_name}, '(?i)\\b{word}\\b', '')" for word in self.stop_words])
        return f"multiIf({stop_words_condition}, {column_name})"

    def clean_full_name (self, column_name) :
        return f"""
        replaceRegexpAll(
            replaceRegexpAll(
                {self.remove_stop_words(column_name)},
                '\\n', '_'
            ),
            '-', ''
        )
        """

    def clean_phone (self, column_name) :
        return f"""
        replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(
                    replaceRegexpAll(
                        replaceRegexpAll(
                            toString({column_name}),
                            ' ', ''
                        ),
                        '\\(|\\)', ''
                    ),
                    '-', ''
                ),
                '\\+7', '8'
            ),
            '^7', '8'
        )
        """

    def clean_email (self, column_name) :
        return f"""
        replaceRegexpAll(
            replaceRegexpAll({column_name}, '[аевкмнорстух]', 
                a -> transform(a, ['а','е','в','к','м','н','о','р','с','т','у','х'], 
                                  ['a','e','b','k','m','h','o','p','c','t','y','x'])),
            '@.*$', ''
        )
        """

    def clean_birthdate (self, column_name) :
        conditions = []
        for condition in self.birthday_conditions :
            when_clause = condition['when']
            then_clause = condition['then'].replace('{column_name}', column_name)
            conditions.append(f"WHEN {when_clause} THEN {then_clause}")

        case_statement = f"CASE {' '.join(conditions)} ELSE {column_name} END"
        return case_statement

    def apply_lowercase (self, column_name) :
        return f"lower({column_name})"

    def clean_data (self) :
        clean_query = f"""
        INSERT INTO {self.target_table}
        SELECT
            {self.apply_lowercase(self.clean_full_name('full_name'))} AS full_name,
            {self.apply_lowercase(self.clean_phone('phone'))} AS phone,
            {self.apply_lowercase(self.clean_email('email'))} AS email,
            {self.apply_lowercase(self.clean_birthdate('birthdate'))} AS birthdate
        FROM {self.source_table}
        """
        self.client.execute(clean_query)

    def run (self) :
        self.create_target_table()
        self.clean_data()
        print(f"Data cleaning completed. Cleaned data saved to {self.target_table}")
