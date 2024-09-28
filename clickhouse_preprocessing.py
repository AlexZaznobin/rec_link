import json
from clickhouse_driver import Client


class ClickHouseDataCleaner :
    def __init__ (self, client, stop_words_file, birthday_conditions_file) :
        self.client = client
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

    def create_target_table (self, source_table, target_table) :
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS {source_table}
        ENGINE = MergeTree() ORDER BY full_name
        """
        self.client.execute(create_table_query)

    def remove_stop_words_from_column (self, column_name) :
        # Apply replaceRegexpAll for each stop word to the given column (full_name)
        stop_words_condition = column_name
        for word in self.stop_words :
            stop_words_condition = f"replaceRegexpAll({stop_words_condition}, '(?i)\\b{word}\\b', '')"

        return stop_words_condition

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

        # Access both conditions and transformations
        condition_clauses = self.birthday_conditions['conditions']
        transformation_clauses = self.birthday_conditions['transformations']

        # Iterate over the conditions
        for key, when_clause in condition_clauses.items() :
            # Get the corresponding transformation (then_clause)
            then_clause = transformation_clauses.get(key)

            if when_clause and then_clause :
                # Replace {column_name} with the actual column name
                when_clause = when_clause.replace('{column_name}', column_name)
                then_clause = then_clause.replace('{column_name}', column_name)
                # Append to the conditions list
                conditions.append(f"WHEN {when_clause} THEN {then_clause}")
            else :
                raise KeyError(f"Missing transformation for condition key: {key}")

        # Build the final CASE statement
        case_statement = f"CASE {' '.join(conditions)} ELSE {column_name} END"
        return case_statement

    def apply_lowercase (self, column_name) :
        return f"lowerUTF8({column_name})"

    # {self.apply_lowercase(self.clean_email('email'))}
    # AS
    # email,
    # {self.apply_lowercase(self.clean_phone('phone'))}
    # AS
    # phone,
    # {self.apply_lowercase(self.clean_birthdate('birthdate'))}
    # AS
    # birthdate
    # self.clean_full_name('full_name')

    def clean_data(self, source_table, target_table):
        inter_table= f"{source_table}_i1"
        self.lower_casing(  source_table, inter_table)
        self.remove_stop_words(inter_table, target_table)
        self.drop_table(inter_table)

    def lower_casing (self, source_table, target_table) :

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {target_table} AS {source_table}
            ENGINE = MergeTree()
            ORDER BY full_name
            """
        self.client.execute(create_table_query)

        clean_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            {self.apply_lowercase('full_name')} AS full_name,
            {self.apply_lowercase('email')} AS email,
            {self.apply_lowercase('address')} AS address,
            sex,
            birthdate,
            phone
        FROM {source_table} limit 10
        """
        self.client.execute(clean_query)

    def remove_stop_words (self, intermediate_table, target_table) :
        # Apply stop word removal to the 'full_name' column only
        clean_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            {self.remove_stop_words_from_column('full_name')} AS full_name,  -- Clean 'full_name'
            email,
            address,
            sex,
            birthdate,
            phone
        FROM {intermediate_table}
        """

        # Create the target table (if it doesn't exist)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS {intermediate_table}
        ENGINE = MergeTree()
        ORDER BY full_name
        """
        self.client.execute(create_table_query)

        # Execute the stop word removal query
        self.client.execute(clean_query)

        print(f"Removed stop words from 'full_name' and inserted cleaned data into {target_table}.")

    def run (self, source_table, target_table) :
        self.create_target_table(source_table, target_table)
        self.clean_data(source_table, target_table)
        print(f"Data cleaning completed. Cleaned data saved to {target_table}")

    def test_clickhouse_connection (self) :
        # A simple query to test connection
        query = "SELECT now()"
        result = self.client.execute(query)
        print("ClickHouse connection test result:", result)
        return result

    def show_tables (self) :
        # A query to list all tables in the default database
        query = "SHOW TABLES"
        result = self.client.execute(query)
        print("Tables in ClickHouse:", result)
        return result

    def select_from_table (self, table_name) :
        # A simple query to select data from the given table
        query = f"SELECT * FROM {table_name} LIMIT 5"
        result = self.client.execute(query)
        print(f"Data from {table_name}:", result)
        return result

    def describe_table_columns(self, table_name):
        # Query to describe the table structure (columns and their types)
        query = f"DESCRIBE TABLE {table_name}"
        result = self.client.execute(query)


    def drop_table(self, table_name):
        # Query to delete (drop) the table
        query = """
            DROP TABLE IF EXISTS table_name
        """

        # Execute the query
        self.client.execute(query)