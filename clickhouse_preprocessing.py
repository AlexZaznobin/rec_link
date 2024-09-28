import json
from clickhouse_driver import Client


class ClickHouseDataCleaner :
    def __init__ (self, client, stop_words_file, birthday_conditions_file) :
        self.client = client
        self.stop_words_file = stop_words_file
        self.birthday_conditions_file = birthday_conditions_file
        self.stop_words = self.load_stop_words()
        self.birthday_conditions = self.load_birthday_conditions()

    def run (self, source_table, target_table) :
        self.create_target_table(source_table, target_table)
        self.clean_data(source_table, target_table)
        print(f"Data cleaning completed. Cleaned data saved to {target_table}")
        self.dedupl(target_table)
        print(f"deduplication cleaning completed. Cleaned data saved to {target_table}_dupl")
        self.insert_unique_uid_lists( f"{target_table}_dupl", "table_results", 'uid', 'Dup', 'id_is1')
        print(f"insert_unique_uid_lists completed")

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
            stop_words_condition = f"replaceRegexpAll({stop_words_condition}, ' {word} ', '')"

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

    def transliterate_column_ru_en (self, column_name) :
        return f"""
        replaceRegexpAll(
            transform({column_name}, ['а','е','в','к','м','н','о','р','с','т','у','х'], 
                                 ['a','e','b','k','m','h','o','p','c','t','y','x']),
            '@.*$', ''
        )
        
        """

    def transliterate_column_en_ru (self, column_name) :
        return f"""
        transform({column_name}, ['a','e','b','k','m','h','o','p','c','t','y','x'], 
                               ['а','е','в','к','м','н','о','р','с','т','у','х'])
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

    # {self.apply_lowercase(self.clean_birthdate('birthdate'))}

    def clean_data(self, source_table, target_table):
        inter_table_1= f"{source_table}_i1"
        inter_table_2= f"{source_table}_i2"
        self.lower_casing(  source_table, inter_table_1)
        self.remove_stop_words(inter_table_1, inter_table_2)
        self.transliteration(inter_table_2, target_table)
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_1}")
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_2}")

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
        FROM {source_table} limit 10000
        """

        self.client.execute(clean_query)

    def remove_stop_words (self, intermediate_table, target_table) :
        clean_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            {self.remove_stop_words_from_column('full_name')} AS full_name, 
            {self.remove_stop_words_from_column('email')} AS email,
            address,
            sex,
            {self.clean_birthdate('birthdate')} as birthdate,
            {self.clean_phone('phone')} as phone
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
    def transliteration (self, intermediate_table, target_table) :
        clean_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            {self.transliterate_column_en_ru('full_name')} AS full_name,            
            {self.transliterate_column_ru_en('email')} AS email,
            {self.transliterate_column_en_ru('address')} AS address,      
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


    def dedupl(self,source_table):
        self.add_duplication_column(source_table,
                                    'full_name')
        self.client.execute(f" DROP TABLE IF EXISTS source_table")


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
        query = f"SELECT * FROM {table_name}"
        result = self.client.execute(query)
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

    def add_duplication_columns (self, source_table, column_names) :
        # Target table where duplicates are identified
        target_table = f"{source_table}_dupl"

        # Join column names to use them in the ORDER BY and denseRank
        columns_concat = ", ".join(column_names)

        # Create a new table based on the source table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} 
        AS {source_table} 
        ENGINE = MergeTree()
        ORDER BY ({columns_concat})
        """
        self.client.execute(create_table_query)

        # Alter the target table to add the Dup column if it doesn't exist
        alter_table_query = f"""
        ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS Dup UInt32
        """
        self.client.execute(alter_table_query)

        # Properly build the denseRank query by joining column names
        rank_columns = ", ".join([f"toString({col})" for col in column_names])

        # Insert data into the new table with the Dup column for duplicates across multiple columns
        clean_query = f"""
        INSERT INTO {target_table} (uid, full_name, email, address, sex, birthdate, phone, Dup)
        SELECT
            uid,
            full_name,
            email,
            address,
            sex,
            birthdate,
            phone,
            denseRank() OVER (ORDER BY {rank_columns}) AS Dup
        FROM {source_table}
        """
        self.client.execute(clean_query)

        print(
            f"Data from {source_table} has been inserted into {target_table} with a new 'Dup' column for duplicates based on {column_names}.")

    def add_duplication_column (self, source_table, column_name) :
        # Target table where duplicates are identified
        target_table = f"{source_table}_dupl"

        # Create a new table based on the source table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} 
        AS {source_table} 
        ENGINE = MergeTree()
        ORDER BY {column_name}
        """
        self.client.execute(create_table_query)

        # Alter the target table to add the Dup column if it doesn't exist
        alter_table_query = f"""
        ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS Dup UInt32
        """
        self.client.execute(alter_table_query)

        # Insert data into the new table with the Dup column for duplicates
        clean_query = f"""
        INSERT INTO {target_table} (uid, full_name, email, address, sex, birthdate, phone, Dup)
        SELECT
            uid,
            full_name,
            email,
            address,
            sex,
            birthdate,
            phone,
            denseRank() OVER (ORDER BY {column_name}) AS Dup
        FROM {source_table}
        """
        self.client.execute(clean_query)

        print(f"Data from {source_table} has been inserted into {target_table} with a new 'Dup' column for duplicates.")

    def insert_unique_uid_lists (self, source_table, target_table, uid_column, dup_column, list_column) :
        """
        This function selects rows with the same Dup value, groups the uids into a list,
        and inserts the list into a specified column in the target table.

        :param source_table: The source table containing uids and Dup columns.
        :param target_table: The target table to insert unique lists of uids.
        :param uid_column: The column name containing uids.
        :param dup_column: The column name containing Dup values.
        :param list_column: The column name where unique lists of uids will be inserted.
        """

        # Step 1: Query to group UIDs by the Dup column
        group_query = f"""
        SELECT {dup_column}, groupArray({uid_column}) AS uid_list
        FROM {source_table}
        GROUP BY {dup_column}
        HAVING COUNT(*) > 1
        """

        # Fetch the grouped UIDs based on Dup
        grouped_uids = self.client.execute(group_query)

        # Step 2: Insert grouped lists into the target table
        for dup_value, uid_list in grouped_uids :
            uid_list_str = str(uid_list)  # Convert the list to a string representation for ClickHouse

            # Insert the unique UID lists into the target table
            insert_query = f"""
            INSERT INTO {target_table} ({dup_column}, {list_column})
            VALUES ('{dup_value}', '{uid_list_str}')
            """
            self.client.execute(insert_query)

        print(f"Inserted unique uid lists into {target_table} for {len(grouped_uids)} Dup groups.")