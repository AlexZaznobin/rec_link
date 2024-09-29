import json
import pandas as pd
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
        # self.insert_unique_uid_lists( f"{target_table}_dupl", "table_results", 'uid', 'Dup', 'id_is1')
        #
        # print(f"insert_unique_uid_lists completed")

    def load_stop_words (self) :
        with open(self.stop_words_file, 'r', encoding='utf-8') as file :
            return [word.strip() for word in file.readlines()]

    def load_birthday_conditions (self) :
        with open(self.birthday_conditions_file, 'r', encoding='utf-8') as file :
            return json.load(file)

    def create_target_table (self, source_table, target_table) :
        self.client.execute(f" DROP TABLE IF EXISTS {target_table}")
        self.client.execute(f" DROP TABLE IF EXISTS {target_table}_dupl")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS {source_table}
        ENGINE = MergeTree() ORDER BY full_name
        """
        self.client.execute(create_table_query)

    def remove_stop_words_from_column (self, column_name) :
        # Apply replaceRegexpAll for each stop word to the given column (full_name)
        stop_words_condition = column_name
        for word in self.stop_words :
            # Handle newline as a special case
            if word == "\\n" :
                stop_words_condition = f"replaceRegexpAll({stop_words_condition}, '\\\\n', '')"
            else :
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
        cyrillic_to_latin = {
            'а' : 'a', 'А' : 'A', 'б' : 'b', 'Б' : 'B', 'ц' : 'c', 'Ц' : 'C', 'д' : 'd', 'Д' : 'D', 'е' : 'e',
            'Е' : 'E',
            'ф' : 'f', 'Ф' : 'F', 'г' : 'g', 'Г' : 'G', 'х' : 'h', 'Х' : 'H', 'и' : 'i', 'И' : 'I', 'й' : 'j',
            'Й' : 'J',
            'к' : 'k', 'К' : 'K', 'л' : 'l', 'Л' : 'L', 'м' : 'm', 'М' : 'M', 'н' : 'n', 'Н' : 'N', 'о' : 'o',
            'О' : 'O',
            'п' : 'p', 'П' : 'P', 'к' : 'q', 'К' : 'Q', 'р' : 'r', 'Р' : 'R', 'с' : 's', 'С' : 'S', 'т' : 't',
            'Т' : 'T',
            'у' : 'u', 'У' : 'U', 'в' : 'v', 'В' : 'V', 'в' : 'w', 'В' : 'W', 'кс' : 'x', 'Кс' : 'X', 'ы' : 'y',
            'Ы' : 'Y',
            'з' : 'z', 'З' : 'Z'
        }

        replace_chain = column_name
        for cyrillic, latin in cyrillic_to_latin.items() :
            replace_chain = f"replaceRegexpAll({replace_chain}, '{cyrillic}', '{latin}')"

        # Remove everything after the '@' symbol
        replace_chain = f"replaceRegexpAll({replace_chain}, '@.*$', '')"

        # Remove all punctuation marks, including '-' and '_'
        replace_chain = f"replaceRegexpAll({replace_chain}, '[[:punct:]_-]', '')"

        return replace_chain

    def transliterate_column_en_ru (self, column_name) :
        latin_to_cyrillic = {
            'a' : 'а', 'A' : 'А', 'b' : 'б', 'B' : 'Б', 'c' : 'ц', 'C' : 'Ц', 'd' : 'д', 'D' : 'Д', 'e' : 'е',
            'E' : 'Е',
            'f' : 'ф', 'F' : 'Ф', 'g' : 'г', 'G' : 'Г', 'h' : 'х', 'H' : 'Х', 'i' : 'и', 'I' : 'И', 'j' : 'й',
            'J' : 'Й',
            'k' : 'к', 'K' : 'К', 'l' : 'л', 'L' : 'Л', 'm' : 'м', 'M' : 'М', 'n' : 'н', 'N' : 'Н', 'o' : 'о',
            'O' : 'О',
            'p' : 'п', 'P' : 'П', 'q' : 'к', 'Q' : 'К', 'r' : 'р', 'R' : 'Р', 's' : 'с', 'S' : 'С', 't' : 'т',
            'T' : 'Т',
            'u' : 'у', 'U' : 'У', 'v' : 'в', 'V' : 'В', 'w' : 'в', 'W' : 'В', 'x' : 'кс', 'X' : 'Кс', 'y' : 'ы',
            'Y' : 'Ы',
            'z' : 'з', 'Z' : 'З'
        }

        replace_chain = column_name
        for latin, cyrillic in latin_to_cyrillic.items() :
            replace_chain = f"replaceRegexpAll({replace_chain}, '{latin}', '{cyrillic}')"

        # Remove everything after the '@' symbol
        replace_chain = f"replaceRegexpAll({replace_chain}, '@.*$', '')"

        # Remove all punctuation marks, including '-' and '_'
        replace_chain = f"replaceRegexpAll({replace_chain}, '[[:punct:]_-]', '')"

        return replace_chain

    def transliterate_column_en_ru_complex (self, column_name) :
        return f"""
        replaceRegexpAll(replaceRegexpAll(replaceRegexpAll({column_name}, 'zh', 'ж'), 'sh', 'ш'), 'ch', 'ч')
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
        inter_table_3= f"{source_table}_i3"
        inter_table_4= f"{source_table}_i4"
        self.lower_casing(  source_table, inter_table_1)
        self.remove_stop_words(inter_table_1, inter_table_2)
        self.transliteration(inter_table_2, inter_table_3)
        self.fix_short_phone_numbers(inter_table_3, inter_table_4)
        self.remove_numbers_from_full_name(inter_table_4, target_table)
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_1}")
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_2}")
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_3}")
        self.client.execute(f" DROP TABLE IF EXISTS {inter_table_4}")

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
        FROM {source_table} limit 100000
        """
        # limit 10000
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
            {self.transliterate_column_en_ru_complex(self.transliterate_column_en_ru('full_name'))} AS full_name,            
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

    def remove_numbers_from_full_name (self, source_table, target_table) :
        """
        This function removes all numbers (0-9) from the 'full_name' column.

        :param source_table: The source table to modify the 'full_name' column.
        :param target_table: The target table where the result will be stored.
        """

        # Create the target table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} 
        AS {source_table} 
        ENGINE = MergeTree()
        ORDER BY full_name
        """
        self.client.execute(create_table_query)

        # Query to remove numbers from the 'full_name' column using replaceRegexpAll
        clean_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            replaceRegexpAll(full_name, '[0-9]', '') AS full_name,
            email,
            address,
            sex,
            birthdate,
            phone
        FROM {source_table}
        """

        self.client.execute(clean_query)

        print(f"Removed numbers from 'full_name' column and saved the cleaned data to {target_table}.")

    def fix_short_phone_numbers (self, source_table, target_table, phone_num=10) :
        """
        This function checks if the phone number has a length less than 10,
        and if so, appends the value of the 'full_name' column to the 'phone' column.

        :param source_table: The source table to check phone numbers.
        :param target_table: The target table where the result will be stored.
        """

        # Create the target table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} 
        AS {source_table} 
        ENGINE = MergeTree()
        ORDER BY full_name
        """
        self.client.execute(create_table_query)

        # Now, update the phone column where the length of the phone number is less than phone_num
        update_query = f"""
        INSERT INTO {target_table}
        SELECT
            uid,
            full_name,
            email,
            address,
            sex,
            birthdate,
            CASE
                WHEN length(phone) < {phone_num} THEN concat(phone, ' ', full_name)
                ELSE phone
            END AS phone
        FROM {source_table}
        """

        self.client.execute(update_query)

        print(f"Updated short phone numbers in {target_table} by appending the full name.")

    def dedupl(self,source_table):


        itbl_1=f"{source_table}_1"
        itbl_2=f"{source_table}_2"
        itbl_3=f"{source_table}_3"
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_1}")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_2}")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_3}")
        self.add_duplication_column(source_table, itbl_1,'full_name')
        self.add_duplication_column(itbl_1, itbl_2, 'email')
        self.add_duplication_column(itbl_2, itbl_3, 'address')
        self.add_duplication_column(itbl_3, f"{source_table}_dupl",'phone')

        self.client.execute(f" DROP TABLE IF EXISTS source_table")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_1}")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_2}")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_3}")
        self.client.execute(f" DROP TABLE IF EXISTS {itbl_3}")
        self.add_final_unique_column(f"{source_table}_dupl",
                                     ['full_name','email','address','phone'])



    def add_final_unique_column (self, target_table, initial_columns) :
        """
        Adds a 'final_unique' column that groups rows based on any non-unique duplication columns.
        :param source_table: The source table where duplication columns have been added.
        :param initial_columns: List of initial columns (e.g., ['full_name', 'email', 'address', 'phone']).
        """

        # Generate the corresponding duplication column names (e.g., full_name -> full_name_Dup)
        duplication_columns = [f"{col}_Dup" for col in initial_columns]

        # Ensure uid is available in the target table (if not already there)
        alter_table_query = f"""
        ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS final_unique UInt32
        """
        self.client.execute(alter_table_query)

        # Dynamically create the list of conditions for the duplication check
        condition_columns = " OR ".join([f"CAST({col} AS UInt32) > 1" for col in duplication_columns])

        # Create a temporary table with the final_unique values and ensure 'uid' is included
        temp_table_query = f"""
        CREATE TEMPORARY TABLE temp_final_unique AS
        SELECT uid, denseRank() OVER (ORDER BY {condition_columns}) AS final_unique
        FROM {target_table}
        """
        self.client.execute(temp_table_query)

        # Update the original table with final_unique values from the temporary table
        update_query = f"""
        ALTER TABLE {target_table}
        UPDATE final_unique = (SELECT final_unique FROM temp_final_unique WHERE temp_final_unique.uid = {target_table}.uid)
        WHERE uid IN (SELECT uid FROM temp_final_unique)
        """
        self.client.execute(update_query)

        # Drop the temporary table
        self.client.execute("DROP TABLE IF EXISTS temp_final_unique")

        print(f"Added 'final_unique' column in {target_table} with repeating indices based on the duplication columns.")

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

    def get_existing_columns (self, table_name) :
        """
        Retrieves the list of columns from the specified table.

        :param table_name: The name of the table from which to retrieve columns.
        :return: A list of column names.
        """
        describe_query = f"DESCRIBE TABLE {table_name}"
        result = self.client.execute(describe_query)
        return [row[0] for row in result]  # Extract column names from the result

    def add_duplication_column (self, source_table, target_table, column_name) :
        """
        Adds a duplication column for the specified column and carries over previous columns automatically.

        :param source_table: The source table to read from.
        :param target_table: The target table where the data will be inserted.
        :param column_name: The column to compute the duplication for.
        """

        # Retrieve existing columns from the source table, including previously created Dup columns
        existing_columns = self.get_existing_columns(source_table)
        existing_columns_select = ", ".join(existing_columns)

        # Create the target table based on the source table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} 
        AS {source_table} 
        ENGINE = MergeTree()
        ORDER BY {column_name}
        """
        self.client.execute(create_table_query)

        # Alter the target table to add the new Dup column for the current column
        alter_table_query = f"""
        ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {column_name}_Dup UInt32
        """
        self.client.execute(alter_table_query)

        # Insert data into the new table with the Dup column for the current column and the previous columns
        clean_query = f"""
        INSERT INTO {target_table} ({existing_columns_select}, {column_name}_Dup)
        SELECT
            {existing_columns_select},
            denseRank() OVER (ORDER BY {column_name}) AS {column_name}_Dup
        FROM {source_table}
        """
        self.client.execute(clean_query)

        print(f"Added '{column_name}_Dup' column to {target_table} without increasing row count.")

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

    def get_dataframe_from_table(self, table_name, limit=10000):
        """
        This function retrieves data from the specified table and returns it as a pandas DataFrame.

        :param table_name: The name of the table to query.
        :param limit: The number of rows to retrieve. Default is 10,000.
        :return: A pandas DataFrame containing the table data.
        """
        # Define the query to fetch data
        query = f"SELECT * FROM {table_name} LIMIT {limit}"

        # Execute the query
        result = self.client.execute(query)

        # Fetch column names (you can use DESCRIBE to get the column names if not available in result)
        describe_query = f"DESCRIBE TABLE {table_name}"
        column_info = self.client.execute(describe_query)
        columns = [col[0] for col in column_info]

        # Convert the result into a pandas DataFrame
        df = pd.DataFrame(result, columns=columns)
        return df