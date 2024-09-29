from fuzzywuzzy import fuzz
from collections import defaultdict
from clickhouse_preprocessing import ClickHouseDataCleaner
import uuid
import time


class DataProcessor :
    def __init__ (self, client) :
        self.client = client
        self.preprocessor=ClickHouseDataCleaner(client=self.client,
                                                stop_words_file='stop_words.txt',
                                                birthday_conditions_file='birthday_cleaninig_conditions.json')

    def process_data (self) :
        start=time.time()
        self.preprocessor.run(source_table="table_dataset1", target_table="table_dataset1_clean")
        finish = time.time()
        print('finish-start',finish-start)
        self.manage_mutation()
        table_dataset1_clean_dupl = self.preprocessor.get_dataframe_from_table('table_dataset1_clean_dupl', limit=10e9)
        print(table_dataset1_clean_dupl['full_name_Dup'].value_counts().sort_values(ascending=False))
        print('Получаем данные из всех таблиц, дупликаты', "table_dataset1 =", table_dataset1_clean_dupl.shape[0] - table_dataset1_clean_dupl.full_name_Dup.unique().shape[0])
        alter_table_query = f"""
                                    ALTER TABLE table_dataset1_clean_dupl 
                                    ADD COLUMN IF NOT EXISTS table String;
                                """
        self.client.execute(alter_table_query)

        self.aggregate_column_values(source_table='table_dataset1_clean_dupl',
                                     value_column='uid',
                                     key_column='full_name_Dup',
                                     target_table='table_results',
                                     target_column='id_is1')



        # self.prepare_table_2(old_name='table_dataset2', new_tablename='table_dataset2_v1')
        table_dataset2_clean_dupl = self.preprocessor.get_dataframe_from_table('table_dataset2_v1', limit=100)
        # update_query = f"""
        #            ALTER TABLE table_dataset2_v1
        #            UPDATE email = address
        #            WHERE 1
        #         """
        # self.client.execute(update_query)
        # self.preprocessor.run(source_table="table_dataset2_v1", target_table="table_dataset2_clean")
        # table_dataset2_clean_dupl = self.preprocessor.get_dataframe_from_table('table_dataset2_clean_dupl', limit=10e9)


    def prepare_table_2(self, old_name='table_dataset2', new_tablename='table_dataset2_v1'):
        merge_t2 = ['first_name', 'middle_name', 'last_name']
        self.preprocessor.merge_columns(old_name,
                                        new_tablename,
                                        merge_t2,
                                        'full_name')
        self.preprocessor.add_column_with_unique_values(  new_tablename,
                                                         'email' )
        alter_table_query = f"""
            ALTER TABLE {new_tablename} 
            ADD COLUMN IF NOT EXISTS sex String DEFAULT 'na'
        """
        self.client.execute(alter_table_query)

    def manage_mutation(self):
        while True :  # Infinite loop
            if self.preprocessor.check_all_mutations() :  # Check if all mutations are finished
                print("All mutations are finished. Exiting loop.")
                break  # Exit the loop if mutations are finished
            else :
                print("Waiting for mutations to finish...")
                time.sleep(5)
    def get_data_from_table (self, table_name) :
        query = f"SELECT * FROM {table_name} LIMIT 1000"
        return self.client.execute(query)

    def aggregate_column_values (self,
                                 source_table,
                                 value_column,
                                 key_column,
                                 target_table,
                                 target_column) :
        # SQL query to aggregate data
        query = f"""
        SELECT {key_column}, groupArray({value_column}) AS aggregated_values
        FROM {source_table}
        GROUP BY {key_column}
        """

        # Execute aggregation query
        result = self.client.execute(query)

        # Insert aggregated data into the target table
        for row in result :
            key_value = row[0]
            aggregated_values = row[1]

            # Properly format UUID array for ClickHouse
            formatted_aggregated_values = [str(uuid) for uuid in aggregated_values]

            # Form insert query with correct formatting
            insert_query = f"""
            INSERT INTO {target_table} ({target_column})
            VALUES (%s)
            """

            # Execute the insertion query
            self.client.execute(insert_query, (formatted_aggregated_values,))

        print(f"Data aggregation and insertion into {target_table} completed.")
