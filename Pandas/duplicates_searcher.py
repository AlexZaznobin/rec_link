import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
from datetime import datetime
from IPython.display import display
import networkx as nx
import re


class DuplicatesSearcher:
    def __init__(self, df, exclude_columns=None, uid_column='uid'):
        self.df = df
        # Колонка, которая служит уникальным идентификатором для строк
        self.uid_column = uid_column
        # Исключённые колонки
        self.exclude_columns = exclude_columns if exclude_columns else []

    # Внутреннее сравнение дубликатов по каждой колонке, кроме указанных в exclude_columns
    def find_duplicates(self):
        # Initialize an empty list to store dictionaries of duplicate pairs
        duplicates_list = []
        result = pd.DataFrame()
        # Iterate through columns, excluding the ones that should be ignored
        for column in self.df.columns:
            print(column)
            if column not in self.exclude_columns and column != self.uid_column:
                # Get the rows that have duplicates in the current column
                dup_mask = self.df[self.df.duplicated(subset=[column], keep=False)]
                print(dup_mask)
                # Group the rows by the duplicated value in the current column
                for value, group in dup_mask.groupby(column):
                    # Get the UIDs of the duplicates
                    uids = group[self.uid_column].values
                    # Create all combinations of duplicate UIDs
                    for i in range(len(uids)):
                        for j in range(i + 1, len(uids)):
                            duplicates_list.append({
                                'uid_self': uids[i],
                                'uid_other': uids[j],
                                'matching_column': column})
                duplicates = pd.DataFrame(duplicates_list)
                duplicates = duplicates.drop_duplicates(subset=['uid_self', 'uid_other'], keep='first')

        print(result)
        result = pd.concat([result, duplicates], ignore_index=True)
        result = result.drop_duplicates(subset=['uid_self', 'uid_other'], keep='first')
        print(result)
        return result

    def build_connected_components(self, duplicates_df):
        # Create a graph
        G = nx.Graph()

        # Add edges to the graph where 'uid_self' and 'uid_other' are connected
        for _, row in duplicates_df.iterrows():
            G.add_edge(row['uid_self'], row['uid_other'], column=row['matching_column'])

        # Find the connected components (groups of UIDs)
        components = list(nx.connected_components(G))

        # Convert the connected components into a DataFrame
        extended_df = pd.DataFrame([list(component) for component in components])

        # Set column names dynamically based on the maximum number of UIDs in a component
        max_len = extended_df.shape[1]
        extended_df.columns = [f'ID_{i+1}' for i in range(max_len)]
        print(extended_df)
        return extended_df

    # Сравнение дубликатов между двумя таблицами по каждой колонке, кроме указанных в exclude_columns
    def find_duplicates_between(self, other_df):
        results = []
        # Находим общие колонки для сравнения, исключая указанные
        common_columns = self.df.columns.intersection(other_df.columns).difference(self.exclude_columns)
        for column in common_columns:
            if column != self.uid_column:
                # Объединяем таблицы по текущей колонке, сохраняем уникальные ID строк и совпавшие значения
                merged_df = pd.merge(
                    self.df[[self.uid_column, column]],
                    other_df[[self.uid_column, column]],
                    on=column,
                    how='inner',
                    suffixes=('_self', '_other')
                )
                if not merged_df.empty:
                    merged_df['matching_column'] = column
                    results.append(merged_df[[f'{self.uid_column}_self', f'{self.uid_column}_other',
                                              'matching_column']])

        # Объединяем все результаты в один DataFrame
        if results:
            final_result = pd.concat(results, ignore_index=True)
        else:
            final_result = pd.DataFrame(columns=[f'{self.uid_column}_self', f'{self.uid_column}_other',
                                                 'matching_column'])

        final_result = final_result.drop_duplicates(subset=['uid_self', 'uid_other'], keep='first')
        return final_result

    # Таблица дубликатов между двумя таблицами
    def build_duplicates_table_between(self, other_df):
        duplicates_between = self.find_duplicates_between(other_df)
        return duplicates_between

    def display_duplicates_between(self, other_df):
        duplicates_between_table = self.build_duplicates_table_between(other_df)
        return duplicates_between_table
