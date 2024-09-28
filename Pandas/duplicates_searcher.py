import pandas as pd


class DuplicatesSearcher:
    def __init__(self, df, exclude_columns=None, uid_column='uid'):
        self.df = df
        # Колонка, которая служит уникальным идентификатором для строк
        self.uid_column = uid_column
        # Исключённые колонки
        self.exclude_columns = exclude_columns if exclude_columns else []

    # Внутреннее сравнение дубликатов по каждой колонке, кроме указанных в exclude_columns
    def find_duplicates(self):
        duplicates = {}
        for column in self.df.columns:
            if column not in self.exclude_columns and column != self.uid_column:
                # Находим дубликаты по текущей колонке
                dup_mask = self.df.duplicated(subset=[column], keep=False)
                duplicates_in_column = self.df[dup_mask]
                if not duplicates_in_column.empty:
                    duplicates[column] = duplicates_in_column
        return duplicates

    # Сравнение дубликатов между двумя таблицами по каждой колонке, кроме указанных в exclude_columns
    def find_duplicates_between(self, other_df):
        results = []
        # Находим общие колонки для сравнения, исключая указанные
        common_columns = self.df.columns.intersection(other_df.columns).difference(self.exclude_columns)
        for column in common_columns:
            if column != self.uid_column:
                # Объединяем таблицы по текущей колонке, сохраняем уникальные ID строк
                merged_df = pd.merge(
                    self.df[[self.uid_column, column]],
                    other_df[[self.uid_column, column]],
                    on=column,
                    how='inner',
                    suffixes=('_self', '_other')
                )
                if not merged_df.empty:
                    merged_df['matching_column'] = column
                    results.append(
                        merged_df[[f'{self.uid_column}_self', f'{self.uid_column}_other', 'matching_column']])

        # Объединяем все результаты в один DataFrame
        if results:
            final_result = pd.concat(results, ignore_index=True)
        else:
            final_result = pd.DataFrame(
                columns=[f'{self.uid_column}_self', f'{self.uid_column}_other', 'matching_column'])

        return final_result

    # Таблица дубликатов между двумя таблицами
    def build_duplicates_table_between(self, other_df):
        duplicates_between = self.find_duplicates_between(other_df)
        return duplicates_between

    def display_duplicates_between(self, other_df):
        duplicates_between_table = self.build_duplicates_table_between(other_df)
        return duplicates_between_table
