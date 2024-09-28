from Levenshtein import distance as levenshtein_distance
import pandas as pd

class DuplicatesSearcher:
    def __init__(self, df, threshold=3):
        self.df = df
        self.threshold = threshold

    # in table comparison
    def find_duplicates(self, column):
        duplicates = []
        for i in range(len(self.df)):
            for j in range(i + 1, len(self.df)):
                dist = levenshtein_distance(str(self.df.iloc[i][column]), str(self.df.iloc[j][column]))
                if dist <= self.threshold:
                    duplicates.append((i, j, dist))
        return duplicates

    # comparison between tables
    def find_duplicates_between(self, other_df, column):
        duplicates = []
        for i in range(len(self.df)):
            for j in range(len(other_df)):
                dist = levenshtein_distance(str(self.df.iloc[i][column]), str(other_df.iloc[j][column]))
                if dist <= self.threshold:
                    duplicates.append((i, j, dist))
        return duplicates

    # table of duplicates within one table
    def build_duplicates_table(self, columns):
        duplicate_rows = set()
        for column in columns:
            duplicates = self.find_duplicates(column)
            for i, j, _ in duplicates:
                duplicate_rows.add(i)
                duplicate_rows.add(j)

        return self.df.loc[list(duplicate_rows)]

    # table of duplicates between two tables
    def build_duplicates_table_between(self, other_df, columns):
        duplicate_rows_self = set()
        duplicate_rows_other = set()

        for column in columns:
            duplicates = self.find_duplicates_between(other_df, column)
            for i, j, _ in duplicates:
                duplicate_rows_self.add(i)
                duplicate_rows_other.add(j)

        return (self.df.loc[list(duplicate_rows_self)], other_df.loc[list(duplicate_rows_other)])

    def display_duplicates(self, columns):
        duplicates_table = self.build_duplicates_table(columns)
        return duplicates_table

    def display_duplicates_between(self, other_df, columns):
        df_duplicates, other_df_duplicates = self.build_duplicates_table_between(other_df, columns)
        return df_duplicates, other_df_duplicates
