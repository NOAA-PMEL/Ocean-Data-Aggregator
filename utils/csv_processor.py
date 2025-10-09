import pandas as pd

class CsvProcessor:
    """
    Used generally to load CTD, Bottle, and Nutrient CSVs into a dataframe
    """

    def __init__(self, csv_file_path, header: int, cast_num_col_name: str = None, 
                 cast_val_str_to_remove: str = None, unit_row: int = None, 
                 cols_to_group_and_avg: list = None):
        """
        csv_file_path: path to the csv file
        header: the row number of the column names
        unit_row: optional the row number of the units
        cast_num_col_name: The name of the column for cast number. Optional - only needed if cast values need processing
        cast_val_str_to_remove: A common string to remove in cast values. Optional - only needed if cast values need string removal
        cols_to_group_and_avg: optional list of column in the csv that need to be grouped 
            by and averages of other columns calculated. Only if desired (e.g. for 
            the SKQ2115S cruise)
        """
        self.csv_file_path = csv_file_path
        self.header = header
        self.unit_row = unit_row
        self.cast_num_col_name = cast_num_col_name
        self.cast_val_strs_to_remove = cast_val_str_to_remove
        self.cols_to_group_and_avg = cols_to_group_and_avg
        self.df = self.process_csv()

    def process_csv(self):
        """
        Loads the csv as a dataframe, and if applicable, 
        takes care of duplicate rows on specified column names
        and calculates the average
        """

        # Load csv as df
        df = self.load_csv_as_df()

        # Fix cast column values if necessary
        if self.cast_val_strs_to_remove:
            df[self.cast_num_col_name] = df[self.cast_num_col_name].apply(self.fix_cast_cols)
        
        if self.cols_to_group_and_avg:
            updated_df = self.group_and_avg_rows(df=df)
            return updated_df
        else:
            return df
    
    def load_csv_as_df(self):
        """
        Loads csv file as a data frame. Adding units to column names, if the
        units exist in a seperate row
        """
        
        # If unit row is included, get the units and append to column names
        if self.unit_row:
            df = pd.read_csv(self.csv_file_path, header=None)
            headers = df.iloc[self.header].values
            units = df.iloc[self.unit_row].values

            combined_header_unit_cols = [f"{h}.{u}" if pd.notna(u) and str(u).strip() else str(h) for h, u in zip(headers, units)]
            # Update column names in the group columns to average list if applicable and cast column name if applicable
            if self.cols_to_group_and_avg:
                self.cols_to_group_and_avg= self.update_grouping_col_names(combined_header_unit_cols=combined_header_unit_cols)
            if self.cast_num_col_name:
                self.cast_num_col_name = self.update_cast_col_name(combined_header_unit_cols=combined_header_unit_cols)

            data_start_row = max(self.header, self.unit_row) + 1
            df_data = df.iloc[data_start_row:].reset_index(drop=True)
            df_data.columns = combined_header_unit_cols

            return df_data
        else:
            return pd.read_csv(self.csv_file_path, header=self.header)

    def group_and_avg_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes a list of columns to group a df by, and calculates 
        the average of the numeric columns or takes the first value
        of string columns. This is used if there are duplicate values
        for fields that are later used to merge on. See SKQ2115S as an 
        example.
        """

        df_clean = df.drop_duplicates()
        avgd_df = df_clean .groupby(self.cols_to_group_and_avg).agg({
            **{col: 'mean' for col in df.select_dtypes(include='number').columns},
            **{col: 'first' for col in df.select_dtypes(include='object').columns},
            **{col: 'first' for col in df.select_dtypes(include='category').columns}
        }).reset_index(drop=True)  

        return avgd_df
    
    def update_grouping_col_names(self, combined_header_unit_cols: list) -> list:
        """
        Update the grouping col names if unit_rows were added to 
        their names.
        """
        updated_list = []
        for item1 in self.cols_to_group_and_avg:
            matched_item = next(
                (item2 for item2 in combined_header_unit_cols if item2.startswith(item1) and len(item2)> len(item1)),
            )
            updated_list.append(matched_item)

        return updated_list

    def update_cast_col_name(self, combined_header_unit_cols: list) -> str:
        """
        Updates the cast column name if it the column was updated with units
        """
        matched_cast_col_name = next(
            (item for item in combined_header_unit_cols if item.startswith(self.cast_num_col_name)),
            self.cast_num_col_name)
        return matched_cast_col_name

    def fix_cast_cols(self, cast_val: str) -> int:
        """
        Fixes the cast col if it is formatted weird, to just get the cast 
        number. Takes a list of strings to remove from the cast value and returns
        the integer value. TODO: May need to expand the functionality for this
        for other edge cases
        """
        for str_to_remove in self.cast_val_strs_to_remove:
            cast_val = cast_val.replace(str_to_remove, '')
        return int(cast_val)





