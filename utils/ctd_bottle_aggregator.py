from utils.aggregator import Aggregator
from utils.csv_processor import CsvProcessor
import pandas as pd


class CtdBottleAggregator(Aggregator):

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)

        # CTD data
        if 'ctd_data' in self.config_file:
            self.ctd_file_info = self.config_file.get('ctd_data', None)
            self.ctd_cast_col_name = self.ctd_file_info.get('cast_num_col_name', None)
            self.ctd_pressure_col_name = self.ctd_file_info.get('pressure_col_name', None)
            self.ctd_df = self.get_ctd_df_from_csv()

        # Bottle data
        if 'bottle_data' in self.config_file:
            self.btl_file_info = self.config_file.get('bottle_data', None)
            self.btl_cast_col_name = self.btl_file_info.get('cast_num_col_name', None)
            self.btl_pressure_col_name = self.btl_file_info.get('pressure_col_name', None)
            self.btl_bottle_col_name = self.btl_file_info.get('bottle_num_col_name', None)
            self.btl_df = self.get_btl_df_from_csv()

        # Nutrient data
        if 'nutrient_data' in self.config_file:
            self.nutr_file_info = self.config_file.get('nutrient_data', None)
            self.nutr_cast_col_name = self.nutr_file_info.get('cast_num_col_name', None)
            self.nutr_pressure_col_name = self.nutr_file_info.get('pressure_col_name', None)
            self.nutr_df = self.get_nutrient_df_from_csv()

        # Update column names (since units and prefixes get added to them)
        self.update_attribute_cols()

    def FINALmerge_quag_ctd_btl_nutrient_for_missing_btlNumbers(self) -> pd.DataFrame:
        """
        Merges a quag, bottle, ctd, and nutrient dataframe together.
        Will first merge the bottle and ctd together on cast and pressure (rounded). 
        Then merge the output df with the quag on Cast and rosette position (keeping rows 
        from quag), then merge the nutrient on rosette position and cast number. This was
        used for the SKQ2115S cruise where botlle number was not always available (completely
        missing from CTD data).
        """

        # Step 1 Merge btl and ctd data frames
        ctd_btl_combined = self.merge_bottle_ctd_on_cast_pressure()
  
        # Step 2 Merge df from above with quag
        ctd_btl_quag_combined = self.merge_quag_OtherBottleDf_on_cast_rosette(df=ctd_btl_combined)

        # concat empty depth value columns back on
        final_df = self.merge_OtherQuagDf_nutr_on_cast_nearest_depth(df=ctd_btl_quag_combined)

        return final_df

    def FINALmerge_quag_btl_nutrient(self) -> pd.DataFrame:
        """
        Merges the quag with the bottle on Cast number and bottle number.
        Then merges the nutrient data with output df on Cast and presser/depth
        """
        # Step 1 Merge quag and btl data frames on cast and rosette/bottle number
        quag_btl_combined = self.merge_quag_OtherBottleDf_on_cast_rosette(df=self.btl_df)

        # Step 2 Merge quag_btl_combined with nutrient on cast and depth/pressure
        final_df = self.merge_OtherQuagDf_nutr_on_cast_nearest_depth(df=quag_btl_combined)

        return final_df
    
    def get_ctd_df_from_csv(self) -> pd.DataFrame:
        """
        Get the ctd df and prepend columns with ctd_
        """
        if self.ctd_file_info:
            ctd_csv_processor = CsvProcessor(csv_file_path=self.ctd_file_info.get('ctd_csv'),
                                             header=self.ctd_file_info.get('header_row'),
                                             cast_num_col_name=self.ctd_cast_col_name, 
                                             cast_val_str_to_remove=self.ctd_file_info.get('cast_val_str_to_remove', None),
                                             unit_row=self.ctd_file_info.get('unit_row', None),
                                             cols_to_group_and_avg=self.ctd_file_info.get('group_by_cols_to_average', None))
            
            df = ctd_csv_processor.df.add_prefix('ctd_')
            return df
        else:
            return None
        
    def get_btl_df_from_csv(self) -> pd.DataFrame:
        """
        Get the bottle df and prepend columns with btl_
        """
        if self.btl_file_info:
            btl_csv_processor = CsvProcessor(csv_file_path=self.btl_file_info.get('bottle_csv'),
                                             header = self.btl_file_info.get('header_row'),
                                             cast_num_col_name=self.btl_cast_col_name, 
                                             cast_val_str_to_remove=self.btl_file_info.get('cast_val_str_to_remove', None),
                                             unit_row=self.btl_file_info.get('unit_row', None),
                                             cols_to_group_and_avg=self.btl_file_info.get('group_by_cols_to_average', None))
            
            df = btl_csv_processor.df.add_prefix('btl_')
            return df
        else:
            return None
        
    def get_nutrient_df_from_csv(self) -> pd.DataFrame:
        """
        Get the nutrient df from a csv and prepend columns with nutr_
        """
        if self.nutr_file_info:
            nutr_csv_processor = CsvProcessor(csv_file_path=self.nutr_file_info.get('nutrient_csv'),
                                              header=self.nutr_file_info.get('header_row'),
                                              cast_num_col_name=self.nutr_cast_col_name, 
                                              cast_val_str_to_remove=self.btl_file_info.get('cast_val_str_to_remove', None),
                                              unit_row= self.nutr_file_info.get('unit_row', None),
                                              cols_to_group_and_avg=self.nutr_file_info.get('group_by_cols_to_average', None))
            
            df = nutr_csv_processor.df.add_prefix('nutr_')
            return df
        else:
            return None

    def update_attribute_cols(self):
        """
        Column names that are also attributes as part of this class will need to be updated 
        as the columns get the file type prepended and sometimes the units added. This
        method updates the attributes to the new column names. It also updates the datatypes
        for rosette/bottle/cast to int.
        """
        # ctd_data
        if 'ctd_data' in self.config_file:
            ctd_cols = [col.replace('ctd_', '') for col in self.ctd_df.columns]
            self.ctd_cast_col_name = self.find_matching_col(old_col_name=self.ctd_cast_col_name, new_cols=ctd_cols, prefix='ctd_')
            self.ctd_pressure_col_name = self.find_matching_col(old_col_name=self.ctd_pressure_col_name, new_cols=ctd_cols, prefix='ctd_')

        # Bottle data
        if 'bottle_data' in self.config_file:
            btl_cols = [col.replace('btl_', '') for col in self.btl_df.columns]
            self.btl_cast_col_name = self.find_matching_col(old_col_name=self.btl_cast_col_name, new_cols=btl_cols, prefix='btl_')
            self.btl_pressure_col_name = self.find_matching_col(old_col_name=self.btl_pressure_col_name, new_cols=btl_cols, prefix='btl_')
            self.btl_bottle_col_name = self.find_matching_col(old_col_name=self.btl_bottle_col_name, new_cols=btl_cols, prefix='btl_')

        # Nutrient data
        if 'nutrient_data' in self.config_file:
            nutr_cols = [col.replace('nutr_', '') for col in self.nutr_df.columns]
            self.nutr_cast_col_name = self.find_matching_col(old_col_name=self.nutr_cast_col_name, new_cols=nutr_cols, prefix='nutr_')
            self.nutr_pressure_col_name = self.find_matching_col(old_col_name=self.nutr_pressure_col_name, new_cols=nutr_cols, prefix='nutr_')

        # Update col dtypes for cast/rosette/btle
        self.update_cast_bottle_roseette_col_dtypes()

    def find_matching_col(self, old_col_name: str, new_cols: list, prefix: str) -> str:
       
        matched_col_name = next(
            (item for item in new_cols if item.startswith(old_col_name)),
            old_col_name)
        return f'{prefix}{matched_col_name}'
    
    def update_cast_bottle_roseette_col_dtypes(self):
        """
        Updates the cast bottle data types of all dataframes so they can be easily merged
        later
        """

        # Nutrient
        if 'nutrient_data' in self.config_file:
            self.nutr_df[self.nutr_cast_col_name] = self.nutr_df[self.nutr_cast_col_name].astype('Int64')
            self.nutr_df[self.nutr_pressure_col_name] = self.nutr_df[self.nutr_pressure_col_name].astype(float)

        # Bottle
        if 'bottle_data' in self.config_file:
            self.btl_df[self.btl_cast_col_name] = self.btl_df[self.btl_cast_col_name].astype('Int64')
            self.btl_df[self.btl_bottle_col_name] = self.btl_df[self.btl_bottle_col_name].astype(float).astype('Int64')

        # CTD
        if 'ctd_data' in self.config_file:
            self.ctd_df[self.ctd_cast_col_name] = self.ctd_df[self.ctd_cast_col_name].astype('Int64')

    def merge_bottle_ctd_on_cast_pressure(self) -> pd.DataFrame:
        """
        Merges teh bottle file with the ctd file based on pressure and
        cast. The pressure is rounded. This assumes its step 1 of a merge 
        process as it takes the bottle df and ctd df as are.
        """

        # round pressure columns in btl and ctd
        self.btl_df['btl_pressure_rounded'] = pd.to_numeric(self.btl_df[self.btl_pressure_col_name],
                                                            errors='coerce').round()
        self.ctd_df['ctd_pressure_rounded'] = pd.to_numeric(self.ctd_df[self.ctd_pressure_col_name],
                                                            errors='coerce').round()
        
        # Step 1 Merge btl and ctd data frames
        ctd_btl_combined = self.btl_df.merge(
            self.ctd_df,
            how='left',
            left_on=[self.btl_cast_col_name, 'btl_pressure_rounded'],
            right_on=[self.ctd_cast_col_name, 'ctd_pressure_rounded']
        )

        return ctd_btl_combined
    
    def merge_quag_OtherBottleDf_on_cast_rosette(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge the quag with a data frame that has bottle fields in it - like the ctd and bottle
        file combined on cast and rosette position or bottle number. This assumes a later step 
        in the merge process, as it has an input of a dataframe.
        """
        df_combined = self.quagmire_df.merge(
            df, 
            how='left',
            left_on=[self.quag_cast_col, self.quag_rosette_pos_col],
            right_on=[self.btl_cast_col_name, self.btl_bottle_col_name]
            
        )

        return df_combined
    
    def merge_OtherQuagDf_nutr_on_cast_nearest_depth(self, df = pd.DataFrame) -> pd.DataFrame:
        """
        Merges a dataframe that's been combined with the quag on the depth/pressure
        column of the nutrient dataframe. Assumes the quag has been combined with 
        another df already.
        """
        # separate out quag values with null depths (which would just be field negatives)
        quag_with_depth = df.dropna(subset=[self.quag_depth_col]).copy()
        quag_without_depth = df[df[self.quag_depth_col].isna()].copy()
        
        # sort values
        quag_with_depth.sort_values(by=self.quag_depth_col, inplace=True)
        self.nutr_df.sort_values(by=self.nutr_pressure_col_name, inplace=True)
        final_df = pd.merge_asof(
            quag_with_depth,
            self.nutr_df,
            left_on=self.quag_depth_col,
            right_on=self.nutr_pressure_col_name,
            left_by=self.quag_cast_col,
            right_by=self.nutr_cast_col_name, 
            direction='nearest',
            tolerance=3.0
        )

        # concat empty depth value columns back on
        final_df = pd.concat([final_df, quag_without_depth], ignore_index=True)

        return final_df
    
