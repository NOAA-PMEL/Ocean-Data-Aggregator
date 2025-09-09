from utils.aggregator import Aggregator
from utils.mat_file_processor import MatFileProcessor
from utils.netcdf_processor import NetcdfProcessor
from utils.cnv_processor import CnvProcessor
from pathlib import Path
import pandas as pd

# TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)


class MooringAggregator(Aggregator):

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)
        
        # For Ocean model data (.NC file)
        self.model_data_files = self.config_file['ocean_model_data']['model_nc_files']
        self.ocean_model_depth_var = self.config_file['ocean_model_data']['depth_variable_name']
        self.ocean_model_vert_dim_name = self.config_file['ocean_model_data']['vertical_dim_name']
        self.ocean_model_time_dim_name = self.config_file['ocean_model_data']['time_dim_name']
        # self.ocean_model_df = self.convert_ocean_model_nc_to_df()
        
        # For Mooring data derived from .mat files
        self.mooring_mat_files = self.config_file['mooring_mat_files']
        self.mooring_measurement_date_col = 'moor_measurement_datetime' # the name of the date column in the mooring data - creatd in the matFileProcessor
        self.mooring_station_id_col = 'moor_station_id' # the name of the station_id col in the mooring data
        # self.mooring_df = self.convert_mat_files_to_df()
        
        # For CTD data derived (can be .NC or .CNV)
        self.ctd_station_col = 'ctd_station_id' # From the netcdfProcessor and/or cnvProcessor (must be the same)
        self.ctd_date_col = 'ctd_time' # from the netcdfProcessor and/or cnvProcessor (must be the same)
        if self.config_file['ctd_data'].get('net_cdf_dir', None):
            self.ctd_nc_file_directory = Path(self.config_file['ctd_data']['net_cdf_dir'])
            self.ctd_df = self.convert_ctd_nc_files_to_df()
        elif self.config_file['ctd_data'].get('cnv_dir', None):
            self.ctd_cnv_file_directory = Path(self.config_file['ctd_data']['cnv_dir'])
            self.ctd_df = self.convert_ctd_cnv_files_to_df()

    def convert_mat_files_to_df(self) -> pd.DataFrame:
        # TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)
        """
        Converts the mooring .mat files associated with the Aggregator to pandas
        data frame (concats all .mat dfs together)
        """

        mooring_dfs = []
        for mat_file in self.mooring_mat_files:
            mat_processor = MatFileProcessor(
                sites=self.quag_station_sites, mat_file=mat_file)

            mooring_df = mat_processor.process_mat_data_like_ocnms()
            mooring_dfs.append(mooring_df)

        df = pd.concat(mooring_dfs, ignore_index=True)
        df = df.add_prefix('moor_')
        return df

    def convert_ctd_nc_files_to_df(self) -> pd.DataFrame:
        """
        Converts all the associated .nc files in the config.yaml into a data frame. Concats them all
        together to return one dataframe. Assumes that ctd files are all in the same directory.
        """
        # Recurseivly find all .nc files in the directory
        all_nc_files = list(self.ctd_nc_file_directory.rglob('*.nc'))

        # Filter the list of all_nc_files based on the station_ids
        nc_files_needed = [
            f for f in all_nc_files if any(station_id in str(f) for station_id in self.quag_station_sites)
        ]

        nc_dfs = []
        for nc_file in nc_files_needed:
            nc_processor = NetcdfProcessor(nc_file=nc_file)
            nc_df = nc_processor.convert_ctd_nc_to_df()
            nc_dfs.append(nc_df)

        df = pd.concat(nc_dfs, ignore_index=True)
        df = df.add_prefix('ctd_')
        return df

    def convert_ocean_model_nc_to_df(self) -> pd.DataFrame:

        nc_dfs = []
        for nc_file in self.model_data_files:
            nc_processor = NetcdfProcessor(nc_file=nc_file)
            nc_df = nc_processor.convert_rom_ocean_model_to_df(min_depth=self.quag_min_depth,
                                                               max_depth=self.quag_max_depth,
                                                               depth_var_name=self.ocean_model_depth_var,
                                                               vertical_dim_name=self.ocean_model_vert_dim_name,
                                                               time_dim_name=self.ocean_model_time_dim_name,
                                                               start_time=self.quag_min_date,
                                                               end_time=self.quag_max_date)
            nc_dfs.append(nc_df)

        df = pd.concat(nc_dfs, ignore_index=True)
        df.add_prefix('model_')
        return df

    def convert_ctd_cnv_files_to_df(self) -> pd.DataFrame:
        """
        Converts all the associated .cnv files in the config.yaml into a data frame. Concats them all
        together to return one dataframe. Assumes the ctd files are all in the same directory
        """
        all_cnv_files = list(self.ctd_cnv_file_directory.rglob('*.cnv'))

        cnv_dfs = []
        for cnv_file in all_cnv_files:
            cnv_processor = CnvProcessor(cnv_file=cnv_file, sites=self.quag_station_sites)
            cnv_df = cnv_processor.cnv_df
            cnv_dfs.append(cnv_df)
        
        df = pd.concat(cnv_dfs, ignore_index=False)
        df = df.add_prefix('ctd_')
        return df
    
    
    
    
    
    
    
    def merge_asof_by_station_and_date(self,
        left_df: pd.DataFrame, 
        right_df: pd.DataFrame, 
        left_date_col: str, 
        right_date_col: str, 
        left_station_id_col: str, 
        right_station_id_col: str, 
        tolerance: pd.Timedelta = None # like pd.Timedelta('1h')
    ) -> pd.DataFrame:

        """
        1st step: merge ctd with pps (PPS data is left hand side) by date and station_id. Merge by getting closest date in CTD data to PPS data.
        Grouped by station_id
        """
         # clean - drop na
        left_df_clean = left_df.dropna(subset=[left_station_id_col, left_date_col])
        rigt_df_clean = right_df.dropna(subset=[right_station_id_col, right_date_col])
        
        # sort
        left_sorted = left_df_clean.sort_values([left_station_id_col, left_date_col]).reset_index(drop=True)
        right_sorted = rigt_df_clean.sort_values([right_station_id_col, right_date_col]).reset_index(drop=True)

        # Ensure in datetime
        left_sorted[left_date_col] = pd.to_datetime(left_sorted[left_date_col])
        right_sorted[right_date_col] = pd.to_datetime(right_sorted[right_date_col])

        merged_list = []
        for station_id in left_sorted[left_station_id_col].unique():
            left_subset =left_sorted[left_sorted[left_station_id_col] == station_id]
            right_subset = right_sorted[right_sorted[right_station_id_col] == station_id]
            
            # Subsets are already sorted but re-sorting is a good practice for robustness
            left_subset = left_subset.sort_values(left_date_col)
            right_subset = right_subset.sort_values(right_date_col)
            
            merged_df = pd.merge_asof(
                left_subset,
                right_subset,
                left_on=left_date_col,
                right_on=right_date_col,
                direction='nearest',
                tolerance=tolerance # default None to just find closest date
            )
            merged_list.append(merged_df)
            
        merged_df = pd.concat(merged_list, ignore_index=True)

        return merged_df
    

    def merge_asof_by_station_event_and_date(self,
        left_df: pd.DataFrame, 
        right_df: pd.DataFrame, 
        left_date_col: str, 
        right_date_col: str, 
        left_station_id_col: str, 
        right_station_id_col: str, 
        left_event_col: str, 
        right_event_col: str,
        tolerance: pd.Timedelta = None 
    ) -> pd.DataFrame:
        """
        Merges two DataFrames by the closest date, grouped by station and event.

        This function uses a nested for-loop to iterate through each unique
        combination of station and event, and then performs a merge_asof on
        the date within that group.
        """
        
        # Step 1: Clean and Prepare Data
        # Drop NaNs from the key columns
        left_df_clean = left_df.dropna(subset=[left_station_id_col, left_date_col, left_event_col])
        right_df_clean = right_df.dropna(subset=[right_station_id_col, right_date_col, right_event_col])
        
        # Ensure date columns are in datetime format
        left_df_clean[left_date_col] = pd.to_datetime(left_df_clean[left_date_col])
        right_df_clean[right_date_col] = pd.to_datetime(right_df_clean[right_date_col])

        # Ensure grouping columns have the same type (e.g., convert to string)
        left_df_clean[left_station_id_col] = left_df_clean[left_station_id_col].astype(str)
        right_df_clean[right_station_id_col] = right_df_clean[right_station_id_col].astype(str)
        left_df_clean[left_event_col] = left_df_clean[left_event_col].astype(str)
        right_df_clean[right_event_col] = right_df_clean[right_event_col].astype(str)

        # Step 2: Sort the DataFrames once to optimize subsetting
        # This sort is on ALL grouping columns plus the date. It's not a required sort for the merge_asof
        # call itself in this approach, but it makes the subsequent subsetting faster.
        sort_columns = [left_station_id_col, left_event_col, left_date_col]
        left_sorted = left_df_clean.sort_values(sort_columns).reset_index(drop=True)
        
        right_sort_columns = [right_station_id_col, right_event_col, right_date_col]
        right_sorted = right_df_clean.sort_values(right_sort_columns).reset_index(drop=True)
        
        merged_list = []
        
        # Step 3: Loop through unique station IDs and then events
        for station_id in left_sorted[left_station_id_col].unique():
            # Subset both DataFrames by station ID
            left_station_subset = left_sorted[left_sorted[left_station_id_col] == station_id]
            right_station_subset = right_sorted[right_sorted[right_station_id_col] == station_id]
            
            for event_number in left_station_subset[left_event_col].unique():
                # Subset the station-specific DataFrames by event number
                left_subset = left_station_subset[left_station_subset[left_event_col] == event_number]
                right_subset = right_station_subset[right_station_subset[right_event_col] == event_number]

                # Critical: Ensure the subsets are sorted by the date column just before the merge.
                # This is the step that resolves the "left keys must be sorted" error.
                left_subset = left_subset.sort_values(left_date_col)
                right_subset = right_subset.sort_values(right_date_col)
                
                if not left_subset.empty and not right_subset.empty:
                    merged_df = pd.merge_asof(
                        left_subset,
                        right_subset,
                        left_on=left_date_col,
                        right_on=right_date_col,
                        direction='nearest',
                        tolerance=tolerance
                    )
                    merged_list.append(merged_df)
        
        if merged_list:
            final_merged_df = pd.concat(merged_list, ignore_index=True)
        else:
            # Handle the case where no matches were found
            final_merged_df = pd.DataFrame(columns=left_df.columns.tolist() + right_df.columns.tolist())

        return final_merged_df
    
    def merge_pps_with_quag(self):

        quag_pps_df = self.merge_asof_by_station_event_and_date(
            left_df=self.quagmire_df,
            right_df=self.pps_df,
            left_date_col=self.quag_utc_date_time_col,
            right_date_col=self.pps_date_col,
            left_station_id_col=self.quag_sit_col_name,
            right_station_id_col=self.pps_station_id_col, 
            left_event_col=self.quag_rosette_pos_col,
            right_event_col=self.pps_event_col_name
        )

        quag_pps_df['pps_quag_time_difference'] = quag_pps_df[self.pps_date_col] - quag_pps_df[self.quag_utc_date_time_col]
        quag_pps_df.to_csv('quag_pps_merge.csv', index=False)
    
    def merge_ctd_with_quag(self):
        
        quag_ctd_df = self.merge_asof_by_station_and_date(
            left_df=self.quagmire_df,
            right_df=self.ctd_df,
            left_date_col=self.quag_utc_date_time_col,
            right_date_col=self.ctd_date_col,
            left_station_id_col=self.quag_sit_col_name,
            right_station_id_col=self.ctd_station_col
        )

        quag_ctd_df[self.quag_utc_date_time_col] = pd.to_datetime(quag_ctd_df[self.quag_utc_date_time_col])
        quag_ctd_df[self.ctd_date_col] = pd.to_datetime(quag_ctd_df[self.ctd_date_col])
        quag_ctd_df['time_difference'] = quag_ctd_df[self.ctd_date_col] - quag_ctd_df[self.quag_utc_date_time_col]
        quag_ctd_df.to_csv('quag_ctd_cnv_merge.csv', index=False)

        return quag_ctd_df
    
    