from utils.aggregator import Aggregator
from utils.mat_file_processor import MatFileProcessor
from utils.netcdf_processor import NetcdfProcessor
from utils.cnv_processor import CnvProcessor
from pathlib import Path
import pandas as pd

# TODO: update the merge_ctd_with_quag function to have a tolerance of '1H' (check with Zack) after running the OCNMS code (needs to be adjustable for the code)


class MooringAggregator(Aggregator):

    OCEAN_MODEL_STATION_COL = "model_station" # The name of the station col in the ocean model data (added in the netcdf_processor function)
    MOORING_STATION_ID_COL = 'moor_station_id' # the name of the station_id col in the mooring data
    CTD_STATION_COL = 'ctd_station_id' # From the netcdfProcessor and/or cnvProcessor (must be the same)
    CTD_DATE_COL = 'ctd_time' # from the netcdfProcessor and/or cnvProcessor (must be the same)

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)

        # For Mooring data derived from .mat files
        self.mooring_mat_dir = Path(self.config_file['mooring_info']['mooring_data_dir'])
        self.mooring_measurement_date_col = 'moor_measurement_datetime' # the name of the date column in the mooring data - creatd in the matFileProcessor
        self.mooring_df = self.convert_mat_files_to_df()
        
        # For CTD data derived (can be .NC or .CNV)
        if self.config_file.get('ctd_data', None):
            self.ctd_quag_merge_tolerance = self.config_file['ctd_data'].get('ctd_quag_merge_tolerance', None)
            if self.config_file['ctd_data'].get('net_cdf_dir', None):
                self.ctd_nc_file_directory = Path(self.config_file['ctd_data']['net_cdf_dir'])
                self.ctd_df = self.convert_ctd_nc_files_to_df()
            elif self.config_file['ctd_data'].get('cnv_dir', None):
                self.ctd_cnv_file_directory = Path(self.config_file['ctd_data']['cnv_dir'])
                self.ctd_df = self.convert_ctd_cnv_files_to_df()

        # For Ocean model data (.NC file)
        self.model_data_files = self.config_file['ocean_model_data']['model_nc_files']
        self.ocean_model_depth_var = self.config_file['ocean_model_data']['depth_variable_name']
        self.ocean_model_time_dim_name = self.config_file['ocean_model_data']['time_dim_name']
        self.ocean_model_df = self.convert_ocean_model_nc_to_df()

    def convert_mat_files_to_df(self) -> pd.DataFrame:
        # TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)
        """
        Converts the mooring .mat files associated with the Aggregator to pandas
        data frame (concats all .mat dfs together)
        """

        all_mat_files = list(self.mooring_mat_dir.rglob('*.mat'))
        mooring_dfs = []
        for mat_file in all_mat_files:
            mat_processor = MatFileProcessor(
                sites=self.quag_station_sites, mat_file=mat_file)

            mooring_df = mat_processor.process_mat_data_like_ocnms()
            mooring_dfs.append(mooring_df)

        df = pd.concat(mooring_dfs, ignore_index=True)

        df = df.add_prefix('moor_')

        # Took out timezone update because will just merge on local time
        # updated_dates_df = self.update_moor_timezone_to_utc(df=df, column_names=['moor_deployment_time', self.mooring_measurement_date_col])
        return df

    # def update_moor_timezone_to_utc(self, df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    #     """
    #     Update the timezone fo the mooring file datetime columns to UTC.
    #     """
    #     for col in column_names:
    #         df[col] = pd.to_datetime(df[col])
    #         df[col] = df[col].dt.tz_localize(self.moor_time_zone)
    #         df[col] = df[col].dt.tz_convert('UTC')
    #         df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S%z') # conver to isoformat

    #     return df
    
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
            # Get station applicable to file
            for station in self.quag_station_sites:
                if station in nc_file:
                    station = station
            nc_processor = NetcdfProcessor(nc_file=nc_file)
            nc_df = nc_processor.convert_rom_ocean_model_to_df(min_depth=self.quag_min_depth,
                                                               max_depth=self.quag_max_depth,
                                                               depth_var_name=self.ocean_model_depth_var,
                                                               time_dim_name=self.ocean_model_time_dim_name,
                                                               start_time=self.quag_min_date,
                                                               end_time=self.quag_max_date, 
                                                               station=station)
            nc_dfs.append(nc_df)

        df = pd.concat(nc_dfs, ignore_index=True)
        df = df.add_prefix('model_')
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

        # Change date columns back to format we want:
        merged_df[left_date_col] = merged_df[left_date_col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        merged_df[right_date_col] = merged_df[right_date_col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        return merged_df
    
    
    def merge_ctd_with_quag(self):
        """
        Merge the Quagmire with the CTD. when calling the merge_asof_by_station_and_date function
        """
        
        quag_ctd_df = self.merge_asof_by_station_and_date(
            left_df=self.quagmire_df,
            right_df=self.ctd_df,
            left_date_col=self.quag_utc_date_time_col,
            right_date_col=self.CTD_DATE_COL,
            left_station_id_col=self.quag_site_col_name,
            right_station_id_col=self.CTD_STATION_COL,
            tolerance=self.ctd_quag_merge_tolerance
        )

        # calculate the time difference between the quagmire and the CTD.
        quag_ctd_df[self.quag_utc_date_time_col] = pd.to_datetime(quag_ctd_df[self.quag_utc_date_time_col])
        quag_ctd_df[self.CTD_DATE_COL] = pd.to_datetime(quag_ctd_df[self.CTD_DATE_COL])
        quag_ctd_df['ctd_quag_time_difference'] = quag_ctd_df[self.CTD_DATE_COL] - quag_ctd_df[self.quag_utc_date_time_col]

        # Change date col formats back
        quag_ctd_df[self.quag_utc_date_time_col] = quag_ctd_df[self.quag_utc_date_time_col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        quag_ctd_df[self.CTD_DATE_COL] = quag_ctd_df[self.CTD_DATE_COL].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        quag_ctd_df.to_csv('quag_ctd_cnv_merge.csv', index=False)

        return quag_ctd_df
    


    def merge_moor_quag_on_station_localtime(self, quag_df: pd.DataFrame):
        """
        Merge mooring data with quagmire data on station and closest time (within one hour). 
        Time is merged assuming local time. Are .mat files always in local time?
        Takes quag_df as an input because quag_df could be the self.quagmire_df already
        merged with another data type
        """
        # Sort quag_df and mooring_df by the 'on' key first which is time, then the 'by' key which is station
        quag_df_sorted = quag_df.sort_values([self.quag_local_date_time_col, self.quag_site_col_name])
        moor_df_sorted = self.mooring_df.sort_values([self.mooring_measurement_date_col, self.MOORING_STATION_ID_COL])

        # Convert columns to the same types
        quag_df_sorted[self.quag_local_date_time_col] = pd.to_datetime(quag_df_sorted[self.quag_local_date_time_col])
        moor_df_sorted[self.mooring_measurement_date_col] = pd.to_datetime(moor_df_sorted[self.mooring_measurement_date_col])
        quag_df_sorted[self.quag_site_col_name] = quag_df_sorted[self.quag_site_col_name].astype(str)
        moor_df_sorted[self.MOORING_STATION_ID_COL] = moor_df_sorted[self.MOORING_STATION_ID_COL].astype(str)
    
        result = pd.merge_asof(
            quag_df_sorted,
            moor_df_sorted,
            left_on = self.quag_local_date_time_col,
            right_on = self.mooring_measurement_date_col,
            left_by = self.quag_site_col_name,
            right_by = self.MOORING_STATION_ID_COL,
            direction = 'nearest', 
            tolerance = pd.Timedelta('1h')
        )

        return result
    
    def merge_oceanmodel_quag_on_station_utctime(self, quag_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge ocean model data with quagmire data on station and closest time (within one hour). 
        Time is merged assuming UTC time. Takes quag_df as an input because quag_df could be 
        the self.quagmire_df already merged with another data type. Didn't merge on depth because
        Ocean model data is filtered to be in range of min/max depth of quag (to the nearest 5).
        Assumes Ocean Model data is in UTC time.
        """
        ocean_model_time_col = f"model_{self.ocean_model_time_dim_name}" # col name has model prepended now.
       
        # Sort quag_df and mooring_df by the 'on' key first which is time, then the 'by' key which is station
        quag_df_sorted = quag_df.sort_values([self.quag_utc_date_time_col, self.quag_site_col_name])
        ocean_model_df_sorted = self.ocean_model_df.sort_values([ocean_model_time_col, self.OCEAN_MODEL_STATION_COL])

        # Convert columns to the same types
        quag_df_sorted[self.quag_utc_date_time_col] = pd.to_datetime(quag_df_sorted[self.quag_utc_date_time_col])
        ocean_model_df_sorted[ocean_model_time_col] = pd.to_datetime(ocean_model_df_sorted[ocean_model_time_col], utc=True) # Updates time zone aware of ocean model to True
        quag_df_sorted[self.quag_site_col_name] = quag_df_sorted[self.quag_site_col_name].astype(str)
        ocean_model_df_sorted[self.OCEAN_MODEL_STATION_COL] = ocean_model_df_sorted[self.OCEAN_MODEL_STATION_COL].astype(str)
    
        result = pd.merge_asof(
            quag_df_sorted,
            ocean_model_df_sorted,
            left_on = self.quag_utc_date_time_col,
            right_on = ocean_model_time_col,
            left_by = self.quag_site_col_name,
            right_by = self.OCEAN_MODEL_STATION_COL,
            direction = 'nearest', 
            tolerance = pd.Timedelta('1h')
        )

        return result

    def merge_quag_pps_mooring_oceanmodel(self):
        """
        Merges the quagmire, pps, mooring, and ocean model data together
        """
        quag_pps_merged = self.merge_pps_quag_on_station_rosette_time(quag_df=self.quagmire_df)
        quag_pps_mooring_merged = self.merge_moor_quag_on_station_localtime(quag_df=quag_pps_merged)
        quag_pps_mooring_ocean_model_merged = self.merge_oceanmodel_quag_on_station_utctime(quag_df=quag_pps_mooring_merged)
        print("Mooring, PPS, and Ocean Model data merged to QAQC Data!!")
        return quag_pps_mooring_ocean_model_merged