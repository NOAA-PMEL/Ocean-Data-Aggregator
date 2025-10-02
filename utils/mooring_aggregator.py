from utils.aggregator import Aggregator
from utils.mat_file_processor import MatFileProcessor
from utils.netcdf_processor import NetcdfProcessor
from utils.cnv_processor import CnvProcessor
from pathlib import Path
import pandas as pd
import numpy as np
from timezonefinder import TimezoneFinder
import pytz
import datetime

# TODO: update the merge_ctd_with_quag function to have a tolerance of '1H' (check with Zack) after running the OCNMS code (needs to be adjustable for the code)


class MooringAggregator(Aggregator):

    OCEAN_MODEL_STATION_COL = "model_station" # The name of the station col in the ocean model data (added in the netcdf_processor function)
    MOORING_STATION_ID_COL = 'moor_station_id' # the name of the station_id col in the mooring data
    CTD_STATION_COL = 'ctd_station_id' # From the netcdfProcessor and/or cnvProcessor (must be the same)
    CTD_DATE_COL = 'ctd_time' # from the netcdfProcessor and/or cnvProcessor (must be the same)
    MOORING_DATE_COL = 'moor_datetime'  #mooring time is assumed to be UTC

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)

        # For Mooring data derived from .mat files
        self.mooring_mat_dir = Path(self.config_file['mooring_info']['mooring_data_dir'])
        self.moor_sensors = self.config_file['mooring_info'].get('sensors', None) # The name of the sensors to grab data
        self.mooring_df = self.convert_mat_files_to_df()
        
        # For CTD data derived (can be .NC or .CNV)
        if self.config_file.get('ctd_data', None):
            self.ctd_quag_merge_tolerance = self.config_file['ctd_data'].get('ctd_quag_merge_tolerance', None)
            if self.config_file['ctd_data'].get('net_cdf_dir', None):
                self.ctd_nc_file_directory = Path(self.config_file['ctd_data']['net_cdf_dir'])
                self.ctd_df = self.convert_ctd_nc_files_to_df()
            elif self.config_file['ctd_data'].get('cnv_dir', None):
                self.ctd_cnv_file_directory = Path(self.config_file['ctd_data']['cnv_dir'])
                self.ctd_day_convention = self.config_file['ctd_data']['julian_day_convention']
                self.ctd_df = self.convert_ctd_cnv_files_to_df()

        # For Ocean model data (.NC file)
        self.model_data_files = self.config_file['ocean_model_data']['model_nc_files']
        self.ocean_model_depth_var = self.config_file['ocean_model_data']['depth_variable_name']
        self.ocean_model_time_dim_name = self.config_file['ocean_model_data']['time_dim_name']
        self.ocean_model_df = self.convert_ocean_model_nc_to_df()

    def FINALmerge_quag_pps_mooring_oceanmodel(self):
        """
        Merges the quagmire, pps, mooring, and ocean model data together.
        The pps, mooring and ocean model data is merged based on the start_time and end_time time frame windows.
        Though the pps is first merged with the Quag (to get the timezone info so can convert PPS times to UTC to merge with ocean model data),
        The rest of the merges (to the mooring and ocean model) occur on the pps data times and station.
        """
        quag_pps_merged = self.merge_pps_quag_on_station_rosette_localtime(quag_df=self.quagmire_df)

        # Since pps data does not have timezone info, but times are in local need to get timezone info from quagmire and create start/end utc times for pps
        # important for merging pps data with ocean_model data (which is in UTC). 
        quag_pps_merged[self.PPS_UTC_START_TIME_COL] = quag_pps_merged.apply(lambda row: self.convert_local_time_to_utc(local_dt=row[self.PPS_LOCAL_START_DATE_COL], timezone=row[self.quag_local_time_zone_col], sample_name=row[self.quag_sample_name_col]), axis=1)
        quag_pps_merged[self.PPS_UTC_END_TIME_COL] = quag_pps_merged.apply(lambda row: self.convert_local_time_to_utc(local_dt=row[self.PPS_LOCAL_END_DATE_COL], timezone=row[self.quag_local_time_zone_col], sample_name=row[self.quag_sample_name_col]), axis=1)
        
        
        quag_pps_mooring_merged = self.merge_pps_mooring_by_utc_timeframe_average_and_station(pps_df=quag_pps_merged)
        quag_pps_mooring_ocean_model_merged = self.merge_pps_ocean_model_by_utc_timeframe_average_and_station(pps_df=quag_pps_mooring_merged)

        # remove any columns that are entirely empty
        final_df = quag_pps_mooring_ocean_model_merged.dropna(axis=1, how='all')
        print("Mooring, PPS, and Ocean Model data merged to QAQC Data!!")
        return final_df
    
    def FINALmerge_quag_ctd_mooring_oceanmodel(self):
        """
        Merges the Quagmire, CTD, mooring, and ocean model data together.
        """
        
        quag_ctd_df = self.merge_ctd_quag_on_station_utctime(quag_df=self.quagmire_df)
        quag_ctd_mooring_df = self.merge_moor_quag_on_station_utctime(quag_df=quag_ctd_df)
        quag_ctd_mooring_ocean_df = self.merge_oceanmodel_quag_on_station_utctime(quag_df=quag_ctd_mooring_df)
        
        # remove any columns that are entirely empty
        final_df = quag_ctd_mooring_ocean_df.dropna(axis=1, how='all')
        print("Mooring, CTD, and Ocean Model data merged to QAQC!")

        return final_df
    
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
                sites=self.quag_station_sites, mat_file=mat_file, sensors=self.moor_sensors)

            mooring_df = mat_processor.get_ocnms_df_from_mat_file()
            mooring_dfs.append(mooring_df)

        df = pd.concat(mooring_dfs, ignore_index=True)

        df = df.add_prefix('moor_')
        df_cleaned = df.dropna(axis=1, how='all')

        return df_cleaned
    
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
        df_cleaned = df.dropna(axis=1, how='all')

        return df_cleaned

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
        df_cleaned = df.dropna(axis=1, how='all')
        return df_cleaned

    def convert_ctd_cnv_files_to_df(self) -> pd.DataFrame:
        """
        Converts all the associated .cnv files in the config.yaml into a data frame. Concats them all
        together to return one dataframe. Assumes the ctd files are all in the same directory
        """
        all_cnv_files = list(self.ctd_cnv_file_directory.rglob('*.cnv'))

        cnv_dfs = []
        for cnv_file in all_cnv_files:
            cnv_processor = CnvProcessor(cnv_file=cnv_file, sites=self.quag_station_sites, day_convention=self.ctd_day_convention)
            cnv_df = cnv_processor.cnv_df
            cnv_dfs.append(cnv_df)
        
        df = pd.concat(cnv_dfs, ignore_index=False)
        df = df.add_prefix('ctd_')
        df_cleaned = df.dropna(axis=1, how='all')
        return df_cleaned
    
    def merge_ctd_quag_on_station_utctime(self, quag_df: pd.DataFrame, tolerance: str = '1h') -> pd.DataFrame:
        """
        Merges quagmire dataframe with ctd data frame on utc time by station. The CnvProcessor 
        makes sure its in utc time. If using a ctd_df that was derived from .NC files, need to 
        make sure that date/time column is utc, or add functionality to merge on local time. 
        quag_df is in input into this function because can add a quag_df that
        has already been merged with other data if desired. Otherwise just use self.quagmire_df.
        tolerance is default 1 hour, but can change this in yaml file.
        """
        # Sort quag_df and mooring_df by the 'on' key first which is time, then the 'by' key which is station
        quag_df_sorted = quag_df.sort_values([self.quag_utc_date_time_col, self.quag_site_col_name])
        ctd_sorted = self.ctd_df.sort_values([self.CTD_DATE_COL, self.CTD_STATION_COL])

        # Convert columns to the same types
        quag_df_sorted[self.quag_utc_date_time_col] = pd.to_datetime(quag_df_sorted[self.quag_utc_date_time_col])
        ctd_sorted[self.CTD_DATE_COL] = pd.to_datetime(ctd_sorted[self.CTD_DATE_COL])
        quag_df_sorted[self.quag_site_col_name] = quag_df_sorted[self.quag_site_col_name].astype(str)
        ctd_sorted[self.CTD_STATION_COL] = ctd_sorted[self.CTD_STATION_COL].astype(str)

        # If the ctd_quag_merge_tolerance is provided in the config.yaml file, update the tolerance, otherwise 
        # use default of 1 hour.
        if self.config_file.get('ctd_data', None):
            if self.config_file['ctd_data'].get('ctd_quag_merge_tolerance', None):
                tolerance = self.config_file['ctd_data'].get('ctd_quag_merge_tolerance')
    
        result = pd.merge_asof(
            quag_df_sorted,
            ctd_sorted,
            left_on = self.quag_utc_date_time_col,
            right_on = self.CTD_DATE_COL,
            left_by = self.quag_site_col_name,
            right_by = self.CTD_STATION_COL,
            direction = 'nearest', 
            tolerance = pd.Timedelta(tolerance)
        )

        result['ctd_quag_time_difference'] = abs(
            result[self.quag_utc_date_time_col] - result[self.CTD_DATE_COL]
        )

        return result

    def merge_moor_quag_on_station_utctime(self, quag_df: pd.DataFrame):
        """
        Merge mooring data with quagmire data on station and closest time (within one hour). 
        Time is merged assuming UTC time. Are .mat files always in UTC time?
        Takes quag_df as an input because quag_df could be the self.quagmire_df already
        merged with another data type
        """
        # Sort quag_df and mooring_df by the 'on' key first which is time, then the 'by' key which is station
        quag_df_sorted = quag_df.sort_values([self.quag_utc_date_time_col, self.quag_site_col_name])
        moor_df_sorted = self.mooring_df.sort_values([self.MOORING_DATE_COL, self.MOORING_STATION_ID_COL])

        # Convert columns to the same types
        quag_df_sorted[self.quag_utc_date_time_col] = pd.to_datetime(quag_df_sorted[self.quag_utc_date_time_col])
        moor_df_sorted[self.MOORING_DATE_COL] = pd.to_datetime(moor_df_sorted[self.MOORING_DATE_COL])
        quag_df_sorted[self.quag_site_col_name] = quag_df_sorted[self.quag_site_col_name].astype(str)
        moor_df_sorted[self.MOORING_STATION_ID_COL] = moor_df_sorted[self.MOORING_STATION_ID_COL].astype(str)
    
        result = pd.merge_asof(
            quag_df_sorted,
            moor_df_sorted,
            left_on = self.quag_utc_date_time_col,
            right_on = self.MOORING_DATE_COL,
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

    def merge_pps_mooring_by_utc_timeframe_average_and_station(self, pps_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the PPS dataframe with the mooring df by averaging all columns 
        that fall between the start and end time (in UTC) plus half the average time interval for
        pps recordings. Merges also by station. pps_df is an input because the pps may 
        have already been merged with other data
        """
        moor_df = self.mooring_df.copy()
        pps_df = pps_df.copy()

        # Find half of pps time interval and conver tto time delta
        pps_time_buffer = pd.Timedelta(self.pps_time_interval/2)

        pps_df[self.PPS_UTC_START_TIME_COL] = pd.to_datetime(pps_df[self.PPS_UTC_START_TIME_COL])
        pps_df[self.PPS_UTC_END_TIME_COL] = pd.to_datetime(pps_df[self.PPS_UTC_END_TIME_COL])
        moor_df[self.MOORING_DATE_COL] = pd.to_datetime(moor_df[self.MOORING_DATE_COL])

        # Calculate the expanded time window for mooring data
        pps_df['pps_expanded_start'] = pps_df[self.PPS_UTC_START_TIME_COL] - pps_time_buffer
        pps_df['pps_expanded_end'] = pps_df[self.PPS_UTC_START_TIME_COL] + pps_time_buffer

        numeric_cols = moor_df.select_dtypes(include=[np.number]).columns.tolist()
        
        results = []

        for idx, pps_row in pps_df.iterrows():
            # Find matching moor_df rows by station and expanded time window
            station_match = moor_df[moor_df[self.MOORING_STATION_ID_COL] == pps_row[self.PPS_STATION_ID_COL]]

            time_match = station_match[
                (station_match[self.MOORING_DATE_COL] >= pps_row['pps_expanded_start']) &
                (station_match[self.MOORING_DATE_COL] <= pps_row['pps_expanded_end'])
            ]

            if len(time_match) > 0:
                # calculate averages for numeric columns
                averaged_data = time_match[numeric_cols].mean()

                # Create result row
                result_row = pps_row

                # Add averaged values
                for col in numeric_cols:
                    result_row[col] = averaged_data[col]

                # Add count of matched rows and mooring min/and max dates that matched to pps. Add the moor_station id column back in (this is just adding it back in, the pps and moor station cols were matched above)
                result_row['moor_min_date'] = time_match[self.MOORING_DATE_COL].min()
                result_row['moor_max_date'] = time_match[self.MOORING_DATE_COL].max()
                result_row[self.MOORING_STATION_ID_COL] = pps_row[self.PPS_STATION_ID_COL]
                result_row['moor_count_avg'] = len(time_match)
                    
                results.append(result_row)

            else:
                # No matching mooring data - add row with NaN values
                result_row = pps_row.copy()
                for col in numeric_cols:
                    result_row[col] = np.nan
                result_row['moor_count_avg'] = 0
                results.append(result_row)

            # Convert results to DataFrame
            result_df = pd.DataFrame(results)

        return result_df

    def merge_pps_ocean_model_by_utc_timeframe_average_and_station(self, pps_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the PPS dataframe with the ocean model df by averaging all columns 
        that fall between the start and end time plus half the average time interval for
        pps recordings. Merges also by station. pps_df is an input because the pps may 
        have already been merged with other data. This uses utc time because ocean model
        data is in UTC time.
        """
        ocean_model_time_col = f"model_{self.ocean_model_time_dim_name}" # col name has model prepended now.

        ocean_model_df = self.ocean_model_df.copy()
        pps_df = pps_df.copy()

        # Find half of pps time interval and conver tto time delta
        pps_time_buffer = pd.Timedelta(self.pps_time_interval/2)

        pps_df[self.PPS_UTC_START_TIME_COL] = pd.to_datetime(pps_df[self.PPS_UTC_START_TIME_COL])
        pps_df[self.PPS_UTC_END_TIME_COL] = pd.to_datetime(pps_df[self.PPS_UTC_END_TIME_COL])
        ocean_model_df[ocean_model_time_col] = pd.to_datetime(ocean_model_df[ocean_model_time_col]).dt.tz_localize('UTC')

        # Calculate the expanded time window for mooring data
        pps_df['pps_expanded_start'] = pps_df[self.PPS_UTC_START_TIME_COL] - pps_time_buffer
        pps_df['pps_expanded_end'] = pps_df[self.PPS_UTC_END_TIME_COL] + pps_time_buffer

        numeric_cols = ocean_model_df.select_dtypes(include=[np.number]).columns.tolist()
        
        results = []

        for idx, pps_row in pps_df.iterrows():
            # Find matching moor_df rows by station and expanded time window
            station_match = ocean_model_df[ocean_model_df[self.OCEAN_MODEL_STATION_COL] == pps_row[self.PPS_STATION_ID_COL]]

            time_match = station_match[
                (station_match[ocean_model_time_col] >= pps_row['pps_expanded_start']) &
                (station_match[ocean_model_time_col] <= pps_row['pps_expanded_end'])
            ]

            if len(time_match) > 0:
                # calculate averages for numeric columns
                averaged_data = time_match[numeric_cols].mean()

                # Create result row
                result_row = pps_row

                # Add averaged values
                for col in numeric_cols:
                    result_row[col] = averaged_data[col]

                # Add count of matched rows and mooring min/and max dates that matched to pps. Add the moor_station id column back in (this is just adding it back in, the pps and moor station cols were matched above)
                result_row['ocean_model_min_date'] = time_match[ocean_model_time_col].min()
                result_row['ocean_model_max_date'] = time_match[ocean_model_time_col].max()
                result_row[self.OCEAN_MODEL_STATION_COL] = pps_row[self.PPS_STATION_ID_COL]
                result_row['ocean_model_count_avg'] = len(time_match)
                    
                results.append(result_row)

            else:
                # No matching mocean model data - add row with NaN values
                result_row = pps_row.copy()
                for col in numeric_cols:
                    result_row[col] = np.nan
                result_row['ocean_model_count_avg'] = 0
                results.append(result_row)

            # Convert results to DataFrame
            result_df = pd.DataFrame(results)

        return result_df
    
    def convert_local_time_to_utc(self, local_dt: str, timezone: str, sample_name: str) -> str:
        """
        Converts a time to utc based on timezone. This is used for the PPS data (already
        merged with the Quag data) because it is in local time and need to use the timzone
        calculated in for the Quag to convert PPS times to UTC. So that PPS data can be 
        merged with other data on UTC time if needed (e.g. ocean model data.)
        """
        try:
            local_tz = pytz.timezone(timezone) # get the timezone object using pytz

            # Localize the naive datetime object
            aware_dt = local_tz.localize(local_dt)

            # convert the aware datetime to UTC
            utc_dt = aware_dt.astimezone(pytz.utc)
            # formatted_string = utc_dt.strftime('%Y-%m-%d %H:%M:%S') 
            return utc_dt
        except ValueError as e:
            print(f"Datetime of {sample_name} is {local_dt} and cannot be converted to UTC. {e}")


        

       
