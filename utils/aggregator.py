import yaml
import pandas as pd
from pathlib import Path
from utils.pps_txt_file_processor import PpsTextFileProcessor
from utils.quagmire_creator import QuagmireCreator
import numpy as np

# TODO: pps_txt_file_dir needs to be optional - not all merges will have PPS data


class Aggregator:
    
    PPS_LOCAL_START_DATE_COL = 'pps_sample_start_date' # the date column of the PPS df (this is created in the PpsTextFileProcessor)
    PPS_LOCAL_END_DATE_COL = 'pps_sample_end_date'
    PPS_UTC_START_TIME_COL = 'pps_utc_start_time' # Created in MooringAggregator in FINALmerge_quag_pps_mooring_oceanmodel()
    PPS_UTC_END_TIME_COL = 'pps_utc_end_time_col' # Created in Mooring Aggregator in FINALmerge_quag_pps_mooring_oceanmodel()
    PPS_STATION_ID_COL = 'pps_station_id' # The name of the station_id col in the pps data (create in the PpstextFileProcessor)
    PPS_EVENT_NUM_COL = 'pps_event_number' # The name of the pps event_number col (create in the PpstextFileProcessor)

    def __init__(self, config_yaml: str):

        self.config_file = self.load_config(config_yaml)

        # quagmire updated
        self.quagmire_creator = QuagmireCreator(machine_readable_files=self.config_file['machine_readable_info']['machine_readable_files'],
                                                station_col=self.config_file['machine_readable_info']['station_col'])
        self.quagmire_df = self.quagmire_creator.quagmire_df
        self.quag_utc_date_time_col = self.quagmire_creator.NEW_UTC_DATE_COMBO_COL
        self.quag_local_date_time_col = self.quagmire_creator.NEW_LOCAL_DATE_COMBO_COL
        self.quag_min_date = self.quagmire_creator.quag_min_date
        self.quag_max_date = self.quagmire_creator.quag_max_date
        self.quag_min_depth = self.quagmire_creator.quag_min_depth
        self.quag_max_depth = self.quagmire_creator.quag_max_depth
        self.quag_site_col_name = self.quagmire_creator.station_col
        self.quag_station_sites = self.quagmire_creator.quag_station_sites
        self.quag_rosette_pos_col = self.quagmire_creator.ROSETTE_POS_COL
        self.quag_local_time_zone_col = self.quagmire_creator.NEW_TIMEZONE_COL
        self.quag_sample_name_col = self.quagmire_creator.SAMPLE_NAME_COL
        
        # pps info - Optional
        if self.config_file.get('pps_data', None):
            self.pps_txt_file_dir = Path(self.config_file['pps_data']['pps_txt_files_dir']) # TODO: Needs to be optional
            self.pps_df = self.get_pps_df()
            self.pps_time_interval = self.find_pps_recording_time_interval() # The average time interval of start and end times for pps recordings

    def load_config(self, config_path):
        # Load configuration yaml file

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def get_pps_df(self):
        """
        Get a single data frame for all the applicable PPS data
        """
        pps_dfs = []
        # Get all .txt files with PPS in the name form the directory
        for file in self.pps_txt_file_dir.rglob('*PPS*.txt'):
            pps_processor = PpsTextFileProcessor(
                pps_txt_file=file, sites=self.quag_station_sites)
            pps_df = pps_processor.convert_pps_txt_to_df()
            pps_dfs.append(pps_df)

        df = pd.concat(pps_dfs, ignore_index=True)

        df = df.add_prefix('pps_')
        df.to_csv('pps.csv', index=False)

        return df
 
    def find_pps_recording_time_interval(self) -> float:
        """
        Finds the time interval of the pps reocrdings. (difference between the start_time
        and the end_time. This will be used to merge the PPS with other data in a time frame 
        that is plus or minus
        half of the interval. So if interval is 10 minutes. Merging PPS data
        with times that fall between the start and end times plus 5 minutes
        """
        durations = self.pps_df[self.PPS_LOCAL_END_DATE_COL] - self.pps_df[self.PPS_LOCAL_START_DATE_COL]
        mean_durations = durations.mean()
        return mean_durations

    def merge_pps_quag_on_station_rosette_localtime(self, quag_df: pd.DataFrame):
        """
        Merge the pps_df and quagmire_df on the site, rosette_position/event_number, and date. PPS data is always in local
        time so merge on local date time with quag. This gets the pps row with the nearest time within 1 hour.
        Uses the input of quag_df because quag_df could already be merged with another data type.
        """

        # It's better to sort explicitly by the `on` key first, followed by the `by` keys.
        # This ensures the primary sort is on the merge key, which is what merge_asof requires.
        quag_df_sorted = quag_df.sort_values(
            [self.quag_local_date_time_col, self.quag_site_col_name, self.quag_rosette_pos_col]
        )
        pps_df_sorted = self.pps_df.sort_values(
            [self.PPS_LOCAL_START_DATE_COL, self.PPS_STATION_ID_COL, self.PPS_EVENT_NUM_COL]
        )

        # Convert columns to the same types
        quag_df_sorted[self.quag_local_date_time_col] = pd.to_datetime(quag_df_sorted[self.quag_local_date_time_col])
        pps_df_sorted[self.PPS_LOCAL_START_DATE_COL] = pd.to_datetime(pps_df_sorted[self.PPS_LOCAL_START_DATE_COL])
        quag_df_sorted[self.quag_rosette_pos_col] = quag_df_sorted[self.quag_rosette_pos_col].astype(int)
        pps_df_sorted[self.PPS_EVENT_NUM_COL] = pps_df_sorted[self.PPS_EVENT_NUM_COL].astype(int)
        quag_df_sorted[self.quag_site_col_name] = quag_df_sorted[self.quag_site_col_name].astype(str)
        pps_df_sorted[self.PPS_STATION_ID_COL] = pps_df_sorted[self.PPS_STATION_ID_COL].astype(str)
        
        result = pd.merge_asof(
            quag_df_sorted, 
            pps_df_sorted, 
            left_on = self.quag_local_date_time_col, 
            right_on = self.PPS_LOCAL_START_DATE_COL, 
            left_by = [self.quag_site_col_name, self.quag_rosette_pos_col],
            right_by = [self.PPS_STATION_ID_COL, self.PPS_EVENT_NUM_COL],
            direction = 'nearest',
            tolerance = pd.Timedelta('1h')
        )
        return result
