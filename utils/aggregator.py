import yaml
import pandas as pd
from pathlib import Path
from utils.pps_txt_file_processor import PpsTextFileProcessor
from utils.quagmire_creator import QuagmireCreator

# TODO: pps_txt_file_dir needs to be optional - not all merges will have PPS data


class Aggregator:

    def __init__(self, config_yaml: str):

        self.config_file = self.load_config(config_yaml)

        # quagmire
        # self.quagmire_pps_port_col_name = self.config_file['quagmire_info']['self.pps_port_col_name']

        # quagmire updated
        self.quagmire_creator = QuagmireCreator(machine_readable_files=self.config_file['machine_readable_info']['machine_readable_files'],
                                                station_col=self.config_file['machine_readable_info']['station_col'])
        self.quagmire_df = self.quagmire_creator.quagmire_df
        self.quag_utc_date_time_col = self.quagmire_creator.new_utc_date_combo_col
        self.quag_min_date = self.quagmire_creator.quag_min_date
        self.quag_max_date = self.quagmire_creator.quag_max_date
        self.quag_min_depth = self.quagmire_creator.quag_min_depth
        self.quag_max_depth = self.quagmire_creator.quag_max_depth
        self.quag_site_col_name = self.quagmire_creator.station_col
        self.quag_station_sites = self.quagmire_creator.quag_station_sites
        self.quag_rosette_pos_col = self.quagmire_creator.rosette_pos_col
        
        # pps info
        # self.pps_date_col = 'pps_sample_start_date' # the date column of the PPS df (this is created in the PpsTextFileProcessor)
        # self.pps_station_id_col = 'pps_station_id' # The name of the station_id col in the pps data (create in the PpstextFileProcessor)
        # self.pps_event_col_name = self.config_file['pps_data']['pps_event_col_name'] # The name of the event number column in the pps data (from the PpsProcessor)
        # self.pps_txt_file_dir = Path( # needs to be optional
        #     self.config_file['pps_data']['pps_txt_files_dir'])
        # self.pps_df = self.get_pps_df()

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
                pps_txt_file=file, sites=self.station_sites)
            pps_df = pps_processor.convert_pps_txt_to_df()
            pps_dfs.append(pps_df)

        df = pd.concat(pps_dfs, ignore_index=True)

        df = df.add_prefix('pps_')

        return df
