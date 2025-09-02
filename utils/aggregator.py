import yaml
import pandas as pd
from pathlib import Path
from utils.pps_txt_file_processor import PpsTextFileProcessor

# TODO: pps_txt_file_dir needs to be optional - not all merges will have PPS data


class Aggregator:

    def __init__(self, config_yaml: str):

        self.config_file = self.load_config(config_yaml)
        self.quagmire_site_col_name = self.config_file['quagmire_info']['station_site_col_name']
        self.pps_date_col = 'pps_sample_start_date' # the date column of the PPS df (this is created in the PpsTextFileProcessor)
        self.pps_station_id_col = 'pps_station_id' # The name of the station_id col in the pps data (create in the PpstextFileProcessor)
        # needs to be optional
        self.pps_txt_file_dir = Path(
            self.config_file['pps_data']['pps_txt_files_dir'])

        # deduced
        self.quagmire_df = pd.read_csv(
            self.config_file['quagmire_info']['quagmire_file'])
        self.station_sites = self.quagmire_df[self.quagmire_site_col_name].unique(
        ).tolist()
        self.pps_df = self.get_pps_df()

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
        for file in self.pps_txt_file_dir.glob('*PPS*.txt'):
            pps_processor = PpsTextFileProcessor(
                pps_txt_file=file, sites=self.station_sites)
            pps_df = pps_processor.convert_pps_txt_to_df()
            pps_dfs.append(pps_df)

        df = pd.concat(pps_dfs, ignore_index=True)

        df = df.add_prefix('pps_')

        return df
