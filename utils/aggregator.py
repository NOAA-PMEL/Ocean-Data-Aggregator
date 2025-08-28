import yaml
import pandas as pd


class Aggregator:

    def __init__(self, config_yaml: str):

        self.config_file = self.load_config(config_yaml)
        self.quagmire_df = pd.read_csv(
            self.config_file['quagmire_info']['quagmire_file'])

    def load_config(self, config_path):
        # Load configuration yaml file

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
