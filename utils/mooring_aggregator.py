from utils.aggregator import Aggregator
from utils.mat_file_processor import MatFileProcessor
import pandas as pd

# TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)


class MooringAggregator(Aggregator):

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)

        # Config file info
        self.mooring_mat_files = self.config_file['mooring_mat_files']
        self.quagmire_site_col_name = self.config_file['quagmire_info']['site_col_name']

        # Deduced
        self.mooring_sites = self.quagmire_df[self.quagmire_site_col_name].unique(
        ).tolist()
        self.mooring_df = self.convert_mat_files_to_dfs()

    def convert_mat_files_to_dfs(self) -> pd.DataFrame:
        # TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)
        """
        Converts the mooring .mat files associated with the Aggregator to pandas
        data frame (concats all .mat dfs together)
        """

        mooring_dfs = []
        for mat_file in self.mooring_mat_files:
            mat_processor = MatFileProcessor(
                sites=self.mooring_sites, mat_file=mat_file)

            mooring_df = mat_processor.process_mat_data_like_ocnms()
            mooring_dfs.append(mooring_df)

        return pd.concat(mooring_dfs, ignore_index=True)
