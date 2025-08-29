from utils.aggregator import Aggregator
from utils.mat_file_processor import MatFileProcessor
from utils.netcdf_processor import NetcdfProcessor
from pathlib import Path
import pandas as pd

# TODO: upate hardcoded sites in the convert_mat_files_to_dfs to not be hardcoded (take from Quagmire sites)


class MooringAggregator(Aggregator):

    def __init__(self, config_yaml: str):
        super().__init__(config_yaml)

        # Config file info
        self.mooring_mat_files = self.config_file['mooring_mat_files']
        self.quagmire_site_col_name = self.config_file['quagmire_info']['site_col_name']
        self.quagmire_depth_col_name = self.config_file['quagmire_info']['depth_col_name']
        self.quagmire_date_col_name = self.config_file['quagmire_info']['date_col_name']
        self.ctd_nc_file_directory = Path(
            self.config_file['ctd_data']['net_cdf_dir'])
        self.model_data_files = self.config_file['ocean_model_data']['model_nc_files']
        self.ocean_model_depth_var = self.config_file['ocean_model_data']['depth_variable_name']
        self.ocean_model_vert_dim_name = self.config_file['ocean_model_data']['vertical_dim_name']
        self.ocean_model_time_dim_name = self.config_file['ocean_model_data']['time_dim_name']

        # Deduced
        self.mooring_sites = self.quagmire_df[self.quagmire_site_col_name].unique(
        ).tolist()
        self.min_date, self.max_date = self.get_quag_min_and_max_dates()
        self.min_depth, self.max_depth = self.get_min_and_max_depths()
        # self.mooring_df = self.convert_mat_files_to_df()
        # self.ctd_from_netcdf_df = self.convert_ctd_nc_files_to_df()
        self.ocean_model_df = self.convert_ocean_model_nc_to_df()

    def get_quag_min_and_max_dates(self) -> tuple:
        """
        Get the min and max dates from the Quagmire - to plug into ocean model data query
        """
        self.quagmire_df[self.quagmire_date_col_name] = pd.to_datetime(
            self.quagmire_df[self.quagmire_date_col_name])

        min_date = self.quagmire_df[self.quagmire_date_col_name].min()
        max_date = self.quagmire_df[self.quagmire_date_col_name].max()

        # Format the dates into YYYY-MM-DD and YYYY-MM strings
        min_date_full_format = min_date.strftime('%Y-%m-%d')
        max_date_full_format = max_date.strftime('%Y-%m-%d')

        return min_date_full_format, max_date_full_format

    def get_min_and_max_depths(self) -> tuple:
        """
        Get the min and max depths from the quagmire - to get range of dates to query ocean model data
        """

        # Get min and max depths from quagmire
        min_depth = self.quagmire_df[self.quagmire_depth_col_name].min()
        max_depth = self.quagmire_df[self.quagmire_depth_col_name].max()

        return min_depth, max_depth

    def convert_mat_files_to_df(self) -> pd.DataFrame:
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

    def convert_ctd_nc_files_to_df(self) -> pd.DataFrame:
        """
        Converts all the associated .nc files in the config.yaml into a data frame. Concats them all
        together to return one dataframe. Assumes that ctd files are all in the same directory.
        """
        # Recurseivly find all .nc files in the directory
        all_nc_files = list(self.ctd_nc_file_directory.rglob('*.nc'))

        # Filter the list of all_nc_files based on the station_ids
        nc_files_needed = [
            f for f in all_nc_files if any(station_id in str(f) for station_id in self.mooring_sites)
        ]

        nc_dfs = []
        for nc_file in nc_files_needed:
            nc_processor = NetcdfProcessor(nc_file=nc_file)
            nc_df = nc_processor.convert_ctd_nc_to_df()
            nc_dfs.append(nc_df)

        return pd.concat(nc_dfs, ignore_index=True)

    def convert_ocean_model_nc_to_df(self) -> pd.DataFrame:

        nc_dfs = []
        for nc_file in self.model_data_files:
            nc_processor = NetcdfProcessor(nc_file=nc_file)
            nc_df = nc_processor.convert_rom_ocean_model_to_df(min_depth=self.min_depth,
                                                               max_depth=self.max_depth,
                                                               depth_var_name=self.ocean_model_depth_var,
                                                               vertical_dim_name=self.ocean_model_vert_dim_name,
                                                               time_dim_name=self.ocean_model_time_dim_name,
                                                               start_time=self.min_date,
                                                               end_time=self.max_date)
            nc_dfs.append(nc_df)

        return pd.concat(nc_dfs, ignore_index=True)
