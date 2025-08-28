import pandas as pd
import xarray as xr
from pathlib import Path


class NetcdfProcessor:

    def __init__(self, nc_file: Path):

        self.nc_file = Path(nc_file)

    def convert_nc_to_df(self):
        """
        Concerts a netCDF file to a dataframe. Extracts the date/time and station_id from the file name
        """
        with xr.open_dataset(self.nc_file) as nc_file:
            nc_df = nc_file.to_dataframe()
            nc_df = nc_df.reset_index()

            # Extract the station and date from the file name. Hopefully this is consistent across projects.
            filename = self.nc_file.name
            parts = filename.split('_')
            station_id = parts[0]

            date = pd.to_datetime(
                parts[1], format="%Y%m%dT%H%M%S", utc=True)

            nc_df.loc[:, 'station_id'] = station_id
            nc_df.loc[:, 'date'] = date

            return nc_df
