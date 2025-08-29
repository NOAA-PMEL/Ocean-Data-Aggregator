import pandas as pd
import xarray as xr
from pathlib import Path


class NetcdfProcessor:

    def __init__(self, nc_file: Path):

        self.nc_file = Path(nc_file)

    def convert_ctd_nc_to_df(self):
        """
        Concerts a CTD netCDF file to a dataframe. Extracts the date/time and station_id from the file name
        """
        with xr.open_dataset(self.nc_file) as nc_file:

            # Get units
            new_col_name_dict = self.get_units_from_nc_vars(
                avg_xr_ds=nc_file, original_xr_ds=nc_file)

            ds_final = nc_file.rename(new_col_name_dict)

            nc_df = ds_final.to_dataframe()
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

    def convert_rom_ocean_model_to_df(self, min_depth: float, max_depth: float,
                                      depth_var_name: str, vertical_dim_name: str,
                                      time_dim_name: str,
                                      start_time: str,
                                      end_time: str) -> pd.DataFrame:
        """
        Extracts time and depth-averages data from a NetCDF file, returning a pandas DataFrame
        """
        ds = xr.open_dataset(self.nc_file)

        # 1. Filter by the specified time range, if provided
        ds = ds.sel({time_dim_name: slice(start_time, end_time)})

        # 2. Identify variables to process based on their dimension
        variables_to_avg = [
            var for var in ds.data_vars
            if vertical_dim_name in ds[var].dims and time_dim_name in ds[var].dims
        ]

        # 3. Identify 1D variables that should be included
        variables_to_keep = [
            var for var in ds.data_vars
            if vertical_dim_name not in ds[var].dims and time_dim_name in ds[var].dims
        ]

        # 4. Filter the dataset by the specified depth range.
        ds_filtered_by_depth = ds.where(
            (ds[depth_var_name] >= min_depth) & (
                ds[depth_var_name] <= max_depth),
            drop=False
        )

        # 5. Calculate the mean of each variable over the vertical dimension.
        ds_averaged = ds_filtered_by_depth[variables_to_avg].mean(
            dim=vertical_dim_name, skipna=True
        )

        # 6. Add the 1D variables back to the averaged dataset.
        for var in variables_to_keep:
            ds_averaged[var] = ds[var]

        # Get units from original dataset
        new_col_name_dict = self.get_units_from_nc_vars(
            avg_xr_ds=ds_averaged, original_xr_ds=ds)

        ds_final = ds_averaged.rename(new_col_name_dict)

        return ds_final.to_dataframe()

    def get_units_from_nc_vars(self, avg_xr_ds: xr, original_xr_ds) -> dict:
        # Gets the units from the xr_dataset vars and returns a dictionary with var: new_name (includes units)
        # avg_xr_ds and original_xr_ds will be the same for one dimensional data like ctd data, otherwise different
        # for ocean model data
        rename_dict = {}
        for var in list(avg_xr_ds.data_vars):
            units = original_xr_ds[var].attrs.get('units', 'no_units')

            # Check if the units are actually present and not an empty string
            if units:
                new_name = f"{var}_{units}".replace(
                    ' ', '_').replace('/', '_per_')
                rename_dict[var] = new_name
            else:
                # If no units, just keep the original name
                rename_dict[var] = var

        return rename_dict
