import pandas as pd
import xarray as xr
from pathlib import Path
import math


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
                                      depth_var_name: str,
                                      time_dim_name: str,
                                      start_time: str,
                                      end_time: str, 
                                      station: str) -> pd.DataFrame:
        """
        Extracts time and depth-averages data from a NetCDF file, returning a pandas DataFrame
        """
        ds = xr.open_dataset(self.nc_file)

        # 1. Filter by the specified time range, if provided
        ds = ds.sel({time_dim_name: slice(start_time, end_time)})
      
        df = ds.to_dataframe().reset_index()
        
        # round depth to nearest 5's (min_depth down and max_depth  up) and make negative since ocean model data is negative
        min_depth = (math.floor(min_depth / 5) * 5) *-1
        max_depth = (math.ceil(max_depth / 5) * 5) * -1
        print(min_depth)
        print(max_depth)
       
        # filter by depth range (switch min and max because the -1 makes the min_Depth the max, and vice versa, but still the range we need)
        depth_filtered_df = df[df[depth_var_name].between(max_depth, min_depth)]
   
        # Performe the depth averageing by grouping by time. mean() calculates the average for all numeric columns
        final_df = depth_filtered_df.groupby(time_dim_name, as_index=False).mean(numeric_only=True)

        units_dict = self.get_units_from_nc_vars(original_xr_ds=ds)
        column_unit_dict = {
            col: units_dict.get(col, col)
            for col in final_df.columns
        }
        final_df.rename(columns=column_unit_dict, inplace=True)
        final_df['station'] = station
        return final_df


    def get_units_from_nc_vars(self, original_xr_ds: xr) -> dict:
        """
        Gets the units from the xr_dataset vars and returns a dictionary with var: new_name (includes units)
        """
        rename_dict = {}
        for var in original_xr_ds.variables:
            if 'units' in original_xr_ds[var].attrs:
                # Store the original name as the key and the new name as the value
                rename_dict[str(var)] = f"{var}.{original_xr_ds[var].attrs['units'].replace(' ', '_')}"
            else:
                rename_dict[str(var)] = str(var)
        return rename_dict
