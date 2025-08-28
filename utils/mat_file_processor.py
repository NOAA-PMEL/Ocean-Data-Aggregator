import scipy.io as sio
import pandas as pd
import re
import numpy as np


class MatFileProcessor:

    def __init__(self, sites: list, mat_file: str):

        # A list of the sites to grab (should match the what will be in the variable name in the .mat file)
        self.sites = sites
        self.mat_file = mat_file  # The file path to the mat_file

    @staticmethod
    def _process_mat_struct(mat_struct, parent_path, data_dict):
        """
        Recursively processes a MATLAB struct and collects all data
        into the provided data_dict. This is a static method as it does
        not need access to the class instance attributes.
        """
        if not hasattr(mat_struct, '_fieldnames'):
            data_dict[parent_path] = mat_struct
            return

        for field in mat_struct._fieldnames:
            field_data = getattr(mat_struct, field)
            current_path = f"{parent_path}_{field}"
            MatFileProcessor._process_mat_struct(
                field_data, current_path, data_dict)

    def process_mat_data_like_ocnms(self) -> pd.DataFrame:
        """
        The main method to load the .mat file, process variables, and return a
        single DataFrame with combined data. This has only been tested on OCNMS data's
        .mat structure. May be different for other .mat files.
        """
        master_dataframe_list = []

        try:
            mat_data = sio.loadmat(
                self.mat_file, squeeze_me=True, struct_as_record=False)
        except FileNotFoundError:
            print(f"Error: The file '{self.mat_file}' was not found.")
            return pd.DataFrame()

        mat_data = {k: v for k, v in mat_data.items()
                    if not k.startswith('__')}

        for var, info in mat_data.items():
            if any(site_id in var for site_id in self.sites):
                print(f"\n--- Processing variable: {var} ---")

                file_data_dict = {}
                self._process_mat_struct(info, var, file_data_dict)

                # 1. Extract and parse units from file_collabels
                units_map = {}
                try:
                    col_labels = file_data_dict[f'{var}_file_collabels']
                    for label in col_labels:
                        unit_match = re.search(r'\[(.*)\]', label)
                        unit = unit_match.group(
                            1).strip() if unit_match else None
                        if 'temperature' in label:
                            units_map['temp'] = unit
                        elif 'conductivity' in label:
                            units_map['cond'] = unit
                        elif 'pressure' in label:
                            units_map['pres'] = unit
                        elif 'salinity' in label:
                            units_map['sal'] = unit
                        elif 'density' in label:
                            units_map['dens'] = unit
                except KeyError:
                    print(
                        f"  Warning: No 'file_collabels' found for {var}. Units will be omitted.")

                # 2. Extract metadata
                station_id = next(
                    (sid for sid in self.sites if sid in var), 'unknown')

                try:
                    depth_m = int(var.split('_')[-1])
                except (ValueError, IndexError):
                    depth_m = np.nan

                try:
                    deployment_time = pd.to_datetime(
                        file_data_dict[f'{var}_db_DeploymentTime'])
                except (KeyError, ValueError):
                    deployment_time = pd.NaT

                # 3. Process only the 'data' source for raw data
                data_source = 'data'
                source_prefix = f'_{data_source}_'

                time_series_data = {
                    key.split('_')[-1]: value for key, value in file_data_dict.items()
                    if source_prefix in key and isinstance(value, np.ndarray) and value.size > 0
                }

                if not time_series_data:
                    print(f"  No time-series data found for {var}. Skipping.")
                    continue

                num_rows = len(list(time_series_data.values())[0])
                final_df_data = {}

                # Add metadata columns
                final_df_data['station_id'] = np.full(num_rows, station_id)
                final_df_data['depth_m'] = np.full(num_rows, depth_m)
                final_df_data['deployment_time'] = np.full(
                    num_rows, deployment_time)

                # --- DYNAMICALLY ADD TIME-SERIES COLUMNS WITH UNITS ---
                for short_name, data_array in time_series_data.items():
                    full_name = short_name
                    if short_name == 'time':
                        full_name = 'time_s_since_deployment'
                    else:
                        unit = units_map.get(short_name)
                        if unit:
                            full_name = f"{short_name}_{unit.replace(' ', '_')}"

                    final_df_data[full_name] = data_array

                # 4. Create the DataFrame
                combined_df = pd.DataFrame(final_df_data)
                master_dataframe_list.append(combined_df)

                print(
                    f"  Successfully processed {var} with {num_rows} data points.")

        if master_dataframe_list:
            return pd.concat(master_dataframe_list, ignore_index=True)
        else:
            return pd.DataFrame()
