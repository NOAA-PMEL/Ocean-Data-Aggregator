import scipy.io as sio
from scipy.io import loadmat
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


class MatFileProcessor:
    """
    Processes MATLAB .mat files and converts them to pandas DataFrames.
    Designed for OCNMS data structure but may work with similar formats.
    """

    def __init__(self, sites: list, mat_file: str, sensors: list):
        """
        Initialize the processor.

        Args:
            sites: List of site IDs to process (should match variable names in .mat file)
            mat_file: Path to the .mat file
            sensors: List of sensor names for which to grab data from
        """
        self.sites = sites
        self.mat_file = mat_file
        self.sensors = sensors

    def get_ocnms_df_from_mat_file(self):
        """
        For OCNMS data - assumes a structure like data.keys() are variables
        which include the sensor_station_recoveryDate_?. Within each variable:
        data[var_name].dtype.names = ('file', 'data', 'db', 'lpdata'). Data is in 
        the 'data' field which includes a shape of (1, 1). Assumes the times are in
        UTC. TODO: May need to update to specify the timezone.
        """
        data = loadmat(self.mat_file)
        variable_structure = ['file', 'data', 'db', 'lpdata']

        data_frames = []
        for var_name in data.keys():
            if not var_name.startswith('_'):

                # only pull variables with desired sites
                for site in self.sites:
                    if site in var_name:
                        # Only pull variables with desired sensor name
                        for sensor in self.sensors:
                            if sensor in var_name:
                                the_site = site
                                var_shape = data[var_name].shape
                                # Check that variable shapes are all the same (e.g. 1,1)
                                if var_shape == (1, 1):
                                    var_struct = data[var_name].dtype.names
                                    # Check that the variable structure have the expected field names (file, data, db, lpdata)
                                    if sorted(var_struct) == sorted(variable_structure):
                        
                                        # Check if data has the expected shape
                                        if data[var_name]['data'].shape == (1, 1):
                                            # Create data frame
                                            data_struct = data[var_name][0,0]['data'][0,0]
                                            data_dict = {}
                                            for field_name in data_struct.dtype.names:
                                                field_data = data_struct[field_name]

                                                # Check it its an array or scalar
                                                if hasattr(field_data, 'shape') and len(field_data.shape) > 0:
                                                    # print(f"{field_name}: {data_struct[field_name].shape}")
                                                    # It's an array - flatten it
                                                    data_dict[field_name] = field_data.flatten()
                                                else:
                                                    #Its scalar wrap it
                                                    data_dict[field_name] = [field_data]

                                            # Check if all the arrays are the same length
                                            lengths = [len(v) for v in data_dict.values()]
                                            # print(f"Array lengths: {set(lengths)}")

                                            df = pd.DataFrame(data_dict)
                                            df['station_id'] = the_site
                                            data_frames.append(df)
                                    

                                    # If variables structure does not have the expected field names (file, data, db, and lpdata)
                                    else:
                                        raise ValueError(f"variable: {var_name} has different fields than 'file', 'data', 'db'. It has {var_struct}")
                                # If variable shapes are not shape of (1,1), raise error
                                else:
                                    raise ValueError(f"variable: {var_name} does not have a 1,1 shape!")

        final_df = pd.concat(data_frames, ignore_index=True)

        # Convert times to datetimes
        final_df['datetime'] = final_df['time'].apply(self._matlab_datenum_to_datetime).dt.tz_localize('UTC')

        return final_df

    @staticmethod
    def _matlab_datenum_to_datetime(datenum: float) -> pd.Timestamp:
        """
        Convert MATLAB datenum to pandas Timestamp.
        MATLAB datenum represents days since January 0, 0000.
        """
        # MATLAB's datenum 719529.0 is Jan 1, 1970 (the Unix Epoch)
        MATLAB_EPOCH_OFFSET = 719529
        unix_epoch = datetime(1970, 1, 1)
        
        # Calculate days since the Unix Epoch
        days_since_unix_epoch = datenum - MATLAB_EPOCH_OFFSET

        try:
            # Create a Timestamp by adding the total timedelta to the Unix Epoch
            return pd.Timestamp(unix_epoch + timedelta(days=days_since_unix_epoch))
        except (ValueError, OverflowError):
            # Return Not a Time (NaT) for invalid or out-of-range dates
            return pd.NaT
