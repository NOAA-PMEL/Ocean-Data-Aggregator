import scipy.io as sio
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta


class MatFileProcessor:
    """
    Processes MATLAB .mat files and converts them to pandas DataFrames.
    Designed for OCNMS data structure but may work with similar formats.
    """

    def __init__(self, sites: list, mat_file: str):
        """
        Initialize the processor.
        
        Args:
            sites: List of site IDs to process (should match variable names in .mat file)
            mat_file: Path to the .mat file
        """
        self.sites = sites
        self.mat_file = mat_file

    @staticmethod
    def _process_mat_struct(mat_struct, parent_path: str, data_dict: dict):
        """
        Recursively processes a MATLAB struct and collects all data.
        
        Args:
            mat_struct: MATLAB struct object
            parent_path: Current path in the struct hierarchy
            data_dict: Dictionary to store the flattened data
        """
        if not hasattr(mat_struct, '_fieldnames'):
            data_dict[parent_path] = mat_struct
            return

        for field in mat_struct._fieldnames:
            field_data = getattr(mat_struct, field)
            current_path = f"{parent_path}_{field}"
            MatFileProcessor._process_mat_struct(field_data, current_path, data_dict)

    @staticmethod
    def _extract_units_from_labels(col_labels: list) -> dict:
        """
        Extract units from column labels that contain units in square brackets.
        
        Args:
            col_labels: List of column labels
            
        Returns:
            Dictionary mapping parameter names to their units
        """
        units_map = {}
        param_mappings = {
            'temperature': 'temp',
            'conductivity': 'cond', 
            'pressure': 'pres',
            'salinity': 'sal',
            'density': 'dens'
        }
        
        for label in col_labels:
            unit_match = re.search(r'\[(.*?)\]', label)
            if unit_match:
                unit = unit_match.group(1).strip()
                for param_name, short_name in param_mappings.items():
                    if param_name in label.lower():
                        units_map[short_name] = unit
                        break
        
        return units_map

    @staticmethod
    def _matlab_datenum_to_datetime(datenum: float) -> pd.Timestamp:
        """
        Convert MATLAB datenum to pandas Timestamp.
        MATLAB datenum represents days since January 0, 0000 (proleptic Gregorian calendar).
        
        Args:
            datenum: MATLAB datenum value
            
        Returns:
            pandas Timestamp
        """
        # MATLAB's datenum epoch is January 0, 0000, but Python's datetime
        # starts at January 1, 1970. The offset is 719529 days.
        matlab_epoch = datetime(1, 1, 1)  # January 1, 0001 (closest we can get)
        
        # More accurate approach: MATLAB datenum 1 = January 1, 0000
        # January 1, 1970 (Unix epoch) = MATLAB datenum 719529
        unix_epoch = datetime(1970, 1, 1)
        days_since_unix_epoch = datenum - 719529
        
        try:
            return pd.Timestamp(unix_epoch + timedelta(days=days_since_unix_epoch))
        except (ValueError, OverflowError):
            # Fallback for extreme dates
            return pd.NaT

    def _extract_metadata(self, var: str, file_data_dict: dict) -> tuple:
        """
        Extract metadata from the processed data dictionary.
        
        Args:
            var: Variable name
            file_data_dict: Flattened data dictionary
            
        Returns:
            Tuple of (station_id, depth_m, deployment_time)
        """
        # Extract station ID
        station_id = next((sid for sid in self.sites if sid in var), 'unknown')
        
        # Extract depth from variable name (assumes format ends with depth)
        try:
            depth_m = int(var.split('_')[-1])
        except (ValueError, IndexError):
            depth_m = np.nan
        
        # Extract deployment time
        try:
            deployment_time_raw = file_data_dict[f'{var}_db_DeploymentTime']
            if isinstance(deployment_time_raw, (int, float)):
                deployment_time = self._matlab_datenum_to_datetime(deployment_time_raw)
            else:
                deployment_time = pd.to_datetime(deployment_time_raw)
        except (KeyError, ValueError, TypeError):
            deployment_time = pd.NaT
            
        return station_id, depth_m, deployment_time

    def _process_time_series_data(self, file_data_dict: dict, var: str, 
                                  units_map: dict, deployment_time: pd.Timestamp) -> dict:
        """
        Process time-series data and apply proper column naming with units.
        
        Args:
            file_data_dict: Flattened data dictionary
            var: Variable name
            units_map: Mapping of parameter names to units
            deployment_time: Deployment timestamp for time conversion
            
        Returns:
            Dictionary of processed time-series data
        """
        data_source = 'data'
        source_prefix = f'_{data_source}_'
        
        # Extract time-series data
        time_series_data = {
            key.split('_')[-1]: value 
            for key, value in file_data_dict.items()
            if (source_prefix in key and 
                isinstance(value, np.ndarray) and 
                value.size > 0)
        }
        
        if not time_series_data:
            return {}
        
        processed_data = {}
        
        for short_name, data_array in time_series_data.items():
            if short_name == 'time':
                # Convert MATLAB datenum time to actual datetime
                if pd.notna(deployment_time):
                    # If we have deployment time, convert relative time to absolute
                    datetime_array = [
                        self._matlab_datenum_to_datetime(t) if not np.isnan(t) else pd.NaT 
                        for t in data_array
                    ]
                    processed_data['datetime'] = datetime_array
                    
                    # Also keep time since deployment in hours for convenience
                    time_since_deployment = [
                        (dt - deployment_time).total_seconds() / 3600 
                        if pd.notna(dt) and pd.notna(deployment_time) else np.nan
                        for dt in datetime_array
                    ]
                    processed_data['time_since_deployment_hours'] = time_since_deployment
                else:
                    # If no deployment time, just convert the raw time
                    datetime_array = [
                        self._matlab_datenum_to_datetime(t) if not np.isnan(t) else pd.NaT 
                        for t in data_array
                    ]
                    processed_data['datetime'] = datetime_array
            else:
                # Apply units to column name
                unit = units_map.get(short_name)
                column_name = f"{short_name}_{unit.replace(' ', '_')}" if unit else short_name
                processed_data[column_name] = data_array
        
        return processed_data

    def process_mat_data_like_ocnms(self) -> pd.DataFrame:
        """
        Main method to load the .mat file, process variables, and return a DataFrame.
        
        Returns:
            Combined DataFrame with all processed data
        """
        master_dataframes = []

        # Load .mat file
        try:
            mat_data = sio.loadmat(
                self.mat_file, squeeze_me=True, struct_as_record=False
            )
            print(f"Successfully loaded {self.mat_file}")
        except FileNotFoundError:
            print(f"Error: The file '{self.mat_file}' was not found.")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error loading .mat file: {e}")
            return pd.DataFrame()

        # Remove MATLAB metadata variables
        mat_data = {k: v for k, v in mat_data.items() if not k.startswith('__')}

        # Process each variable that matches our sites
        for var, info in mat_data.items():
            if not any(site_id in var for site_id in self.sites):
                continue
                
            print(f"\n--- Processing variable: {var} ---")

            # Flatten the MATLAB struct
            file_data_dict = {}
            self._process_mat_struct(info, var, file_data_dict)

            # Extract units from column labels
            units_map = {}
            try:
                col_labels = file_data_dict[f'{var}_file_collabels']
                units_map = self._extract_units_from_labels(col_labels)
                print(f"  Found units: {units_map}")
            except KeyError:
                print(f"  Warning: No 'file_collabels' found for {var}. Units will be omitted.")

            # Extract metadata
            station_id, depth_m, deployment_time = self._extract_metadata(var, file_data_dict)
            print(f"  Station: {station_id}, Depth: {depth_m}m, Deployment: {deployment_time}")

            # Process time-series data
            processed_data = self._process_time_series_data(
                file_data_dict, var, units_map, deployment_time
            )
            
            if not processed_data:
                print(f"  No time-series data found for {var}. Skipping.")
                continue

            # Create DataFrame
            num_rows = len(list(processed_data.values())[0])
            
            # Add metadata columns
            df_data = {
                'station_id': np.full(num_rows, station_id),
                'depth_m': np.full(num_rows, depth_m),
                'deployment_time': np.full(num_rows, deployment_time),
                **processed_data
            }

            df = pd.DataFrame(df_data)
            master_dataframes.append(df)
            
            print(f"  Successfully processed {var} with {num_rows} data points.")

        # Combine all DataFrames
        if master_dataframes:
            combined_df = pd.concat(master_dataframes, ignore_index=True)
            print(f"\nTotal combined data points: {len(combined_df)}")
            return combined_df
        else:
            print("No data was processed.")
            return pd.DataFrame()

    def save_to_csv(self, output_file: str) -> bool:
        """
        Process the .mat file and save the result to CSV.
        
        Args:
            output_file: Path to save the CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df = self.process_mat_data_like_ocnms()
            if not df.empty:
                df.to_csv(output_file, index=False)
                print(f"Data saved to {output_file}")
                return True
            else:
                print("No data to save.")
                return False
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return False


# Example usage:
if __name__ == "__main__":
    # Example usage
    sites = ['CE01', 'CE02', 'CE04']  # Replace with your actual site IDs
    mat_file = 'your_data.mat'  # Replace with your .mat file path
    
    processor = MatFileProcessor(sites, mat_file)
    df = processor.process_mat_data_like_ocnms()
    
    if not df.empty:
        # Save to CSV
        processor.save_to_csv('output_data.csv')
        
        # Print summary
        print("\nDataFrame Info:")
        print(df.info())
        print("\nFirst few rows:")
        print(df.head())
    else:
        print("No data was processed.")
