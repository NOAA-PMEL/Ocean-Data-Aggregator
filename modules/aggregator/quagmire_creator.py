import pandas as pd
import re
import pytz
from timezonefinder import TimezoneFinder
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np

# TODO: Figure out logic in edit_dates() method to to account for if local date/tims exiest and utc doesn't and if utc date/times exist and local
# doesn't
class QuagmireCreator:
    """
    Creates the QAQC file from the MachineReadable File. Edits things like dates and longitude and latitude
    """
    # Existing column names in Machine Readable file
    MR_OG_LAT_COL = 'Lat'
    MR_OG_LON_COL = 'Lon'
    LOCAL_TIME_COL = 'Collection_Time_local'
    LOCAL_DATE_COL = 'Collection_Date_local'
    UTC_TIME_COL = 'Collection_Time_UTC'
    UTC_DATE_COL = 'Collection_Date_UTC'
    CAST_COL = 'Cast'
    ROSETTE_POS_COL = 'Rosette_position'
    DEPTH_COL = 'Depth_m'
    SAMPLE_NAME_COL = 'FINAL Sample NAME'
    
    # New columns created in this class
    NEW_TIMEZONE_COL = 'local_timezone'
    NEW_UTC_DATE_COMBO_COL = 'utc_date_combined'
    NEW_LOCAL_DATE_COMBO_COL = "local_date_combined"
    NEW_LAT_DEC_DEG_COL = 'Lat_dec'
    NEW_LON_DEC_DEG_COL = 'Lon_dec'

    def __init__(self, machine_readable_files: list, station_col: str, lat_dir: str = None, lon_dir: str = None):

        self.station_col = station_col
        self.lat_dir = lat_dir
        self.lon_dir = lon_dir
        self.machine_readable_files = machine_readable_files
        self.quagmire_df = self.process_mr_file()
        self.quag_min_date, self.quag_max_date = self.get_quag_min_and_max_dates()
        self.quag_min_depth, self.quag_max_depth = self.get_quag_min_and_max_depths()
        self.quag_station_sites = self.quagmire_df[self.station_col].unique().tolist()

        # update cast and rosette postiion data types to Int64
        self.update_quag_cast_bottle_cols_dtype()

    def process_mr_file(self) -> pd.DataFrame:

        # Append all machine readable dfs together
        mr_dfs = []
        for file in self.machine_readable_files:
            df = pd.read_csv(file)

            # If the cast column is named 'Cast_No.' change to cast
            if 'Cast_No.' in df.columns:
                df.rename(columns={'Cast_No.': self.CAST_COL}, inplace=True)
            
            mr_dfs.append(df)
        mr_df = pd.concat(mr_dfs, ignore_index=True)

        # get lat/lon in decimal degrees
        mr_df_lat_lon_updated = self.convert_lat_lon_coords(mr_df=mr_df)
    
        # Edit dates - calculating local time or UTC time, and adding combined date/time columns for both local and UTC
        mr_df_dates_updated = self.edit_dates(mr_df=mr_df_lat_lon_updated)

        # Update Rosette_position column
        mr_df_rosette_updated = self.clean_rosette_position_and_cast(df=mr_df_dates_updated)

        return mr_df_rosette_updated
    
    def edit_dates(self, mr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Edit the dates in the Machine Readable df (mr_df) to create a UTC Date combined for Local Date
        TODO: Need to add for other case when need to find local from UTC maybe?
        """

        # Get timezone for each row based on latittude and longitude
        mr_df[self.NEW_TIMEZONE_COL] = mr_df.apply(lambda row: self.get_the_timzone_by_lat_lon(
            lat=row[self.NEW_LAT_DEC_DEG_COL], lon=row[self.NEW_LON_DEC_DEG_COL]), axis=1)


        # Create local combined date column by the local date and local time columns. 
        mr_df[self.NEW_LOCAL_DATE_COMBO_COL] = mr_df.apply(lambda row: self.combine_dates_and_times(
            date=row[self.LOCAL_DATE_COL], 
            time=row[self.LOCAL_TIME_COL])
            if pd.notna(row[self.LOCAL_DATE_COL]) and pd.notna(row[self.LOCAL_TIME_COL]) else None, # Explicitly return None if either is missing
        axis=1)
        
        # creates UTC combo date/time col based on new combo local date/time col
        mr_df[self.NEW_UTC_DATE_COMBO_COL] = mr_df.apply(
            lambda row: self.convert_local_time_to_utc(
            local_date_time_combined=row[self.NEW_LOCAL_DATE_COMBO_COL], 
            timezone=row[self.NEW_TIMEZONE_COL]),
        axis=1)
            
        # Replaces any utc date or utc time with updated utc date time calculated from local times
        mr_df[self.UTC_DATE_COL] = mr_df[self.NEW_UTC_DATE_COMBO_COL].fillna('').str.split('T').str[0]
        mr_df[self.UTC_TIME_COL] = mr_df[self.NEW_UTC_DATE_COMBO_COL].fillna('').str.split('T').str[1].str.replace('Z', '')

        return mr_df

    def convert_lat_lon_coords(self, mr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the latitude and longitude to decimal degrees. Right now only have function that will convert from this format 47˚ 52.467' N
        """
        try:
            mr_df[self.NEW_LAT_DEC_DEG_COL] = mr_df[self.MR_OG_LAT_COL].apply(lambda coord_str: self.get_coord_dec_degree_from_deg_min(
                coord_str=coord_str, coord_type='lat'
            ))
            mr_df[self.NEW_LON_DEC_DEG_COL] = mr_df[self.MR_OG_LON_COL].apply(lambda coord_str: self.get_coord_dec_degree_from_deg_min(
                coord_str=coord_str, coord_type='lon'
            ))
            return mr_df
        except ValueError as e: # TODO: update to try other formats if doesn't work
            raise ValueError(e)

    def get_coord_dec_degree_from_deg_min(self, coord_str, coord_type: str) -> float:
        """
        Converts a coordinate from Degrees, Mintues, Decimal Minutes to Decimal Degrees. E.g. -> 47˚ 52.467' N to 47.87445 
        coord_type should be 'lat' or 'lon'
        """
        # Return None if nan value
        if pd.isna(coord_str):
            return None
        
        coord_str = str(coord_str).strip()

        # If coord_str is an empty string return None
        if not coord_str:
            return None
        
        # If coordinates are missing then add in the direction
        if not coord_str.endswith(('N', 'S', 'E', 'W')):
            if coord_type == 'lat':
                coord_str = f"{coord_str} {self.lat_dir}"
            else:
                coord_str = f"{coord_str} {self.lon_dir}"
        
        pattern = r"(\d+)[°˚]\s*(\d+\.?\d*)\s*'\s*([NSEW])"
        match = re.match(pattern, coord_str.strip())

        if not match:
            raise ValueError(f"Invalid coordinate format: {coord_str}")
        
        degrees = float(match.group(1))
        minutes = float(match.group(2))
        direction = match.group(3)

        decimal = degrees + minutes/60
        if direction.upper() in ['W', 'S']:
            decimal = -decimal

        return decimal

    def get_the_timzone_by_lat_lon(self, lat, lon):
        """
        Finds the time zone by latitude and longitude
        """
        # If lat/lon is nan or empty strings, return None
        if pd.isna(lat) or pd.isna(lon) or not lat or not lon:
            return None
        
        tf = TimezoneFinder()
        tz = tf.timezone_at(lng=lon, lat=lat)
        if tz is None:
            raise ValueError(f"Time zone of lat: {lat}, lon: {lon} is None!")
        return tz
    
    def combine_dates_and_times(self, date:str, time:str) -> str:
        """
        Combine a date and a time into one str formated for ISO
        """
        if pd.isna(date) or pd.isna(time) or not date or not time:
            return None
       
        # 1. Format: MM/DD/YYYY HH:MM (Four-digit year)
        # Example: '6/15/2023 14:30'
        if '/' in date and len(time.split(':')) == 2 and len(date.split('/')[2]) == 4:
            combined_str = f"{date} {time}"
            datetime_obj = datetime.strptime(combined_str, '%m/%d/%Y %H:%M')
            return datetime_obj.isoformat()
        
        # 2. Format: MM/DD/YY HH:MM (Two-digit year) - FIX FOR YOUR DATA
         # Example: '11/8/21 13:04'
        elif '/' in date and len(time.split(':')) == 2 and len(date.split('/')[2]) == 2:
            combined_str = f"{date} {time}"
            datetime_obj = datetime.strptime(combined_str, '%m/%d/%y %H:%M') # Use %y
            return datetime_obj.isoformat()

        else:
            raise ValueError(f"The dates and times do not matcha format that we currently account for in the combine_dates_and_times function! Please add functionlity!")

    def convert_local_time_to_utc(self, local_date_time_combined: str, timezone: str):
        """
        Converts a local datetime to utc time and returns the 1) combined date/time, 2) just the date, 3) just the time.
        """
        if pd.isna(local_date_time_combined) or not local_date_time_combined:
            return np.nan # Must return nan for get_quag_min_max_dates to work.
        
        local_tz = pytz.timezone(timezone)
        local_dt_naive = datetime.fromisoformat(local_date_time_combined)

        # locallize the datetime object with the timezone found
        local_dt_aware = local_tz.localize(local_dt_naive)
        # print(f"local: {local_dt_aware}")

        # Convert the localized datetime to UTC
        utc_dt_aware = local_dt_aware.astimezone(pytz.utc)
        # print(utc_dt_aware)

        # The Full ISO date/time
        iso_format_with_Z = utc_dt_aware.strftime('%Y-%m-%dT%H:%M:%SZ')

        return iso_format_with_Z
    
    def get_quag_min_and_max_dates(self) -> tuple:
        """
        Get the min and max dates (UTC) from the Quagmire - to plug into ocean model data query or other possible reason
        """
        self.quagmire_df[self.UTC_DATE_COL] = pd.to_datetime(
            self.quagmire_df[self.UTC_DATE_COL])

        min_date = self.quagmire_df[self.UTC_DATE_COL].min()
        max_date = self.quagmire_df[self.UTC_DATE_COL].max()

        # Format the dates into YYYY-MM-DD and YYYY-MM strings
        min_date_full_format = min_date.strftime('%Y-%m-%d')
        max_date_full_format = max_date.strftime('%Y-%m-%d')

        return min_date_full_format, max_date_full_format
    
    def get_quag_min_and_max_depths(self) -> tuple:
        """
        Get the min and max depths from the quagmire - to get range of depths or ocean model, or other reasons
        """

        # Get min and max depths from quagmire
        min_depth = self.quagmire_df[self.DEPTH_COL].min()
        max_depth = self.quagmire_df[self.DEPTH_COL].max()

        return min_depth, max_depth
    
    def clean_rosette_position_and_cast(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove any letters from Rosette_position or cast column. So if 'Port 1' will just be 1"""
        # Need to add this because some of the OCNMS cruises use the cast column as the station column. For others, they should be different.
        cols_to_process = [self.ROSETTE_POS_COL]
        if self.CAST_COL != self.station_col:
            cols_to_process.append(self.CAST_COL)

        df[cols_to_process] = df[cols_to_process].astype(str)
        df[cols_to_process] = df[cols_to_process].apply(lambda x: x.str.extract(r'(\d+)', expand=False), axis=0)
        return df
    
    def update_quag_cast_bottle_cols_dtype(self):
        """
        Update the quag cast bottle column data types to int64, and depth to float
        """
        if self.CAST_COL != self.station_col:
            self.quagmire_df[self.CAST_COL] = self.quagmire_df[self.CAST_COL].astype('Int64')
        self.quagmire_df[self.ROSETTE_POS_COL] = self.quagmire_df[self.ROSETTE_POS_COL].astype('Int64')
        self.quagmire_df[self.DEPTH_COL] = self.quagmire_df[self.DEPTH_COL].astype(float)