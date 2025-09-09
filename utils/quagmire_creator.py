import pandas as pd
import re
import pytz
from timezonefinder import TimezoneFinder
from datetime import datetime
from zoneinfo import ZoneInfo

class QuagmireCreator:
    """
    Creates the QAQC file from the MachineReadable File. Edits things like dates and longitude and latitude
    """
    def __init__(self, machine_readable_files: list, station_col: str):

        # Existing column names in Machine Readable file
        self.mr_og_lat_col = 'Lat'
        self.mr_og_lon_col = 'Lon'
        self.local_time_col = 'Collection_Time_local'
        self.local_date_col = 'Collection_Date_local'
        self.utc_time_col = 'Collection_Time_UTC'
        self.utc_date_col = 'Collection_Date_UTC'
        self.station_col = station_col
        self.cast_col = 'Cast'
        self.rosette_pos_col = 'Rosette_position'
        self.depth_col = 'Depth_m'
        
        # New columns created in this class
        self.new_timezone_col = 'local_timezone'
        self.new_utc_date_combo_col = 'utc_date_combined'
        self.new_local_date_combo_col = "local_date_combined"
        self.new_lat_dec_deg_col = 'Lat_dec'
        self.new_lon_dec_deg_col = 'Lon_dec'
        
        self.machine_readable_files = machine_readable_files
        self.quagmire_df = self.process_mr_file()
        self.quag_min_date, self.quag_max_date = self.get_quag_min_and_max_dates()
        self.quag_min_depth, self.quag_max_depth = self.get_quag_min_and_max_depths()
        self.quag_station_sites = self.quagmire_df[self.station_col].unique().tolist()

    def process_mr_file(self) -> pd.DataFrame:

        # Append all machine readable dfs together
        mr_dfs = []
        for file in self.machine_readable_files:
            df = pd.read_csv(file)
            mr_dfs.append(df)
        mr_df = pd.concat(mr_dfs, ignore_index=True)
        
        # get lat/lon in decimal degrees
        mr_df_lat_lon_updated = self.convert_lat_lon_coords(mr_df=mr_df)
    
        # Edit dates - calculating local time or UTC time, and adding combined date/time columns for both local and UTC
        mr_df_dates_updated = self.edit_dates(mr_df=mr_df_lat_lon_updated)

        return mr_df_dates_updated
    
    def edit_dates(self, mr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Edit the dates in the Machine Readable df (mr_df) to update for local or UTC time, depending on what is missing
        """

        # Get timezone for each row
        mr_df[self.new_timezone_col] = mr_df.apply(lambda row: self.get_the_timzone_by_lat_lon(
            lat=row[self.new_lat_dec_deg_col], lon=row[self.new_lon_dec_deg_col]), axis=1)


        ### THIS BLOCK OF CODE check to see if all the local time and local dates are filled out and if they are, then creates a combined_local_date
        # column, it then checks fall the utc date OR the utc time columns are empty, and if tone of them is empty, it calculates the combined_utc_date/time from
        # the local date. This overrides any UTC date/times that already exist in the mr_df with the calculated from the local. Not sure if for some reason this
        # wouldn't be okay? 
        if not mr_df[self.local_date_col].isnull().any() and not mr_df[self.local_time_col].isnull().any():
            # create a local_date_combined_col
            mr_df[self.new_local_date_combo_col] = mr_df.apply(lambda row: self.combine_dates_and_times(
                date=row[self.local_date_col], 
                time=row[self.local_time_col]), 
                axis=1)
        
            # If UTC time or UTC date is empty for all rows, use local time/date to get UCT time:
            if mr_df[self.utc_time_col].isnull().all() or mr_df[self.utc_date_col].isnull().all():
                # Get a series of tuples
                mr_df[self.new_utc_date_combo_col] = mr_df.apply(
                    lambda row: self.convert_local_time_to_utc(
                    local_date_time_combined=row[self.new_local_date_combo_col], 
                    timezone=row[self.new_timezone_col]), 
                    axis=1)
                
                mr_df[self.utc_date_col] = mr_df[self.new_utc_date_combo_col].str.split('T').str[0]
                mr_df[self.utc_time_col] = mr_df[self.new_utc_date_combo_col].str.split('T').str[1].str.replace('Z', '')

        return mr_df

    def convert_lat_lon_coords(self, mr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the latitude and longitude to decimal degrees. Right now only have function that will convert from this format 47˚ 52.467' N
        """
        try:
            mr_df[self.new_lat_dec_deg_col] = mr_df[self.mr_og_lat_col].apply(self.get_coord_dec_degree_from_deg_min)
            mr_df[self.new_lon_dec_deg_col] = mr_df[self.mr_og_lon_col].apply(self.get_coord_dec_degree_from_deg_min)
            return mr_df
        except ValueError as e: # TODO: update to try other formats if doesn't work
            raise ValueError(e)

    def get_coord_dec_degree_from_deg_min(self, coord_str) -> float:
        """
        Converts a coordinate from Degrees, Mintues, Decimal Minutes to Decimal Degrees. E.g. -> 47˚ 52.467' N to 47.87445 
        """
        pattern = r"(\d+)[˚]\s*(\d+\.?\d*)['\s]*([NSEW])"
        match = re.match(pattern, coord_str.strip())

        if not match:
            raise ValueError("Invalid coordinate format")
        
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
        tf = TimezoneFinder()
        tz = tf.timezone_at(lng=lon, lat=lat)
        if tz is None:
            raise ValueError(f"Time zone of lat: {lat}, lon: {lon} is None!")
        return tz
    
    def combine_dates_and_times(self, date:str, time:str) -> str:
        """
        Combine a date and a time into one str formated for ISO
        """

        # If date in format like '6/15/2023' and time like 14:30 so '%m/%d/%Y %H:%M'
        if '/' in date and len(time.split(':')) == 2 and len(date.split('/')[2]) == 4:
            combined_str = f"{date} {time}"
            datetime_obj = datetime.strptime(combined_str, '%m/%d/%Y %H:%M')
            iso_format_str = datetime_obj.isoformat()

            return iso_format_str
        else:
            raise ValueError(f"The dates and times do not matcha format that we currently account for in the combine_dates_and_times function! Please add functionlity!")

    def convert_local_time_to_utc(self, local_date_time_combined: str, timezone: str):
        """
        Converts a local datetime to utc time and returns the 1) combined date/time, 2) just the date, 3) just the time.
        """
        local_tz = pytz.timezone(timezone)
        local_dt_naive = datetime.fromisoformat(local_date_time_combined)

        # locallize the datetime object with the timezone found
        local_dt_aware = local_tz.localize(local_dt_naive)

        # Convert the localized datetime to UTC
        utc_dt_aware = local_dt_aware.astimezone(pytz.utc)

        # The Full ISO date/time
        iso_format_with_Z = utc_dt_aware.strftime('%Y-%m-%dT%H:%M:%SZ')

        return iso_format_with_Z
    
    def get_quag_min_and_max_dates(self) -> tuple:
        """
        Get the min and max dates (UTC) from the Quagmire - to plug into ocean model data query or other possible reason
        """
        self.quagmire_df[self.utc_date_col] = pd.to_datetime(
            self.quagmire_df[self.utc_date_col])

        min_date = self.quagmire_df[self.utc_date_col].min()
        max_date = self.quagmire_df[self.utc_date_col].max()

        # Format the dates into YYYY-MM-DD and YYYY-MM strings
        min_date_full_format = min_date.strftime('%Y-%m-%d')
        max_date_full_format = max_date.strftime('%Y-%m-%d')

        return min_date_full_format, max_date_full_format
    
    def get_quag_min_and_max_depths(self) -> tuple:
        """
        Get the min and max depths from the quagmire - to get range of depths or ocean model, or other reasons
        """

        # Get min and max depths from quagmire
        min_depth = self.quagmire_df[self.depth_col].min()
        max_depth = self.quagmire_df[self.depth_col].max()

        return min_depth, max_depth