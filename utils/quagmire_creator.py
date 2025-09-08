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
    def __init__(self, machine_readable_file: str):
        
        self.mr_df = pd.read_csv(machine_readable_file)
        self.mr_og_lat_col = 'Lat'
        self.mr_og_lon_col = 'Lon'
        self.new_lat_dec_deg_col = 'Lat_dec'
        self.new_lon_dec_deg_col = 'Lon_dec'
        self.local_time_col = 'Collection_Time_local'
        self.local_date_col = 'Collection_Date_local'
        self.new_local_date_combo_col = "local_date_combined"
        self.utc_time_col = 'Collection_Time_UTC'
        self.utc_date_col = 'Collection_Date_UTC'
        self.new_utc_date_combo_col = "utc_date_combined"
        self.new_timezone_col = 'local_timezone'

    def process_mr_file(self) -> pd.DataFrame:
        
        # get lat/lon in decimal degrees
        self.convert_lat_lon_coords()
    
        # Edit dates - calculating local time or UTC time, and adding combined date/time columns for both local and UTC
        self.edit_dates()
    
    def edit_dates(self):
        """
        Edit the dates in the Machine Readable df (mr_df) to update for local or UTC time, depending on what is missing
        """

        # Get timezone for each row
        self.mr_df[self.new_timezone_col] = self.mr_df.apply(lambda row: self.get_the_timzone_by_lat_lon(
            lat=row[self.new_lat_dec_deg_col], lon=row[self.new_lon_dec_deg_col]), axis=1)


        ### THIS BLOCK OF CODE check to see if all the local time and local dates are filled out and if they are, then creates a combined_local_date
        # column, it then checksi fall the utc date and utc time columns are empty, and if they are then calculates the combined_utc_date/time from
        # the local date
        # May need to rework logic if for some reason, some rows would have local date and some would have utc date. 
        if not self.mr_df[self.local_date_col].isnull().any() and not self.mr_df[self.local_time_col].isnull().any():
            # create a local_date_combined_col
            self.mr_df[self.new_local_date_combo_col] = self.mr_df.apply(lambda row: self.combine_dates_and_times(
                date=row[self.local_date_col], time=row[self.local_time_col]), axis=1)
        
            # If UTC time and UTC date is empty for all rows, use local time/date to get UCT time:
            if self.mr_df[self.utc_time_col].isnull().all() and self.mr_df[self.utc_date_col].isnull().all():
                self.mr_df[self.new_utc_date_combo_col] = self.mr_df.apply(lambda row: self.convert_local_time_to_utc(
                    local_date_time_combined=row[self.new_local_date_combo_col], timezone=row[self.new_timezone_col]), axis=1)



    def convert_lat_lon_coords(self):
        """
        Convert the latitude and longitude to decimal degrees. Right now only have function that will convert from this format 47˚ 52.467' N
        """
        try:
            self.mr_df[self.new_lat_dec_deg_col] = self.mr_df[self.mr_og_lat_col].apply(self.get_coord_dec_degree_from_deg_min)
            self.mr_df[self.new_lon_dec_deg_col] = self.mr_df[self.mr_og_lon_col].apply(self.get_coord_dec_degree_from_deg_min)
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
        Converts a local datetime to utc time
        """
        local_tz = pytz.timezone(timezone)
        local_dt_naive = datetime.fromisoformat(local_date_time_combined)

        # locallize the datetime object with the timezone found
        local_dt_aware = local_dt_naive.replace(tzinfo=local_tz)

        # Convert the localized datetime to UTC
        utc_dt = str(local_dt_aware.astimezone(ZoneInfo('UTC')))
        utc_dt_obj = datetime.fromisoformat(utc_dt)
        iso_format_with_Z = utc_dt_obj.strftime('%Y-%m-%dT%H:%M:%SZ')

        return iso_format_with_Z