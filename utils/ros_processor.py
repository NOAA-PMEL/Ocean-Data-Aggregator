import ctd
import pandas as pd
import re
from datetime import datetime

# TODO: Add time zone conversion in check_time_zone. Right now it just makes sure local time == utc which means they are the same

class RosProcessor:
    def __init__(self, ros_file: str, sites: list, day_convention: str):

        self.ros_file = ros_file
        self.sites = sites
        self.day_convention = day_convention # specifies julian day 0 or 1. 
        self.start_time = self.get_the_start_time()
        self.ros_df = self.convert_ros_to_df()

    def convert_ros_to_df(self) -> pd.DataFrame:

        df = self.get_initial_ros_df()

        # Calculate collection_date if timeJ_Julian_Days in the df
        df_dates_updated = self.get_collection_dates_from_julian_days(ros_df=df)

        # Add the site
        df_site_updated = self.get_site(ros_df=df_dates_updated)

        return df_site_updated 
    
    def get_initial_ros_df(self) -> pd.DataFrame:
        """
        Puts the initial .ros file into a df
        """
        with open(self.ros_file, 'r', encoding='latin-1') as f:
            lines = f.readlines()

        # find where header ends and data begins
        header_end = 0 
        column_names = []
        self.system_times = {}

        for i, line in enumerate(lines):
            if '*END*' in line: 
                header_end = i + 1
                break
            # Look for column name definitions (often starts with # or 'name' )
            if line.startswith('# name'):
                col_name_with_units = self.get_units_from_ros_line(line=line)
                column_names.append(col_name_with_units)

            if line.startswith('* System UpLoad Time'):
                self.get_system_time(line=line)

        # Read the data portion
        data_lines = lines[header_end:]
        data = []
        for line in data_lines:
            line = line.strip()
            if line and not line.startswith('*'):
                values = line.split()
                data.append(values)

        df = pd.DataFrame(data, columns=column_names if column_names else None)

        return df

    def get_units_from_ros_line(self, line) -> dict:
        """
        Gets the column name and creates a new column name with units from 
        a line in the ros file
        """
        parts = re.split('[=:]', line) # split by = and :
        if '[' in line:
            og_col = parts[1].strip()
            new_col_name = parts[2].strip()
        else:
            og_col = parts[1].strip()
            new_col_name = f"{og_col}_{parts[2].strip()}"
            new_col_name = new_col_name.replace(' ', '_').replace('\n', '')
    
        return new_col_name
    
    def get_the_start_time(self):
        """
        Get the start_time from the .cnv file
        """
        with open(self.ros_file, 'r') as cnv_file:
            for line in cnv_file:
                if line.startswith('# start_time'):
                    lined = line.replace('[', '=') # replace bracket if like 'start_time = Jun 23 2022 18:21:23 [Instrument's time stamp, header]' and then split by = sign
                    start_time = lined.split('=')[1].strip()

                    # Convert to ISO format
                    dt = datetime.strptime(start_time, '%b %d %Y %H:%M:%S')
                    return dt                
                    
    def get_collection_dates_from_julian_days(self, ros_df: pd.DataFrame) -> pd.DataFrame:
        """
        If the df has a column called timeJ_Julian_Days calculate the time stamps because its absolute (Julian days = number of days stince January 1 of the start of the year)
        """
        # If the data is '1-day' (JD 1.0 = Jan 1st 00:00:00), we use the raw value.
        # (This is the convention your original code was inadvertently applying before the fix.)
        if self.day_convention == '0 day':
            offset = -1
        # If the data is '1-day' (JD 1.0 = Jan 1st 00:00:00), we use the raw value.
        # (This is the convention your original code was inadvertently applying before the fix.)
        elif self.day_convention == '1 day':
            offset = 0
        else:
            raise ValueError(f"Invalid 'day_convention' specified: {self.day_convention}. Must be '0-day' or '1-day'.")
        # Apply the offset to the Julian Day column
        ros_df['timeJ_Julian_Days'] = ros_df['timeJ_Julian_Days'].astype(float)
        corrected_jd = ros_df['timeJ_Julian_Days'] + offset

        # See if the system is in localtime or UTC time. # Checks if closest is UTC or if the localtime and UTC time are the same.
        closest_time_to_start_time = min(self.system_times.keys(), key=lambda k: abs(self.system_times[k] - self.start_time))
        if closest_time_to_start_time == 'UTC' or (self.system_times['localtime'] == self.system_times['UTC']):
            try:
                ros_df['time'] = pd.to_datetime(
                    corrected_jd, 
                    unit='D', 
                    origin= f'{self.start_time.year}-01-01'
                    ).dt.tz_localize('UTC')
            except KeyError as e:
                raise KeyError(f"No 'timeJ_Julian_Days' column found in the cnv_df: {e}")
        else:
            raise ValueError(f"Have not yet accounted for .cnv file to be in local time - please add functionality to get_collectiond_dates_from_julian_days")

        return ros_df

    def get_system_time(self, line) -> dict:
        """
        Gets the '* System UpLoad Time' line to return a dictionary like {'local_time': 'Jun 15 2023 09:23:07', 'UTC': 'Jun 15 2023 09:23:07'}"""
        # Find out if times are in UTC or local (assumes a line in the .cnv like this '* System UpLoad Time = Jun 15 2023 09:23:07 (localtime) = Jun 15 2023 16:23:07 (UTC))'
        # The pattern looks for three-letter month, day, year, hour, minute, and second (e.g. Jun 15 2023 09:23:07)
        pattern = r'(\w{3}\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}:\d{2})'  
        if line.startswith('* System UpLoad Time'):
            line_parts = line.split('=')
            for part in line_parts:
                matches = re.findall(pattern, part)
                if matches:
                    if 'localtime' in part:
                        self.system_times['localtime'] = datetime.strptime(matches[0], '%b %d %Y %H:%M:%S')
                    elif 'UTC' in part:
                        self.system_times['UTC'] = datetime.strptime(matches[0], '%b %d %Y %H:%M:%S')

        return self.system_times

    def get_site(self, ros_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add the site to the df
        """
        with open(self.ros_file, 'r') as cnv_file:
            file_content = cnv_file.read()

        # Find which sites exist
        found_sites = [site for site in self.sites if site in file_content]
        
        if len(found_sites) == 1:
            ros_df['station_id'] = found_sites[0]
            return ros_df
        if len(found_sites) > 1:
            raise ValueError(f'Multiple sites found in .ros file {self.ros_file} - please look into!')