import ctd
import pandas as pd
import re
from datetime import datetime

# TODO: Add time zone conversion in check_time_zone. Right now it just makes sure local time == utc which means they are the same

class CnvProcessor:
    def __init__(self, cnv_file: str, sites: list):

        self.cnv_file = cnv_file
        self.sites = sites
        self.start_time = self.get_the_start_time()
        self.system_times = self.get_system_time()
        self.cnv_df = self.convert_cnv_to_df()

    def convert_cnv_to_df(self) -> pd.DataFrame:

        df = ctd.from_cnv(self.cnv_file)

        self.units_dict = self.get_units_from_cnv_file()

        # Change column names to include longer name and units
        df.columns = [self.units_dict.get(col) for col in df.columns]

        # Calculate collection_date if timeJ_Julian_Days in the df
        df_dates_updated = self.get_collection_dates_from_julian_days(cnv_df=df)

        # Add the site
        df_site_updated = self.get_site(cnv_df=df_dates_updated)

        return df_site_updated 
    
    def get_units_from_cnv_file(self) -> dict:
        """
        Change the col name to be the longer name plus the units
        """
        units_dict = {}
        with open(self.cnv_file, 'r') as cnv_file:
            for line in cnv_file:
                if line.startswith('# name'):
                    parts = re.split('[=:]', line) # split by = and :
                    if '[' in line:
                        og_col = parts[1].strip()
                        new_col_name = parts[2].strip()
                    else:
                        og_col = parts[1].strip()
                        new_col_name = f"{og_col}_{parts[2].strip()}"
                    units_dict[og_col] = new_col_name.replace(' ', '_').replace('\n', '')
        
        return units_dict
    
    def get_the_start_time(self):
        """
        Get the start_time from the .cnv file
        """
        with open(self.cnv_file, 'r') as cnv_file:
            for line in cnv_file:
                if line.startswith('# start_time'):
                    lined = line.replace('[', '=') # replace bracket if like 'start_time = Jun 23 2022 18:21:23 [Instrument's time stamp, header]' and then split by = sign
                    start_time = lined.split('=')[1].strip()

                    # Convert to ISO format
                    dt = datetime.strptime(start_time, '%b %d %Y %H:%M:%S')
                    return dt
                    
                    
    def get_collection_dates_from_julian_days(self, cnv_df: pd.DataFrame) -> pd.DataFrame:
        """
        If the df has a column called timeJ_Julian_Days calculate the time stamps because its absolute (Julian days = number of days stince January 1 of the start of the year)
        """
        # See if the system is in localtime or UTC time. # Checks if closest is UTC or if the localtime and UTC time are the same.
        closest_time_to_start_time = min(self.system_times.keys(), key=lambda k: abs(self.system_times[k] - self.start_time))
        if closest_time_to_start_time == 'UTC' or (self.system_times['localtime'] == self.system_times['UTC']):
            try:
                cnv_df['time'] = pd.to_datetime(cnv_df['timeJ_Julian_Days'], unit='D', origin= f'{self.start_time.year}-01-01').dt.tz_localize('UTC')
            except KeyError as e:
                raise KeyError(f"No 'timeJ_Julian_Days' column found in the cnv_df: {e}")
        else:
            raise ValueError(f"HAve not yet accounted for .cnv file to be in local time - please add functionality to get_collectiond_dates_from_julian_days")

        return cnv_df

    def get_system_time(self) -> dict:
        """
        Gets the '* System UpLoad Time' line to return a dictionary like {'local_time': 'Jun 15 2023 09:23:07', 'UTC': 'Jun 15 2023 09:23:07'}"""
        # Find out if times are in UTC or local (assumes a line in the .cnv like this '* System UpLoad Time = Jun 15 2023 09:23:07 (localtime) = Jun 15 2023 16:23:07 (UTC))'
        # The pattern looks for three-letter month, day, year, hour, minute, and second (e.g. Jun 15 2023 09:23:07)
        self.system_times = {}
        pattern = r'(\w{3}\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}:\d{2})'  
        with open(self.cnv_file, 'r') as cnv_file:
            for line in cnv_file:
                if line.startswith('* System UpLoad Time'):
                    line_parts = line.split('=')
                    for part in line_parts:
                        matches = re.findall(pattern, part)
                        if matches:
                            if 'localtime' in part:
                                self.system_times['localtime'] = datetime.strptime(matches[0], '%b %d %Y %H:%M:%S')
                            elif 'UTC' in part:
                                self.system_times['UTC'] = datetime.strptime(matches[0], '%b %d %Y %H:%M:%S')
                    break # break after it finds the '*System Upload Time' line

        return self.system_times

    def get_site(self, cnv_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add the site to the df
        """
        with open(self.cnv_file, 'r') as cnv_file:
            file_content = cnv_file.read()

        # Find which sites exist
        found_sites = [site for site in self.sites if site in file_content]
        
        if len(found_sites) == 1:
            cnv_df['station_id'] = found_sites[0]
            return cnv_df
        if len(found_sites) > 1:
            raise ValueError(f'Multiple sites found in .cnv file {self.cnv_file} - please look into!')