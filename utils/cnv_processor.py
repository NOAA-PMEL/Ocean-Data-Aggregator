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
                    break

        # Check the local time equals UTC time which means its in UTC time
        utc_time = self.check_time_zone()
        if utc_time == True:
            return dt
    
    def check_time_zone(self):
        """
        Check the time zone. Checks that the localtime mattches the utc time and if so assumes utc time
        * System UpLoad Time = Jun 23 2022 18:53:58 (localtime) = Jun 23 2022 18:53:58 (UTC)
        """
        try: # try checking the 'System UpLoad Time = ' line in header first (from OCNMS .cnv files)
            with open(self.cnv_file, 'r') as cnv_file:
                for line in cnv_file:
                    if line.startswith('* System UpLoad Time'):
                        parts = line.split('=')
                        for part in parts:
                            if '(localtime)' in part:
                                local_time = part.split(' (')[0]
                            if '(UTC)' in part:
                                utc_time = part.split(' (')[0]

                        if local_time == utc_time:
                            return True
                        else:
                            return False
        except:
            raise ValueError(f"Uhoh trouble checking what the time zone is - check how this is given in the .cnv file for {self.cnv_file}")
                    
    def get_collection_dates_from_julian_days(self, cnv_df: pd.DataFrame) -> pd.DataFrame:
        """
        If the df has a column called timeJ_Julian_Days, this uses the start_time to calculate the actual collection date
        """
        start_dt = pd.to_datetime(self.start_time)

        try:
            cnv_df['collection_date'] = start_dt + pd.to_timedelta(cnv_df['timeJ_Julian_Days'], unit='days')
        except KeyError:
            print("No 'Julian_Days' column found in the cnv_df so collection_date can not be calculated from Julian_Days")

        return cnv_df
                    
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