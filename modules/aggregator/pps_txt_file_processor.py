import pandas as pd
from pathlib import Path


class PpsTextFileProcessor:
    """
    Processes a PPS text file and converts a specific data section into a pandas DataFrame.
    The file is expected to have a 'DEPLOYMENT DATA' section with a specific 3-line format per event.
    """

    SAMPLE_START_DATE_COL = 'sample_start_date'
    SAMPLE_DURATION_COL = 'sample_duration'
    SAMPLE_END_DATE_COL = 'sample_end_date'

    def __init__(self, pps_txt_file: str, sites: list):
        self.pps_txt_file = Path(pps_txt_file)
        # The list of sites applicable to project. Will pull out of file name and add to df
        self.sites = sites

    def convert_pps_txt_to_df(self):
        """
        Parses the PPS text file to extract 'DEPLOYMENT DATA' and converts it into a pandas DataFrame.
        This method is more robust than the original and handles inconsistent file headers.
        """
        with open(self.pps_txt_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        events = []
        data_section_started = False
        data_lines_buffer = []

        # First pass: identify and collect only the relevant data lines
        for line in lines:
            line_stripped = line.strip()

            if "DEPLOYMENT DATA" in line_stripped:
                # Found the start of the data section
                data_section_started = True
                continue

            if "PUMPING DATA" in line_stripped:
                # Found the end of the section, stop processing
                break

            if data_section_started:
                # Ignore header, blank, and separator lines
                if line_stripped and not line_stripped.startswith(('Event', '|', 'Number')):
                    # The data lines seem to have a leading space, strip it
                    clean_line = line.lstrip(' ')
                    data_lines_buffer.append(clean_line)
        
        # Second pass: parse the collected data lines
        # The data is structured in blocks of 3 lines per event.
        # However, there are blank lines in between, so we need to filter them out.
        clean_data_lines = [line.strip() for line in data_lines_buffer if line.strip()]

        for i in range(0, len(clean_data_lines), 3):
            if i + 2 >= len(clean_data_lines):
                break # Ensure a complete 3-line event block is available

            sample_line = clean_data_lines[i+2]
            fixative_flush_line = clean_data_lines[i+3]
            
            try:
                # The data is separated by pipes, so we split and then clean up empty strings
                sample_parts = [part.strip() for part in sample_line.split('|') if part.strip()]
                fixative_flush_parts = [part.strip() for part in fixative_flush_line.split('|') if part.strip()]

                # We need to check if the lists have the expected number of elements before accessing them
                if len(sample_parts) >= 6 and len(fixative_flush_parts) >= 6:
                    event_number = int(sample_parts[0])
                    sample_vol_pumped = int(sample_parts[5])
                    sample_duration = int(sample_parts[6]) # This is actually column 7
                    fixative_flush_vol_pumped = int(fixative_flush_parts[5])

                    # Create a record for this event
                    event_record = {
                        'event_number': event_number,
                        'sample_vol_pumped': sample_vol_pumped,
                        'sample_duration': sample_duration,
                        'sample_start_date': sample_parts[2],
                        'fixative_flush_vol_pumped': fixative_flush_vol_pumped
                    }
                    events.append(event_record)
                else:
                    # Log a warning if a line doesn't have the expected structure
                    print(f"Warning: Skipping malformed data lines starting at index {i} in the clean data buffer.")
            
            except (ValueError, IndexError) as e:
                # The try-except block is important for catching parsing errors
                print(f"Error parsing event starting at line group {i+1} in the clean data buffer: {e}")
                print(f"Problematic lines: \n1: {clean_data_lines[i]}\n2: {sample_line}\n3: {fixative_flush_line}")
                continue # Skip to the next event group

        # Create the final DataFrame from the list of events
        df = pd.DataFrame(events)

        # Make sample_start_date a datetime object
        df['sample_start_date'] = pd.to_datetime(df['sample_start_date'], format='%m/%d/%Y %H:%M:%S')

        # Add station_id if a match is found in the filename
        for site in self.sites:
            if site in self.pps_txt_file.name:
                df['station_id'] = site
                break  # Exit loop once a match is found

        # Calculate sample_end_date
        final_df = self.get_sample_end_date(pps_df=df)

        return final_df

    def get_sample_end_date(self, pps_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the sample end date based on the sample_start_date and the sample_duration.
        """
        # Make sure in date time format
        pps_df[self.SAMPLE_START_DATE_COL] = pd.to_datetime(pps_df[self.SAMPLE_START_DATE_COL])

        # Add end date
        pps_df[self.SAMPLE_END_DATE_COL] = pps_df[self.SAMPLE_START_DATE_COL] + pd.to_timedelta(pps_df[self.SAMPLE_DURATION_COL], unit='s')

        # Convert back to ISO format
        pps_df[self.SAMPLE_END_DATE_COL] = pd.to_datetime(pps_df[self.SAMPLE_END_DATE_COL], format='%m/%d/%Y %H:%M:%S')

        return pps_df
    
    