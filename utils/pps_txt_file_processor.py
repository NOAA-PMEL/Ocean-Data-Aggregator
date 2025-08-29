import pandas as pd
from pathlib import Path


class PpsTextFileProcessor:

    def __init__(self, pps_txt_file: str, sites: list):

        self.pps_txt_file = Path(pps_txt_file)
        # The list of sites applicable to project. Will pull out of file name and add to df
        self.sites = sites

    def convert_pps_txt_to_df(self):

        with open(self.pps_txt_file, 'r') as file:
            lines = file.readlines()

            start_idx = None
            end_idx = None
            for i, line in enumerate(lines):
                if "DEPLOYMENT DATA" in line:
                    # find first row of data (6 lines after DEPLOYMENT DATA)
                    start_idx = i + 6
                if "PUMPING DATA" in line:
                    end_idx = i - 4

            data_lines = []
            for line in lines[start_idx:end_idx]:
                line = line.strip()
                if not line.strip().startswith('|') and line.strip():  # ignore empty lines
                    data_lines.append(line)

            # Parse data in 3 lines per event
            events = []
            for i in range(0, len(data_lines), 3):
                if i + 2 >= len(data_lines):  # Make sure we have all 3 lines
                    break

                # Get all three lines for this event
                sample_line = data_lines[i + 1].strip()
                fixative_flush_line = data_lines[i + 2].strip()

                try:
                    # Parse each line
                    sample_parts = [part.strip()
                                    for part in sample_line.split('|')]
                    fixative_flush_parts = [part.strip()
                                            for part in fixative_flush_line.split('|')]

                    # Extract the data I want
                    event_number = int(sample_parts[0])
                    sample_vol_pumped = int(sample_parts[5])
                    sample_duration = int(sample_parts[6])
                    sample_start_date = sample_parts[2]
                    fixative_flush_vol_pumped = int(fixative_flush_parts[5])

                    # create event_record
                    event_record = {
                        'event_number': event_number,
                        'sample_vol_pumped': sample_vol_pumped,
                        'sample_duration': sample_duration,
                        'sample_start_date': sample_start_date,
                        'fixative_flush_vol_pumped': fixative_flush_vol_pumped
                    }

                    events.append(event_record)

                except (ValueError, IndexError) as e:
                    print(f"Error parsing event starting at line {i}: {e}")

            df = pd.DataFrame(events)

            # Add station_id to df
            for site in self.sites:
                if site in self.pps_txt_file.name:
                    df['station_id'] = site

            return df
