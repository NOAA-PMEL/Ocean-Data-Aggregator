import pandas as pd
import gspread #library that makes it easy for us to interact with the sheet
from google.oauth2.service_account import Credentials
from datetime import datetime

def load_google_sheet_as_df(google_sheet_id: str, sheet_name: str, header: int, google_sheet_json_cred: str) -> pd.DataFrame:
        """
        Load a google sheet as a data frame. The google_sheet_json_cred is the path to the credentials.json file 
        with credentials for acessing google sheets programatically."""

        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        creds = Credentials.from_service_account_file(google_sheet_json_cred, scopes=scopes)
        client = gspread.authorize(creds)

        sheet = client.open_by_key(google_sheet_id)
        worksheet = sheet.worksheet(sheet_name)
        
        # Get all values
        values = worksheet.get_all_values()
        headers = values[header]
        data_rows = values[header+1:]
        df = pd.DataFrame(data_rows, columns=headers)
        
        return df