import pandas as pd

# Mappings of short cruise code to long cruise code
# long cruise code was incorrect in files, but short cruise code was correct. This is used in the ocnms main.py files.
ocnms_short_long_cruises = {
    "OC0919": "2019_Sept_OCNMS_Tatoosh",
    "OC0821": "2021_Aug_OCNMS_StormPetrel",
    "OC1021": "2021_Oct_OCNMS_StormPetrel",
    "OC0622": "2022_June_OCNMS_StormPetrel",
    "OC0722": "2022_July_OCNMS_StormPetrel",
    "OC0822": "2022_Aug_OCNMS_StormPetrel",
    "OC0922": "2022_Sept_OCNMS_StormPetrel",
    "OC0623": "2023_June_OCNMS_StormPetrel",
    "OC0723": "2023_July_OCNMS_StormPetrel",
    "OC0524": "2024_May_OCNMS_StormPetrel",
    "OC0624": "2024_June_OCNMS_StormPetrel",
    "OC0724-1": "2024_July9_OCNMS_StormPetrel",
    "OC0724-2": "2024_July27_OCNMS_StormPetrel",
    "OC0824": "2024_Aug_OCNMS_StormPetrel",
    "OC0924": "2024_Sept_OCNMS_StormPetrel",
    "OC0525": "2025_May_OCNMS_StormPetrel",
    "OC0625": "2025_June_OCNMS_StormPetrel",
    "CE042-PPS-0821" : "2021_AugSept_OCNMS_CE042-PPS",
    "TH042-PPS-0821": "2021_AugSept_OCNMS_TH042-PPS",
    "TH042-PPS-0622": "2022_JuneJuly_OCNMS_TH042-PPS",
    "CE042-PPS-0622": "2022_JuneJuly_OCNMS_CE042-PPS",
    "TH042-PPS-0822": "2022_AugSept_OCNMS_TH042-PPS",
    "TH042-PPS-0623": "2023_JuneJuly_OCNMS_TH042-PPS",
    "TH042-PPS-0524": "TH042-PPS-0524-0624",
    "TH042-PPS-0624": "TH042-PPS-0624-0724",
    "TH042-PPS-0525": "TH042-PPS-0525-0625"
}

def fix_long_cruise_code(row: pd.Series):

    short_code = row['Cruise_ID_short']

    if short_code in ocnms_short_long_cruises:
        return ocnms_short_long_cruises.get(short_code)
        
    else: 
        raise ValueError(f"short cruise code not in the ocnms_short_long_cruises dictionary: {short_code}")