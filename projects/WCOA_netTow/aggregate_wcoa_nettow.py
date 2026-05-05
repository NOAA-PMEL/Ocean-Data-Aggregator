import pandas as pd
from modules.utils import load_google_sheet_as_df

ome_sample_sheet = {
    'google_id': '1Fw1Bk81wrZsztWo-Ef_qnuUdHhnVk0Q7IVmkvQaQ9-I',
    'sheets': ['Plankton Samples'],
    'header': 0
    }

net_tow_env_sheet = {
    'google_id': '1so9jBaotH_zo7tr9EmSisB8yNhvj9Byy2m4fslJUOnA',
    'sheets': [
        'Vertical (200 µm)',
        'Bongo (100 + 200 µm)',
        'Bongo (335 µm)'
               ],
    'header': 1
}

json_cred_path = '/Users/zalmanek/Development/Ocean-Data-Aggregator/credentials.json'

tow_type = {
    'Vertical (200 µm)': 'Vertical 200',
    'Bongo (100 + 200 µm)': 'Bongo 200 and Bongo 100',
    'Bongo (335 µm)': 'Bongo 335'
}
def get_dfs():
    
    sample_df = load_google_sheet_as_df(google_sheet_id=ome_sample_sheet.get('google_id'),
                                        sheet_name=ome_sample_sheet.get('sheets')[0],
                                        header=ome_sample_sheet.get('header'),
                                        google_sheet_json_cred=json_cred_path)
    # filter to samples that are needed
    sample_df = sample_df[sample_df['FINAL Sample NAME'].str.extract(r'P(\d+)')[0].astype(float).between(101, 210)]
    # Remove extra character in some of the station names
    sample_df["Dive No./ Station No./ Cast No."] = sample_df["Dive No./ Station No./ Cast No."].str.replace(r'\(.*?\)', '', regex=True).str.strip()
    sample_df["Dive No./ Station No./ Cast No."] = sample_df["Dive No./ Station No./ Cast No."].str.replace('CC73.3-70', '73.3-70')
    sample_df["Dive No./ Station No./ Cast No."] = sample_df["Dive No./ Station No./ Cast No."].str.replace("SB4", "SCORP")
    env_dfs = []
    for sheet in net_tow_env_sheet.get('sheets'):
        df = load_google_sheet_as_df(google_sheet_id=net_tow_env_sheet.get('google_id'),
                                    sheet_name=sheet,
                                    header=net_tow_env_sheet.get('header'),
                                    google_sheet_json_cred=json_cred_path)
        tow = tow_type.get(sheet)
        df['tow_type'] = tow
        env_dfs.append(df)

    env_df = pd.concat(env_dfs)

    return sample_df, env_df

def merge_dfs(samp_df: pd.DataFrame, env_df: pd.DataFrame):

    merged = samp_df.merge(env_df, 
                           left_on="Dive No./ Station No./ Cast No.",
                           right_on="Station #", how='left')
    
    result = merged[merged.apply(
        lambda row: pd.isna(row['tow_type']) or row['Sampling method'] in row['tow_type'],
        axis=1
    )].drop_duplicates(subset=samp_df.columns.to_list()).reset_index()

    return result

def main() -> None:

    sample_df, env_df = get_dfs()


    final_df = merge_dfs(samp_df=sample_df, env_df=env_df)
    final_df['Negative_control'] = 'FALSE'

    # Fix the incorrect longitude
    final_df = final_df.replace('118 14.9357 N', '118 14.9357 W')
    
    final_df.to_csv('/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/WCOA_netTow/FinalOMEMerge_WCOA_netTow.csv', index=False)
if __name__ == "__main__":
    main()


