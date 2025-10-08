from utils.mooring_aggregator import MooringAggregator
from utils.mat_file_processor import MatFileProcessor


# python -m projects.OCNMS.OCNMS_CTD.main.py

mooring_aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_CTD/config.yaml')

df = mooring_aggregator.FINALmerge_quag_ctd_mooring_oceanmodel()
df.to_csv('/Users/zalmanek/Development/PMEL-OME-OCNMS/Brynn_OCNMS_data_reorganized/FinalOME_Merge_OCNMS23to23CTD_fixed.csv', index=False)

### Use to get .mat file as a csv
# mat_processor = MatFileProcessor(mat_file="/Users/zalmanek/Documents/OCNMS_data_organized/mooring_data/2023.mat", sites=['TH042', 'CE042'], sensors=['CTPO'])
# df = mat_processor.get_ocnms_df_from_mat_file()
# df.to_csv('2023_mat_files.csv')
