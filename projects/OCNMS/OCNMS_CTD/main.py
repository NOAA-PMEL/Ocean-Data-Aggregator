from utils.mooring_aggregator import MooringAggregator


# python -m projects.OCNMS.OCNMS_CTD.main.py

mooring_aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_CTD/config.yaml')

df = mooring_aggregator.FINALmerge_quag_ctd_mooring_oceanmodel()
df.to_csv('/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_CTD/output/FinalOME_Merge_OCNMS23to23CTD_fixed.csv', index=False)


