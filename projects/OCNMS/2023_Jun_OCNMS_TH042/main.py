from utils.mooring_aggregator import MooringAggregator


# python -m projects.OCNMS.2023_Jun_OCNMS_TH042.main

mooring_aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/2023_Jun_OCNMS_TH042/config.yaml')
print(mooring_aggregator.quagmire_df)


