from utils.mooring_aggregator import MooringAggregator


# python -m main.main

mooring_agg = MooringAggregator(
    config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/main/config.yaml')

df = mooring_agg.merge_ctd_with_pps()
df.to_csv('mpps_ctd_merge.csv', index=False)