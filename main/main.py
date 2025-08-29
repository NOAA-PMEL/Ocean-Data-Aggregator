from utils.mooring_aggregator import MooringAggregator


# python -m main.main

mooring_agg = MooringAggregator(
    config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/main/config.yaml')

mooring_agg.mooring_df.to_csv('mooring_data.csv')
