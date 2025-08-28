from utils.mooring_aggregator import MooringAggregator

# python -m main.main

mooring_agg = MooringAggregator(
    config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/main/config.yaml')

mooring_df = mooring_agg.convert_mat_files_to_dfs()
print(mooring_df)
