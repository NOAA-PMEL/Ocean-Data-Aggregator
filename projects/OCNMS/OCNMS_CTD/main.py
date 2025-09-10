from utils.mooring_aggregator import MooringAggregator


# python -m projects.OCNMS.OCNMS_CTD.main.py

mooring_aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_CTD/config.yaml')

mooring_aggregator.merge_quag_moor()


