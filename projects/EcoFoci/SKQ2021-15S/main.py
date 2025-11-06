from utils.ctd_bottle_aggregator import CtdBottleAggregator
import pandas as pd

ctd_bottle_aggregator = CtdBottleAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/EcoFoci/SKQ2021-15S/config.yaml')
final_merge_df = ctd_bottle_aggregator.FINALmerge_quag_ctd_btl_nutrient_for_missing_btlNumbers()
final_merge_df.to_csv('/Users/zalmanek/Development/OME-EcoFOCI/EcoFOCI/data/1_SampleCollection/2021_SikuliaqDBO_CruiseData/2021_SikuliaqDBO_CruiseData/Fixed_FinalOME_merge_SKQ21-15S.csv')