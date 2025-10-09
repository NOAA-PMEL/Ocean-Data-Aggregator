from utils.ctd_bottle_aggregator import CtdBottleAggregator
import pandas as pd

ctd_bottle_aggregator = CtdBottleAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/EcoFoci/DY2306/config.yaml')
final_merge_df = ctd_bottle_aggregator.FINALmerge_quag_btl_nutrient()
final_merge_df.to_csv('/Users/zalmanek/Development/OME-EcoFOCI/EcoFOCI/data/1_SampleCollection/2023_EcoFociSpringMooring_CruiseData/Fixed_FinalOME_merge_DY23-06_with_nutrients.csv')