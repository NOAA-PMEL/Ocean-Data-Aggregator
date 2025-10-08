from utils.mooring_aggregator import MooringAggregator
from utils.aggregator import Aggregator

# E1857.OC0723, and E1858.OC0723 didn't merge with any pps data because there are non within 1 hour, or even 12 hours to 1 day, 
# depending on the sample. 
aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_PPS/config.yaml')

final_df = aggregator.FINALmerge_quag_pps_mooring_oceanmodel()

final_df.to_csv("/Users/zalmanek/Development/PMEL-OME-OCNMS/Brynn_OCNMS_data_reorganized/FinalOME_Merge_OCNMS23to23PPS_fixed.csv", index=False)




