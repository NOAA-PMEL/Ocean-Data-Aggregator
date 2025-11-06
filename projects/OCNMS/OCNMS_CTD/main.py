from utils.mooring_aggregator import MooringAggregator
from utils.mat_file_processor import MatFileProcessor
from utils.ros_processor import RosProcessor


# python -m projects.OCNMS.OCNMS_CTD.main.py

mooring_aggregator = MooringAggregator(config_yaml='/Users/zalmanek/Development/Ocean-Data-Aggregator/projects/OCNMS/OCNMS_CTD/config.yaml')

df = mooring_aggregator.FINALmerge_quag_ctd_mooring_oceanmodel()
df.to_csv('/Users/zalmanek/Development/PMEL-OME-OCNMS/Brynn_OCNMS_data_reorganized/FinalOME_Merge_OCNMS23to23CTD_fixed.csv', index=False)

# ros_processor = RosProcessor(ros_file='/Users/zalmanek/Downloads/CAST_CE042_PPS.ros', sites = ['CE042'], day_convention='0 day')

