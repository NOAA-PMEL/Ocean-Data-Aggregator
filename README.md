## A repo for OME's workflow of integrating OME's machine readable sample collection.csv files with various combinations of:

1. Mooring data (.mat files). May only work for OCNMS .mat files
2. PPS data (.txt files)
3. Ocean Model data (.nc files)
4. CTD data (.ros files, or .cnv files, or .nc files)
5. Bottle data (.csv files)
6. Nutrient data (.csv files)

So far this has only been tested with OCNMS and EcoFoci's DY2306 and SKQ2021-15S.

See projects' nested `main.py` files to understand how to use. Depending on the merge needed, may need to write new merge methods. 

### Right now the available merge combination methods are:
##### MooringAggregator module methods:
1. `FINALmerge_quag_pps_mooring_oceanmodel`: merges the quagmire (the cleaned up version of OME's machine readable file) with pps, mooring, and ocean model data.
   - step 1: merges the quagmire with the pps data on station and rosette and local time.
   - step 2: get pps utc time (since pps is in local time)
   - step 3. Merges the output df from above with mooring data by utc time of the start and end time window of the pps.
   - step 4: Merges the output of that previous df with ocean model data on the utc time window of the pps and station and averages the numeric columns. Also creates std_dev columns for numeric columns. 
3. `FINALmerge_quag_ctd_mooring_oceanmodel`: merges the quagmire, CTD, mooring, and ocean model data together
   - step 1: merges the quagmire with the ctd data on station and utc time.
   - step 2. Merges that df with mooring data on station and utc time.
   - step 3: Merges that df with ocean model data on station and utc time.
##### CTDBottleAggregator module methods:
1. `FINALmerge_quag_ctd_btl_nutrient_for_missing_btlNumbers`: merges the quagmire, bottle, ctd, and nutrient data together, but used in case the bottle numbers are missing for some of the data.
   - step 1: Merges the ctd data with the bottle data on cast and pressure.
   - step 2: Merges that df with the quagmire on cast and rosette
   - step 3: Merges that df with the nutrient data on cast and nearest depth
3. `FINALmerge_quag_btl_nutrient`: Merges the quagmire with the bottle on cast number and bottle number. Depends on bottle numbers, cast numbers and pressure/depth being present.
   - step 1: Merges the quagmire with the bottle data on cast and rosette.
   - step 2: Merges that df with the nutrient data on cast and nearest depth.

### To run:
1. install dependencies in a conda environment. **TODO**: create requirements.txt for conda environment and add directions for setting up a conda environment.
2. Create a directory in the project folder with the name of the project. Note: there can be multiple "subprojects" or cruise directories or what not within the folder. It all depends on what will get standardized together. E.g. a cruise will get its own directory, pps samples will get their own, etc. See other folders in the projects directory for examples.
3. Create a config.yaml file that points to all the necessary files to be integrated. See other projects' config.yaml for examples. Note that ctd data can point to net cdfs, cnv, or ros files. Need to name the key in the yaml file accordingly. See [these lines of code](https://github.com/NOAA-PMEL/Ocean-Data-Aggregator/blob/a457d4157458f55a4619dd808ab505adaee06ff4/utils/mooring_aggregator.py#L35C13-L45C65) to see what the options are for the keys depending on the file type.
4. create a `main.py` file in your project directory, import the appropriate modules, instantiate your aggregator, and run the final merge method. save as csv in desired place. See other `main.py` files for examples.
