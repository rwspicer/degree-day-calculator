# Changelog

## 3.0.0 [2022-11-28]
### changed
- refactored utility code
- refactored other code as needed
- modified calc_cell coded to supported new save worker
- filling of holes re-implemented

### added
- worker to save degree day and root data out side of calculation process.

## 2.2.0 [2022-11-28]
### changed
- fill_missing_by_interpolation has been rewritten, with loc_type option added


## 2.1.0 [2022-08-09]
### added
- resluting multigrid files have utilty vesion written to metadata

### fixed
- major bug in calc_degreee_days.calc_degree_days_for_cell were if a smoothing 
factor was needed to be set on spline curve the loop for iterating to find the 
factor to used always exited after one iteration


## 2.0.2 [2022-06-14]
### fixed
- bug causing crash if logging dir did not exist
- bug where Multigrid was used instead of TemporalGrid when loading 
  from monthly data tiff files  

## 2.0.1 [2022-06-06]
### fixed
- adds additional logic to fallback step to fix bug where default method fails
  due to array length mismatch

### changed 
- progress bar display shows percent as well as current/max now


## 2.0.0 [2022-06-01]
### added 
- new fallback method to use when spline method fails
- new cli options see readme/cli docs
- masking features

### changed
- much of code refactored to support new multigrids features (from multigrids 0.8.0)
- rewrote cli
- saving/loading has been rewritten

### removed
-- local copy of multigrids

## 1.1.1 [2022-02-08]
### fixed
- Bug with multiprocessing Manager()
- Bug on macos caused by changes to multiprocessing in python 3.8+

### changed
- multigirds.tools creates memory mapped arrays
- minor changes to handle large datasets
- cleanup old commented out code

## 1.1.0 [2022-02-08] 
### fixed 
- bug fixes in ddc/CLIlib.py

### added
- options to save roots
- loading/saving inputs and results to and from multigrids
- option to restart processing at a given pixel

## 1.0.1 [2020-09-10]
### Fixed 
- bug in sort snap caused by numbers in paths

### Added
- Displaying of last 15 file name is when loading data

## 1.0.0 [2020-06-21]
First release  
