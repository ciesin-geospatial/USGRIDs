import os 
# import warnings
# warnings.filterwarnings("ignore")
import arcpy
import datetime
import xarray as xr
import glob
import multiprocessing
import rioxarray
from pathlib import Path
import sys

def process(state):
    # List of filenames containing raster data
    iso = Path(state).stem
    if not arcpy.Exists(f"G:/usgrids2020/multidimensional_files/{iso}"):
        readTime = datetime.datetime.now()
        print(f"Processing for {iso}", state)
        filenames = glob.glob(f"{state}/*.tif")  # Add your list of filenames here
        filenames = filenames[54:63] + filenames[66:] #Dropping the GPW variables
        # List of variable names (optional)
        variable_names = [field[25:-4] for field in filenames]  # Add variable names corresponding to each filename
        # Create a list of xarray.DataArray objects
        data_arrays = []
        for f, var_name in zip(filenames, variable_names):
            ds = xr.Dataset()
            data_array = rioxarray.open_rasterio(f, chunks={'x': 5490, 'y': 5490})
            data_array.name = var_name  # Assign variable name
            data_arrays.append(data_array)
        os.mkdir(os.path.join(r"G:/usgrids2020/multidimensional_files/", iso))
        outFolder = f"G:/usgrids2020/multidimensional_files/{iso}"
        dataset = xr.Dataset({data_array.name: data_array for data_array in data_arrays})

        # Concatenate the list of DataArray objects along the time dimension
        dataset.to_netcdf(os.path.join(outFolder, f"usa{iso.lower()}_multidimensional.nc"))
        print(f"Finished processing for {iso} in {str(datetime.datetime.now() - readTime)}")

def main():
    state_path = r"G:/usgrids2020/raster"
    state_list = os.listdir(state_path)
    state_list = [os.path.join(state_path, state) for state in state_list] 
    multiprocessing.set_executable(os.path.join(get_install_path(), 'python.exe'))
    pool = multiprocessing.Pool(processes=20,maxtasksperchild=1)
    results = pool.map(process, state_list)
    for result in results:
        print(result)

    # Synchronize the main process with the job processes to
    # ensure proper cleanup.
    pool.close()
    pool.join()

def get_install_path():
    if sys.maxsize > 2**32: return sys.exec_prefix #We're running in a 64bit process
    #We're 32 bit so see if there's a 64bit install
    path = "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
    from winreg import OpenKey, QueryValue
    from winreg import HKEY_LOCAL_MACHINE, KEY_READ, KEY_WOW64_64KEY
    try:
        with OpenKey(HKEY_LOCAL_MACHINE, path, 0, KEY_READ | KEY_WOW64_64KEY) as key:
            return QueryValue(key, "InstallPath").strip(os.sep) #We have a 64bit install, so return that.
    except: return sys.exec_prefix #No 64bit, so return 32bit path
if __name__ == '__main__':
    main()
