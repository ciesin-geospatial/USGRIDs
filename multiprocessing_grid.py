import os
import datetime
import arcpy
import multiprocessing 
import sys
from pathlib import Path

def add_and_calculate_geodesic_unit(fc):
    #Create the temporary id for joining fields later
    arcpy.AddField_management(fc,'tempid','LONG')
    arcpy.CalculateField_management(fc,"tempid",'!OBJECTID!','PYTHON')
    #Calculate the area(land+water) by GEODESIC method => Kytt recommended
    arcpy.management.AddField(fc, "GEODESIC_AREA", "FLOAT")
    arcpy.management.CalculateGeometryAttributes(fc, "GEODESIC_AREA AREA_GEODESIC", "", "SQUARE_METERS")
def get_and_calculate_water(fc, waterfc, state):
    #Create water mask to calculate the water area
    water_mask = arcpy.analysis.Clip(fc, waterfc, f"in_memory/{state}water_clipped")
    arcpy.management.AddField(water_mask, "GEODESIC_WATER", "FLOAT")
    arcpy.management.CalculateGeometryAttributes(water_mask, "GEODESIC_WATER AREA_GEODESIC", "", "SQUARE_METERS")
    #join the GEODESIC_WATER field back to the boundary feature based on tempid
    arcpy.management.JoinField(fc, "tempid", water_mask, "tempid", ["GEODESIC_WATER"])
def calculate_land_area(fc):
    #Add field GEODESIC_LAND and calculate by AREA - WATER => LAND
    arcpy.AddField_management(fc, "GEODESIC_LAND", "FLOAT")
    with arcpy.da.UpdateCursor(fc, ["GEODESIC_AREA", "GEODESIC_WATER", "GEODESIC_LAND"]) as cursor:
            for row in cursor:
                if row[1] == None:
                    row[1] = 0 #After join there are Null values so I make it to be 0
                    row[2] = row[0] #GEODESIC_LAND is being calculated
                    cursor.updateRow(row)
                else:
                    row[2] = row[0] - row[1] 
                    cursor.updateRow(row)
def calculate_density(fc):
    #Get all the population related fields
    fields = [field.name for field in arcpy.ListFields(fc)]
    new_fields = fields[35:92] + fields[93:-6]
    #Calculate the densities for all those fields
    for variableField in new_fields:
        arcpy.AddField_management(fc,f"{variableField}_DENS",'DOUBLE')
        exp = f"get_dens(!{variableField}!, !GEODESIC_LAND!)"

        codeblock = """def get_dens(field, land):
                            if (land == 0 and field >= 0):
                                return 0
                            else:
                                return field/land

        """

        arcpy.management.CalculateField(fc,f"{variableField}_DENS",exp,"PYTHON", codeblock)
        
#Functions for PIXEL calculations (After intersect with fishnet) => Get counts
def add_and_calculate_geodesic_pixel(intersectFc):
    arcpy.management.AddField(intersectFc, "INTERSECTID", "LONG")
    arcpy.management.CalculateField(intersectFc, "INTERSECTID", "!OBJECTID!", "PYTHON")
    arcpy.management.AddField(intersectFc, "PIXEL_AREA", "FLOAT")
    arcpy.management.CalculateGeometryAttributes(intersectFc, "PIXEL_AREA AREA_GEODESIC", "", "SQUARE_METERS")
def get_and_calculate_water_pixel(intersectFc, waterfc, state):
    #Create water mask to calculate the water area
    water_mask = arcpy.analysis.Clip(intersectFc, waterfc, f"in_memory/{state}pixel_water_clipped")
    arcpy.management.AddField(water_mask, "PIXEL_WATER", "FLOAT")
    arcpy.management.CalculateGeometryAttributes(water_mask, "PIXEL_WATER AREA_GEODESIC", "", "SQUARE_METERS")
    #join the PIXEL_WATER field back to the boundary feature based on tempid
    arcpy.management.JoinField(intersectFc, "INTERSECTID", water_mask, "INTERSECTID", ["PIXEL_WATER"])
def calculate_land_pixel(intersectFc):
    #Add field PIXEL_LAND and calculate by AREA - WATER => LAND
    arcpy.AddField_management(intersectFc, "PIXEL_LAND", "FLOAT")
    with arcpy.da.UpdateCursor(intersectFc, ["PIXEL_AREA", "PIXEL_WATER", "PIXEL_LAND"]) as cursor:
            for row in cursor:
                if row[1] == None:
                    row[1] = 0 #After join there are Null values so I make it to be 0
                    cursor.updateRow(row)
                row[2] = row[0] - row[1] #PIXEL_LAND is being calculated
                cursor.updateRow(row)
def calculate_count(intersectFc):
    #Get all the population related fields
    dens_fields = [field.name for field in arcpy.ListFields(intersectFc) if "_DENS" in field.name]
    #Calculate the densities for all those fields
    for variableField in dens_fields:
        arcpy.AddField_management(intersectFc,f"{variableField[:-5]}_CNT",'DOUBLE')
        exp = f"get_count(!{variableField}!, !PIXEL_LAND!)"

        codeblock = """def get_count(field, land):
                            if (land == 0 and field >= 0):
                                return 0
                            else:
                                return field*land

        """

        arcpy.management.CalculateField(intersectFc,f"{variableField[:-5]}_CNT",exp,"PYTHON", codeblock)
        
#Function for getting the COUNT for the fields based on the intersected pixels 
def get_stats(fishnet, intersectFc, state):
    # create list of fields to generate statistics for
    statsFields = [["PIXEL_AREA","SUM"],["PIXEL_WATER","SUM"],["PIXEL_LAND","SUM"]]
    joinFields = ["SUM_PIXEL_AREA","SUM_PIXEL_WATER","SUM_PIXEL_LAND"]
    cntFields = arcpy.ListFields(intersectFc,"*_CNT")
    if len(cntFields)>0:
        [statsFields.append([field.name,"SUM"]) for field in cntFields]
        [joinFields.append("SUM_"+f.name) for f in cntFields]
    sumTable = f"in_memory/{state}summarizeTable1"
#     fishnetInMem = arcpy.management.CopyFeatures(fishnet, "in_memory/fishnet")
    # summarize the variables for each grid cell
    arcpy.Statistics_analysis(intersectFc,sumTable,statsFields,"PIXELID")
    arcpy.JoinField_management(fishnet,'PIXELID',sumTable,'PIXELID',joinFields)
    for joinField in joinFields:
        arcpy.AlterField_management(fishnet,joinField,joinField.replace("SUM_",""),joinField.replace("SUM_",""))
def get_raster(fishnet, state):
    # Convert fishnet to grids
    cellSize = 0.0083333333/2 #it's used for 1km but now 500m => cellSize/2
    gridFields = ["PIXEL_LAND","PIXEL_WATER"] + [field.name for field in arcpy.ListFields(fishnet, "*_CNT")]
    os.mkdir(os.path.join(r"G:/usgrids2020/raster/", state))
    outFolder = f"G:/usgrids2020/raster/{state}"
    for gridField in gridFields:
        outGrid = outFolder + os.sep + state + "_" + gridField + ".tif"
        arcpy.PolygonToRaster_conversion(fishnet,gridField,outGrid,'CELL_CENTER','#',cellSize)


def process(fc):
    fc_path = Path(fc)
    arcpy.env.workspace = str(fc_path.parent)
    arcpy.env.overwriteOutput=True
    state = fc_path.stem[3:5]   
    readTime = datetime.datetime.now()
    #Import the feature for boundary and water
    fcInMem = arcpy.management.CopyFeatures(fc, f"in_memory/usa{state}_fcInMem")
    water_fc = f"{fc[:-7]}_water_layer"
    waterInMem = arcpy.management.CopyFeatures(water_fc, f"in_memory/usa{state}_waterInMem")
    # add_and_calculate_geodesic_unit(fcInMem)
    #Use Polygon.getArea() since arcpy.CalculateGeometryAttributes could be a bug
    if not arcpy.Exists(f"G:/usgrids2020/raster/{state}"):  
        print(f"Processing for {state.upper()}")
        arcpy.AddField_management(fcInMem,'tempid','LONG')
        arcpy.CalculateField_management(fcInMem,"tempid",'!OBJECTID!','PYTHON')
        arcpy.management.AddField(fcInMem, "GEODESIC_AREA", "DOUBLE")
        with arcpy.da.UpdateCursor(fcInMem, ["SHAPE@", "GEODESIC_AREA"]) as cursor:
            for i in range(10):
                for row in cursor:
                    row[1] = row[0].getArea('GEODESIC', 'SquareMeters')
                    cursor.updateRow(row)   
        get_and_calculate_water(fcInMem, waterInMem, state)
        calculate_land_area(fcInMem)
        calculate_density(fcInMem)
        print("Starting intersect fishnet to boundary...")
        fish_net = f"G:/usgrids2020/fishnets/usa{state}_fishnet.gdb/usa{state}_fishnet"
        fishnetInMem = arcpy.management.CopyFeatures(fish_net, f"in_memory/{state}_fishnetInMem")
        intersectOut = f"in_memory/{state}intersect_fc"
        arcpy.analysis.Intersect([[fcInMem, 1], [fish_net, 2]], intersectOut, "NO_FID")
        # add_and_calculate_geodesic_pixel(intersectOut)
        arcpy.management.AddField(intersectOut, "INTERSECTID", "LONG")
        arcpy.management.CalculateField(intersectOut, "INTERSECTID", "!OBJECTID!", "PYTHON")
        arcpy.management.AddField(intersectOut, "PIXEL_AREA", "DOUBLE")
        with arcpy.da.UpdateCursor(intersectOut, ["SHAPE@", "PIXEL_AREA"]) as cursor:
            for i in range(10):
                for row in cursor:
                    row[1] = row[0].getArea('GEODESIC', 'SquareMeters')
                    cursor.updateRow(row)   
        get_and_calculate_water_pixel(intersectOut, waterInMem, state)
        calculate_land_pixel(intersectOut)
        calculate_count(intersectOut)
        print("Summarizing...")
        get_stats(fishnetInMem, intersectOut, state)
        get_raster(fishnetInMem, state.upper())
        print(f"Finished processing for {state.upper()} in {str(datetime.datetime.now() - readTime)}")


def main():
    arcpy.env.workspace = "G:/usgrids2020/final_gdbs/"
    gdbs = arcpy.ListWorkspaces("*", "FILEGDB")
    procList = [os.path.join(gdb,f"{os.path.basename(gdb)[:-4]}_joined") for gdb in gdbs]
    multiprocessing.set_executable(os.path.join(get_install_path(), 'python.exe'))
    pool = multiprocessing.Pool(processes=20,maxtasksperchild=1)
    results = pool.map(process, procList)
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
