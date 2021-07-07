from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *
import sys
from download_process_emissions import *

from io import BytesIO, TextIOWrapper
from zipfile import ZipFile
import urllib.request
import csv
from shapely.geometry import Point
import geopandas as gpd

emis_save_path =  'data/downloaded_nei_emissions/'

# To view the files in this NEI directory, copy and paste the link in a web browser. 
# There will be a pop-up box; login as guest.
nei_dir_path = 'ftp://newftp.epa.gov/air/emismod/2017/2017emissions/'#2017gb_inventory_CMV_12US1_29jun2020.zip'

files_to_download  = sys.argv[1:]

print('\nFiles to download: ')
for f in files_to_download:
    print(f)
print()

for f in files_to_download:
    
    # Download file from EPA website.
    url = urllib.request.urlopen(str(nei_dir_path + f + '.zip'))

    #"ftp://newftp.epa.gov/air/emismod/2017/2017emissions/2017gb_inventory_point_09feb2021.zip"
    #https://gaftp.epa.gov/air/emismod/2017/2017emissions/2017gb_inventory_point_09feb2021.zip
        
    VOC, NOx, NH3, SOx, PM2_5 = [], [], [], [], []
    height, diam, temp, velocity = [], [], [],  []
    coords = []

    def add_record(row):
        """ Process one row of the emissions file """
        pol = row[12] # The pollutant is in the 13th column of the CSV file
                      # (In Python, the first column is called column 0.)
        emis = row[13] # We are only extracting annual total emissions here. 
                       # If monthly emissions are reported, we'll miss them.
                       # Emissions are short tons/year.
        if emis == '': return
        if pol in ['VOC', 'VOC_INV', 'XYL', 'TOL', 'TERP', 'PAR', 'OLE', 'NVOL', 'MEOH', 
                   'ISOP', 'IOLE', 'FORM', 'ETOH', 'ETHA', 'ETH', 'ALD2', 'ALDX', 'CB05_ALD2', 
                   'CB05_ALDX', 'CB05_BENZENE', 'CB05_ETH', 'CB05_ETHA', 'CB05_ETOH', 
                   'CB05_FORM', 'CB05_IOLE', 'CB05_ISOP', 'CB05_MEOH', 'CB05_OLE', 'CB05_PAR', 
                   'CB05_TERP', 'CB05_TOL', 'CB05_XYL', 'ETHANOL', 'NHTOG', 'NMOG', 'VOC_INV']:
            VOC.append(float(emis))
            NOx.append(0)
            NH3.append(0)
            SOx.append(0)
            PM2_5.append(0)
        elif pol in ['PM25-PRI', 'PM2_5', 'DIESEL-PM25', 'PAL', 'PCA', 'PCL', 'PEC', 'PFE', 'PK', 
                    'PMG', 'PMN', 'PMOTHR', 'PNH4', 'PNO3', 'POC', 'PSI', 'PSO4', 'PTI']:
            VOC.append(0)
            NOx.append(0)
            NH3.append(0)
            SOx.append(0)
            PM2_5.append(float(emis))
        elif pol in ['NOX', 'HONO', 'NO', 'NO2']:
            VOC.append(0)
            NOx.append(float(emis))
            NH3.append(0)
            SOx.append(0)
            PM2_5.append(0)
        elif pol == 'NH3':
            VOC.append(0)
            NOx.append(0)
            NH3.append(float(emis))
            SOx.append(0)
            PM2_5.append(0)
        elif pol == 'SO2':
            VOC.append(0)
            NOx.append(0)
            NH3.append(0)
            SOx.append(float(emis))
            PM2_5.append(0)
        else: return

        h = row[17]
        height.append(float(h) * 0.3048) if h != '' else height.append(0)

        d = row[18]
        diam.append(float(d) * 0.3048) if d != '' else diam.append(0)

        t = row[19]
        temp.append((float(t) - 32) * 5.0/9.0 + 273.15) if t != '' else temp.append(0)

        v = row[21]
        velocity.append(float(v) * 0.3048) if v != '' else velocity.append(0)

        coords.append(Point(float(row[23]), float(row[24])))
    
    
    with ZipFile(BytesIO(url.read())) as zf:
        for contained_file in zf.namelist():
#             if "egu" in contained_file: # Only process files with electricity generating unit (EGU) emissions.
            for row in csv.reader(TextIOWrapper(zf.open(contained_file, 'r'), newline='')):
                if (len(row) == 0) or (len(row[0]) == 0) or (row[0][0] == '#'): 
                    continue
                add_record(row)

    emis = gpd.GeoDataFrame({
        "VOC": VOC, "NOx": NOx, "NH3": NH3, "SOx": SOx, "PM2_5": PM2_5,
        "height": height, "diam": diam, "temp": temp, "velocity": velocity,
    }, geometry=coords, crs={'init': 'epsg:4269'})
    
    
    try:
        # Saves in shapefile format
        emis.to_file(emis_save_path + f)
        print(f + ' saved.')
        
    except ValueError as err:
        print()
        print('Error processing file: ' + f)
        print(err)
        continue
