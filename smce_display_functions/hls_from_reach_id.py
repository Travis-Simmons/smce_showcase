# https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/02_Data_Discovery_CMR-STAC_API.html

from pystac_client import Client  
from collections import defaultdict    
import json
import geopandas
# import geoviews as gv
from cartopy import crs
# gv.extension('bokeh', 'matplotlib')
import geopandas as gpd

# find hls tiles given a point

def find_hls_tiles(point=False, band=False, limit=False, collections = ['HLSL30.v2.0', 'HLSS30.v2.0'], date_range = False):

    STAC_URL = 'https://cmr.earthdata.nasa.gov/stac'


    catalog = Client.open(f'{STAC_URL}/LPCLOUD/')



    try:
        x, y = point[0], point[1]
        # print(x,y)
    except TypeError:
        print("Point must be in the form of [lat,lon]")
        raise

    point = geopandas.points_from_xy([x],[y])
    point = point[0]



    # JOHN - THIS IS WHERE YOU WOULD ADD IN SEARCH PARAMETERS
    if date_range:

        search = catalog.search(
            collections=collections, intersects = point, datetime=date_range)
    else:
        search = catalog.search(
            collections=collections, intersects = point)



    # print(f'{search.matched()} Tiles Found...')


    item_collection = search.get_all_items()

    if limit:
        item_collection = item_collection[:limit]

    if band:
        links = []
        if type(band) == list:
            for i in item_collection:
                for b in band:
                    link = i.assets[b].href
                    # print(link)
                    links.append(link)
        
        else:
            for i in item_collection:
                link = i.assets[band].href
                links.append(link)
    
    else:
        links =[]
        for i in item_collection:
            # print(i.assets)
            for key in i.assets:
                if key.startswith('B'):
                    # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                    link = i.assets[key].href

                    # print(link)
                    links.append(link)

    return links

# given a reach ID, find the nodes

import glob
import netCDF4
import os
import numpy as np


data_dir = '/home/confluence/data/mnt/input/sword'





def get_reach_nodes(data_dir, reach_id):

    all_nodes = []

    files = glob.glob(os.path.join(data_dir, '*'))
    print(f'Searching across {len(files)} continents for nodes...')

    for i in files:

        rootgrp = netCDF4.Dataset(i, "r", format="NETCDF4")

        node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

        if len(node_ids_indexes[0])!=0:
            for y in node_ids_indexes[0]:
                node_id = str(rootgrp.groups['nodes'].variables['node_id'][y].data.astype('U'))
                all_nodes.append(node_id)



            # all_nodes.extend(node_ids[0].tolist())

        rootgrp.close()

    print(f'Found {len(set(all_nodes))} nodes...')
    return list(set(all_nodes))





# get_reach_nodes(data_dir,74270100221)



# given a reach ID, find the lat/lon points of all nodes



import glob
import netCDF4
import os
import numpy as np



def get_reach_node_cords(data_dir, reach_id):

    all_nodes = []

    files = glob.glob(os.path.join(data_dir, '*'))
    print(f'Searching across {len(files)} continents for nodes...')

    for i in files:

        rootgrp = netCDF4.Dataset(i, "r", format="NETCDF4")

        node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

        if len(node_ids_indexes[0])!=0:
            for y in node_ids_indexes[0]:

                lat = str(rootgrp.groups['nodes'].variables['x'][y].data.astype('U'))
                lon = str(rootgrp.groups['nodes'].variables['y'][y].data.astype('U'))
                all_nodes.append([lat,lon])



            # all_nodes.extend(node_ids[0].tolist())

        rootgrp.close()

    print(f'Found {len(all_nodes)} nodes...')
    return all_nodes



def find_download_links_for_reach_tiles(data_dir, reach_id):
    node_coords = get_reach_node_cords(data_dir,reach_id)
    all_links = []
    for i in node_coords:
        # print(i)
        links = find_hls_tiles(i,limit=1)
        # print(links)
        all_links.extend(links)
        break

    return list(set(all_links))



# https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/05_Data_Access_Direct_S3.html

# need to make netrc file



# %matplotlib inline
import matplotlib.pyplot as plt
from datetime import datetime
import os
import requests
import boto3
import numpy as np
import xarray as xr
import rasterio as rio
from rasterio.session import AWSSession
from rasterio.plot import show
import rioxarray
# import geoviews as gv
# import hvplot.xarray
# import holoviews as hv
# gv.extension('bokeh', 'matplotlib')


s3_cred_endpoint = 'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'

def get_temp_creds():
    temp_creds_url = s3_cred_endpoint
    print(temp_creds_url)
    return requests.get(temp_creds_url).json()


temp_creds_req = get_temp_creds()


session = boto3.Session(aws_access_key_id=temp_creds_req['accessKeyId'], 
                        aws_secret_access_key=temp_creds_req['secretAccessKey'],
                        aws_session_token=temp_creds_req['sessionToken'],
                        region_name='us-west-2')


rio_env = rio.Env(AWSSession(session),
                  GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
                  GDAL_HTTP_COOKIEFILE=os.path.expanduser('~/cookies.txt'),
                  GDAL_HTTP_COOKIEJAR=os.path.expanduser('~/cookies.txt'))

            
rio_env.__enter__()

