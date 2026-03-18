# Access Landsat data form Planetary computer
# output raster and csv 
# v1
import os
import pystac_client
import planetary_computer
import pandas as pd
import rioxarray
import numpy as np
import sys
import matplotlib.pyplot as plt

proj_path = os.path.join(sys.prefix, 'share', 'proj')
os.environ['PROJ_LIB'] = proj_path
os.environ['PROJ_DATA'] = proj_path

# 2. Configuration
point_lon, point_lat = 36.4, -1.5 
time_range = "2023-01-01/2025-01-01"
output_folder = "landsat_final_rasters"
buffer_deg = 0.005 # ~500m window

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 3. Connection
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# 4. Search
search = catalog.search(
    collections=["landsat-c2-l2"],
    intersects={"type": "Point", "coordinates": [point_lon, point_lat]},
    datetime=time_range,
    query={"eo:cloud_cover": {"lt": 60}}
)

items = search.item_collection()
print(f"Found {len(items)} items. Extracting Science Products...")

data = []
SCALE, OFFSET = 0.00341802, 149.0

# 5. Extraction Loop
for item in items:
    # Safely find the Thermal (LST) and QA assets
    assets = item.assets
    lst_asset = assets.get('lwir11') or assets.get('st_b10')
    qa_asset = assets.get('qa_pixel')

    if lst_asset is None or qa_asset is None:
        continue # Skip scenes without Surface Temperature data

    date_str = item.datetime.strftime("%Y-%m-%d")
    
    try:
        # Open both bands - use 'masked=True' to handle NoData immediately
        with rioxarray.open_rasterio(planetary_computer.sign(lst_asset).href, chunks=True) as ds_lst, \
             rioxarray.open_rasterio(planetary_computer.sign(qa_asset).href, chunks=True) as ds_qa:
            
            # Reproject a small window to Lat/Lon to avoid the UTM mismatch
            # This is only possible if PROJ_LIB is set correctly (Step 1)
            ds_lst_geo = ds_lst.rio.reproject("EPSG:4326")
            ds_qa_geo = ds_qa.rio.reproject("EPSG:4326")

            # Extract point
            raw_val = ds_lst_geo.sel(x=point_lon, y=point_lat, method="nearest").values[0]
            qa_val = ds_qa_geo.sel(x=point_lon, y=point_lat, method="nearest").values[0]

            # Cloud Masking (Bit 3: Cloud, Bit 4: Cloud Shadow)
            is_cloudy = (int(qa_val) & (1 << 3)) != 0
            is_shadow = (int(qa_val) & (1 << 4)) != 0
            
            if raw_val > 0 and not (is_cloudy or is_shadow):
                temp_c = (raw_val * SCALE + OFFSET) - 273.15
                
                # Check for physical reality
                if 10 < temp_c < 65:
                    # Clip and Export GeoTIFF
                    clipped = ds_lst_geo.rio.clip_box(
                        minx=point_lon - buffer_deg, miny=point_lat - buffer_deg,
                        maxx=point_lon + buffer_deg, maxy=point_lat + buffer_deg
                    ).compute()
                    
                    # Scale to Celsius
                    final_raster = (clipped * SCALE + OFFSET) - 273.15
                    
                    tif_path = os.path.join(output_folder, f"LST_{date_str}.tif")
                    final_raster.rio.to_raster(tif_path)
                    
                    data.append({
                        "Date": date_str,
                        "LST_Celsius": round(float(temp_c), 2),
                        "QA_Value": int(qa_val),
                        "Satellite": item.properties.get("platform", "")
                    })
                    print(f"Captured: {date_str} -> {round(temp_c, 1)}°C")
                else:
                    print(f"Filtered: {date_str} temp ({round(temp_c, 1)}°C) out of bounds.")
            else:
                if raw_val > 0:
                    print(f"Skipped: {date_str} was cloudy.")

    except Exception as e:
        # Silently skip errors (usually projection/out of bounds)
        continue

# 6. Save and Final Summary
if data:
    df = pd.DataFrame(data).sort_values("Date")
    df.to_csv("landsat_timeseries_results.csv", index=False)
    print(f"\nSUCCESS: {len(data)} clear GeoTIFFs saved to /{output_folder}")
else:
    print("\nNo CLEAR data found. Try increasing 'eo:cloud_cover' in the search or check point coordinates.")