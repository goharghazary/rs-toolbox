# Fetch the crop-type map from the Thünen STAC API, locate the GeoTIFF asset, and save it locally


import requests
import rasterio
from urllib.parse import urljoin

# STAC API base
API = "https://eodata.thuenen.de/stac/api/v1/"

collection = "crop-type-map-latest"
item_id    = "crop-type-map-latest-2025"

# Build URL to item JSON
item_url = urljoin(API, f"collections/{collection}/items/{item_id}")

# Fetch STAC item JSON
item = requests.get(item_url).json()

# Extract GeoTIFF link
for k, asset in item["assets"].items():
    href = asset.get("href", "")
    if href.lower().endswith((".tif", ".tiff")):
        tif_href = href
        break
else:
    raise RuntimeError("No .tif asset found.")

print("GeoTIFF URL:", tif_href)

# Test read directly (COG streaming)
with rasterio.open(tif_href) as src:
    print("CRS:", src.crs)
    print("Size:", src.width, src.height)
    print("Bands:", src.count)

# Optional: download locally
out_path = "crop_type_2025.tif"
with requests.get(tif_href, stream=True) as r:
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)

print("Saved:", out_path)
