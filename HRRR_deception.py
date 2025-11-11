import xarray as xr
import numpy as np
from pathlib import Path
from herbie import Herbie
from pathlib import Path
import pandas as pd
import glob
import os
import re
from datetime import datetime
import sys
import requests
import time
import cfgrib  
import traceback


selected_stid = sys.argv[1]
latitude = float(sys.argv[2])
longitude = float(sys.argv[3])


subset= pd.DataFrame([   
    {"latitude": latitude,        
    "longitude": longitude,
    "stid": selected_stid}
])


max_retries = 3
retry_delay = 5


base_folder = r"/rcfs/projects/nationalwind/yliu/HRRR_grib2"
all_data = []

date_folders = sorted([
    os.path.join(base_folder, name)
    for name in os.listdir(base_folder)
    if os.path.isdir(os.path.join(base_folder, name)) and re.match(r"\d{8}", name)
])

for folder_path in date_folders:

    grib_files = glob.glob(os.path.join(folder_path, "*.grib2"))
    base_date = datetime.strptime(os.path.basename(folder_path), "%Y%m%d")

    for file_path in grib_files:
        print(f"Processing {file_path}")
        m = re.search(r"\.t(\d{2})z\.", os.path.basename(file_path))
        if not m:
            raise ValueError(f"Can't find run hour in {file_path}")
        hour = int(m.group(1))
     
        run_time = base_date.replace(hour=hour)
        try:
            for attempt in range(max_retries):
                try:   
                    H = Herbie(
                        date=run_time,  
                        model="hrrr",
                        product="sfc",
                        fxx=0,
                        file=file_path,
                        verbose=False,
                        save_index = True
                    )
                
                

                    # --- 80 m wind (u & v) ---
                    wind_ds = H.xarray("(?:UGRD|VGRD):80 m")
                    wind_pts = wind_ds.herbie.pick_points(subset)

                    # --- surface T & P ---
                    met_ds  = H.xarray("(?:TMP|PRES):surface")
                    met_pts = met_ds.herbie.pick_points(subset)

                # --- 2 m RH ---
                    rh_ds   = H.xarray("(?:RH):2 m")
                    rh_pts  = rh_ds.herbie.pick_points(subset)

                    # Merge the three point‑datasets into one time slice
            
                    combined = xr.merge([wind_pts, met_pts,rh_pts],compat="override")
                    all_data.append(combined)
                    #del H.index_as_dataframe
                    break
                except requests.exceptions.ConnectionError as e:
                    print(f"Connection error (attempt {attempt+1}/{max_retries}): {e}")
                    time.sleep(retry_delay)
            else:
                print(f"Skipping {file_path} after {max_retries} failed attempts")
                continue
        except ValueError as e:
            if "No index file was found" in str(e):
                print(f"Skipping bad file (index error): {file_path}")
                #bad_results.append(folder_path)
                #nan_placeholder = combined.copy(deep=True)
                #for var in nan_placeholder.data_vars:
                    #nan_placeholder[var].values[:] = np.nan
                #all_data.append(nan_placeholder)
                continue
            else:
                raise  
        except EOFError as e:  # catches the cfgrib truncated / empty file case
            print(f"Skipping corrupted/truncated GRIB file: {file_path}. Error: {e}")
            continue
        except Exception as e:
            # optional: catch other unexpected issues but log them
            print(f"Unexpected error on {file_path}: {e}")
            traceback.print_exc()
            continue

    full_ds = xr.concat(all_data, dim="valid_time")
    # ---------- tidy up ----------
    df = full_ds.to_dataframe().reset_index()

    # Herbie keeps GRIB short‑names; rename to something simpler
    rename_map = {
        "ugrd80": "u",
        "vgrd80": "v",
        "tmp":    "t",
        "pres":   "sp",
        "rh":     "r2",
    }
    df = df.rename(columns=rename_map)

    # Wind speed & direction
    df["wind_speed"] = np.hypot(df["u"], df["v"])
    df["wind_dir"]   = (np.degrees(np.arctan2(-df["u"], -df["v"])) % 360)

    out_dir = Path("HRRR_data")          
    out_dir.mkdir(exist_ok=True)

out = (
    df[["valid_time", "point_stid", "t", "sp", "u", "v",
        "wind_speed", "wind_dir", "r2"]]
      .rename(columns={
          "point_stid": "station",
          "t":    "temperature_K",
          "sp":   "pressure_Pa",
          "r2":   "rh",
      })
)
station_col = "station" 
for stid, group in out.groupby(station_col):
    safe_name = "".join(c if c.isalnum() else "_" for c in stid)
    fname = out_dir / f"{safe_name}.csv"
    group.to_csv(fname, index=False)
    print(f"✓ wrote {fname}")

