# src/###_data_design_streaming.ipynb - Cell 1: Data Inspection
import pandas as pd
import numpy as np
from pathlib import Path

# Load all datasets
vehicle_df = pd.read_csv(Path("..") / "data" / "vehicle.csv")
camera_df = pd.read_csv(Path("..") / "data" / "camera.csv")
event_a = pd.read_csv(Path("..") / "data" / "camera_event_A.csv")
event_b = pd.read_csv(Path("..") / "data" / "camera_event_B.csv")
event_c = pd.read_csv(Path("..") / "data" / "camera_event_C.csv")
historic = pd.read_csv(Path("..") / "data" / "camera_event_historic.csv")  # For reference only

# Key checks per spec & briefing:
checks = {
    'vehicle_duplicates': vehicle_df.duplicated('car_plate').sum(),
    'camera_count': len(camera_df),
    'event_timestamps_sorted': event_a['timestamp'].is_monotonic_increasing,
    'batch_id_range': event_a['batch_id'].max(),
    'speed_numeric': pd.to_numeric(event_a['speed_reading'], errors='coerce').notna().all(),
    'camera_consistency': set(event_a['camera_id']) == {1},
}
print("Data Quality Checks:", checks)