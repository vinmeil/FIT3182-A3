from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from pathlib import Path

client = MongoClient("mongodb://localhost:27017/")
db = client["fit3182_a2"]

# Create collections with indexes
def setup_collections():
    # drop existing collections for idempotency (optional, comment out in production)
    db.vehicles.drop()
    db.cameras.drop()
    db.violations.drop()
    
    # Vehicles
    vehicles = db["vehicles"]
    vehicles.create_index([("car_plate", ASCENDING)], unique=True)
    
    # Cameras
    cameras = db["cameras"]
    cameras.create_index([("camera_id", ASCENDING)], unique=True)
    cameras.create_index([("location", "2dsphere")])
    
    # Violations
    violations = db["violations"]
    violations.create_index([("car_plate", ASCENDING), ("date", DESCENDING)])
    violations.create_index([("last_updated", ASCENDING)])
    
    print("✓ Collections and indexes created")

setup_collections()

import pandas as pd

vehicle_df = pd.read_csv(Path("..") / "data" / "vehicle.csv")
camera_df = pd.read_csv(Path("..") / "data" / "camera.csv")

# Handle duplicate vehicle plates: keep latest registration_date
vehicle_df_sorted = vehicle_df.sort_values('registration_date', ascending=False)
vehicle_deduped = vehicle_df_sorted.drop_duplicates('car_plate', keep='first')

# Insert vehicles (upsert for idempotency)
for _, row in vehicle_deduped.iterrows():
    doc = {
        "car_plate": row['car_plate'],
        "owner_name": row['owner_name'],
        "owner_addr": row['owner_addr'],
        "vehicle_type": row['vechicle_type'],  # Note: typo in source CSV
        "registration_date": pd.to_datetime(row['registration_date']),
        "created_at": pd.Timestamp.utcnow()
    }
    db.vehicles.update_one(
        {"car_plate": row['car_plate']},
        {"$setOnInsert": doc, "$set": {"updated_at": pd.Timestamp.utcnow()}},
        upsert=True
    )

# Load cameras
for _, row in camera_df.iterrows():
    doc = {
        "camera_id": int(row['camera_id']),
        "location": {
            "type": "Point",
            "coordinates": [float(row['longitude']), float(row['latitude'])]
        },
        "position": float(row['position']),
        "speed_limit": int(row['speed_limit']),
        "created_at": pd.Timestamp.utcnow()
    }
    db.cameras.update_one(
        {"camera_id": int(row['camera_id'])},
        {"$setOnInsert": doc},
        upsert=True
    )

print(f"✓ Loaded {db.vehicles.count_documents({})} vehicles, {db.cameras.count_documents({})} cameras")