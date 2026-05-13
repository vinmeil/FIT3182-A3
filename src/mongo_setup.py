from pymongo import MongoClient
from pathlib import Path
import pandas as pd

client = MongoClient("mongodb://localhost:27017/") # net stop mongodb if local is running
db = client["fit3182_a2"]

data_path = (Path(__file__).resolve().parent / ".." / "data").resolve()

# Creating collections
def db_setup():
    # drop existing collections
    db.vehicles.drop()
    db.cameras.drop()
    db.violations.drop()
    
    # Vehicle collection setup
    vehicle_collection = db["vehicles"]
    
    vehicle_collection.create_index([("car_plate", 1)], unique=True)
    
    # Camera collection setup
    camera_collection = db["cameras"]
    camera_collection.create_index([("camera_id", 1)], unique=True)
    camera_collection.create_index([("location", "2dsphere")])
    
    # Violations collection setup
    violation_collection = db["violations"]
    violation_collection.create_index([("car_plate", 1), ("date", -1)])

# DB population (for vehicles and camera, violations will be populated by streaming events)
def populate_db():
    vehicle_csv = pd.read_csv(data_path / "vehicle.csv")
    camera_csv = pd.read_csv(data_path / "camera.csv")
    
    # handle vehicle insertion
    for _, row in vehicle_csv.iterrows():
        data_to_insert = {
            "car_plate": row['car_plate'],
            "owner_name": row['owner_name'],
            "owner_addr": row['owner_addr'],
            "vehicle_type": row['vechicle_type'],  # Note: typo in source CSV
            "registration_date": pd.to_datetime(row['registration_date']),
            "created_at": pd.Timestamp.utcnow()
        }
        
        # Duplication handling: check if a record with the same car_plate exists
        vehicle_exists = db.vehicles.find_one({"car_plate": row['car_plate']})
        
        if vehicle_exists:
            vehicle_exists_date = vehicle_exists.get("registration_date", pd.Timestamp.min)
            # replace old record if new one has a more recent registration_date
            if pd.to_datetime(row['registration_date']) > vehicle_exists_date: 
                db.vehicles.update_one(
                    {"car_plate": row['car_plate']},
                    {"$set": {**data_to_insert, "updated_at": pd.Timestamp.utcnow()}}
                )
        else:
            db.vehicles.insert_one(data_to_insert)
    
    # handle camera insertion
    for _, row in camera_csv.iterrows():
        data_to_insert = {
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
            {"$setOnInsert": data_to_insert},
            upsert=True
        )

if __name__ == "__main__":
    db_setup()
    print("Database collections and indexes created.")
    populate_db()
    print("Initial data population complete.")
    print("Database setup and initial population complete.")