from pymongo import MongoClient

# NOTE: if you have local mongodb do net stop mongodb to make sure this points to docker mongodb

client = MongoClient("mongodb://localhost:27017/")

print(client.list_database_names())

db = client["fit3182"]

# Vehicles setup
vehicle_collection = db["vehicle"]

vehicle_collection.create_index(
    [("car_plate", 1)],
    unique=True
)

vehicle_collection.insert_one({
    "car_plate": "ABC123",
    "owner": "John Doe",
    "model": "Toyota"
})

# Cameras setup
camera_collection = db["camera"]

camera_collection.create_index(
    [("camera_id", 1)],
    unique=True
)

camera_collection.insert_one({
    "camera_id": "C1",
    "location": "Highway A",
    "speed_limit": 80
})

# Violations setup
violation_collection = db["violation"]

violation_collection.create_index(
    [("car_plate", 1), ("date", 1)]
)

violation_collection.insert_one({
    "car_plate": "ABC123",
    "date": "2026-05-01",
    "violations": [
        {
            "type": "instant",
            "camera_id": "C1",
            "speed": 100,
            "limit": 80
        }
    ]
})

# check whether the data is inserted correctly
print("Vehicles:", list(vehicle_collection.find()))
print("Cameras:", list(camera_collection.find()))
print("Violations:", list(violation_collection.find()))

# clean up database after testing
vehicle_collection.delete_many({})
camera_collection.delete_many({})
violation_collection.delete_many({})