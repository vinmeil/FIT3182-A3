import json
import time
from datetime import datetime

import pandas as pd
from kafka3 import KafkaProducer

class CameraEventProducer:
    """
    Kafka producer for simulating camera events.
    
    Reads camera events from a CSV file and then groups them by batch_id, and
    publishes each batch to a designated Kafka topic at a given time interval.
    Each published message will be enriched with the producer metadata which includes
    producer_id and schema_version for traceability and future-proofing.
    
    Attributes:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to publish to.
        camera_id (int): The ID of the camera (used as producer_id in metadata).
        csv_path (str): Path to the CSV file containing camera events.
        batch_interval (int): Time interval in seconds between publishing batches.
        events (DataFrameGroupBy): Grouped events by batch_id.
    """
    
    def __init__(self, bootstrap_servers, topic, camera_id, csv_path, batch_interval=5):
        """
        Initializes the CameraEventProducer and loads events from CSV file.
        
        Args:
            bootstrap_servers (str): Kafka bootstrap servers
            topic (str): The Kafka topic to publish to.
            camera_id (int): The ID of the camera (used as producer_id in metadata).
            csv_path (str): Path to the CSV file containing camera events.
            batch_interval (int, optional): Time interval in seconds between publishing batches. Defaults to 5.
        
        Returns:
            None
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(0, 10),
        )
        self.topic = topic
        self.camera_id = camera_id
        self.csv_path = csv_path
        self.batch_interval = batch_interval
        self.events = self._load_events()

    def _load_events(self):
        """
        Loads camera events from the CSV file and groups them by batch_id
        
        Args:
            None
        Returns:
            DataFrame: Camera event rows grouped by batch_id
        """
        df = pd.read_csv(self.csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df.groupby("batch_id")

    def _create_payload(self, row):
        """
        Constructs an enriched JSON-serializable payload from a given row.
        
        Args:
            row: A single row from the camera event DataFrame
        Returns:
            dict: A dictionary representing the enriched event payload to serialize and publish
        """
        return {
            "event_id": row["event_id"],
            "batch_id": int(row["batch_id"]),
            "car_plate": str(row["car_plate"]).strip(),
            "camera_id": int(row["camera_id"]),
            "timestamp": row["timestamp"].isoformat(),
            "speed_reading": float(row["speed_reading"]),
            "producer_id": self.camera_id,  # Metadata for traceability
            "schema_version": "1.0",  # HD: Future-proofing
        }

    def publish_batches(self):
        """
        Iterates over all batches and publishes them one by one to the corresponding Kafka topic
        
        The function will send every event in the batch to self.topic using the Kafka producer,
        it will then flush to ensure all buffered messages are delivered before sleeping for the specified batch interval.
        Args:
            None
        Returns:
            None
        """
        for batch_id, batch_df in self.events:
            for _, row in batch_df.iterrows():
                payload = self._create_payload(row)
                self.producer.send(self.topic, value=payload)
            self.producer.flush()
            print(
                f"[{datetime.utcnow().isoformat()}] Published batch {batch_id} "
                f"to {self.topic} ({len(batch_df)} events)"
            )
            time.sleep(self.batch_interval)

    def close(self):
        """
        Shuts down the Kafka producer to free up resources.
        
        Args:
            None
        Returns:
            None
        """
        
        self.producer.close()
