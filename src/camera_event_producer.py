import json
import time
from datetime import datetime

import pandas as pd
from kafka3 import KafkaProducer

class CameraEventProducer:
    def __init__(self, bootstrap_servers, topic, camera_id, csv_path, batch_interval=5):
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
        """Load and group events by batch_id"""
        df = pd.read_csv(self.csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df.groupby("batch_id")

    def _create_payload(self, row):
        """Create enriched event payload"""
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
        """Publish one batch every batch_interval seconds"""
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
        self.producer.close()
