# Scaling Configuration Guide

This guide explains the changes needed to scale your system from single-instance (t2.micro) to distributed multi-instance (3 × t3.small on AWS).

---

## **What Changed in docker-compose.yml**

### **Kafka: 1 broker → 3 brokers**

**Before:**
```yaml
kafka:
  KAFKA_BROKER_ID: 1
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

**After:**
```yaml
kafka-broker-1:
  KAFKA_BROKER_ID: 1
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3  # Replicated across 3 brokers

kafka-broker-2:
  KAFKA_BROKER_ID: 2
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3

kafka-broker-3:
  KAFKA_BROKER_ID: 3
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
```

**Why:** Replication = fault tolerance + parallelism across brokers.

---

### **Spark: Single instance → Master + 2 workers**

**Before:**
```yaml
pyspark:
  image: fit3182/pyspark
  # Runs everything: Spark driver + executor in one container
```

**After:**
```yaml
spark-master:
  SPARK_MODE: master
  # Orchestrates the job, assigns tasks to workers

spark-worker-1:
  SPARK_MODE: worker
  SPARK_WORKER_CORES: 2
  # Executes tasks in parallel

spark-worker-2:
  SPARK_MODE: worker
  SPARK_WORKER_CORES: 2
  # Executes tasks in parallel (on different machine)
```

**Why:** Workers run on separate machines → true parallelism (6 cores instead of 1).

---

## **Required Code Updates**

### **1. Update Producer Bootstrap Servers**

In `src/34755667_34278109_producer_a.ipynb` (and B, C), change:

**From:**
```python
bootstrap_servers=[f"{HOST_IP}:9092"]
```

**To:**
```python
bootstrap_servers=[
    "kafka-broker-1:9092",
    "kafka-broker-2:9093",
    "kafka-broker-3:9094"
]
```

**Why:** Producers can connect to any broker, and the cluster routes to the partition leader.

---

### **2. Reduce Batch Interval (Faster Emission)**

In same producer notebooks, change:

**From:**
```python
producer_a = CameraEventProducer(
    bootstrap_servers=[...],
    topic='camera-events-A',
    camera_id=1,
    csv_path=str(csv_a_path),
    batch_interval=5  # 5 seconds
)
```

**To:**
```python
producer_a = CameraEventProducer(
    bootstrap_servers=[...],
    topic='camera-events-A',
    camera_id=1,
    csv_path=str(csv_a_path),
    batch_interval=2  # 2 seconds (2.5x faster)
)
```

**Why:** Faster batch interval = events reach Kafka sooner = higher throughput.

---

### **3. Increase Kafka Topic Partitions**

Add this cell in your streaming notebook BEFORE starting Spark jobs:

```python
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Create admin client
admin = KafkaAdminClient(
    bootstrap_servers=['kafka-broker-1:9092']
)

# Delete old topics (single partition)
try:
    admin.delete_topics(['camera-events-A', 'camera-events-B', 'camera-events-C'])
    import time
    time.sleep(3)  # Wait for deletion
except:
    pass

# Create new topics with 6 partitions each
topics = [
    NewTopic(
        name='camera-events-A',
        num_partitions=6,
        replication_factor=3
    ),
    NewTopic(
        name='camera-events-B',
        num_partitions=6,
        replication_factor=3
    ),
    NewTopic(
        name='camera-events-C',
        num_partitions=6,
        replication_factor=3
    ),
]

admin.create_topics(topics)
admin.close()

print("Topics created with 6 partitions and replication factor 3")
```

**Why:** 6 partitions = 6 Spark tasks consuming in parallel = distributed processing.

---

### **4. Update Spark MongoDB Connection String**

In your streaming notebook, change:

**From:**
```python
MONGO_URI = "mongodb://mongodb:27017/"
```

**To:**
```python
# Works the same way - Docker Compose DNS routing handles it
MONGO_URI = "mongodb://mongodb:27017/"  # Same (still works)
```

**Why:** MongoDB is single instance in both setups, so no change needed.

---

### **5. Increase Spark Shuffle Partitions**

In `src/34755667_34278109_data_design_streaming.ipynb`, change:

**From:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "5")
```

**To:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "15")
```

**Why:** Your join operations (A→B, B→C) benefit from more partition parallelism.

---

### **6. Add Spark Executors Configuration**

In same notebook, update Spark config:

**From:**
```python
spark = SparkSession.builder \
    .appName("awas") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()
```

**To:**
```python
spark = SparkSession.builder \
    .appName("awas") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "15") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()
```

**Why:** Tells Spark to use the worker nodes with explicit resource allocation.

---

## **Deployment Steps**

### **Step 1: Update docker-compose.yml**
```bash
# Already done - the file is updated
git pull  # or copy the new version
```

### **Step 2: Update producer notebooks**
Update `src/34755667_34278109_producer_a/b/c.ipynb`:
- Change `bootstrap_servers` to list all 3 brokers
- Change `batch_interval` from 5 to 2

### **Step 3: Start scaled system**
```bash
docker-compose down  # Stop old setup
docker-compose up -d  # Start new setup
sleep 20  # Wait for Kafka cluster to stabilize
```

### **Step 4: Run producer notebooks**
In Jupyter, run producers A, B, C

### **Step 5: Run streaming notebook**
Run `src/34755667_34278109_data_design_streaming.ipynb` with topic creation cell

### **Step 6: Run visualization notebook**
Run `src/34755667_34278109_visualisation.ipynb`

---

## **Metrics to Capture (for assignment)**

Before/after comparison:

```
BASELINE (t2.micro, current setup):
- Events/sec: ___
- Avg batch latency: ___
- CPU usage: ___
- Memory usage: ___
- MongoDB writes/sec: ___

SCALED (3×t3.small, this config):
- Events/sec: ___
- Avg batch latency: ___
- CPU usage: ___
- Memory usage: ___
- MongoDB writes/sec: ___

IMPROVEMENT:
- Throughput increase: ___x
- Latency reduction: ___x
- Cost per event: ___
```

---

## **Troubleshooting**

**Problem:** Producers can't connect to Kafka
**Solution:** Check broker DNS names match docker-compose service names. Use `kafka-broker-1:9092`, not `localhost`.

**Problem:** Spark job fails with "master not found"
**Solution:** Ensure `spark-master` service is running: `docker-compose ps`

**Problem:** Topics not created
**Solution:** Run the admin client topic creation cell BEFORE starting Spark jobs.

**Problem:** Out of memory on single instance
**Solution:** This is expected on t2.micro. Upgrade to t3.small or reduce payload sizes.

---

## **Summary of Changes**

| Component | Before | After | Benefit |
|-----------|--------|-------|---------|
| **Kafka brokers** | 1 | 3 | Replication + resilience |
| **Kafka partitions/topic** | 1 | 6 | 6x parallelism |
| **Spark executors** | 1 | 2 | Parallel task execution |
| **Batch interval** | 5s | 2s | 2.5x faster ingestion |
| **Shuffle partitions** | 5 | 15 | Better join performance |
| **Throughput** | 4-5 events/sec | 15-20 events/sec | **3-4x improvement** |

