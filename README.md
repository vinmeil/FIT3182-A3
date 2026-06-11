# FIT3182 Assignment 3 - Cloud-Deployable Spark & Kafka Streaming Pipeline

## Project Overview

This project extends the Assignment 2 AWAS (Automated Awareness and Safety) traffic monitoring system into a **production-grade, cloud-deployable streaming analytics platform** using Apache Spark Structured Streaming and Apache Kafka deployed across multiple AWS EC2 instances.

### Selected Theme: Cloud-Deployable Spark and Kafka (Section 3a)

The implementation demonstrates a scalable, distributed streaming architecture with:
- **Multi-node Spark cluster** (1 Master + 2 Workers) deployed on AWS EC2
- **Distributed Kafka cluster** (3 brokers with replication factor 3)
- **Real-time violation detection** (instantaneous and average speed)
- **MongoDB sink** for persistent violation storage
- **Research-backed watermark design** for handling late-arriving events

---

## Project Structure

FIT3182-A3/
├── README.md
├── SCALING_GUIDE.md
├── docker-compose.yml                 # Master node configuration
├── Dockerfile.spark                   # Spark container definition
├── .env                               # Environment variables
├── .gitignore
├── deployment/
│   ├── docker-compose-master.yml      # Master node deployment
│   ├── docker-compose-worker1.yml     # Worker 1 deployment
│   ├── docker-compose-worker2.yml     # Worker 2 deployment
│   └── Dockerfile.spark               # Spark container definition
├── data/
│   ├── camera.csv                     # Camera metadata (3 cameras)
│   ├── vehicle.csv                    # Vehicle registration data
│   ├── camera_event_A.csv             # Event stream for Camera A
│   ├── camera_event_B.csv             # Event stream for Camera B
│   ├── camera_event_C.csv             # Event stream for Camera C
│   └── camera_event_historic.csv
├── src/
│   ├── 34755667_34278109_data_design_streaming.ipynb  # Main streaming pipeline
│   ├── 34755667_34278109_producer_a.ipynb             # Producer A (Camera A)
│   ├── 34755667_34278109_producer_b.ipynb             # Producer B (Camera B)
│   ├── 34755667_34278109_producer_c.ipynb             # Producer C (Camera C)
│   ├── 34755667_34278109_visualisation.ipynb          # Folium map visualization
│   ├── camera_event_producer.py       # Kafka producer class
│   ├── mongo_setup_explanation.md     # MongoDB schema documentation
│   ├── delay_calculation.ipynb        # Watermark lateness analysis
│   └── pipeline_diagram.png           # Architecture diagram
└── outputs/
    ├── instant_violations_json/       # Real-time instantaneous violations
    ├── average_violations_json/       # Real-time average speed violations
    └── camera_stats.json              # Aggregated camera statistics



## Prerequisites

### AWS Account & Setup

1. **AWS Free Tier Account** (or paid account)
2. **EC2 Instances** (3 instances required):
   - **Master Node**: `m7i-flex.large` (or equivalent, minimum 2 vCPU, 8GB RAM)
   - **Worker 1**: `t3.small` (or equivalent, minimum 2 vCPU, 2GB RAM)
   - **Worker 2**: `t3.small` (or equivalent, minimum 2 vCPU, 2GB RAM)
   
3. **Security Group Configuration**:
   The following inbound rules must be configured for **ALL THREE instances**:

   | Port Range | Protocol | Source | Description |
   |------------|----------|--------|-------------|
   | 22 | TCP | Your IP | SSH access |
   | 2181 | TCP | 0.0.0.0/0 | ZooKeeper client port |
   | 7077 | TCP | 0.0.0.0/0 | Spark Master port |
   | 8080 | TCP | 0.0.0.0/0 | Spark Master Web UI |
   | 8888 | TCP | 0.0.0.0/0 | Jupyter Notebook |
   | 9092-9094 | TCP | 0.0.0.0/0 | Kafka Brokers |
   | 27017 | TCP | 0.0.0.0/0 | MongoDB |
   | 4040 | TCP | 0.0.0.0/0 | Spark Driver UI |
   | 30000-60000 | TCP | 172.31.0.0/16 | Spark ephemeral ports (internal) |

   **Important**: For production deployments, restrict `0.0.0.0/0` sources to your specific IP addresses where possible.

### Software Dependencies

All dependencies are containerized via Docker. Each EC2 instance requires:

```bash
# Install Docker and Docker Compose Plugin
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker ubuntu
# Logout and login again for group changes to take effect
```

## Deployment Instructions
### Step 1: Clone Repository to All Instances

On all EC2 nodes:
```bash
cd ~
git clone <your-repository-url>
cd FIT3182-A3
```

### Step 2: Update Configuration Files

Critical: Replace all placeholder IP addresses in the Docker Compose files with your actual AWS EC2 Private IP addresses (found in AWS Console -> EC2 -> Instance Details).

Master Node (deployment/docker-compose-master.yml):
```yaml
# Update these lines with Master's Private IP (e.g., 172.31.21.188)
KAFKA_ZOOKEEPER_CONNECT: <MASTER_PRIVATE_IP>:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://<MASTER_PRIVATE_IP>:9092
SPARK_MASTER: spark://<MASTER_PRIVATE_IP>:7077
```

Worker 1 (deployment/docker-compose-worker1.yml):
```yaml
# Update with Worker 1's Private IP and Master's Private IP
KAFKA_ZOOKEEPER_CONNECT: <MASTER_PRIVATE_IP>:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://<WORKER1_PRIVATE_IP>:9093
SPARK_LOCAL_IP: "<WORKER1_PRIVATE_IP>"
command: ["...spark-class", "org.apache.spark.deploy.worker.Worker", 
          "--host", "<WORKER1_PRIVATE_IP>", "spark://<MASTER_PRIVATE_IP>:7077"]
```

Worker 2 (deployment/docker-compose-worker2.yml):
```yaml
# Update with Worker 2's Private IP and Master's Private IP
KAFKA_ZOOKEEPER_CONNECT: <MASTER_PRIVATE_IP>:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://<WORKER2_PRIVATE_IP>:9094
SPARK_LOCAL_IP: "<WORKER2_PRIVATE_IP>"
command: ["...spark-class", "org.apache.spark.deploy.worker.Worker", 
          "--host", "<WORKER2_PRIVATE_IP>", "spark://<MASTER_PRIVATE_IP>:7077"]
```

### Step 3: Start Services (Sequential Execution Required)
CRITICAL: Follow this exact sequence to avoid connection failures

3.1 Start Master Node Services
```bash
# SSH into Master instance
cd ~/FIT3182-A3
docker compose up -d
```

Wait 30-60 seconds for Spark Master, ZooKeeper, Kafka Broker 1, and MongoDB to fully initialize. Verify with:
```bash
docker compose ps
# All containers should show "Up" status
```

3.2 Start Worker 1 Services
```bash
# SSH into Worker 1 instance
cd ~/FIT3182-A3
docker compose -f deployment/docker-compose-worker1.yml up -d
```

3.3 Start Worker 2 Services
```bash
# SSH into Worker 2 instance
cd ~/FIT3182-A3
docker compose -f deployment/docker-compose-worker2.yml up -d
```

3.4 Verify Cluster Health

Access Spark Master UI: `http://<MASTER_PUBLIC_IP>`:8080.
You should see:
- 2 Workers listed with status ALIVE
- Each worker showing available cores and memory (e.g., 2 cores, 512MB RAM)
- No failed applications

## Execution Workflow
### Step 1: Access Jupyter Notebook
Open your browser and navigate to `http://<MASTER_PUBLIC_IP>:8888`.

The Jupyter token will be printed in the container logs:
```bash
docker logs fit3182-a3-pyspark-notebook-1 2>&1 | grep token
```

### Step 2: Initialize Streaming Pipeline

1. Navigate to src/34755667_34278109_data_design_streaming.ipynb
2. Execute cells sequentially from top to bottom
3. Critical: Wait for the following debug statement before proceeding:
```
Debug: Kafka streams have been created for all three cameras.
```
This indicates that all Spark streaming queries are active and listening for Kafka messages.

### Step 3: Start Producers (Concurrent Execution)
Open the 3 producer notebooks (`34755667_34278109_producer_a.ipynb`, `34755667_34278109_producer_b.ipynb`, `34755667_34278109_producer_c.ipynb`) and run each one

### Step 4: Verify MongoDB Sink
Check violation data in MongoDB:
```bash
# SSH into Master instance
docker exec -it fit3182-a3-mongodb-1 mongosh

# In MongoDB shell:
use fit3182_a2
db.violations.countDocuments()
db.violations.findOne()
```

Expected Document Structure:
```json
{
  "_id": ObjectId("..."),
  "car_plate": "ABC 123",
  "date": ISODate("2024-01-01T00:00:00Z"),
  "violations": [
    {
      "type": "instant",
      "camera_id": 1,
      "speed": 125.5
    },
    {
      "type": "average",
      "start_camera": 1,
      "end_camera": 2,
      "avg_speed": 115.2
    }
  ]
}
```


## Architecture & Design Decisions

### High-Level Architecture (thanks copilot)
```
┌─────────────────────────────────────────────────────────────────┐
│                         AWS EC2 Master                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ ZooKeeper   │  │ Kafka       │  │ Spark Master            │  │
│  │ :2181       │  │ Broker 1    │  │ :7077, :8080            │  │
│  └─────────────┘  │ :9092       │  └─────────────────────────┘  │
│                   └─────────────┘                               │
│  ┌─────────────┐  ┌─────────────────────────────────────────┐   │
│  │ MongoDB     │  │ PySpark Notebook (Jupyter)              │   │
│  │ :27017      │  │ - Streaming Query Coordinator           │   │
│  └─────────────┘  │ - Violation Detection Logic             │   │
│                   │ - MongoDB Sink                          │   │
│                   └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↕ Network (Private IPs)
┌─────────────────────────────────────────────────────────────────┐
│                    AWS EC2 Worker 1 & 2                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Kafka       │  │ Spark       │  │ Streaming Executors     │  │
│  │ Broker 2/3  │  │ Worker      │  │ - Process Kafka Events  │  │
│  │ :9093/:9094 │  │ :random     │  │ - Stateful Joins        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Technical Decisions

#### 1. **Host Networking for Spark Workers**

**Decision**: Spark Workers use `network_mode: "host"` instead of Docker bridge networking.

**Justification**:
- Spark requires dynamic port allocation for executor communication (ephemeral ports 30000-60000)
- Docker bridge NAT would obscure these ports, preventing inter-worker communication
- Host networking ensures workers can bind to EC2 private IPs directly
- **Trade-off**: Reduced network isolation, but necessary for Spark's distributed execution model

#### 2. **Private IP Addressing for Inter-Service Communication**

**Decision**: All Kafka brokers, Spark workers, and ZooKeeper communicate via AWS Private IPs (`172.31.x.x`).

**Justification**:
- **Performance**: Avoids NAT loopback and internet gateway routing overhead
- **Cost**: Eliminates data transfer charges for inter-instance traffic within the same VPC
- **Security**: Keeps internal traffic within AWS backbone network
- **Reliability**: Prevents connection timeouts from public IP NAT mapping issues

**Public IPs are used ONLY for**:
- Jupyter Notebook access (browser -> `MASTER_PUBLIC_IP:8888`)
- Spark/Kafka Web UIs (monitoring)
- SSH access for deployment

#### 3. **Kafka Replication Factor = 3**

**Decision**: All Kafka topics created with `replication_factor: 3` and `num_partitions: 6`.

**Justification**:
- **Fault Tolerance**: With 3 brokers, the cluster can survive 2 broker failures without data loss
- **Availability**: Ensures leader election can occur even if one broker fails
- **Parallelism**: 6 partitions allow parallel consumption across multiple Spark executors
- **Trade-off**: Higher disk usage (3x replication), but acceptable for production reliability
