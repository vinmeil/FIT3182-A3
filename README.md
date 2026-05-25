# FIT3182 Assignment 2: AWAS Real-Time Traffic Monitoring

A real-time streaming pipeline that simulates multi-camera traffic events, detects instantaneous and average speed violations using Apache Spark Structured Streaming, and persists consolidated daily violation records to MongoDB. Includes live violation visualization and analytical plotting.

## How to Run (Docker)

1. **Start the streaming infrastructure** (Kafka, MongoDB, Jupyter/Spark):
    docker-compose up -d
    *Note: All services communicate via Docker networking*
2. **Install dependencies (IMPORTANT)**
    Folium is missing, so:
    - Go to pyspark container
    - **pip install folium**
3. **Execute Notebooks in Order:**
    - `data_design_streaming.ipynb` to initialize MongoDB connections, Spark, and Kafka
    - Execute `producer_a`, `producer_b`, and `producer_c` in parallel after Task 2.1.2 is finished running, a debug print `Kafka streams have been created for all three cameras` should be visibile in the Jupyter Notebook.
    - Violations should now start getting logged to MongoDB and also printed out in Jupyter Notebook.
    - Execute `visualization.ipynb` to see visualizations.

## Generative AI Declaration
We used Github Copilot and Claude to understand the assignment, come up with ideas, and validating our work. Both of these AI tools were used to ask for explanations, code, and comments. However, every output was reviewed by us and verified by us. Code was also further edited to better fit the assignment context. Overall, we reviewed, tested, and verified the final submitted work.
