# A -> B segment join (camera 1 to camera 2)
segment_ab = (
    joined_stream_a.alias("entry")
    .join(
        joined_stream_b.alias("exit"),
        expr(f"""
            entry.car_plate = exit.car_plate
            AND exit.event_time > entry.event_time
            AND exit.event_time <= entry.event_time + interval {max_travel_ab} seconds
        """),
        "inner"
    )
    .select(
        col("entry.car_plate").alias("car_plate"),
        col("entry.camera_id").alias("start_camera_id"),
        col("exit.camera_id").alias("end_camera_id"),
        col("entry.batch_id").alias("entry_batch_id"),
        col("exit.batch_id").alias("exit_batch_id"),
        col("entry.event_time").alias("entry_time"),
        col("exit.event_time").alias("exit_time"),
        col("entry.position").alias("entry_position"),
        col("exit.position").alias("exit_position"),
        col("exit.speed_limit").alias("speed_limit"),
        col("exit.source").alias("source"),
        col("entry.latitude").alias("entry_latitude"),
        col("entry.longitude").alias("entry_longitude"),
        col("exit.latitude").alias("exit_latitude"),
        col("exit.longitude").alias("exit_longitude")
        
    )
)

# B -> C segment join (camera 2 to camera 3)
segment_bc = (
    joined_stream_b.alias("entry")
    .join(
        joined_stream_c.alias("exit"),
        expr(f"""
            entry.car_plate = exit.car_plate
            AND exit.event_time > entry.event_time
            AND exit.event_time <= entry.event_time + interval {max_travel_bc} seconds
        """),
        "inner"
    )
    .select(
        col("entry.car_plate").alias("car_plate"),
        col("entry.camera_id").alias("start_camera_id"),
        col("exit.camera_id").alias("end_camera_id"),
        col("entry.batch_id").alias("entry_batch_id"),
        col("exit.batch_id").alias("exit_batch_id"),
        col("entry.event_time").alias("entry_time"),
        col("exit.event_time").alias("exit_time"),
        col("entry.position").alias("entry_position"),
        col("exit.position").alias("exit_position"),
        col("exit.speed_limit").alias("speed_limit"),
        col("exit.source").alias("source"),
        col("entry.latitude").alias("entry_latitude"),
        col("entry.longitude").alias("entry_longitude"),
        col("exit.latitude").alias("exit_latitude"),
        col("exit.longitude").alias("exit_longitude")
    )
)

from pyspark.sql.functions import col, lit, isnull

# 1. Define a dedicated drop logger
def log_drops_per_batch(batch_df, batch_id, segment_name):
    """Logs records that expired due to watermark or failed to match."""
    count = batch_df.count()
    if count > 0:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*60}")
        print(f"[{now}] [{segment_name}] Batch {batch_id}: {count} DROPPED/EXPIRED pair(s)")
        print(f"   Reason: Watermark advanced without a matching exit event.")
        batch_df.show(truncate=False)
        print(f"{'='*60}\n")

# 2. Create & Start A -> B Drop Logger
ab_drops_query = (
    joined_stream_a.alias("entry")
    .join(
        joined_stream_b.alias("exit"),
        expr(f"""
            entry.car_plate = exit.car_plate 
            AND exit.event_time > entry.event_time 
            AND exit.event_time <= entry.event_time + interval {max_travel_ab} seconds
        """),
        "left_outer" 
    )
    # Keep only rows where the exit event NEVER arrived
    .filter(isnull(col("exit.car_plate")))
    .select(
        col("entry.car_plate"),
        col("entry.camera_id").alias("start_camera_id"),
        col("entry.event_time").alias("entry_time"),
        lit("EXPIRED_WATERMARK").alias("drop_reason"),
        lit(f"No match within {max_travel_ab}s").alias("details")
    )
    .writeStream
    .outputMode("append")
    .foreachBatch(lambda df, batch_id: log_drops_per_batch(df, batch_id, "Segment A -> B Drops"))
    .start()
)

# 3. Create & Start B -> C Drop Logger
bc_drops_query = (
    joined_stream_b.alias("entry")
    .join(
        joined_stream_c.alias("exit"),
        expr(f"""
            entry.car_plate = exit.car_plate 
            AND exit.event_time > entry.event_time 
            AND exit.event_time <= entry.event_time + interval {max_travel_bc} seconds
        """),
        "left_outer"
    )
    .filter(isnull(col("exit.car_plate")))
    .select(
        col("entry.car_plate"),
        col("entry.camera_id").alias("start_camera_id"),
        col("entry.event_time").alias("entry_time"),
        lit("EXPIRED_WATERMARK").alias("drop_reason"),
        lit(f"No match within {max_travel_bc}s").alias("details")
    )
    .writeStream
    .outputMode("append")
    .foreachBatch(lambda df, batch_id: log_drops_per_batch(df, batch_id, "Segment B -> C Drops"))
    .start()
)

print("Debug: Drop logging streams started. Unmatched pairs will be printed when their watermark expires.")

print("Debug: Segment joins have been defined for A -> B and B -> C.")