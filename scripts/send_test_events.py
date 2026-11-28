import boto3
import json
import random
import time
from datetime import datetime, timezone

kinesis = boto3.client("kinesis", region_name="us-east-2")
STREAM_NAME = "realtime-sensors-dev-events"

SENSOR_IDS = [f"sensor-{i:03d}" for i in range(1, 6)]

def build_event(sensor_id):
    now = datetime.now(timezone.utc)
    return {
        "sensor_id": sensor_id,
        "temperatura": round(random.uniform(18.0, 32.0), 2),
        "humedad": round(random.uniform(30.0, 80.0), 2),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z",
    }

total_sent = 0
NUM_BATCHES = 30
EVENTS_PER_BATCH = 5

for batch_num in range(NUM_BATCHES):
    records = []
    for _ in range(EVENTS_PER_BATCH):
        sensor_id = random.choice(SENSOR_IDS)
        event = build_event(sensor_id)
        records.append({
            "Data": json.dumps(event).encode("utf-8"),
            "PartitionKey": sensor_id,
        })
    resp = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
    total_sent += len(records) - resp.get("FailedRecordCount", 0)
    print(f"Batch {batch_num + 1}/{NUM_BATCHES}: enviados {len(records)}, fallidos {resp.get('FailedRecordCount', 0)}")
    time.sleep(3)

print(f"\nTotal de eventos enviados exitosamente: {total_sent}")
