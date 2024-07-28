import pymongo
from pymongo.errors import CollectionInvalid, OperationFailure
from datetime import datetime, timedelta
import random
import pika
import uuid
import json
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timedelta
from typing import Optional, List


class MeasurementMetadata(BaseModel):
    appName: str
    measurementName: str
    user: str


class RecordMeasurementRequest(BaseModel):
    action: str
    value: float
    timestamp: Optional[datetime]
    metadata: MeasurementMetadata

class QueryWithInterpolationRequest(BaseModel):
    action: str
    start_time: datetime
    end_time: datetime
    metadata: MeasurementMetadata


class AMQPRPCService:
    def __init__(self, amqp_url, input_queue, response_queue, on_request):
        self.amqp_url = amqp_url
        self.input_queue = input_queue
        self.response_queue = response_queue
        self.on_request = on_request

        self.connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.input_queue)
        self.channel.queue_declare(queue=self.response_queue)
        self.channel.basic_qos(prefetch_count=1) 
        self.channel.basic_consume(queue=self.input_queue, on_message_callback=self.on_request_received)

    def on_request_received(self, ch, method, props, body):
        correlation_id = props.correlation_id
        try:
            request_data = json.loads(body)
            response_data = self.on_request(request_data)
            response_body = json.dumps(response_data, default=str)
        except Exception as e:
            response_body = json.dumps({"error": str(e)})

        ch.basic_publish(
            exchange='',
            routing_key=self.response_queue,  # Publish to the response queue
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=response_body
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        print(f" [x] Awaiting RPC requests on {self.input_queue}")
        self.channel.start_consuming()



# MongoDB Setup (Replace with your credentials)
client = pymongo.MongoClient("mongodb://192.168.68.253")
db = client["measurements"]


def record_measurement(request: RecordMeasurementRequest):
    """Records an individual measurement in the measurements collection."""
    timestamp = request.timestamp or datetime.utcnow()
    measurements_collection_name = f"{request.metadata.appName}_{request.metadata.measurementName}_measurements"

    try:
        # Insert individual measurement
        db[measurements_collection_name].insert_one({
            "timestamp": timestamp,
            "value": request.value,
            "metadata": {
                "appName": request.metadata.appName,
                "measurementName": request.metadata.measurementName,
                "user": request.metadata.user,
            }
        })
    except CollectionInvalid:
        # Create time-series collection if it doesn't exist
        db.create_collection(
            measurements_collection_name,
            timeseries={"timeField": "timestamp", "metaField": "metadata"},
        )
        # Retry insertion after creation
        record_measurement(request) 

def query_with_interpolation(request: QueryWithInterpolationRequest):
    """
    Queries data with filtering, on-demand linear interpolation, and daily average handling.

    Args:
        app_name: Name of the application.
        measurement_name: Name of the measurement.
        start_time: Start time for querying data.
        end_time: End time for querying data.
        user: (Optional) User associated with the measurement.
        daily_average: (Optional) If True, queries daily averages instead of individual measurements.
        interval: (Optional) Interpolation interval (timedelta).

    Returns:
        List of dictionaries containing the queried data.
    """

    collection_name = (
        f"{request.metadata.appName}_{request.metadata.measurementName}_measurements"
    )

    # Ensure timestamps are datetime objects without timezone info
    start_time = request.start_time.replace(tzinfo=None)
    end_time = request.end_time.replace(tzinfo=None)

    pipeline = [
                {
                    '$densify': {
                    'field': "timestamp",
                    'range': {
                        "step": 1,
                        "unit": "day",
                        "bounds": [start_time, end_time]
                    },
                    'partitionByFields': [
                        "metadata.appName",
                        "metadata.measurementName",
                        "metadata.user"
                    ]
                }},
                {
                    '$fill': {
                        'sortBy': { 'timestamp': 1 },
                        'partitionByFields': [
                            "metadata.appName",
                            "metadata.measurementName",
                            "metadata.user"
                        ],
                        'output': {
                            "value": { 'method': "linear" }
                        }
                    }
                },
                {
                    '$fill': {
                        'sortBy': { 'timestamp': 1 },
                        'partitionByFields': [
                            "metadata.appName",
                            "metadata.measurementName",
                            "metadata.user"
                        ],
                        'output': {
                            "value": { 'method': "locf" }
                        }
                    }
                }
            ]



    results = list(db[collection_name].aggregate(pipeline))
    return results



def handle_measurement(request_data: dict):
    # Validate the request
    try:
        if request_data['action'] == "record":
            request = RecordMeasurementRequest(**request_data)
            record_measurement(request)
            return {"message": "Measurement recorded successfully"}
        elif request_data['action'] == "query":
            request = QueryWithInterpolationRequest(**request_data)
            result = query_with_interpolation(request)
            return result
        else:
            return {"error": "Action not found"}
    except ValidationError as e:
        return {"error": e.errors()}

service = AMQPRPCService("amqp://192.168.68.253", "measurement.request", "measurement.response", handle_measurement)
service.start()
