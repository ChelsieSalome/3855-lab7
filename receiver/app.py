import connexion
from connexion import NoContent
import uuid
import yaml
import logging
import logging.config
import json
import datetime
from kafka import KafkaProducer

# LOAD CONFIGURATION
with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# SUPPRESS KAFKA DEBUG LOGS
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.client').setLevel(logging.WARNING)
logging.getLogger('kafka.producer').setLevel(logging.WARNING)
logging.getLogger('kafka.protocol').setLevel(logging.WARNING)

logger.info("Configuration loaded - Kafka DEBUG logs suppressed")


KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC = app_config['events']['topic']

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logger.info(f"Connected to Kafka at {KAFKA_SERVER}")


app = connexion.FlaskApp(__name__, specification_dir='')

app.add_api("receiver_openapi.yaml",
            strict_validation=True,
            validate_responses=True)

flask_app = app.app


@flask_app.route('/')
def home():
    """Home page with links to API endpoints"""
    return '''
    <html>
    <head>
        <title>Monitoring API</title>
    </head>
    <body>
        <h1>Receiver Service</h1>
        <h2>Available Endpoints:</h2>
        <ol>
            <li><strong>POST:</strong> /monitoring/performance</li>
            <li><strong>POST:</strong> /monitoring/errors</li>
        </ol>
    </body>
    </html>
    '''



def report_performance_metrics(body):
    """Receive performance metrics and send to Kafka"""
    server_id = body['server_id']
    reporting_timestamp = body['reporting_timestamp']
    metrics = body['metrics']

    for metric in metrics:
        trace_id = str(uuid.uuid4())

        logger.info(f"RECEIVED: Performance metric from server {server_id} (trace: {trace_id})")

        individual_event = {
            "trace_id": trace_id,
            "server_id": server_id,
            "cpu": metric['cpu'],
            "memory": metric['memory'],
            "disk_io": metric['disk_io'],
            "reporting_timestamp": reporting_timestamp
        }
        
        msg = {
            "type": "performance_metric",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": individual_event
        }
        
        producer.send(KAFKA_TOPIC, value=msg)
        producer.flush()
        
        logger.info(f"SENT TO KAFKA: Performance metric (trace: {trace_id})")
    
    return NoContent, 201


def report_error_metrics(body):
    """Receive error metrics and send to Kafka"""
    server_id = body['server_id']
    reporting_timestamp = body['reporting_timestamp']
    errors = body['errors']

    logger.info(f"RECEIVED: Error metrics from server {server_id}")

    if not errors:
        return NoContent, 201
    
    for error in errors:
        trace_id = str(uuid.uuid4())

        individual_event = {
            "trace_id": trace_id,
            "server_id": server_id,
            "error_code": error['error_code'],
            "severity_level": error['severity_level'],
            "avg_response_time": error['avg_response_time'],
            "error_message": error.get('error_message', ''),
            "reporting_timestamp": reporting_timestamp
        }

        msg = {
            "type": "error_metric",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": individual_event
        }
        
        producer.send(KAFKA_TOPIC, value=msg)
        producer.flush()
        
        logger.info(f"SENT TO KAFKA: Error metric (trace: {trace_id}, code: {error['error_code']})")
    
    return NoContent, 201


if __name__ == "__main__":
    logger.info("Starting Receiver Service on port 8080")
    app.run(port=8080)
