import connexion
from connexion import NoContent
import httpx
import uuid
import yaml
import logging
import logging.config

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

STORAGE_SERVICE_URL = app_config['eventstore']['url']

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
    server_id = body['server_id']
    reporting_timestamp = body['reporting_timestamp']
    metrics = body['metrics']

    for metric in metrics:
        trace_id = str(uuid.uuid4())  

        # Log when an event is received
        logger.info(f"Received performance metrics request from server: {server_id} with trace id of {trace_id}")

        individual_event = {
            "trace_id": trace_id,
            "server_id": server_id,
            "cpu": metric['cpu'],
            "memory": metric['memory'],
            "disk_io": metric['disk_io'],
            "reporting_timestamp": reporting_timestamp
        }
        
        endpoint = app_config['endpoints']['performance']

        # logger.debug(f"Sending performance metric with trace_id: {trace_id}")

        response = httpx.post(
            f"{STORAGE_SERVICE_URL}{endpoint}",
            json=individual_event
        )
        
        status_code = response.status_code
        
        # Log the response from the storage service
        logger.info(f"Response for event performance_metric (trace_id: {trace_id}) has status {status_code}")

        # logger.info(f"Sent performance metric (trace_id: {trace_id}) to storage. Status: {status_code}")
    
    return NoContent, status_code


def report_error_metrics(body):
    server_id = body['server_id']
    reporting_timestamp = body['reporting_timestamp']
    errors = body['errors']

    logger.info(f"Received error metrics request from server: {server_id}")

    if not errors:
        return NoContent, 201
    
    status_code = 201
    
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

        endpoint = app_config['endpoints']['errors']

        # logger.debug(f"Sending error metric with trace_id: {trace_id}")
        
        response = httpx.post(
            f"{STORAGE_SERVICE_URL}{endpoint}",
            json=individual_event
        )
        
        status_code = response.status_code
        
        # Log the response from the storage service
        logger.info(f"Response for event error_metric (trace_id: {trace_id}) has status {status_code}")

        # logger.info(f"Sent error metric (trace_id: {trace_id}) to storage. Status: {status_code}")
    
    return NoContent, status_code


if __name__ == "__main__":
    logger.info("Starting Receiver Service on port 8080")
    app.run(port=8080)
