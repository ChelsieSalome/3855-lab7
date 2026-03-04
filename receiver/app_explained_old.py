import connexion
from connexion import NoContent
import json
import datetime
import os
import httpx

MAX_BATCH_EVENTS = 5
PERFORMANCE_FILE = "performance.json"  
ERRORS_FILE = "errors.json"

def read_data_from_file(filename):
    if os.path.isfile(filename):

        file_handle = open(filename, "r")
        file_contents = file_handle.read()
        if not file_contents.strip():            return {"total_count": 0, "recent_batches": []}

        try:
            file_handle.close()            
            return json.loads(file_contents)
        except json.JSONDecodeError: 
             return {"total_count": 0, "recent_batches": []}
        
    else:
       
        return {
            "total_count": 0,
            "recent_batches": []
        }

def write_data_to_file(filename, data):
   
    file_handle = open(filename, "w")
    parsed_data = json.dumps(data, indent=2)
    file_handle.write(parsed_data)
    file_handle.close()
   


app = connexion.FlaskApp(__name__, specification_dir='')

app.add_api("lab1_openapi.yaml",
            strict_validation=True,               validate_responses=True)


flask_app = app.app
@flask_app.route('/')
def home():   
     """Home page with links to API endpoints"""    
     return '''    
     <html>        
     <head>            
     <title>Monitoring API </title>                    
     </head>        
     <body>            
     <h1>Receiver Service</h1>                                           
     <h2>Available Endpoints:</h2>
     <ol>
     <li> <strong>POST:</strong> /monitoring/performance </li>
     <li> <strong>POST:</strong> /monitoring/errors </li>
     </ol>                                                                           
     </body>    
     </html>    
     '''



# First batch event
# Data used for API testing in Postman
"""
{
  "server_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "reporting_timestamp": "2026-01-14T10:00:00Z",
  "metrics": [
    {
      "cpu": 72.5,
      "memory": 64.2,
      "disk_io": 120.5,
      "recorded_timestamp": "2026-01-14T09:59:59Z"
    }
  ]
}

"""
def report_performance_metrics(body):

    metrics = body['metrics']
    
    # print(f'{batch_count} is the initial count')
    existing_data = read_data_from_file(PERFORMANCE_FILE)
    


    total_cpu = 0
    total_memory = 0
    total_disk_io = 0
    count = 0
    for metric in metrics:
        cpu = metric['cpu']
        memory = metric['memory']
        disk_io = metric['disk_io']

        total_cpu += cpu
        total_memory += memory
        total_disk_io += disk_io
        count += 1
    
    print(f'{count} is the iterated count ')

    avg_cpu = total_cpu / count
    avg_memory = total_memory / count
    avg_disk_io = total_disk_io / count
    current_datetime = datetime.datetime.now()
    timestamp_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")


    data_to_write = {
        "avg_cpu": avg_cpu,
        "avg_memory": avg_memory,
        "avg_disk_io" : avg_disk_io,
        "num_readings": count,
        "timestamp": timestamp_str
    }

    existing_data['total_count'] += 1
    existing_data['recent_batches'].append(data_to_write)

    if len(existing_data['recent_batches']) > MAX_BATCH_EVENTS:
        existing_data['recent_batches'].pop(0)
    
    write_data_to_file(PERFORMANCE_FILE, existing_data)

    # print(body)
    return NoContent, 201
    


# Second batch event

def report_error_metrics(body):
    errors = body['errors']

    if not errors:       
        return NoContent, 201

    client_errors = 0
    server_errors = 0
    total_severity = 0
    total_response_time = 0

    for error in errors:
        if 400 <= error['error_code'] < 500:
            client_errors += 1
        elif 500 <= error['error_code'] < 600:
            server_errors += 1

        total_severity += error['severity_level'] 
        total_response_time += error['avg_response_time']

    num_errors = len(errors)

    data_to_write = {
        'client_errors_4xx': client_errors,
        'server_errors_5xx': server_errors,
        'avg_severity_level': round(total_severity / num_errors, 2),
        'avg_response_time': round(total_response_time / num_errors, 2),
        
        'num_errors': num_errors,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    existing_data = read_data_from_file(ERRORS_FILE)
    existing_data['total_count'] += 1

    existing_data['recent_batches'].append(data_to_write)

    if len(existing_data['recent_batches']) > MAX_BATCH_EVENTS:        existing_data['recent_batches'].pop(0)

    write_data_to_file(ERRORS_FILE, existing_data)

    return NoContent, 201



if __name__ == "__main__":
    app.run(port=8080)
