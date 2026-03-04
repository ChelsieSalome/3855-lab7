import connexion
import json
import logging
import logging.config
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone
import requests

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('log_conf.yaml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

filename = app_config['datastore']['filename']
performance_url = app_config['eventstores']['performance_url']
errors_url = app_config['eventstores']['errors_url']
scheduler_interval = app_config['scheduler']['interval']


def get_stats():
    logger.info("Request for statistics received")  
    
    try:
        with open(filename, 'r') as f:
            stats = json.load(f)
        
        logger.debug(f"Statistics contents: {stats}")  
        logger.info("Request has completed")  
        return stats, 200
    except FileNotFoundError:
        logger.error("Statistics file does not exist")  
        return {"message": "Statistics do not exist"}, 404  


def populate_stats():
    logger.info("Periodic processing has started")
    
    try:
        # Step 1: Test file access
        logger.info("STEP 1: Testing file access...")
        
        try:
            with open(filename, 'r') as f:
                stats = json.load(f)
            logger.info("STEP 1: Successfully loaded existing stats file")
        except FileNotFoundError:
            stats = {
                "num_performance_readings": 0,
                "max_cpu_reading": 0,
                "num_error_readings": 0,
                "max_severity_level": 0,
                "last_updated": "2026-01-01T00:00:00Z"
            }
            logger.info("STEP 1: Created new stats (file didn't exist)")
        except Exception as e:
            logger.error(f"STEP 1 ERROR: Failed to handle stats file: {e}")
            return
        
        # Step 2: Test datetime parsing
        logger.info("STEP 2: Testing datetime parsing...")
        
        try:
            last_updated = datetime.strptime(stats['last_updated'], "%Y-%m-%dT%H:%M:%SZ")
            current_datetime = datetime.now(timezone.utc)
            logger.info("STEP 2: Successfully parsed datetime")
        except Exception as e:
            logger.error(f"STEP 2 ERROR: Failed to parse datetime: {e}")
            return
        
        # Step 3: Test parameter creation
        logger.info("STEP 3: Creating query parameters...")
        
        try:
            params = {
                'start_timestamp': last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
                'end_timestamp': current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            logger.info(f"STEP 3: Query params created: {params}")
        except Exception as e:
            logger.error(f"STEP 3 ERROR: Failed to create params: {e}")
            return
        
        # Step 4: Test performance request
        logger.info("STEP 4: Testing performance request...")
        
        try:
            logger.info(f"STEP 4: Requesting from {performance_url}")
            performance_response = requests.get(performance_url, params=params, timeout=10)
            logger.info(f"STEP 4: Got response status: {performance_response.status_code}")
            logger.info(f"STEP 4: Response text: {performance_response.text}")
            
            if performance_response.status_code == 200:
                performance_readings = performance_response.json()
                logger.info(f"Number of performance events received: {len(performance_readings)}")
                
                stats['num_performance_readings'] += len(performance_readings)
                
                if performance_readings:
                    max_cpu = max(reading['cpu'] for reading in performance_readings)
                    if max_cpu > stats['max_cpu_reading']:
                        stats['max_cpu_reading'] = max_cpu
                    logger.info(f"STEP 4: Max CPU: {max_cpu}")
            else:
                logger.error(f"Did not get 200 response code for performance data: {performance_response.status_code}")
                
        except Exception as e:
            logger.error(f"STEP 4 ERROR: Performance request failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Continue to next step even if this fails
        
        # Step 5: Test error request
        logger.info("STEP 5: Testing error request...")
        
        try:
            logger.info(f"STEP 5: Requesting from {errors_url}")
            error_response = requests.get(errors_url, params=params, timeout=10)
            logger.info(f"STEP 5: Got response status: {error_response.status_code}")
            
            if error_response.status_code == 200:
                error_readings = error_response.json()
                logger.info(f"Number of error events received: {len(error_readings)}")
                
                stats['num_error_readings'] += len(error_readings)
                
                if error_readings:
                    max_severity = max(reading['severity_level'] for reading in error_readings)
                    if max_severity > stats['max_severity_level']:
                        stats['max_severity_level'] = max_severity
            else:
                logger.error(f"Did not get 200 response code for error data: {error_response.status_code}")
                
        except Exception as e:
            logger.error(f"STEP 5 ERROR: Error request failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Continue even if this fails
        
        # Step 6: Test file save
        logger.info("STEP 6: Saving stats file...")
        
        try:
            stats['last_updated'] = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            with open(filename, 'w') as f:
                json.dump(stats, f, indent=4)
            
            logger.info("STEP 6: Successfully saved stats file")
            logger.debug(f"Updated statistics: {stats}")
            
        except Exception as e:
            logger.error(f"STEP 6 ERROR: Failed to save stats: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
    except Exception as e:
        logger.error(f"CRITICAL ERROR in populate_stats: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("Periodic processing has ended")
  


def init_scheduler():  
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['interval'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("processing_openapi.yaml", strict_validation=True, validate_responses=True)

@app.route('/debug-populate')  
def debug_populate():
    populate_stats()
    return "populate_stats() executed - check logs and data.json"


if __name__ == "__main__":
    init_scheduler() 
    app.run(port=8100)

