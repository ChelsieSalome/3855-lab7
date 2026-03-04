from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import yaml
import logging
import logging.config 


# Load configuration
with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config['datastore']

with open("log_conf.yaml", "r") as f:    
    LOG_CONFIG = yaml.safe_load(f.read())    
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# MySQL connection string
DATABASE_URL = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
logger.info(f"Connecting to database at {db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}")

# Create engine with MySQL
engine = create_engine(DATABASE_URL, echo=True)


# Function to create database sessions
def make_session():
    Session = sessionmaker(bind=engine)
    return Session()

# Create tables
Base.metadata.create_all(engine)

print("Database and tables created")
