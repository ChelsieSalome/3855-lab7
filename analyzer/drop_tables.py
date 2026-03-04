from sqlalchemy import create_engine
from models import Base
import yaml

# Load configuration
with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config['datastore']

# MySQL connection string
DATABASE_URL = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"

engine = create_engine(DATABASE_URL, echo=True)
Base.metadata.drop_all(engine)

print("Database and tables dropped")
