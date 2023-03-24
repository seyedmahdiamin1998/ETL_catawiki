from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class DataLake:
    '''
    Connect to Data Lake
    '''
    def __init__(self):
        self.USER = 'postgres'
        self.PASSWORD = '1234'
        self.DATABASE_NAME = 'catawiki'
        self.SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://{0}:{1}@localhost:5432/{2}".\
                format(self.USER, self.PASSWORD, self.DATABASE_NAME)
    

    def connection(self):
        self.engine = create_engine(self.SQLALCHEMY_DATABASE_URL, echo=False)
        self.Base = declarative_base()

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

