from sqlalchemy import create_engine, inspect, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database


class DataWarehouse:
    '''
    Connect to Data Warehouse
    '''
    def __init__(self):
        self.USER = 'postgres'
        self.PASSWORD = '1234'
        self.DATABASE_NAME = 'catawiki_datawarehouse'
        self.SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://{0}:{1}@localhost:5432/{2}".\
                format(self.USER, self.PASSWORD, self.DATABASE_NAME)
    

    def connection(self):
        self.engine = create_engine(self.SQLALCHEMY_DATABASE_URL, echo=False)
        self.Base = declarative_base()

        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def create_myDatabase(self):
        '''
        Create a database to collect data there and if it exists don't do anything.
        '''
        is_database_exists = database_exists(self.SQLALCHEMY_DATABASE_URL)
        if not is_database_exists:
            engine = create_engine(self.SQLALCHEMY_DATABASE_URL)
            create_database(engine.url)
            engine.dispose()


    def table_Products(self, create:bool=False):
        '''
        Create a table to collect products data in there.
        '''
        class Products(self.Base):
            __tablename__ = 'products'
            id = Column(Integer, primary_key=True)
            lotId = Column(Integer, nullable=True)
            category = Column(String(200), nullable=False)
            lotTitle = Column(String(50), nullable=True)
            lotSubtitle = Column(String(50), nullable=True)
            description = Column(String(50), nullable=True)
            seller_id = Column(Integer, nullable=False)
            auctionId = Column(Integer, nullable=True)
            expertsEstimate_min_EUR = Column(Integer, nullable=True)
            expertsEstimate_min_USD = Column(Integer, nullable=True)
            expertsEstimate_min_GBP = Column(Integer, nullable=True)
            expertsEstimate_max_EUR = Column(Integer, nullable=True)
            expertsEstimate_max_USD = Column(Integer, nullable=True)
            expertsEstimate_max_GBP = Column(Integer, nullable=True)
            favoriteCount = Column(Integer, nullable=True)
            Size = Column(String(1000), nullable=True)
            Gender = Column(String(1000), nullable=True)
            Total_Weight = Column(String(1000), nullable=True)
            Type = Column(String(1000), nullable=True)
            Material = Column(String(1000), nullable=True)
            Condition = Column(String(1000), nullable=True)
            Laboratory_Report = Column(String(1000), nullable=True)
            Material_Fineness = Column(String(1000), nullable=True)
            Main_Stone = Column(String(1000), nullable=True)
            Brand = Column(String(1000), nullable=True)

        if create:
            self.Base.metadata.create_all(self.engine)

        return Products

    
    def store_product(self, table, item):
        # productTable = table()
        new_record = table(
            lotId = item['lotId'],
            category = item['category'],
            lotTitle = item['lotTitle'],
            lotSubtitle = item['lotSubtitle'],
            description = item['description'],
            seller_id = item['seller_id'],
            auctionId = item['auctionId'],
            expertsEstimate_min_EUR = item['expertsEstimate_min_EUR'],
            expertsEstimate_min_USD = item['expertsEstimate_min_USD'],
            expertsEstimate_min_GBP = item['expertsEstimate_min_GBP'],
            expertsEstimate_max_EUR = item['expertsEstimate_max_EUR'],
            expertsEstimate_max_USD = item['expertsEstimate_max_USD'],
            expertsEstimate_max_GBP = item['expertsEstimate_max_GBP'],
            favoriteCount = item['favoriteCount'],
            Size = item['Size'],
            Gender = item['Gender'],
            Total_Weight = item['Total_Weight'],
            Type = item['Type'],
            Material = item['Material'],
            Condition = item['Condition'],
            Laboratory_Report = item['Laboratory_Report'],
            Material_Fineness = item['Material_Fineness'],
            Main_Stone = item['Main_Stone'],
            Brand = item['Brand'])
        
        self.session.add(new_record)
        self.session.commit()