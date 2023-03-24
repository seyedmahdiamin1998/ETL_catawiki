from DataWarehouse import DataWarehouse
from DataLake import DataLake

#####################################################################
# Data Warehouse

DW = DataWarehouse()
DW.create_myDatabase()
DW.connection()
product_DW=DW.table_Products(create=True)

#####################################################################
# Data Lake

DL = DataLake()
DL.connection()

#####################################################################
# ETL

products = DL.engine.execute('''select * from products''')

for product in products:

    product = dict(zip(products.keys(), product))

    specifications = DL.engine.execute(f'''SELECT * FROM specifications
                                           WHERE "lotId" = {product['lotId']}''')

    specification_keys = [  'Size', 'Gender', 'Total_Weight',\
                            'Type', 'Material', 'Condition',\
                            'Laboratory_Report', 'Material_Fineness',\
                            'Main_Stone','Brand']
    
    specification_list = {}
    for specification in specifications:
        result_dict = dict(zip(specifications.keys(), specification ))
        product[result_dict['name']] = result_dict['value'] 

    for item in specification_keys:
        if item not in product.keys():
            product[item] = None  
    
    DW.store_product(table=product_DW, item=product)