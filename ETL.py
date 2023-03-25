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
    print('\n'*3)
    product = dict(zip(products.keys(), product))

    specifications = DL.engine.execute(f'''SELECT * FROM specifications
                                           WHERE "lotId" = {product['lotId']}''')

    specification_keys = [  'Size', 'Gender', 'Total Weight',\
                            'Type', 'Material', 'Condition',\
                            'Laboratory Report', 'Material Fineness',\
                            'Main Stone','Brand']
    
    specification_list = {}
    for specification in specifications:
        result_dict = dict(zip(specifications.keys(), specification ))
        if result_dict['name']:
            product[result_dict['name'].replace(' ', '_')] = result_dict['value'] 


    for item in specification_keys:
        item = item.replace(' ', '_')
        if item not in product.keys():
            product[item] = None  
    
    if product['Total_Weight']:
        product['Total_Weight'] = product['Total_Weight'].replace(' g','')
    
    if product['Material_Fineness']:
        product['Material_Fineness'] = product['Material_Fineness'].replace(' kt.','')
    
    DW.store_product(table=product_DW, item=product)