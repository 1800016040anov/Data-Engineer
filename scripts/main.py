
import consts
import databases
import pandas as pd
import os 
import config


"""
cause the csv's is more than one so, i need to know the list of csv first with a method get every 
file in folder path which end with .csv 
"""

csv_files = [f for f in os.listdir(consts.FOLDER_SOURCE) if f.endswith('.csv')]

tables = {}
for file in csv_files:
    dataset_name = file.split('.')[0]
    table_name = dataset_name
    table_name= pd.read_csv(f'data/{file}')
    #i use dictionary to save the table that i will process 
    tables.update({f"{dataset_name}":table_name})
    #save to db as a checkpoint,to anticipate if next time we need original data
    databases.df_to_db(table_name,dataset_name,consts.RAW_SCHEMA, databases.conn)

#preview all data 
config.view_all_data(tables)

#after that transform data 
"""
products_dataset
"""
orders_dataset = tables['orders_dataset']
orders_dataset['order_status']=pd.Series(orders_dataset['order_status'], dtype="string")
orders_dataset['customer_id']=pd.Series(orders_dataset['customer_id'], dtype="string")
orders_dataset['order_id']=pd.Series(orders_dataset['order_id'], dtype="string")

#change object to datetime
orders_dataset['order_approved_at']=pd.to_datetime(orders_dataset['order_approved_at'])
orders_dataset['order_purchase_timestamp']=pd.to_datetime(orders_dataset['order_purchase_timestamp'])
orders_dataset['order_delivered_carrier_date']=pd.to_datetime(orders_dataset['order_delivered_carrier_date'])
orders_dataset['order_delivered_customer_date']=pd.to_datetime(orders_dataset['order_delivered_customer_date'])
orders_dataset['order_estimated_delivery_date']=pd.to_datetime(orders_dataset['order_estimated_delivery_date'])

#set default nan value into min time in pandas
orders_dataset['order_approved_at'] = orders_dataset['order_approved_at'].fillna(pd.Timestamp.min)
orders_dataset['order_purchase_timestamp'] = orders_dataset['order_purchase_timestamp'].fillna(pd.Timestamp.min)
orders_dataset['order_delivered_carrier_date']=orders_dataset['order_delivered_carrier_date'].fillna(pd.Timestamp.min)
orders_dataset['order_delivered_customer_date']=orders_dataset['order_delivered_customer_date'].fillna(pd.Timestamp.min)
orders_dataset['order_estimated_delivery_date']=orders_dataset['order_estimated_delivery_date'].fillna(pd.Timestamp.min)



"""order_payments_dataset"""
df_order_payments_dataset= tables['order_payments_dataset']
df_order_payments_dataset['payment_type']=pd.Series(df_order_payments_dataset['payment_type'], dtype="string")
df_order_payments_dataset['order_id']=pd.Series(df_order_payments_dataset['order_id'], dtype="string")


""" product_category_name_translation """
product_category_name_translation = tables['product_category_name_translation']
product_category_name_translation['product_category_name']= pd.Series(product_category_name_translation['product_category_name'], dtype="string")
product_category_name_translation['product_category_name_english']= pd.Series(product_category_name_translation['product_category_name_english'], dtype="string")

"""

order_items_dataset

"""

order_items_dataset = tables['order_items_dataset']
order_items_dataset['order_id'] = pd.Series(order_items_dataset['order_id'], dtype="string")
order_items_dataset['product_id'] = pd.Series(order_items_dataset['product_id'], dtype="string")
order_items_dataset['seller_id'] = pd.Series(order_items_dataset['seller_id'], dtype="string")
order_items_dataset['shipping_limit_date'] = pd.Series(order_items_dataset['shipping_limit_date'], dtype="string")
order_items_dataset['price'] = pd.Series(order_items_dataset['price'], dtype="int64")

"""order_reviews_dataset"""
order_reviews_dataset = tables['order_reviews_dataset']

order_reviews_dataset['review_id']=pd.Series(order_reviews_dataset['review_id'], dtype="string")
order_reviews_dataset['order_id']=pd.Series(order_reviews_dataset['order_id'], dtype="string")
order_reviews_dataset['review_comment_title']=pd.Series(order_reviews_dataset['review_comment_title'], dtype="string")
order_reviews_dataset['review_comment_message']=pd.Series(order_reviews_dataset['review_comment_message'], dtype="string")
order_reviews_dataset['review_creation_date']=pd.Series(order_reviews_dataset['review_creation_date'], dtype="string")
order_reviews_dataset['review_answer_timestamp']=pd.to_datetime(order_reviews_dataset['review_answer_timestamp'])
order_reviews_dataset['review_comment_title'] =order_reviews_dataset['review_comment_title'].fillna("no-title")
order_reviews_dataset['review_comment_message'] =order_reviews_dataset['review_comment_message'].fillna("no-comment")


"""sellers_dataset"""
sellers_dataset= tables['sellers_dataset']
sellers_dataset['seller_id']=pd.Series(sellers_dataset['seller_id'],dtype="string")
sellers_dataset['seller_city']=pd.Series(sellers_dataset['seller_city'],dtype="string")
sellers_dataset['seller_state']=pd.Series(sellers_dataset['seller_state'],dtype="string")

"""
geolocation_dataset
"""

geolocation_dataset=tables['geolocation_dataset']

geolocation_dataset['geolocation_city'] = pd.Series(geolocation_dataset['geolocation_city'],dtype="string")
geolocation_dataset['geolocation_state'] = pd.Series(geolocation_dataset['geolocation_state'],dtype="string")


"""products_dataset"""
products_dataset= tables['products_dataset']

products_dataset.rename(columns={"product_name_lenght": "product_name_length",
                                 "product_description_lenght":"product_description_length"},
                        inplace=True)
products_dataset['product_category_name'] =products_dataset['product_category_name'].fillna("unknown")
products_dataset['product_description_length'] =products_dataset['product_description_length'].fillna(0)
products_dataset['product_photos_qty'] =products_dataset['product_photos_qty'].fillna(0)
products_dataset['product_weight_g'] =products_dataset['product_weight_g'].fillna(0)
products_dataset['product_length_cm'] =products_dataset['product_length_cm'].fillna(0)
products_dataset['product_height_cm'] =products_dataset['product_height_cm'].fillna(0)
products_dataset['product_width_cm'] =products_dataset['product_width_cm'].fillna(0)
products_dataset['product_name_length'] = products_dataset['product_name_length'].fillna(0)

products_dataset['product_id']=pd.Series(products_dataset['product_id'],dtype="string")
products_dataset['product_category_name']=pd.Series(products_dataset['product_category_name'],dtype="string")
products_dataset['product_name_length']=pd.Series(products_dataset['product_name_length'],dtype="int64")
products_dataset['product_description_length']=pd.Series(products_dataset['product_description_length'],dtype="int64")
products_dataset['product_photos_qty']=pd.Series(products_dataset['product_photos_qty'],dtype="int64")

"""customers_dataset"""
customers_dataset= tables['customers_dataset']

customers_dataset['customer_id']=pd.Series(customers_dataset['customer_id'],dtype="string")
customers_dataset['customer_unique_id']=pd.Series(customers_dataset['customer_unique_id'],dtype="string")
customers_dataset['customer_city']=pd.Series(customers_dataset['customer_city'],dtype="string")
customers_dataset['customer_state']=pd.Series(customers_dataset['customer_state'],dtype="string")


#lastly we could save in to presentation schema 

for table in tables:
    databases.df_to_db(tables[table],table,consts.PRESENTATION_SCHEMA,databases.conn)
    print("success to save table {table} ")





