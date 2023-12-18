# scripts/data_preparation.py
import pymongo
import pandas as pd
from yaml import safe_load

# Load MongoDB configuration from config.yaml
with open('config.yaml', 'r') as config_file:
    config = safe_load(config_file)

# Connect to MongoDB
client = pymongo.MongoClient(config['mongo']['uri'])
db = client[config['mongo']['database']]

# Retrieve data from MongoDB collections
users_data = pd.DataFrame(list(db['users'].find()))
# skus_data = pd.DataFrame(list(db['skus'].find()))
# orders_data = pd.DataFrame(list(db['orders'].find()))
# frequent_bundles_data = pd.DataFrame(list(db['frequent_bundles'].find()))
# products_data = pd.DataFrame(list(db['products'].find()))

# Perform data preprocessing as needed
# For example, handle missing values, encode categorical variables, etc.

# Ensure column names are unique
users_data.columns = [f"user_{col}" for col in users_data.columns]
# skus_data.columns = [f"sku_{col}" for col in skus_data.columns]
# orders_data.columns = [f"order_{col}" for col in orders_data.columns]
# frequent_bundles_data.columns = [f"bundle_{col}" for col in frequent_bundles_data.columns]
# products_data.columns = [f"product_{col}" for col in products_data.columns]

# Save the preprocessed data to a new MongoDB collection
# For simplicity, let's assume the new collection is named 'preprocessed_data'
# preprocessed_data = pd.concat([users_data, skus_data, orders_data, frequent_bundles_data, products_data], axis=1)
preprocessed_data = pd.concat([users_data], axis=1)

# Convert the DataFrame to a list of dictionaries (JSON-like)
preprocessed_data_list = preprocessed_data.to_dict(orient='records')

# Create a new collection named 'preprocessed_data' and insert the data
db['preprocessed_data'].insert_many(preprocessed_data_list)
