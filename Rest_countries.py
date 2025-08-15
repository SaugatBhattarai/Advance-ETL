import requests
import pandas as pd
from sqlalchemy import create_engine

#Step 1: Extract data from the API
# url = "https://restcountries.com/v3.1/all?fields=name,capital,population,area,languages,currencies"
url = "https://restcountries.com/v3.1/all?fields=name,region,subregion,population,area"
response = requests.get(url)
data = response.json()

# # Step 2: Transform the data into a DataFrame
records = []
for country in data:
    record = {
        'name': country.get('name', {}).get('common', ''),
        'region': country.get('region', ''),
        'subregion': country.get('subregion', ''),
        'population': country.get('population', 0),
        'area': country.get('area', 0)
    }
    records.append(record)

df = pd.DataFrame(records)


# # Step 3: Load Data Into the Database
# Save the DataFrame to the database

# #load libraries required
# #create the engine
#push dataframe to postgreSQL database

# Database connection settings
db_user = "demo_user"
db_password = "demo_pass"
db_host = "localhost"
db_port = "5432"
db_name = "countriesdb"

# Create the database engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Push DataFrame to PostgreSQL database
df.to_sql('countries', engine, if_exists='replace', index=False)
