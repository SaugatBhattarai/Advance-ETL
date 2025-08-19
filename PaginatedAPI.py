#paginated post request
import requests
import pandas as pd
from sqlalchemy import create_engine

#Extract data from the paginated API
url = "https://api.spacexdata.com/v4/launches/query"

headers = {
    "Content-Type": "application/json"
}

payload = {
    "query": {},
    "options": {
        "limit": 100,
        "offset": 0,
        "page": 1
    }
}

all_launches = []

while True:
    res = requests.post(url, json=payload, headers=headers)
    output = res.json()

    docs = output.get('docs', [])
    for doc in docs:
        all_launches.append({
            'name': doc.get('name', ''),
            'date_utc': doc.get('date_utc', ''),
            'success': doc.get('success', False),
            'details': doc.get('details', ''),
            'rocket': doc.get('rocket', {}),
        })

    if not output.get('hasNextPage', False):
        break
    payload['options']['page'] += 1

#convert all_launches to a DataFrame
df = pd.DataFrame(all_launches)


#push it to the postgreSQL database
# Database connection settings
db_user = "demo_user"
db_password = "demo_pass"
db_host = "localhost"
db_port = "5432"
db_name = "countriesdb"

# Create the database engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Push DataFrame to PostgreSQL database
df.to_sql('launches', engine, if_exists='replace', index=False)