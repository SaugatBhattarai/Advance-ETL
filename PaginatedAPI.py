#paginated post request
import requests

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
#push it to the postgreSQL database
