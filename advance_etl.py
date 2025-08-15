import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from apscheduler.schedulers.blocking import BlockingScheduler


#SETUP LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#rate limit settings
MAX_REQUESTS_PER_SECOND = 1
RATE_LIMIT_DELAY = 1 / MAX_REQUESTS_PER_SECOND

#postgreSQL database connection settings
DATABASE_URL = "postgresql://user:password@localhost:5432/mydatabase"

#retry decorator for handling API request failures
@retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10), 
        retry=retry_if_exception_type(requests.RequestException)
        )

def fetch_data(page):
    url=f"https:/catfact.ninja/fact?page={page}"
    response = requests.get(url,timeout=10)
    response.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()

def etl_datafacts():
    all_facts=[]
    page=1
    while True:
        try:
            data=fetch_data(page)
            facts = data.get('data', [])
            for fact in facts:
                #extract key values from the dictionary
                # all_facts.append({
                #     'fact': fact.get('fact', ''),
                #     'length': fact.get('length', 0),
                # })
                pass
            if not data.get("next_page_url", False):
                break
            page += 1
        except Exception as e:
            print(f"Error fetching data on page {page}: {e}")
            break
    #create a DataFrame from the list of facts
    df = pd.DataFrame(all_facts)
    #load data into the PostgreSql DATABASE
    engine = create_engine("postgresql://user:password@localhost:5432/mydatabase")
    df.to_sql("cat_facts", engine, if_exists="replace", index=False)



scheduler = BlockingScheduler()
scheduler.add_job(etl_datafacts, 'interval', hours=1)

if __name__ == "__main__":
    scheduler.start()
