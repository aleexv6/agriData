import pandas as pd
from bs4 import BeautifulSoup
from seleniumbase import Driver
from io import StringIO
import time
from datetime import datetime
from unidecode import unidecode
import database as db
import pymongo
from discord_webhook import DiscordWebhook
import config

URLS = {
    'EBM': 'https://live.euronext.com/fr/product/commodities-futures/EBM-DPAR/settlement-prices',
    'EMA': 'https://live.euronext.com/fr/product/commodities-futures/EMA-DPAR/settlement-prices',
    'ECO': 'https://live.euronext.com/fr/product/commodities-futures/ECO-DPAR/settlement-prices'
}
MONTHS = {
    'FEV': 'FEB',
    'MAI': 'MAY',
    'JUIN': 'JUN',
    'AOU': 'AUG'
}
RESPONSE = '**####### EURONEXT FUTURES DATA #######\n**'

def insert_db(df, RESPONSE=RESPONSE):
    dbname = db.get_database()
    collection_name_france = dbname["futures"]
    data = df.to_dict('records') #df to dict
    last_doc_france = collection_name_france.find_one( #find last item in database
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc_france is not None: #check if there is data in db, if not we need historical data
            if not df.empty: #if df not empty
                if df['Date'].iloc[0] != last_doc_france['Date']: #check if we already have this date as last date, if not insert data
                    inserted = str(collection_name_france.insert_many(data))
                    RESPONSE += inserted
                else:
                    RESPONSE += 'Euronext Futures : Document non inséré, doublon date avec le dernier document en base.'
            else:
                RESPONSE += 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    else:
        RESPONSE += 'NO DATA IN DB, INSERT HISTORICAL FIRST'
    return RESPONSE

def maturity_to_expiration(series, months_map=MONTHS):
    strs = series.apply(unidecode).str.upper()
    month = strs.str.strip().str.split().str[0]
    year = strs.str.strip().str.split().str[1].str[-2:]
    mapped = month.map(months_map).fillna(month)
    expi = mapped+year
    return expi

def scrapper(url):
    driver = Driver(uc=True, incognito=True, headless=True) #set driver
    driver.get(url)
    driver.wait_for_element(".table")
    html = driver.page_source #get html code from page
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table', class_='table') #find the datatable
    tmp = pd.read_html(StringIO(str(table)))[0].iloc[:-1] #prep dataframe form table
    driver.quit()
    return tmp

def clean_scrapped(urls, RESPONSE=RESPONSE):
    df_lists = []
    for idx, item in urls.items(): #loop throught tickers and URLs
        retry = 0
        while retry < 5:  # Retry up to 5 times
            tmp = scrapper(item) #set scrapped df in tmp
            if not tmp.empty: # if tmp not empty meaning we scrapped something
                if len(tmp['Compens.'].unique()) == 1 or 'nan' in tmp['Compens.'].astype(str).values: #if only one unique compensation, it means it is either full nan values or not the full values -> we retry, or if their is at least one nan value in compens. -> we retry 
                    #retry
                    RESPONSE += f"{idx} full data not received, retrying..."
                    time.sleep(300) #5m sleep
                    retry += 1
                    continue  # Retry the current iteration
                else:
                    #we good,
                    tmp['Ticker'] = idx #add ticker to df
                    tmp['Date'] = datetime.today().strftime('%Y-%m-%d')
                    df_lists.append(tmp)
                    RESPONSE += f"{idx}, data scrapped ok"
                    break
            else: #if there is nothing scrapped
                RESPONSE += f"Error scrapping data, get empty dataframe for {idx}"
                break
        else:
            # Exceeded max retries, skip this item
            RESPONSE += f'Skipping {idx} after max retries, no full data found\n'
       
    df = pd.concat(df_lists) #concat into one df
    return df

if __name__ == "__main__":
    df = clean_scrapped(URLS).reset_index(drop=True)
    df['Expiration'] = maturity_to_expiration(df['Maturité'])
    df = df.rename(columns={'Ouvert': 'Open', 'Haut': 'High', 'Bas': 'Low', 'Compens.': 'Close', 'Position ouverte': 'Open Interest'})
    df = df[['Date', 'Ticker', 'Expiration', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']]
    df['Close'] = df['Close'].astype(float)

    r = insert_db(df) #insert to db
    #Logs into my Discord server to be keep track of bugs 
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content=r)
    response = webhook.execute()
