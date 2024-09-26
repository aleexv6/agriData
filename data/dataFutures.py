import pandas as pd
import config
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import time
import database as db
import pymongo
from discord_webhook import DiscordWebhook

def export_historical(driver):
    data = []
    list_produit = {
        'EBM': ['MAR24', 'MAY24', 'SEP24', 'DEC24', 'MAR25'],
        'EMA': ['MAR24', 'JUN24', 'AUG24', 'NOV24', 'MAR25'],
        'ECO': ['MAY24', 'AUG24', 'NOV24', 'FEB25', 'MAY25']
    }
    for ticker, expiration in list_produit.items():
        for expi in expiration:
            try:
                driver.get(f"https://flux.agritel.com/agritelwebsite/Chart.aspx?KEY=677e9348-833d-4bf2-bc73-b8dfb61e1bb2&CODE={ticker}&EXPIRY={expi}")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@class='highcharts-container']"))
                )
                dates = driver.execute_script(f'return Highcharts.charts[0].series[0].data.map(x => x.series).map(x => x.xData)[0]')
                values = driver.execute_script(f'return Highcharts.charts[0].series[0].data.map(x => x.series).map(x => x.yData)[0]')

                df = pd.DataFrame({'Date': dates, 'Ticker': ticker, 'Expiration': expi, 'Prix': values})
                df['Date'] = pd.to_datetime(df['Date'], unit='ms')
                data.append(df)
            except():
                print('Erreur')
    data = pd.concat(data, ignore_index=True)   
    data = data.sort_values(by='Date', ascending=True)
    return data

def export_historical_one(driver, ticker, expiration):
    try:
        driver.get(f"https://flux.agritel.com/agritelwebsite/Chart.aspx?KEY=677e9348-833d-4bf2-bc73-b8dfb61e1bb2&CODE={ticker}&EXPIRY={expiration}")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='highcharts-container']"))
        )
        dates = driver.execute_script(f'return Highcharts.charts[0].series[0].data.map(x => x.series).map(x => x.xData)[0]')
        values = driver.execute_script(f'return Highcharts.charts[0].series[0].data.map(x => x.series).map(x => x.yData)[0]')

        df = pd.DataFrame({'Date': dates, 'Ticker': ticker, 'Expiration': expiration, 'Prix': values})
        df['Date'] = pd.to_datetime(df['Date'], unit='ms')
    except():
        print('Erreur')
    df = df.sort_values(by='Date', ascending=True)
    return df

def export_last(driver):
    driver.get("https://www.agritel.com/fr/")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//p[@id='euronextDate']"))
    )
    time.sleep(10)
    data = []
    date = pd.Timestamp.today().normalize()
    tables = driver.find_elements(By.XPATH, "//div[@id='quotes_tab1']/table")
    for table in tables:
            body = table.find_elements(By.XPATH, ".//tbody/tr/td")
            for td in body:
                if td.get_attribute("id") != "":
                    value = float(td.text.replace(',', '.'))
                    produit = td.get_attribute("id").replace('_', ' ').split()[0]
                    ticker = produit[:3] 
                    expiration = produit[3:]
                    df = pd.DataFrame({'Date': [date], 'Ticker': [ticker], 'Expiration': [expiration], 'Prix': [value]})
                    data.append(df)
    data = pd.concat(data, ignore_index=True)
    return data

def historical_insert_db(df):
    dbname = db.get_database()
    collection_name = dbname["euronext"]
    data = df.to_dict('records')
    last_doc = collection_name.find_one({})
    if last_doc is None:
        r = collection_name.insert_many(data)
    else:
        r = 'Impossible de push les données historiques quand il y a déjà des données en base.'
    return r

def check_if_exist_db(ticker, expi):
    exist = True
    dbname = db.get_database()
    collection_name = dbname["euronext"]
    doc = collection_name.find_one({'Ticker': ticker, 'Expiration': expi})
    if doc is None:
        exist = False
    return exist

def insert_db(driver, df):
    r = ''
    dbname = db.get_database()
    collection_name = dbname["euronext"]
    data = df.to_dict('records')
    last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )

    if df['Date'][0] != last_doc['Date']:
        collection_name.update_many({}, {'$set': {'Expired': True}})
        for index, row in df.iterrows():
            collection_name.update_many({'Ticker': row['Ticker'], 'Expiration': row['Expiration']}, {'$set': {'Expired': False}})
            if not check_if_exist_db(row['Ticker'], row['Expiration']):
                hist = export_historical_one(driver, row['Ticker'], row['Expiration']).iloc[:-1].to_dict('records')
                r = 'ADDED NEW HISTORICAL DATA FOR NEW EXPI ' + str(collection_name.insert_many(hist))
        r = r + str(collection_name.insert_many(data))
        r = 'EURONEXT FUTURES DATA : ' + r
    else:
        r = 'EURONEXT FUTURES DATA : Document non inséré, doublon date avec le dernier document en base.'
    return r
    
if __name__ == "__main__":
    #SETUP PATH
    driver_path = 'C:/Users/alexl/Documents/GitHub/CodeReaderCOT/agri/data/driver/chromedriver.exe'
    brave_path = 'C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe'

    #SETUP BROWSER OPTIONS
    option = Options()
    option.binary_location = brave_path
    option.add_argument("--headless=new")
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    option.add_argument(f'user-agent={user_agent}')

    #SETUP DRIVER
    s = Service(driver_path)
    driver = webdriver.Chrome(service=s, options=option)

    #SCRAPE DATA
    #data = export_historical(driver)
    data = export_last(driver)
    data['Expired'] = False  
    #r = historical_insert_db(data)
    r = insert_db(driver, data)
    driver.close()
    print(r)
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## EURONEXT DATA FUTURES LOG #########**' + '\n' + r)
    response = webhook.execute()
    
    

    