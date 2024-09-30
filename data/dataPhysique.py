import pandas as pd
import config
from selenium import webdriver
from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import time
import database as db
import pymongo
from datetime import timedelta
from discord_webhook import DiscordWebhook

def export_historical(driver):

    z = []
    #count = 0
    products = {
        'mais': ['Bordeaux Rendu', 'La Pallice Rendu', 'Rhin Fob', 'Bordeaux Fob'],
        'ble-tendre': ['Rouen Rendu', 'Dunkerque Rendu', 'La Pallice Rendu', 'Creil Fob', 'Moselle Fob', 'Rouen Fob'],
        'ble-dur': ['La Pallice Rendu', 'Port-La-Nouvelle Rendu'],# 'Sud-Est Départ', 'Sud-Ouest Départ'],
        'colza': ['Rouen Rendu', 'Moselle Fob']

    }
    for produit in products.keys():
        count2 = 0
        driver.get(f"https://www.terre-net.fr/marche-agricole/{produit}/physique")      
        time.sleep(5)  
        #if count == 0:
            # WebDriverWait(driver, 60).until(
            #     EC.presence_of_element_located((By.CLASS_NAME, "didomi-popup-container"))
            # )
            # elem = driver.find_element(By.CLASS_NAME, "didomi-continue-without-agreeing").click()
        #count +=1
        for place in products[produit]:
            select = Select(driver.find_element(By.ID, 'place-graph-selector'))
            select.select_by_visible_text(place)
            time.sleep(3)
            produit = driver.execute_script(f'return Highcharts.charts[{count2}].series[0].userOptions.name')
            dates = driver.execute_script(f'return Highcharts.charts[{count2}].series[0].xData')
            values = driver.execute_script(f'return Highcharts.charts[{count2}].series[0].yData')
            char_to_replace = {'É':'E', 'Ï':'I', 'é':'e', 'ï':'i'}
            for key, value in char_to_replace.items():
                produit = produit.replace(key, value)

            if produit.split(' ', 1)[0] == 'Ble':
                prod = " ".join(produit.split()[:2])
                place_name = " ".join(produit.split()[2:])
            else:
                prod = produit.split(' ', 1)[0]
                place_name = produit.split(' ', 1)[1]

            df = pd.DataFrame({'Date': dates, 'Produit': prod, 'Place':place_name, 'Prix': values })
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            count2 += 1
            z.append(df)
        end = pd.concat(z, ignore_index=True)  
    end = end.sort_values(by='Date', ascending=True)
    return end

def export_last(driver):
    data = export_historical(driver)
    data = data[data['Date'] == (pd.Timestamp.today().normalize() + timedelta(hours=22))]
    return data

def insert_db(df):
    r = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name = dbname["physique"]
    data = df.to_dict('records')
    last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc is not None:
            if not df.empty:
                if df['Date'].iloc[0] != last_doc['Date']:
                    r = str(collection_name.insert_many(data))
                    r = 'AGRI DATA PHYSIQUE : ' + r
                else:
                    r = 'Document non inséré, doublon date avec le dernier document en base.'
            else:
                r = 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    return r

def historical_insert_db(df):
    dbname = db.get_database()
    collection_name = dbname["physique"]
    data = df.to_dict('records')
    last_doc = collection_name.find_one({})
    if last_doc is None:
        r = collection_name.insert_many(data)
    else:
        r = 'Impossible de push les données historiques quand il y a déjà des données en base.'
    return r

if __name__ == "__main__":
    #SETUP SELENIUM
    driver_path = 'C:/Users/alexl/Documents/GitHub/agriData/data/driver/chromedriver.exe'
    brave_path = 'C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe'
    option = Options()
    option.binary_location = brave_path
    option.add_argument("--headless=new")
    s = Service(driver_path)
    driver = webdriver.Chrome(service=s, options=option)

    #data = export_historical(driver)
    data = export_last(driver)
    driver.quit() 

    #r = historical_insert_db(data)
    r = insert_db(data)
    print(r)
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## EURONEXT DATA PHYSIQUE LOG #########**' + '\n' + r)
    response = webhook.execute()

      
    
