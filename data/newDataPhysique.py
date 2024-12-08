import pandas as pd
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
from datetime import datetime
import pymongo
import database as db
from discord_webhook import DiscordWebhook
import config

def scrapper(productsList):
      dictList = [] #create empty list for stockage
      driver = Driver(uc=True, incognito=True, headless=True) #set driver
      for product in productsList: #iterates through defined products
            cleanProduct = product.lower().replace(' ', '-')
            url = f'https://www.terre-net.fr/marche-agricole/{cleanProduct}/physique' #set url 
            driver.get(url)
            driver.wait_for_element("#place-graph-selector") #wait for select element to be present on page
            options = driver.find_elements(By.XPATH, "//select[@id='place-graph-selector']/option") #find every place options for product
            chartsCount = 0 #compteur set to 0
            for option in options: #for every options we found : 
                        select = Select(driver.find_element(By.XPATH, "//select[@id='place-graph-selector']")) #Select the select element to change it
                        select.select_by_visible_text(option.text) #select the element by text
                        time.sleep(3) #small sleep for setting of the Highchart object
                        dates = driver.execute_script(f'return Highcharts.charts[{chartsCount}].series[0].data.map(point => point.x)') #get x values from highchart object (dates)
                        values = driver.execute_script(f'return Highcharts.charts[{chartsCount}].series[0].data.map(point => point.y)') #get y values from highchart object (price)
                        dictList.append({ 
                              'Date': [date for date in dates if date is not None],
                              'Produit': product,
                              'Place': option.text,
                              'Prix': [value for value in values if value is not None]
                        }) #append a dict to the list 
                        chartsCount += 1 #increment compteur
      driver.quit()
      return dictList

def insert_db(df):
    r = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name = dbname["new_physique"]
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

if __name__ == "__main__":
    products = ['Mais', 'Ble tendre', 'Ble dur', 'Colza'] #product to scrap
    data = scrapper(products)
    concatedList = []
    for item in data: #make a df for each scrapped items
        tmp = pd.DataFrame(item)
        tmp['Date'] = pd.to_datetime(tmp['Date'], unit='ms').dt.floor('D') #remove the hours from date
        concatedList.append(tmp)
    df = pd.concat(concatedList).reset_index(drop=True) #concat all the dfs
    df = df[df['Date'] == datetime.today().strftime('%Y-%m-%d')].reset_index(drop=True) #keep the data of today

    r = insert_db(df) #push todays data to db
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## NEW PHYSIQUE DATA #########**' + '\n' + r)
    response = webhook.execute()