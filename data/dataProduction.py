import pandas as pd
import requests
import zipfile
from datetime import datetime
import database as db
import pymongo
import openpyxl.reader.excel

def find_report():
    url = 'https://visionet.franceagrimer.fr/Pages/OpenDocument.aspx?fileurl=SeriesChronologiques%2fproductions%20vegetales%2fgrandes%20cultures%2fcollecte%2cstocks%2cd%c3%a9p%c3%b4ts%2fSCR-GRC-histNAT_collecte_stock_depuis_2000-C24.zip&telechargersanscomptage=oui'
    try:
        r = requests.get(url)
        r.raise_for_status()
        with open("report.zip", "wb") as f:
            f.write(r.content)
        print("File downloaded successfully!")

        with zipfile.ZipFile('report.zip', 'r') as zip_ref:
            zip_ref.extractall('prod_report')
        print(f"Files extracted")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid ZIP file.")
    except Exception as e:
        print(f"An error occurred: {e}")

def prep_dataframe():
    find_report()
    openpyxl.reader.excel.ARC_CORE = "random letters to avoid error"
    dfBle = pd.read_excel('prod_report/SCR-GRC-histNAT_collecte_stock_depuis_2000-C24.xlsx', sheet_name='Blé tendre', skiprows=4)
    dfColza = pd.read_excel('prod_report/SCR-GRC-histNAT_collecte_stock_depuis_2000-C24.xlsx', sheet_name='Colza', skiprows=4)
    dfMais = pd.read_excel('prod_report/SCR-GRC-histNAT_collecte_stock_depuis_2000-C24.xlsx', sheet_name='Maïs', skiprows=4)
    dfBle = dfBle[:-11]
    dfMais = dfMais[:-11]
    dfColza = dfColza[:-11]

    df = pd.concat([dfBle, dfMais, dfColza])
    df.reset_index(drop=True, inplace=True)
    df.rename(columns=lambda x: x.replace('\n', '_'), inplace=True)
    df.drop(columns=['STOCKS_DEPOTS.1'], inplace=True)
    df['day'] = 1
    df['year'] = df['ANNEE'].astype(int)
    df['month'] = df['MOIS'].astype(int)
    df['Date'] = pd.to_datetime(df[['year', 'month', 'day']], format='%Y %m %d')
    return df[['Date', 'CAMPAGNE', 'ESPECES', 'GRAINS_CONSOMMATION', 'COLLECTE_SEMENCES', 'TOTAL_COLLECTE', 'STOCKS_CONSOMMATION', 'STOCKS_SEMENCES', 'TOTAL_STOCKS', 'STOCKS_DEPOTS', 'ENTREES_DEPOTS', 'REPRISES_DEPOTS']]

def insert_db(df):
    r = ''
    dbname = db.get_database()
    collection_name = dbname["production"]
    data = df.to_dict('records')
    last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    
    if df['Date'].iloc[0] != last_doc['Date']:
        collection_name.update_many({}, {'$set': {'Expired': True}})
        r = r + str(collection_name.insert_many(data))
        r = 'France Agri Mer Production Data : ' + r
    else:
        r = 'France Agri Mer Production Data : Document non inséré, doublon date avec le dernier document en base.'
    return r

df = prep_dataframe()
data = pd.concat([df[df['ESPECES'] == 'Blé tendre'].tail(4), df[df['ESPECES'] == 'Maïs'].tail(4), df[df['ESPECES'] == 'Colza'].tail(4)])
r = insert_db(data)
print(r)