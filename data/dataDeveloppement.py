import pandas as pd
import requests
import pymongo
import pymongo
import database as db

def find_report():
    urlMais = 'https://visionet.franceagrimer.fr/Pages/OpenDocument.aspx?fileurl=SeriesChronologiques%2fproductions%20vegetales%2fgrandes%20cultures%2fetats%20des%20cultures%2fSCR-GRC-CEREOBS_M_depuis_2015-A24.xlsx&telechargersanscomptage=oui'
    urlCereales = 'https://visionet.franceagrimer.fr/Pages/OpenDocument.aspx?fileurl=SeriesChronologiques%2fproductions%20vegetales%2fgrandes%20cultures%2fetats%20des%20cultures%2fSCR-GRC-CEREOBS_CP_depuis_2015-A24.xlsx&telechargersanscomptage=oui'
    try:
        r = requests.get(urlMais)
        r.raise_for_status()
        with open("data/mais.xlsx", "wb") as f:
            f.write(r.content)
        print("File maïs downloaded successfully!")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        r = requests.get(urlCereales)
        r.raise_for_status()
        with open("data/cereales.xlsx", "wb") as f:
            f.write(r.content)
        print("File céréales downloaded successfully!")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def monday_of_week(year, week):
    first_day_of_year = pd.to_datetime(f'{year}-01-01')
    first_monday = first_day_of_year - pd.Timedelta(days=first_day_of_year.dayofweek)
    monday_of_given_week = first_monday + pd.Timedelta(weeks=week-1)
    
    return monday_of_given_week

def prep_dataframe():
    find_report()
    dfMaisRegion = pd.read_excel('data/mais.xlsx', sheet_name='Données régions', skiprows=5)
    dfMaisRegion = dfMaisRegion.drop('Unnamed: 0', axis=1)
    dfMaisRegion['Week'] = dfMaisRegion['Semaine'].str.extract(r'S(\d+)')[0].astype(int)
    dfMaisRegion['Year'] = dfMaisRegion['Semaine'].str.extract(r'(\d+)-S\d+')[0].astype(int)
    dfMaisRegion['Date'] = dfMaisRegion.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)

    dfBleRegion = pd.read_excel('data/cereales.xlsx', sheet_name='Données régions', skiprows=5)
    dfBleRegion = dfBleRegion[(dfBleRegion['Culture'] == 'Blé dur') | (dfBleRegion['Culture'] == 'Blé tendre')]
    dfBleRegion = dfBleRegion.drop('Unnamed: 0', axis=1)
    dfBleRegion['Week'] = dfBleRegion['Semaine'].str.extract(r'S(\d+)')[0].astype(int)
    dfBleRegion['Year'] = dfBleRegion['Semaine'].str.extract(r'(\d+)-S\d+')[0].astype(int)
    dfBleRegion['Date'] = dfBleRegion.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)
    dfDataRegion = pd.concat([dfMaisRegion, dfBleRegion])

    dfMaisFrance = pd.read_excel('data/mais.xlsx', sheet_name='Données France', skiprows=5)
    dfMaisFrance = dfMaisFrance.drop('Unnamed: 0', axis=1)
    dfMaisFrance['Week'] = dfMaisFrance['Semaine'].str.extract(r'S(\d+)')[0].astype(int)
    dfMaisFrance['Year'] = dfMaisFrance['Semaine'].str.extract(r'(\d+)-S\d+')[0].astype(int)
    dfMaisFrance['Date'] = dfMaisFrance.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)

    dfBleFrance = pd.read_excel('data/cereales.xlsx', sheet_name='Données France', skiprows=5)
    dfBleFrance = dfBleFrance[(dfBleFrance['Culture'] == 'Blé dur') | (dfBleFrance['Culture'] == 'Blé tendre')]
    dfBleFrance = dfBleFrance.drop('Unnamed: 0', axis=1)
    dfBleFrance['Week'] = dfBleFrance['Semaine'].str.extract(r'S(\d+)')[0].astype(int)
    dfBleFrance['Year'] = dfBleFrance['Semaine'].str.extract(r'(\d+)-S\d+')[0].astype(int)
    dfBleFrance['Date'] = dfBleFrance.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)
    dfDataFrance = pd.concat([dfMaisFrance, dfBleFrance])
    
    dbname = db.get_database()
    collection_name_france = dbname["dev_cond_france"]
    collection_name_region = dbname["dev_cond_region"]
    last_doc_france = collection_name_france.find_one(
        sort=[( 'Date', pymongo.DESCENDING )]
    )
    last_doc_region = collection_name_region.find_one(
        sort=[( 'Date', pymongo.DESCENDING )]
    )

    #lastDateRegion = dfDataRegion['Date'].iloc[-1]
    #lastDateFrance = dfDataFrance['Date'].iloc[-1]
    return dfDataRegion[dfDataRegion['Date'] > last_doc_region['Date']], dfDataFrance[dfDataFrance['Date'] > last_doc_france['Date']]

def insert_db(dfFrance, dfRegion):
    rFrance = 'Nothing in the database, add historical data first'
    rRegion = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name_france = dbname["dev_cond_france"]
    collection_name_region = dbname["dev_cond_region"]
    dataFrance = dfFrance.to_dict('records')
    dataRegion= dfRegion.to_dict('records')
    last_doc_france = collection_name_france.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc_france is not None:
            if not dfFrance.empty:
                if dfFrance['Date'].iloc[0] != last_doc_france['Date']:
                    rFrance = str(collection_name_france.insert_many(dataFrance))
                    rFrance = 'Développement des cultures France : ' + rFrance
                else:
                    rFrance = 'Moyenne France : Document non inséré, doublon date avec le dernier document en base.'
            else:
                rFrance = 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    last_doc_region = collection_name_region.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc_region is not None:
            if not dfRegion.empty:
                if dfRegion['Date'].iloc[0] != last_doc_region['Date']:
                    rRegion = str(collection_name_region.insert_many(dataRegion))
                    rRegion = 'Développement des cultures Region : ' + rRegion
                else:
                    rRegion = 'Region : Document non inséré, doublon date avec le dernier document en base.'
            else:
                rRegion = 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    return rFrance, rRegion

dfRegion, dfFrance = prep_dataframe()
rFrance, rRegion = insert_db(dfFrance, dfRegion)
print(rFrance, rRegion)