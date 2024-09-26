import pandas as pd
import requests, io
import os
from datetime import datetime, timedelta
import csv
import database as db
import config
import pymongo
import cot_reports as cot
from discord_webhook import DiscordWebhook

pd.options.mode.chained_assignment = None

#EURONEXT

def download_cot(date, type, year=None): #date is a datetime object #type : EBM : blé, EMA : mais, ECO : colza
    date = date.strftime('%Y-%m-%d')
    url = f'https://live.euronext.com/sites/default/files/commodities_reporting/{date.replace("-", "/")}/fr/cdwpr_{type}_{date.replace("-", "")}.csv'
    try:
        r = requests.get(url)
        r.raise_for_status()  # Raise an HTTPError for bad responses
        if not os.path.exists(f'reports/{type}'):
            os.makedirs(f'reports/{type}')
        if year is None:
            with open(f'reports/{type}/cot_{type.lower()}_{date}.csv', 'wb') as file:
                file.write(r.content)
            print(f"Download successful {date}")
        else:
            if not os.path.exists(f'reports/{type}/{str(year)}'):
                os.makedirs(f'reports/{type}/{str(year)}')
            with open(f'reports/{type}/{year}/cot_{type.lower()}_{date}.csv', 'wb') as file:
                file.write(r.content)
            print(f"Download successful {date}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")

def dowload_last_cot(type):
    data = []
    end = []
    last = (datetime.now().date() - timedelta(days=datetime.now().date().weekday())) + timedelta(days=2)
    download_cot(last, type, datetime.now().year)
    year = last.year
    last = last.strftime('%Y-%m-%d')
    with open(f"reports/{type}/{year}/cot_{type.lower()}_{last}.csv", 'r', encoding='utf-8') as file:
                csvreader = csv.reader(file, delimiter=';')
                for row in csvreader:
                    filter = [element for element in row if element != '']
                    data.append(filter)
                end.append(data)
                data = []
    return end

def insert_db(dict):
    dbname = db.get_database()
    collection_name = dbname["euronext_commodity"]
    if len(dict) == 3:
        last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
        if dict[0]['Date'] != last_doc['Date']:
            r = str(collection_name.insert_many(dict))
            r = ' Euronext COT : Document inséré ' + r
        else:
            r = 'Euronext COT : Document Euronext non inséré, doublon date avec le dernier document en base.'
    else:
        r = 'Euronext COT : Data non reconnue, non inséré en base.'
    return r

def dowload_historical_cot(start_date, type, end_date=datetime.now().date()): #start and end date dormat 'YYYY-MM-DD'
    data2 = []
    data3 = []
    if end_date <= start_date:
        raise ValueError("La date de fin ne peut pas être supérieur ou égale à la date de début.")

    mercredi = (start_date - timedelta(days=start_date.weekday())) + timedelta(days=2)
    n_semaine = (end_date - start_date).days // 7
    for i in range(n_semaine+1):
        year = mercredi.year
        download_cot(mercredi, type, year)
        mercredi = mercredi +timedelta(weeks=1)
    
    for i in os.listdir(f'reports/{type}'):
        for j in os.listdir(f'reports/{type}/{i}'):
            with open(f"./reports/{type}/{i}/{j}", 'r', encoding='utf-8') as file2:
                csvreader = csv.reader(file2, delimiter=';')
                for row in csvreader:
                    filter2 = [element for element in row if element != '']
                    data2.append(filter2)
                data3.append(data2)
                data2 = []
    return data3

def data_to_dict(data_array):

    dict_list = []
    
    for x in range(len(data_array)):
        date = data_array[x][2]
        produit = data_array[x][4]
        ticker = data_array[x][5]
        nb_position_hedge = data_array[x][9][3:]
        nb_position_autre = data_array[x][10][1:]
        nb_position_totale = data_array[x][11][1:]
        variations_hedge = data_array[x][12][2:]
        variations_autre = data_array[x][13][1:]
        variations_totale = data_array[x][14][1:]
        pourcentage_total_pos_hedge = data_array[x][15][2:]
        pourcentage_total_pos_autre = data_array[x][16][1:]
        pourcentage_total_pos_totale = data_array[x][17][1:]
        nb_acteurs = data_array[x][19][1:6]

        dict = {
            "Date" : date[0],
            "Produit" : produit[0],
            "Ticker" : ticker[0],
            "Entreprises d'investissement et établissements de crédit positions Long (Total)" : nb_position_totale[0],
            "Entreprises d'investissement et établissements de crédit positions Short (Total)" : nb_position_totale[1],
            "Entreprises d'investissement et établissements de crédit positions Long (Hedge)" : nb_position_hedge[0],
            "Entreprises d'investissement et établissements de crédit positions Short (Hedge)" : nb_position_hedge[1],
            "Entreprises d'investissement et établissements de crédit positions Long (Autre)" : nb_position_autre[0],
            "Entreprises d'investissement et établissements de crédit positions Short (Autre)" : nb_position_autre[1],
            
            "Entreprises d'investissement et établissements de crédit variation Long (Total)" : variations_totale[0],
            "Entreprises d'investissement et établissements de crédit variation Short (Total)" : variations_totale[1],
            "Entreprises d'investissement et établissements de crédit variation Long (Hedge)" : variations_hedge[0],
            "Entreprises d'investissement et établissements de crédit variation Short (Hedge)" : variations_hedge[1],
            "Entreprises d'investissement et établissements de crédit variation Long (Autre)" : variations_autre[0],
            "Entreprises d'investissement et établissements de crédit variation Short (Autre)" : variations_autre[1],
            
            "Entreprises d'investissement et établissements de crédit pourcentage position Long (Total)" : pourcentage_total_pos_totale[0],
            "Entreprises d'investissement et établissements de crédit pourcentage position Short (Total)" : pourcentage_total_pos_totale[1],
            "Entreprises d'investissement et établissements de crédit pourcentage position Long (Hedge)" : pourcentage_total_pos_hedge[0],
            "Entreprises d'investissement et établissements de crédit pourcentage position Short (Hedge)" : pourcentage_total_pos_hedge[1],
            "Entreprises d'investissement et établissements de crédit pourcentage position Long (Autre)" : pourcentage_total_pos_autre[0],
            "Entreprises d'investissement et établissements de crédit pourcentage position Short (Autre)" : pourcentage_total_pos_autre[1],
            
            "Entreprises d'investissement et établissements de crédit nombre acteurs" : nb_acteurs[0],
        
            
            "Fonds d'investissement positions Long (Total)" : nb_position_totale[2],
            "Fonds d'investissement positions Short (Total)" : nb_position_totale[3],
            "Fonds d'investissement positions Long (Hedge)" : nb_position_hedge[2],
            "Fonds d'investissement positions Short (Hedge)" : nb_position_hedge[3],
            "Fonds d'investissement positions Long (Autre)" : nb_position_autre[2],
            "Fonds d'investissement positions Short (Autre)" : nb_position_autre[3],
            
            "Fonds d'investissement variation Long (Total)" : variations_totale[2],
            "Fonds d'investissement variation Short (Total)" : variations_totale[3],
            "Fonds d'investissement variation Long (Hedge)" : variations_hedge[2],
            "Fonds d'investissement variation Short (Hedge)" : variations_hedge[3],
            "Fonds d'investissement variation Long (Autre)" : variations_autre[2],
            "Fonds d'investissement variation Short (Autre)" : variations_autre[3],
            
            "Fonds d'investissement pourcentage position Long (Total)" : pourcentage_total_pos_totale[2],
            "Fonds d'investissement pourcentage position Short (Total)" : pourcentage_total_pos_totale[3],
            "Fonds d'investissement pourcentage position Long (Hedge)" : pourcentage_total_pos_hedge[2],
            "Fonds d'investissement pourcentage position Short (Hedge)" : pourcentage_total_pos_hedge[3],
            "Fonds d'investissement pourcentage position Long (Autre)" : pourcentage_total_pos_autre[2],
            "Fonds d'investissement pourcentage position Short (Autre)" : pourcentage_total_pos_autre[3],
            
            "Fonds d'investissement nombre acteurs" : nb_acteurs[1],
        
            
            "Autres institutions financières positions Long (Total)" : nb_position_totale[4],
            "Autres institutions financières positions Short (Total)" : nb_position_totale[5],
            "Autres institutions financières positions Long (Hedge)" : nb_position_hedge[4],
            "Autres institutions financières positions Short (Hedge)" : nb_position_hedge[5],
            "Autres institutions financières positions Long (Autre)" : nb_position_hedge[4],
            "Autres institutions financières positions Short (Autre)" : nb_position_hedge[5],
            
            "Autres institutions financières variation Long (Total)" : variations_totale[4],
            "Autres institutions financières variation Short (Total)" : variations_totale[5],
            "Autres institutions financières variation Long (Hedge)" : variations_hedge[4],
            "Autres institutions financières variation Short (Hedge)" : variations_hedge[5],
            "Autres institutions financières variation Long (Autre)" : variations_autre[4],
            "Autres institutions financières variation Short (Autre)" : variations_autre[5],
        
            "Autres institutions financières pourcentage position Long (Total)" : pourcentage_total_pos_totale[4],
            "Autres institutions financières pourcentage position Short (Total)" : pourcentage_total_pos_totale[5],
            "Autres institutions financières pourcentage position Long (Hedge)" : pourcentage_total_pos_hedge[4],
            "Autres institutions financières pourcentage position Short (Hedge)" : pourcentage_total_pos_totale[5],
            "Autres institutions financières pourcentage position Long (Autre)" : pourcentage_total_pos_totale[4],
            "Autres institutions financières pourcentage position Short (Autre)" : pourcentage_total_pos_totale[5],
        
            "Autres institutions financières nombre acteurs" : nb_acteurs[2],
        
        
            "Entreprises commerciales positions Long (Total)" : nb_position_totale[6],
            "Entreprises commerciales positions Short (Total)" : nb_position_totale[7],
            "Entreprises commerciales positions Long (Hedge)" : nb_position_hedge[6],
            "Entreprises commerciales positions Short (Hedge)" : nb_position_hedge[7],
            "Entreprises commerciales positions Long (Autre)" : nb_position_autre[6],
            "Entreprises commerciales positions Short (Autre)" : nb_position_autre[7],
        
            "Entreprises commerciales variation Long (Totale)" : variations_totale[6],
            "Entreprises commerciales variation Short (Totale)" : variations_totale[7],
            "Entreprises commerciales variation Long (Hedge)" : variations_hedge[6],
            "Entreprises commerciales variation Short (Hedge)" : variations_hedge[7],
            "Entreprises commerciales variation Long (Autre)" : variations_autre[6],
            "Entreprises commerciales variation Short (Autre)" : variations_autre[7],
        
            "Entreprises commerciales pourcentage position Long (Total)" : pourcentage_total_pos_totale[6],
            "Entreprises commerciales pourcentage position Short (Total)" : pourcentage_total_pos_totale[7],
            "Entreprises commerciales pourcentage position Long (Hedge)" : pourcentage_total_pos_hedge[6],
            "Entreprises commerciales pourcentage position Short (Hedge)" : pourcentage_total_pos_hedge[7],
            "Entreprises commerciales pourcentage position Long (Autre)" : pourcentage_total_pos_autre[6],
            "Entreprises commerciales pourcentage position Short (Autre)" : pourcentage_total_pos_autre[7],
        
            "Entreprises commerciales nombre acteurs" : nb_acteurs[3],
        
        
            "Exploitants soumis à des obligations de conformité positions Long (Total)" : nb_position_totale[8],
            "Exploitants soumis à des obligations de conformité positions Short (Total)" : nb_position_totale[9],
            "Exploitants soumis à des obligations de conformité positions Long (Hedge)" : nb_position_hedge[8],
            "Exploitants soumis à des obligations de conformité positions Short (Hedge)" : nb_position_hedge[9],
            "Exploitants soumis à des obligations de conformité positions Long (Autre)" : nb_position_autre[8],
            "Exploitants soumis à des obligations de conformité positions Short (Autre)" : nb_position_autre[9],
        
            "Exploitants soumis à des obligations de conformité variation Long (Totale)" : variations_totale[8],
            "Exploitants soumis à des obligations de conformité variation Short (Totale)" : variations_totale[9],
            "Exploitants soumis à des obligations de conformité variation Long (Hedge)" : variations_hedge[8],
            "Exploitants soumis à des obligations de conformité variation Short (Hedge)" : variations_hedge[9],
            "Exploitants soumis à des obligations de conformité variation Long (Autre)" : variations_autre[8],
            "Exploitants soumis à des obligations de conformité variation Short (Autre)" : variations_autre[9],
        
            "Exploitants soumis à des obligations de conformité pourcentage position Long (Totale)" : pourcentage_total_pos_totale[8],
            "Exploitants soumis à des obligations de conformité pourcentage position Short (Totale)" : pourcentage_total_pos_totale[9],
            "Exploitants soumis à des obligations de conformité pourcentage position Long (Hedge)" : pourcentage_total_pos_hedge[8],
            "Exploitants soumis à des obligations de conformité pourcentage position Short (Hedge)" : pourcentage_total_pos_hedge[9],
            "Exploitants soumis à des obligations de conformité pourcentage position Long (Autre)" : pourcentage_total_pos_autre[8],
            "Exploitants soumis à des obligations de conformité pourcentage position Short (Autre)" : pourcentage_total_pos_autre[9],
        
            "Exploitants soumis à des obligations de conformité nombre acteurs" : nb_acteurs[4]  
        }
        dict_list.append(dict)
    return dict_list

def historical_push_db(type):
    data2 = []
    data3 = []
    for i in os.listdir(f'reports/{type}'):
        for j in os.listdir(f'reports/{type}/{i}'):
            with open(f"./reports/{type}/{i}/{j}", 'r', encoding='utf-8') as file2:
                csvreader = csv.reader(file2, delimiter=';')
                for row in csvreader:
                    filter2 = [element for element in row if element != '']
                    data2.append(filter2)
                data3.append(data2)
                data2 = []
    dict = data_to_dict(data3)
    insert = insert_db(dict)
    print(insert)

#CFTC
    
def format_cftc_data(df):
    wanted_tickers = ['WHEAT - CHICAGO BOARD OF TRADE ', 'WHEAT-SRW - CHICAGO BOARD OF TRADE ', 'WHEAT-SRW - CHICAGO BOARD OF TRADE', 'WHEAT-HRW - CHICAGO BOARD OF TRADE ', 
                      'WHEAT-HRW - CHICAGO BOARD OF TRADE', 'WHEAT - MINNEAPOLIS GRAIN EXCHANGE ', 'WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE ', 'WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE', 
                      'CORN - CHICAGO BOARD OF TRADE', 'CORN - CHICAGO BOARD OF TRADE ', 'SOYBEANS - CHICAGO BOARD OF TRADE ', 'SOYBEANS - CHICAGO BOARD OF TRADE', 
                      'SOYBEAN OIL - CHICAGO BOARD OF TRADE ', 'SOYBEAN OIL - CHICAGO BOARD OF TRADE', 'SOYBEAN MEAL - CHICAGO BOARD OF TRADE ', 'SOYBEAN MEAL - CHICAGO BOARD OF TRADE']
    df = df[df['Market_and_Exchange_Names'].isin(wanted_tickers)]

    df.loc[df['Market_and_Exchange_Names'] == 'WHEAT - CHICAGO BOARD OF TRADE ', 'Market_and_Exchange_Names'] = 'WHEAT-SRW - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'WHEAT-SRW - CHICAGO BOARD OF TRADE ', 'Market_and_Exchange_Names'] = 'WHEAT-SRW - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'WHEAT-HRW - CHICAGO BOARD OF TRADE ', 'Market_and_Exchange_Names'] = 'WHEAT-HRW - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'WHEAT - MINNEAPOLIS GRAIN EXCHANGE ', 'Market_and_Exchange_Names'] = 'WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE'
    df.loc[df['Market_and_Exchange_Names'] == 'WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE ', 'Market_and_Exchange_Names'] = 'WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE'
    df.loc[df['Market_and_Exchange_Names'] == 'CORN - CHICAGO BOARD OF TRADE ', 'Market_and_Exchange_Names'] = 'CORN - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'SOYBEANS - CHICAGO BOARD OF TRADE  ', 'Market_and_Exchange_Names'] = 'SOYBEANS - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'SOYBEAN OIL - CHICAGO BOARD OF TRADE   ', 'Market_and_Exchange_Names'] = 'SOYBEAN OIL - CHICAGO BOARD OF TRADE'
    df.loc[df['Market_and_Exchange_Names'] == 'SOYBEAN MEAL - CHICAGO BOARD OF TRADE  ', 'Market_and_Exchange_Names'] = 'SOYBEAN MEAL - CHICAGO BOARD OF TRADE'

    df['As_of_Date_In_Form_YYMMDD'] = pd.to_datetime(df['As_of_Date_In_Form_YYMMDD'], format='%y%m%d')
    df = df.rename(columns={'As_of_Date_In_Form_YYMMDD' : 'Date'})
    df = df.set_index('Date')
    df = df.sort_index()

    return df
    
def get_historical_cot_cftc(startDate):
    data = []
    debutYear = startDate
    endYear = datetime.today().year
    for i in range(debutYear, endYear + 1):
        df = cot.cot_year(year = i, cot_report_type='disaggregated_futopt')
        data.append(df)
    end = pd.concat(data, ignore_index=True)
    end = format_cftc_data(end)
    return end
    
def get_cot_cftc():
    df = cot.cot_year(year = datetime.today().year , cot_report_type= 'disaggregated_futopt') 
    os.remove('c_year.txt')
    return df

def get_last_cot_cftc():
    df = get_cot_cftc()
    df = format_cftc_data(df)
    df = df.tail(7)
    return df

def historical_push_db_cftc(df):

    dbname = db.get_database()
    collection_name = dbname["us_commodity"]
    if collection_name.find_one({}) == None:
        df.reset_index(inplace=True)
        data_dict = df.to_dict('records')
        r = collection_name.insert_many(data_dict)
    else:
        r = 'Document non inséré, push historique déjà effectué.'
    return r

def insert_db_cftc(df):
    dbname = db.get_database()
    collection_name = dbname["us_commodity"]
    df.reset_index(inplace=True)
    data_dict = df.to_dict('records')
    last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )

    if df['Date'][0] != last_doc['Date']:
        r = str(collection_name.insert_many(data_dict))
        r = 'CFTC COT inséré : Document inséré ' + r
    else:
        r = 'CFTC COT : Document CFTC non inséré, doublon date avec le dernier document en base.'
    return r

if __name__ == "__main__":
    #EURONEXT
    
    #date = datetime.strptime('2018-01-01', "%Y-%m-%d").date()
    #data = dowload_historical_cot(date, 'ECO')
    
    dataBle = dowload_last_cot('EBM')
    dataMais = dowload_last_cot('EMA')
    dataColza = dowload_last_cot('ECO')
    dictData = data_to_dict(dataBle)
    dictData.append(data_to_dict(dataMais)[0])
    dictData.append(data_to_dict(dataColza)[0])

    insData = insert_db(dictData)
    print(insData)

    #CFTC

    df = get_last_cot_cftc()
    r = insert_db_cftc(df)
    print(r)
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## AGRI COT DATA LOG #########**' + '\n' + insData + '\n' + r)
    response = webhook.execute()