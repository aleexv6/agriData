import pandas as pd
import requests
import io
import re
import pymongo
import database as db
from discord_webhook import DiscordWebhook
import config

WEEK = [] 
with open('week.txt', 'r') as file:
    SEMAINE = int(file.read().strip()) #Keep track of the week in a separate txt file

def cereobsReport(type, semaine, id_culture, stade_dev=None):
    if type == 'Condition':
        url = f"https://cereobs.franceagrimer.fr/cereobs-sp/api/public/publications/rapportCereobs?semaineObservation={semaine}&idCulture={id_culture}&typePublication=5"
    elif type == 'Developpement':
        url = f"https://cereobs.franceagrimer.fr/cereobs-sp/api/public/publications/rapportCereobs?semaineObservation={semaine}&idCulture={id_culture}&idStadeDev={stade_dev}&typePublication=3"
    else:
        print('Wrong type')
        return 0
    r = requests.get(url)
    content_type = r.headers.get('Content-Type')
    content_disposition = r.headers.get('Content-Disposition')
    if content_disposition:
        match = re.search(r'\d{4}-S\d{2}', content_disposition) 
        if match:
            WEEK.append(match.group())
        else:
            print('Something went wrong with Year and Week in file extraction')
            return 0            
    if content_type == 'application/vnd.ms-excel':
        if type == 'Developpement': 
            df = pd.read_excel(io.BytesIO(r.content), skiprows=3)[:-4]
            df = df.rename(columns={df.columns[0]: 'Region', df.columns[1]: 'Actual'})
            df = df[['Region', 'Actual']]
        elif type == 'Condition':
            df = pd.read_excel(io.BytesIO(r.content), skiprows=3)[:-8]
            df = df.rename(columns={df.columns[0]: 'Region', df.columns[1]: 'Très mauvaises', df.columns[2]: 'Mauvaises', df.columns[3]: 'Assez bonnes', df.columns[4]: 'Bonnes', df.columns[5]: 'Très bonnes'})
            df = df[['Region', 'Très mauvaises', 'Mauvaises', 'Assez bonnes', 'Bonnes', 'Très bonnes']]
    else:
        df = pd.DataFrame()
    return df

def monday_of_week(year, week): # find the monday of the year and week number
    first_day_of_year = pd.to_datetime(f'{year}-01-01')
    first_monday = first_day_of_year - pd.Timedelta(days=first_day_of_year.dayofweek)
    monday_of_given_week = first_monday + pd.Timedelta(weeks=week-1)
    return monday_of_given_week

def insert_db(dfFrance):
    rFrance = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name_france = dbname["dev_cond"]
    dataFrance = dfFrance.to_dict('records')
    last_doc_france = collection_name_france.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc_france is not None:
            if not dfFrance.empty:
                if dfFrance['Date'].iloc[0] != last_doc_france['Date']:
                    rFrance = str(collection_name_france.insert_many(dataFrance))
                    rFrance = 'Développement des cultures France : ' + rFrance
                    with open('week.txt', 'w') as file:
                        file.write(str(SEMAINE+1))
                else:
                    rFrance = 'DevCond : Document non inséré, doublon date avec le dernier document en base.'
            else:
                rFrance = 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    return rFrance

if __name__ == "__main__":
    #Map the data we found to be indexes on the URL parameters
    cultureMap = {
    2: 'Blé tendre',
    3: 'Blé dur',
    5: 'Maïs grain'
    }
    bleMap = {
        1: 'Semis',
        2: 'Levée',
        3: 'Début tallage',
        4: 'Épi 1cm',
        5: 'Deux noeuds',
        6: 'Épiaison', 
        7: 'Récolte'
    }
    maisMap = {
        8: 'Semis',
        9: 'Levée',
        10: '6/8 feuilles visibles',
        11: 'Floraison femelle',
        12: 'Humidité du grain 50%',
        13: 'Récolte'
    }
    
    #Creating a list of dict with found data
    devMap = []
    for idx, da in cultureMap.items():
        if da.startswith('Blé'):
            for i, d in bleMap.items(): #checks for every developpement state
                tmp = cereobsReport('Developpement', SEMAINE, idx, i)
                if not tmp.empty:
                    tmp = tmp.rename(columns={'Actual': d})
                    tmp['Culture'] = da
                    data = tmp
                else:
                    data = pd.DataFrame([{'Region': 'Auvergne-Rhône-Alpes', d: None, 'Culture': da}, {'Region': 'Bourgogne-Franche-Comté', d: None, 'Culture': da}, {'Region': 'Bretagne', d: None, 'Culture': da}, {'Region': 'Centre-Val de Loire', d: None, 'Culture': da}, {'Region': 'Grand-Est', d: None, 'Culture': da}, {'Region': 'Hauts-de-France', d: None, 'Culture': da}, {'Region': 'Ile-de-France', d: None, 'Culture': da}, {'Region': 'Normandie', d: None, 'Culture': da}, {'Region': 'Nouvelle-Aquitaine', d: None, 'Culture': da}, {'Region': 'Occitanie', d: None, 'Culture': da}, {'Region': 'Pays-de-la-Loire', d: None, 'Culture': da}, {'Region': "Provence-Alpes-Côte d'Azur", d: None, 'Culture': da}, {'Region': 'Moyenne France', d: None, 'Culture': da}])
                devMap.append(data)
        else: # do the same for maïs
            for i, d in maisMap.items():
                tmp = cereobsReport('Developpement', SEMAINE, idx, i)
                if not tmp.empty:
                    tmp = tmp.rename(columns={'Actual': d})
                    tmp['Culture'] = da
                    data = tmp
                else:
                    data = pd.DataFrame([{'Region': 'Auvergne-Rhône-Alpes', d: None, 'Culture': da}, {'Region': 'Bourgogne-Franche-Comté', d: None, 'Culture': da}, {'Region': 'Bretagne', d: None, 'Culture': da}, {'Region': 'Centre-Val de Loire', d: None, 'Culture': da}, {'Region': 'Grand-Est', d: None, 'Culture': da}, {'Region': 'Hauts-de-France', d: None, 'Culture': da}, {'Region': 'Ile-de-France', d: None, 'Culture': da}, {'Region': 'Normandie', d: None, 'Culture': da}, {'Region': 'Nouvelle-Aquitaine', d: None, 'Culture': da}, {'Region': 'Occitanie', d: None, 'Culture': da}, {'Region': 'Pays-de-la-Loire', d: None, 'Culture': da}, {'Region': "Provence-Alpes-Côte d'Azur", d: None, 'Culture': da}, {'Region': 'Moyenne France', d: None, 'Culture': da}])
                devMap.append(data)

    #Same for conditions
    condMap = []
    regions = ["Auvergne-Rhône-Alpes", "Provence-Alpes-Côte d'Azur", "Grand-Est", "Centre-Val de Loire", "Nouvelle-Aquitaine", "Occitanie", "Bretagne", "Hauts-de-France", "Bourgogne-Franche-Comté", "Ile-de-France", "Normandie", "Pays-de-la-Loire", "Moyenne France (1)"]
    for idx, da in cultureMap.items():
        tmp = cereobsReport('Condition', SEMAINE, idx)
        if not tmp.empty:
            tmp['Culture'] = da
            data = tmp.to_dict(orient='records')
        else:
            data = []
            for region in regions:
                dataDict = {'Region': region,'Culture': da, 'Très mauvaises': None, 'Mauvaises': None, 'Assez bonnes': None, 'Bonnes': None, 'Très bonnes': None}
                data.append(dataDict)
        condMap.append(data)

    #set a dev dataframe for manipulation
    concated = pd.concat(devMap).reset_index(drop=True)
    group = concated.groupby(['Region', 'Culture']).max()
    dfDev = group.reset_index()
    #same for conditions
    dfCond = pd.DataFrame([item for dataset in condMap for item in dataset])
    dfCond['Region'] = dfCond['Region'].str.replace('Moyenne France (1)', 'Moyenne France')

    #merge both dfs
    dfDevCond = pd.merge(dfCond, dfDev, on=['Region', 'Culture'], how='outer')

    #add data and reformat df to be the same form as historical data and app in the database
    if len(list(set(WEEK))) == 1:
        dfDevCond['Semaine'] = WEEK[0]
    else:
        print('Non-unique weeks in data, check download')
    dfDevCond['Year'] = int(WEEK[0].split('-')[0])
    dfDevCond['Week'] = int(WEEK[0].split('-')[1][1:])
    dfDevCond['Date'] = dfDevCond.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)
    dfDevCond = dfDevCond.rename(columns={'Épi 1cm': 'Épi 1 cm', 'Deux noeuds': '2 noeuds', 'Region': 'Région'})
    dfDevCond = dfDevCond[['Culture', 'Région', 'Semaine', 'Semis', 'Levée', '6/8 feuilles visibles', 'Floraison femelle', 'Humidité du grain 50%', 'Récolte', 'Très mauvaises', 'Mauvaises', 'Assez bonnes', 'Bonnes', 'Très bonnes', 'Week', 'Year', 'Date', 'Début tallage', 'Épi 1 cm', '2 noeuds', 'Épiaison']]
    rFrance = insert_db(dfDevCond) #insert to db
    #Logs into my Discord server to be keep track of bugs 
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## CEREOBS WEEKLY DEV/COND DATA #########**' + '\n' + rFrance)
    response = webhook.execute()