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

def cereobsReport(type, semaine, id_culture, stade_dev=None): #download data from url
    if type == 'Condition':
        url = f"https://cereobs.franceagrimer.fr/cereobs-sp/api/public/publications/rapportCereobs?semaineObservation={semaine}&idCulture={id_culture}&typePublication=5"
    elif type == 'Developpement':
        url = f"https://cereobs.franceagrimer.fr/cereobs-sp/api/public/publications/rapportCereobs?semaineObservation={semaine}&idCulture={id_culture}&idStadeDev={stade_dev}&typePublication=3"
    else:
        print('Wrong type')
        return 0
    r = requests.get(url)
    content_type = r.headers.get('Content-Type') # check if json or excel, if json, no data
    content_disposition = r.headers.get('Content-Disposition') #check for file name wich contains the week string we are looking for
    if content_disposition:
        match = re.search(r'\d{4}-S\d{2}', content_disposition) #get the week in filename if it exists
        if match:
            WEEK.append(match.group()) #append to week for usage 
        else:
            print('Something went wrong with Year and Week in file extraction')
            return 0            
    if content_type == 'application/vnd.ms-excel':
        df = pd.read_excel(io.BytesIO(r.content))
        if type == 'Developpement': 
            df = df.rename(columns={df.columns[0]: 'Region', df.columns[1]: 'Actual'})
        elif type == 'Condition':
            df = df.rename(columns={df.columns[0]: 'Region', df.columns[1]: 'Très mauvaises', df.columns[2]: 'Mauvaises', df.columns[3]: 'Assez bonnes', df.columns[4]: 'Bonnes', df.columns[5]: 'Très bonnes'})
            df = df[['Region', 'Très mauvaises', 'Mauvaises', 'Assez bonnes', 'Bonnes', 'Très bonnes']]
    else:
        df = pd.DataFrame() #if no data, return an empty df
    return df

def monday_of_week(year, week): # find the monday of the year and week number
    first_day_of_year = pd.to_datetime(f'{year}-01-01')
    first_monday = first_day_of_year - pd.Timedelta(days=first_day_of_year.dayofweek)
    monday_of_given_week = first_monday + pd.Timedelta(weeks=week-1)
    return monday_of_given_week

def insert_db(dfFrance): # insert data to database
    rFrance = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name_france = dbname["dev_cond_france"]
    dataFrance = dfFrance.to_dict('records')
    last_doc_france = collection_name_france.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    if last_doc_france is not None: #chech if there are data already if not we need to insert historical
            if not dfFrance.empty:
                if dfFrance['Date'].iloc[0] != last_doc_france['Date']:
                    rFrance = str(collection_name_france.insert_many(dataFrance))
                    rFrance = 'Développement des cultures France : ' + rFrance
                    with open('week.txt', 'w') as file: #after appending to database, we increment the week number in txt file for next week
                        file.write(str(SEMAINE+1))
                else:
                    rFrance = 'Moyenne France : Document non inséré, doublon date avec le dernier document en base.'
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
        tmpMap = {}
        tmpMap['Culture'] = da
        if da.startswith('Blé'):
            for i, d in bleMap.items(): #checks for evry state of dev, if empty, no data, else we append the dict
                tmp = cereobsReport('Developpement', SEMAINE, idx, i)
                if not tmp.empty:
                    data = tmp[tmp['Region'] == 'Moyenne France']['Actual'].item()
                    tmpMap[d] = data
                else:
                    tmpMap[d] = None
        #We do the same for maïs
        else:
            for i, d in maisMap.items():
                tmp = cereobsReport('Developpement', SEMAINE, idx, i)
                if not tmp.empty:
                    data = tmp[tmp['Region'] == 'Moyenne France']['Actual'].item()
                    tmpMap[d] = data
                else:
                    tmpMap[d] = None
        devMap.append(tmpMap)

    #Same for conditions
    condMap = []
    for idx, da in cultureMap.items():
        tmpMap = {}
        tmpMap['Culture'] = da
        tmp = cereobsReport('Condition', SEMAINE, idx)
        if not tmp.empty:
            data = tmp[tmp['Region'] == 'Moyenne France (1)'][['Très mauvaises', 'Mauvaises', 'Assez bonnes', 'Bonnes', 'Très bonnes']].to_dict(orient='records')[0]
        else:
            data = {'Très mauvaises': None, 'Mauvaises': None, 'Assez bonnes': None, 'Bonnes': None, 'Très bonnes': None}
        condMap.append(tmpMap | data) #Logical or between dicts

    #set dicts to df to facilitate the merge
    devDf = pd.DataFrame(devMap)
    condDf = pd.DataFrame(condMap)
    df = pd.merge(devDf, condDf, on=['Culture'], how='inner') #merge data
    if len(list(set(WEEK))) == 1: #We check if we downloaded data for one week ONLY
        df['Semaine'] = WEEK[0]
    else:
        print('Non-unique weeks in data, check download')
    #Add data to df and set a specific order to be the same as the one on database for easier app implementation
    df['Year'] = int(WEEK[0].split('-')[0])
    df['Week'] = int(WEEK[0].split('-')[1][1:])
    df['Date'] = df.apply(lambda row: monday_of_week(row['Year'], row['Week']), axis=1)
    df = df[['Culture', 'Semaine', 'Semis', 'Levée', '6/8 feuilles visibles', 'Floraison femelle', 'Humidité du grain 50%', 'Récolte', 'Très mauvaises', 'Mauvaises', 'Assez bonnes', 'Bonnes', 'Très bonnes', 'Week', 'Year', 'Date', 'Début tallage', 'Épi 1cm', 'Deux noeuds', 'Épiaison']]

    rFrance = insert_db(df) #insert to db
    #Logs into my Discord server to be keep track of bugs 
    webhook = DiscordWebhook(url=config.discordLogWebhookUrl, content='**########## CEREOBS WEEKLY DEV/COND DATA #########**' + '\n' + rFrance)
    response = webhook.execute()