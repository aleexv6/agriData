import pandas as pd
import requests
import json
from io import BytesIO
import dateparser
import pymongo
import database as db

def test_data(semaineObservation, idCulture, typePublication):
    url = f"https://cereobs.franceagrimer.fr/cereobs-sp/api/public/publications/rapportCereobs?semaineObservation={semaineObservation}&idCulture={idCulture}&typePublication={typePublication}"    
#   try:
    r = requests.get(url)
    if not 'application/json' in r.headers.get('content-type', ''):
        df = pd.read_excel(BytesIO(r.content), skiprows=3)
        return df
    else:
        return None
    
def format_data(data):
    date = data[data['Unnamed: 6'].notnull()]['Unnamed: 6'].iloc[0]
    data = data.drop(columns=['Unnamed: 6'])
    data = data.dropna()
    index_to_keep = data[data['Unnamed: 0'].str.startswith('Moyenne')].index[0]
    data = data.iloc[:index_to_keep + 1]
    columns_to_drop = data.columns[data.isnull().all()]
    data = data.drop(columns=columns_to_drop)
    data = data.rename(columns={'Unnamed: 0': 'Region'})
    selected_columns = [col for col in data.columns if not col.startswith('Moyenne')]
    data = data[selected_columns]
    data.columns = data.columns.str.replace('\n', '').str.replace('%', '')
    data.set_index(data.columns[0], inplace=True)
    df_transposed = data.T
    df_transposed['Date'] = pd.to_datetime(date, dayfirst=True) - pd.Timedelta(weeks=1)
    df_transposed['Condition'] = df_transposed.index
    df_transposed = df_transposed.reset_index(drop=True)
    return df_transposed

def insert_db(df):
    r = 'Nothing in the database, add historical data first'
    dbname = db.get_database()
    collection_name = dbname["condition"]
    df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data = df.to_dict('records')
    last_doc = collection_name.find_one(
            sort=[( 'Date', pymongo.DESCENDING )]
        )
    print(df['Date'].iloc[0], last_doc['Date'])
    if last_doc is not None:
            if not df.empty:
                if df['Date'].iloc[0] != last_doc['Date']:
                    r = str(collection_name.insert_many(data))
                    r = 'Développement des cultures : ' + r
                else:
                    r = 'Document non inséré, doublon date avec le dernier document en base.'
            else:
                r = 'NO DATA TO IMPORT TODAY, EMPTY DATAFRAME'
    return r

dataBle = test_data(834, 2, 5) #change biggest value with +1 for new week
dfBle = format_data(dataBle)
dfBle['Produit'] = 'Ble tendre'
dataBleDur = test_data(834, 3, 5) #change biggest value with +1 for new week
dfBleDur = format_data(dataBleDur)
dfBleDur['Produit'] = 'Ble dur'
dataMais = test_data(834, 5, 5) #change biggest value with +1 for new week
if dataMais is not None:
    dfMais = format_data(dataMais)
    dfMais['Produit'] = 'Mais'
else:
    dfMais = None
    print('No data for Maïs')


df = pd.concat([dfBle, dfBleDur, dfMais])
r = insert_db(df)
print(r)
