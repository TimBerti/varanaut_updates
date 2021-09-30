import requests
import json
from time import time


def update_russel_3000_components(db, API_URL, API_TOKEN):
    '''
    Updates current Russel 3000 components
    '''

    start = time()

    print('Started Russel 3000 update...')

    url = API_URL + f'fundamentals/RUI.INDX?api_token={API_TOKEN}&fmt=json'

    response = requests.get(url, timeout=60)

    russel_1000 = json.loads(response.content)

    tickers_1000 = [component['Code']
                    for component in russel_1000['Components'].values()]

    url = API_URL + f'fundamentals/RUT.INDX?api_token={API_TOKEN}&fmt=json'

    response = requests.get(url, timeout=60)

    russel_2000 = json.loads(response.content)

    tickers_2000 = [component['Code']
                    for component in russel_2000['Components'].values()]

    tickers = tickers_1000 + tickers_2000

    sql = '''
        UPDATE indices SET components = :tickers WHERE ticker = 'RUA';
    '''

    db.execute(sql, {'tickers': tickers})
    db.commit()

    end = time()

    print(f'Finished Russel 3000 update ({end - start:.2f}s).')

    return tickers
