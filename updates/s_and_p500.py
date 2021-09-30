import requests
import json
from datetime import datetime, timedelta
from time import time


def update_historical_s_and_p500_components(db, API_URL, API_TOKEN):
    '''
    Updates contained_in_s_and_p500 in eod.
    '''

    start = time()

    print('Started contained_in_s_and_p500 update...')

    url = API_URL + f'fundamentals/GSPC.INDX?api_token={API_TOKEN}&fmt=json'

    response = requests.get(url, timeout=60)

    s_and_p500 = json.loads(response.content)

    current_tickers = [component['Code']
                       for component in s_and_p500['Components'].values()]

    sql = ''''''

    for component in s_and_p500['HistoricalTickerComponents'].values():

        if component['Code'] in current_tickers:
            if datetime.strptime(component['EndDate'], '%Y-%m-%d') >= (datetime.today() - timedelta(days=3*31)):
                component['EndDate'] = datetime.today().strftime('%Y-%m-%d')

        sql += f'''
            UPDATE eod SET contained_in_s_and_p500 = true
            WHERE ticker = '{component['Code']}'
            AND time BETWEEN TO_TIMESTAMP('{component['StartDate']}', 'YYYY-MM-DD')
            AND TO_TIMESTAMP('{component['EndDate']}', 'YYYY-MM-DD')
            ;
            
            UPDATE companies_annual SET contained_in_s_and_p500 = true
            WHERE ticker = '{component['Code']}'
            AND time BETWEEN TO_TIMESTAMP('{component['StartDate']}', 'YYYY-MM-DD')
            AND TO_TIMESTAMP('{component['EndDate']}', 'YYYY-MM-DD')
            ;
            
            UPDATE companies_quarterly SET contained_in_s_and_p500 = true
            WHERE ticker = '{component['Code']}'
            AND time BETWEEN TO_TIMESTAMP('{component['StartDate']}', 'YYYY-MM-DD')
            AND TO_TIMESTAMP('{component['EndDate']}', 'YYYY-MM-DD')
            ;
        '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished contained_in_s_and_p500 update ({end - start:.2f}s).')
