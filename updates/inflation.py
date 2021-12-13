import pandas as pd
import requests
import json


def get_cpi(API_KEY):
    '''
    Returns CPI array.
    '''

    url = f'https://data.nasdaq.com/api/v3/datasets/RATEINF/CPI_USA.json?api_key={API_KEY}'

    response = requests.get(url, timeout=60)

    try:
        cpi = json.loads(response.content)['dataset']['data']

    except json.decoder.JSONDecodeError:
        return None

    return cpi


def update_cpi(db, API_KEY):
    '''
    Updates CPI.
    '''

    print(f'Started CPI update...')

    for i in range(10):
        try:
            cpi = get_cpi(API_KEY)
            break
        except requests.exceptions.ReadTimeout:
            if i < 9:
                continue
            else:
                print(f'Too many timeouts!')

    if cpi is not None:
        if len(cpi) > 0:

            # update ticker table

            df = pd.DataFrame(cpi, columns=['time', 'open'])

            df['time'] = pd.to_datetime(df['time'])

            df['high'] = df['open']
            df['low'] = df['open']
            df['close'] = df['open']
            df['adjusted_close'] = df['open']
            df['volume'] = df['open']*0

            df = df.set_index('time')

            df.to_sql(f"pCPI_tmp", db.get_bind(), if_exists='replace')

            sql = f'''
                CREATE TABLE IF NOT EXISTS pCPI_partition
                PARTITION OF eod
                FOR VALUES IN ('CPI_US');

                INSERT INTO eod (
                    ticker,
                    time,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume
                )
                SELECT
                    'CPI_US',
                    time,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume
                FROM public."pCPI_tmp"
                ON CONFLICT DO NOTHING
                ;                
            '''

            sql += f'''
                DROP TABLE public."pCPI_tmp";
            '''

            db.execute(sql)
            db.commit()

    print('Finished CPI update.')
