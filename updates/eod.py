import pandas as pd
import requests
import json
from ciso8601 import parse_datetime
from calendar import timegm
from tqdm import tqdm
from datetime import datetime


def get_eod(ticker, API_URL, API_TOKEN):
    '''
    Returns eod array of ticker.
    '''

    url = API_URL + \
        f'eod/{ticker}?api_token={API_TOKEN}&fmt=json&from={datetime.now().year-10}-01-01'

    response = requests.get(url, timeout=60)

    try:
        eod = [[timegm(parse_datetime(p['date']).timetuple()), p['open'], p['high'], p['low'], p['close'],
                p['adjusted_close'], p['volume']] for p in json.loads(response.content)]

    except json.decoder.JSONDecodeError:
        return None

    return eod


def update_eod(db, tickers, API_URL, API_TOKEN):
    '''
    Updates stock prices.
    '''

    workload = len(tickers)

    print(f'Started stock prices update, workload: {workload}')

    for ticker in tqdm(tickers):

        for i in range(10):
            try:
                eod = get_eod(ticker, API_URL, API_TOKEN)
                break
            except requests.exceptions.ReadTimeout:
                if i < 9:
                    continue
                else:
                    print(f'Too many timeouts at {ticker}.')

        if eod is not None:
            if len(eod) > 0:

                # update companies_display

                sql = f'''
                    UPDATE companies_display
                    SET stock_prices = :eod
                    WHERE ticker = :ticker
                    ;
                '''

                values = {'eod': eod, 'ticker': ticker}

                # update ticker table

                df = pd.DataFrame(eod, columns=[
                                  'time', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'])

                df['time'] = pd.to_datetime(df['time'], unit='s').round('D')

                df = df.set_index('time')

                df.to_sql(f"p{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp",
                          db.get_bind(), if_exists='replace')

                sql += f'''
                    CREATE TABLE IF NOT EXISTS p{ticker.replace('.', '_dot_').replace('-', '_dash_')}_partition
                    PARTITION OF eod
                    FOR VALUES IN ('{ticker}');

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
                        '{ticker}',
                        time,
                        open,
                        high,
                        low,
                        close,
                        adjusted_close,
                        volume
                    FROM public."p{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp"
                    ON CONFLICT DO NOTHING
                    ;                
                '''

                sql += f'''
                    DROP TABLE public."p{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp";
                '''

                db.execute(sql, values)
                db.commit()

    print('Finished stock prices update.')
