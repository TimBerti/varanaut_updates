import pandas as pd
import requests
import json
from tqdm import tqdm
from datetime import datetime


def get_forex_rates(ticker, API_URL, API_TOKEN):
    '''
    Returns forex rates array of ticker.
    '''

    url = API_URL + \
        f'eod/{ticker}.FOREX?api_token={API_TOKEN}&fmt=json&from={datetime.now().year-10}-01-01'

    response = requests.get(url, timeout=60)

    try:
        forex_rates = [[p['date'], p['open'], p['high'], p['low'],
                        p['close']] for p in json.loads(response.content)]
    except json.decoder.JSONDecodeError:
        return None

    return forex_rates


def update_forex_rates(db, tickers, API_URL, API_TOKEN):
    '''
    Updates forex rates.
    '''

    workload = len(tickers)

    print(f'Started forex rates update, workload: {workload}')

    for ticker in tqdm(tickers):

        forex_rates = get_forex_rates(ticker, API_URL, API_TOKEN)

        if forex_rates is not None:
            if len(forex_rates) > 0:

                df = pd.DataFrame(forex_rates, columns=[
                                  'time', 'open', 'high', 'low', 'close'])

                df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d')

                df = df.set_index('time')

                df.to_sql(f"{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp",
                          db.get_bind(), if_exists='replace')

                sql = f'''
                    CREATE TABLE IF NOT EXISTS {ticker.replace('.', '_dot_').replace('-', '_dash_')}_USD
                    PARTITION OF forex_rates
                    FOR VALUES IN ('{ticker}');

                    INSERT INTO forex_rates (
                        ticker,
                        time,
                        open,
                        high,
                        low,
                        close
                    )
                    SELECT
                        '{ticker}',
                        time,
                        open,
                        high,
                        low,
                        close
                    FROM public."{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp"
                    ON CONFLICT DO NOTHING;

                    DROP TABLE public."{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp";
                '''

                db.execute(sql)
                db.commit()

    print('Finished forex rates update.')
