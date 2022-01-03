import pandas as pd
import requests
import json
from tqdm import tqdm


def get_options(ticker, API_URL, API_TOKEN):
    '''
    Returns options array of ticker.
    '''

    url = API_URL + \
        f'options/{ticker}?api_token={API_TOKEN}&fmt=json'

    response = requests.get(url, timeout=60)

    try:
        options = [[p['expirationDate'],
                    p['impliedVolatility'],
                    p['putVolume'],
                    p['callVolume'],
                    p['putOpenInterest'],
                    p['callOpenInterest'],
                    p['optionsCount']]
                   for p in json.loads(response.content)['data']]

    except json.decoder.JSONDecodeError:
        return None

    return options


def update_options(db, tickers, API_URL, API_TOKEN):
    '''
    Updates options.
    '''

    workload = len(tickers)

    print(f'Started options update, workload: {workload}')

    for ticker in tqdm(tickers):

        for i in range(10):
            try:
                options = get_options(ticker, API_URL, API_TOKEN)
                break
            except requests.exceptions.ReadTimeout:
                if i < 9:
                    continue
                else:
                    print(f'Too many timeouts at {ticker}.')

        if options is not None:
            if len(options) > 0:

                # update ticker table

                df = pd.DataFrame(options, columns=[
                                  'expiration_date', 'implied_volatility', 'put_volume', 'call_volume', 'put_open_interest', 'call_open_interest', 'options_count'])

                df['expiration_date'] = pd.to_datetime(df['expiration_date'])

                df = df.set_index('expiration_date')

                df.to_sql(f"o{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp",
                          db.get_bind(), if_exists='replace')

                sql = f'''
                    CREATE TABLE IF NOT EXISTS o{ticker.replace('.', '_dot_').replace('-', '_dash_')}_partition
                    PARTITION OF options
                    FOR VALUES IN ('{ticker}');

                    INSERT INTO options (
                        ticker,
                        expiration_date, 
                        implied_volatility, 
                        put_volume, 
                        call_volume, 
                        put_open_interest, 
                        call_open_interest, 
                        options_count
                    )
                    SELECT
                        '{ticker}',
                        expiration_date, 
                        implied_volatility, 
                        put_volume, 
                        call_volume, 
                        put_open_interest, 
                        call_open_interest, 
                        options_count
                    FROM public."o{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp"
                    ON CONFLICT DO NOTHING
                    ;                
                '''

                sql += f'''
                    DROP TABLE public."o{ticker.replace('.', '_dot_').replace('-', '_dash_')}_tmp";
                '''

                # update companies_display

                values = {'implied_volatility': float(df['implied_volatility'].iloc[-1]),
                          'ticker': ticker}

                sql += f'''
                    UPDATE companies_display
                    SET implied_volatility = :implied_volatility
                    WHERE ticker = :ticker
                    ;
                '''

                db.execute(sql, values)
                db.commit()

    print('Finished options update.')
