import requests
import json
from tqdm import tqdm


def update_etfs(db, etf_tickers, API_URL, API_TOKEN):
    '''
    Updates etf table
    '''

    workload = len(etf_tickers)

    print(f'Started etf update, workload: {workload}')

    for ticker in tqdm(etf_tickers):
        try:

            for i in range(10):
                try:
                    url = API_URL + \
                        f'fundamentals/{ticker}?api_token={API_TOKEN}&fmt=json'

                    response = requests.get(url, timeout=60)

                    etf_data = json.loads(response.content)

                    break

                except requests.exceptions.ReadTimeout:
                    if i < 9:
                        continue
                    else:
                        print(f'Too many timeouts at {ticker}.')

            if etf_data is not None:

                holdings = [holding['Code']
                            for holding in etf_data['ETF_Data']['Holdings'].values()]

                if ticker == 'VTI':
                    tickers = holdings

                if len(holdings) > 20:

                    values = {
                        'ticker': ticker,
                        'name': etf_data['General']['Name'],
                        'currency': etf_data['General']['CurrencyCode'],
                        'country': etf_data['General']['CountryName'],
                        'holdings': holdings

                    }

                    sql = f'''
                        INSERT INTO etf(ticker, name, currency, country, holdings) 
                        VALUES(:ticker, :name, :currency, :country, :holdings)
                        ON CONFLICT(ticker) DO
                        UPDATE SET 
                            name = EXCLUDED.name,
                            currency = EXCLUDED.currency,
                            country = EXCLUDED.country,
                            holdings = EXCLUDED.holdings
                        ;
                    '''

                    db.execute(sql, values)
                    db.commit()
        except:
            continue

    print(f'Finished etf update.')

    return tickers
