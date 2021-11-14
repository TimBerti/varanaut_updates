from .fundamentals import update_fundamentals
from .figures_annual import update_annual_figures
from .figures_quarterly import update_quarterly_figures
from .display import update_companies_display
from .statistics import update_statistics
from .eod import update_eod
from .price_and_liquidity import update_price_and_liqudity
# from .s_and_p500 import update_historical_s_and_p500_components
from .russell3000 import update_russel_3000_components
from .forex_rates import update_forex_rates
from .forex_rate import update_forex_rate
from .market_cap import update_market_cap
from .beta import update_beta
from .rsi import update_rsi_180
from .scores import update_scores
from .fama_french_FF import update_fama_french_FFs, update_fama_french_expectations
from .cluster import update_clusters, update_cluster_correlation
from time import time
from datetime import timedelta
import requests
import json


def get_tickers(API_URL, API_TOKEN):
    '''
    Returns all available tickers of the api.
    '''

    print('Loading available tickers...')

    start = time()

    url = API_URL + f'exchanges-list/?api_token={API_TOKEN}&fmt=json'
    response = requests.get(url, timeout=60)

    exchanges = {exchange['Code'] for exchange in json.loads(response.content)}

    exchanges -= {'INDX', 'BOND', 'EUFUND', 'MONEY',
                  'ETLX', 'GBOND', 'TWO', 'CC', 'COMM', 'FOREX'}

    tickers = []

    for exchange in exchanges:

        url = API_URL + \
            f'exchange-symbol-list/{exchange}?api_token={API_TOKEN}&fmt=json'
        response = requests.get(url, timeout=60)

        tickers += [f'{stock["Code"]}' + (f'.{exchange}' if exchange != 'US' else '')
                    for stock in json.loads(response.content)]

    end = time()

    print(f'Finished loading tickers. ({end - start:.2f}s)')

    return tickers


def daily(db, API_URL, API_TOKEN):
    '''
    Executes daily updates.
    '''

    print('Daily update started...')

    start = time()

    forex_tickers = ['USD', 'EUR', 'RUB', 'GBP', 'CNY', 'JPY', 'SGD', 'INR', 'CHF', 'AUD', 'CAD', 'HKD', 'MYR', 'NOK', 'NZD', 'ZAR', 'SEK',
                     'DKK', 'BRL', 'ZAC', 'MXN', 'TWD', 'KRW', 'CLP', 'CZK', 'HUF', 'IDR', 'ISK', 'MXV', 'PLN', 'TRY', 'UYU', 'XAUUSD', 'THB', 'SAR', 'ILS']

    stock_tickers = update_russel_3000_components(db, API_URL, API_TOKEN)
    update_fundamentals(db, stock_tickers, API_URL, API_TOKEN)
    stock_tickers += ['SPY', 'US10Y.GBOND']
    update_eod(db, stock_tickers, API_URL, API_TOKEN)
    update_price_and_liqudity(db)
    # update_historical_s_and_p500_components(db, API_URL, API_TOKEN)
    update_market_cap(db)
    update_beta(db)
    update_rsi_180(db)
    update_forex_rates(db, forex_tickers, API_URL, API_TOKEN)
    update_forex_rate(db)
    update_annual_figures(db)
    update_quarterly_figures(db)
    update_scores(db)
    update_companies_display(db)
    update_fama_french_FFs(db)
    update_fama_french_expectations(db)
    update_clusters(db)
    update_cluster_correlation(db)
    update_statistics(db)

    end = time()

    print(f'Daily update finished. ({timedelta(seconds=round(end - start))})')
