from .fundamentals import update_fundamentals
from .figures_annual import update_annual_figures
from .figures_quarterly import update_quarterly_figures
from .display import update_companies_display
from .statistics import update_statistics
from .eod import update_eod
from .options import update_options
from .inflation import update_cpi
from .price_and_liquidity import update_price_and_liqudity
from .etf import update_etfs
from .market_cap import update_market_cap
from .beta import update_beta
from .rsi import update_rsi_180
from .scores import update_scores
from .fama_french import update_fama_french_factors, update_fama_french_regressions
from .cluster import update_clusters, update_cluster_correlation
from .risk_factor import update_risk_factors
from time import time
from datetime import timedelta


def daily(db, EOD_URL, EOD_TOKEN, NASDAQ_KEY):
    '''
    Executes daily updates.
    '''

    print('Daily update started...')

    start = time()

    etf_tickers = ['VTI', 'ESGV']

    stock_tickers = update_etfs(db, etf_tickers, EOD_URL, EOD_TOKEN)
    # update_fundamentals(db, stock_tickers, EOD_URL, EOD_TOKEN)
    # stock_tickers += ['SPY', 'US10Y.GBOND', 'GSG', 'SHY', 'VCSH']
    # update_eod(db, stock_tickers, EOD_URL, EOD_TOKEN)
    update_options(db, stock_tickers[1190:], EOD_URL, EOD_TOKEN)
    update_cpi(db, NASDAQ_KEY)
    update_price_and_liqudity(db)
    update_market_cap(db)
    update_beta(db)
    update_rsi_180(db)
    update_quarterly_figures(db)
    update_annual_figures(db)
    update_scores(db)
    update_companies_display(db)
    update_risk_factors(db)
    update_fama_french_factors(db)
    update_fama_french_regressions(db)
    update_clusters(db)
    # update_cluster_correlation(db)
    update_statistics(db)

    end = time()

    print(f'Daily update finished. ({timedelta(seconds=round(end - start))})')
