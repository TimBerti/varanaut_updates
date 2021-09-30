import pandas as pd
import numpy as np
from tqdm import tqdm
from time import time


def update_fama_french_FFs(db):
    '''
    Calculates the quarterly factors of the fama and french five factors model for the last 10 years.
    '''

    start = time()

    print('Started fama and french 5 factors update...')

    tickers = ['A', 'AAL', 'AAP', 'AAPL', 'ABBV', 'ABC', 'ABG', 'ABMD', 'ABNB', 'ABT', 'AC', 'ACA', 'ACGL', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADS', 'ADSK', 'AEE', 'AEM', 'AEP', 'AES', 'AFG', 'AFL', 'AGL', 'AGNC', 'AGS', 'AHT', 'AI', 'AIG', 'AIZ', 'AJG', 'AKAM', 'ALB', 'ALC', 'ALE', 'ALGN', 'ALL', 'ALLE', 'ALLY', 'ALNY', 'ALRS', 'ALV', 'AMAT', 'AMD', 'AME', 'AMGN', 'AMP', 'AMS', 'AMT', 'AMZN', 'ANET', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'APD', 'APH', 'API', 'APO', 'APT', 'APTV', 'AQN', 'ARE', 'ARGX', 'ARMK', 'ARW', 'ASM', 'ASML', 'ASX', 'ATH', 'ATHM', 'ATO', 'ATUS', 'ATVI', 'AUD', 'AUTO', 'AVB', 'AVGO', 'AVLR', 'AVTR', 'AVY', 'AWK', 'AXP', 'AZN', 'AZO', 'BA', 'BAC', 'BAH', 'BAP', 'BAX', 'BB', 'BBVA', 'BBY', 'BCE', 'BDX', 'BEKE', 'BEN', 'BEPC', 'BFB', 'BG', 'BGNE', 'BHC', 'BIDU', 'BIIB', 'BILI', 'BIO', 'BK', 'BKI', 'BKNG', 'BKR', 'BLDP', 'BLK', 'BLL', 'BMO', 'BMRN', 'BMY', 'BNR', 'BNS', 'BRO', 'BSX', 'BSY', 'BURL', 'BVN', 'BWA', 'BX', 'BXP', 'BZUN', 'C', 'CABO', 'CAE', 'CAG', 'CAP', 'CARR', 'CAT', 'CB', 'CBOE', 'CBRE', 'CCEP', 'CCI', 'CCK', 'CCL', 'CCO', 'CCU', 'CDAY', 'CDNS', 'CDR', 'CDW', 'CE', 'CERN', 'CF', 'CFG', 'CFR', 'CGNX', 'CHD', 'CHKP', 'CHRW', 'CHTR', 'CHWY', 'CI', 'CINF', 'CL', 'CLS', 'CLVT', 'CM', 'CMCSA', 'CME', 'CMG', 'CMI', 'CMS', 'CNC', 'CNHI', 'CNP', 'CNQ', 'CNR', 'COF', 'COO', 'COP', 'COST', 'COUP', 'CP', 'CPB', 'CPG', 'CPRT', 'CPS', 'CPT', 'CRM', 'CRWD', 'CS', 'CSCO', 'CSGP', 'CSL', 'CSU', 'CSX', 'CTAS', 'CTSH', 'CTVA', 'CTXS', 'CVE', 'CVNA', 'CVS', 'CVX', 'CYBR', 'CZR', 'D', 'DADA', 'DAL', 'DBX', 'DD', 'DDOG', 'DE', 'DELL', 'DFS', 'DG', 'DGX', 'DHI', 'DHR', 'DIS', 'DISCA', 'DISCK', 'DISH', 'DKNG', 'DLR', 'DLTR', 'DNB', 'DOCU', 'DOV', 'DOW', 'DPW', 'DPZ', 'DQ', 'DRE', 'DRI', 'DT', 'DTE', 'DUK', 'DVA', 'DVN', 'DXCM', 'EA', 'EBAY', 'EBS', 'ECL', 'ED', 'EFX', 'EIX', 'ELAN', 'ELS', 'EMN', 'EMR', 'ENB', 'ENG', 'ENPH', 'ENR', 'EOG', 'EPAM', 'EQH', 'EQIX', 'EQNR', 'EQR', 'EQT', 'ERF', 'ERIE', 'ES', 'ESLT', 'ESS', 'ETN', 'ETR', 'ETSY', 'EVK', 'EVN', 'EVR', 'EVRG', 'EW', 'EXAS', 'EXC', 'EXPD', 'EXPE', 'EXR', 'F', 'FAST', 'FB', 'FBHS', 'FBK', 'FCX', 'FDS', 'FDX', 'FE', 'FERG', 'FFIV', 'FICO', 'FIS', 'FISV', 'FITB', 'FIVE', 'FLT', 'FMC', 'FNF', 'FNV', 'FPH', 'FR', 'FRC', 'FSR', 'FSV', 'FTNT', 'FTS', 'FTV', 'FUTU', 'FWONK', 'G', 'GD', 'GDDY', 'GDS', 'GE', 'GFI', 'GFL', 'GIL', 'GILD', 'GIS', 'GL', 'GLOB', 'GLW', 'GM', 'GMAB', 'GNRC', 'GOOG', 'GOOGL', 'GOTU', 'GPC', 'GPN', 'GRF', 'GRMN', 'GS', 'GSK', 'GWRE', 'GWW', 'H', 'HAL', 'HAS', 'HBAN', 'HCA', 'HCM', 'HD', 'HEI', 'HES', 'HIG', 'HII', 'HLT', 'HOLX', 'HON', 'HPE', 'HPQ', 'HRL', 'HSIC', 'HST', 'HSY', 'HTHT', 'HUBS', 'HUM', 'HUYA', 'HWM', 'HZNP', 'IAC', 'IAG', 'IBM', 'ICE', 'ICL', 'IDXX', 'IEX', 'IFF', 'IHG', 'III', 'ILMN', 'IMAB', 'IMO', 'INCY', 'INFO', 'INFY', 'INTC', 'INTU', 'INVH', 'IP', 'IPG', 'IPGP', 'IQ', 'IQV', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'J', 'JAZZ', 'JBHT', 'JCI', 'JD', 'JHX', 'JNJ', 'JNPR', 'JOBS', 'JPM', 'K', 'KC', 'KDP', 'KEY', 'KEYS', 'KHC', 'KKR', 'KL', 'KMB', 'KMI', 'KMX', 'KNX', 'KO', 'KR', 'KSU', 'L', 'LAND', 'LB', 'LBRDK', 'LBTYA', 'LBTYK', 'LDOS', 'LEA', 'LEG', 'LEN', 'LH', 'LHX', 'LI', 'LII', 'LIN', 'LKQ', 'LLY', 'LMT', 'LNC', 'LNG', 'LNT', 'LOGN', 'LOW', 'LSPD', 'LSXMA', 'LSXMK', 'LULU', 'LUMN', 'LUV', 'LVS', 'LYB', 'LYFT', 'LYV', 'MA', 'MAA', 'MAR', 'MARK', 'MAS', 'MASI', 'MBT', 'MC', 'MCB', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDB', 'MDLZ', 'MDT', 'MELI', 'MET', 'MF', 'MFC', 'MFG', 'MG', 'MGM', 'MGR', 'MHK', 'MKC', 'MKL', 'MKTX', 'MLCO', 'MLM', 'MMC', 'MMM', 'MNST', 'MO', 'MOH', 'MOMO', 'MOS', 'MPC', 'MPW', 'MPWR', 'MRK', 'MRNA', 'MRO', 'MRVL', 'MS',
               'MSCI', 'MSI', 'MT', 'MTB', 'MTCH', 'MTD', 'MTN', 'MTX', 'MU', 'NBIX', 'NDAQ', 'NDSN', 'NEE', 'NEM', 'NET', 'NEXI', 'NFLX', 'NI', 'NICE', 'NIO', 'NKE', 'NLOK', 'NLY', 'NOAH', 'NOC', 'NOK', 'NOVN', 'NOW', 'NRG', 'NRP', 'NSC', 'NTAP', 'NTES', 'NTR', 'NTRS', 'NUE', 'NVAX', 'NVCR', 'NVDA', 'NVR', 'NWG', 'NWL', 'NXPI', 'O', 'OC', 'ODFL', 'OHI', 'OKE', 'OKTA', 'OMC', 'ON', 'OR', 'ORA', 'ORCL', 'ORI', 'ORLY', 'OSH', 'OTIS', 'OXY', 'PAAS', 'PANW', 'PAYC', 'PAYX', 'PCAR', 'PCG', 'PDD', 'PEAK', 'PEG', 'PEP', 'PFE', 'PFG', 'PGR', 'PHM', 'PINS', 'PKG', 'PKI', 'PLD', 'PLTR', 'PLUG', 'PM', 'PNC', 'PNR', 'PNW', 'PODD', 'POLY', 'POOL', 'PPD', 'PPG', 'PPL', 'PRU', 'PSA', 'PSN', 'PSX', 'PTC', 'PXD', 'PYPL', 'QCOM', 'QFIN', 'QRVO', 'QSR', 'RACE', 'RAND', 'RBA', 'RCL', 'RE', 'REG', 'REGN', 'RF', 'RGA', 'RHI', 'RIO', 'RJF', 'RKT', 'RNG', 'RNR', 'ROG', 'ROK', 'ROKU', 'ROL', 'ROP', 'ROST', 'RPRX', 'RSG', 'RTX', 'RUN', 'RY', 'SAF', 'SAM', 'SAN', 'SAND', 'SAP', 'SAR', 'SBAC', 'SBUX', 'SCCO', 'SCHN', 'SCHW', 'SCR', 'SE', 'SEDG', 'SEE', 'SEIC', 'SGEN', 'SHOP', 'SHW', 'SIRI', 'SIVB', 'SJM', 'SLB', 'SLF', 'SLM', 'SM', 'SNA', 'SNAP', 'SNOW', 'SNPS', 'SO', 'SOL', 'SPG', 'SPGI', 'SPLK', 'SQ', 'SRE', 'SRG', 'SSNC', 'ST', 'STE', 'STLA', 'STLD', 'STM', 'STT', 'STX', 'STZ', 'SU', 'SUI', 'SUN', 'SVT', 'SWK', 'SWKS', 'SYF', 'SYK', 'T', 'TAL', 'TAP', 'TCOM', 'TCS', 'TD', 'TDG', 'TDOC', 'TDY', 'TEF', 'TEL', 'TEN', 'TER', 'TEVA', 'TFC', 'TFX', 'TGT', 'TIGR', 'TJX', 'TLS', 'TM', 'TME', 'TMO', 'TMUS', 'TRI', 'TRMB', 'TRN', 'TROW', 'TRP', 'TRU', 'TRV', 'TSCO', 'TSLA', 'TSN', 'TT', 'TTD', 'TTE', 'TTWO', 'TW', 'TWLO', 'TWTR', 'TXG', 'TXN', 'TXT', 'TYL', 'U', 'UBER', 'UDR', 'UGI', 'UHAL', 'UHS', 'ULTA', 'UNH', 'UNP', 'UNVR', 'UPS', 'URI', 'USB', 'V', 'VEEV', 'VER', 'VFC', 'VIAC', 'VICI', 'VIPS', 'VIV', 'VLO', 'VMC', 'VMW', 'VNET', 'VNO', 'VOD', 'VOYA', 'VRSK', 'VRSN', 'VRTX', 'VST', 'VTR', 'VTRS', 'VZ', 'W', 'WAB', 'WAT', 'WB', 'WBA', 'WCN', 'WDAY', 'WEC', 'WELL', 'WES', 'WFC', 'WFG', 'WHR', 'WIX', 'WLTW', 'WM', 'WMB', 'WMT', 'WOW', 'WPC', 'WPM', 'WPP', 'WRB', 'WRK', 'WST', 'WTRG', 'WU', 'WY', 'WYNN', 'X', 'XEL', 'XLNX', 'XOM', 'XPEV', 'XPO', 'XRAY', 'XYL', 'Y', 'YNDX', 'YUM', 'YUMC', 'YY', 'Z', 'ZBH', 'ZBRA', 'ZEN', 'ZLAB', 'ZM', 'ZS', 'ZTO', 'ZTS']

    sql = f'''
        SELECT
            ticker,
            DATE_TRUNC('quarter', time) AS quarter,
            100 * (market_cap / LAG(market_cap, 4) OVER (
                PARTITION BY ticker
                ORDER BY time
            ) - 1) AS return,
            market_cap,
            price_book,
            - total_cashflows_from_investing_activities / total_assets AS investing,
            gross_profit / (total_assets - total_liabilities) AS profitability
        FROM companies_quarterly WHERE ticker in ('{"', '".join(tickers)}')
        AND market_cap IS NOT NULL
        AND price_book IS NOT NULL
        AND total_cashflows_from_investing_activities IS NOT NULL
        AND total_assets IS NOT NULL
        AND total_liabilities IS NOT NULL
        AND gross_profit IS NOT NULL
        AND total_assets > total_liabilities
        AND time > CURRENT_DATE - INTERVAl '10 years'
        ORDER BY time DESC;
    '''

    stocks_df = pd.read_sql(sql, con=db.get_bind()).dropna()

    # Market Return (ACWI.INDX)

    sql = '''
        WITH RECURSIVE
            risk_free AS (
                SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS risk_free_rate
                FROM eod WHERE ticker = 'US10Y.GBOND'
                GROUP BY DATE_TRUNC('quarter', time)
                ORDER BY quarter DESC
            ),
            market AS (
                SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS average_price
                FROM eod WHERE ticker = 'ACWI.INDX'
                GROUP BY DATE_TRUNC('quarter', time)
                ORDER BY quarter DESC
            )
        SELECT
            m.quarter as time,
            risk_free_rate,
            (average_price / (LAG(average_price, 4) OVER (ORDER BY m.quarter)) - 1) * 100 - risk_free_rate AS excess_market_return
        FROM risk_free r
        INNER JOIN market m
        ON r.quarter = m.quarter
    '''

    market_df = pd.read_sql(sql, con=db.get_bind())

    # split in months

    quarter_groups = stocks_df.groupby(by='quarter')

    # calculate factors

    factors = []

    for quarter in quarter_groups:

        timestamp = quarter[0]

        quarter_df = quarter[1]

        quantiles = quarter_df.quantile(0.5)

        # Small Minus Big (market_cap)

        SMB = quarter_df[quarter_df['market_cap'] < quantiles['market_cap']]['return'].mean() \
            - quarter_df[quarter_df['market_cap'] >
                         quantiles['market_cap']]['return'].mean()

        # High Minus Low valuation (price_book)

        HML = quarter_df[quarter_df['price_book'] > quantiles['price_book']]['return'].mean() \
            - quarter_df[quarter_df['price_book'] <
                         quantiles['price_book']]['return'].mean()

        # Conservative Minus Aggresive Investing (total_cashflows_from_investing_activities on total_assets)

        CMA = quarter_df[quarter_df['investing'] < quantiles['investing']]['return'].mean() \
            - quarter_df[quarter_df['investing'] >
                         quantiles['investing']]['return'].mean()

        # Robust Minus Weak Profitability (gross_proft on equity)

        RMW = quarter_df[quarter_df['profitability'] > quantiles['profitability']]['return'].mean() \
            - quarter_df[quarter_df['profitability'] <
                         quantiles['profitability']]['return'].mean()

        factors.append([timestamp, SMB, HML, CMA, RMW])

    df = pd.DataFrame(factors, columns=['time', 'SMB', 'HML', 'CMA', 'RMW'])
    df = df.merge(market_df, on='time').dropna()
    df.to_sql('fama_french_FFs', con=db.get_bind(),
              if_exists='replace', index=False)

    end = time()

    print(f'Finished fama and french 5 factors update. ({end - start :.2f}s)')


def calculate_fama_french_expectation(db, fama_french_df, ticker):
    '''
    Calculate expected return of ticker based on the fama and french five factor model
    '''

    sql = f'''
        WITH cte AS (
            SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS average_price
            FROM eod WHERE ticker = '{ticker}' 
            GROUP BY DATE_TRUNC('quarter', time)
            ORDER BY quarter DESC
        )
        SELECT 
            quarter AS time,
            (average_price / (LAG(average_price, 4) OVER (ORDER BY quarter)) - 1) * 100 AS return
        FROM cte
        ;
    '''

    stock_df = pd.read_sql(sql, con=db.get_bind())

    stock_df = stock_df.merge(fama_french_df, on='time')

    A = stock_df[['SMB', 'HML', 'CMA', 'RMW',
                  'excess_market_return']].to_numpy()
    y = (stock_df['return'] - stock_df['risk_free_rate']).to_numpy()

    x, *_ = np.linalg.lstsq(A[:-1], y, rcond=None)

    return np.dot(A[-1], x)


def update_fama_french_expectations(db):
    '''
    Updates fama_french_expecations in db.
    '''

    sql = '''
        SELECT DISTINCT ticker FROM companies_display;
    '''

    tickers_df = pd.read_sql(sql, con=db.get_bind())

    tickers = tickers_df['ticker']

    workload = len(tickers)

    print(f'Started fama and french expecations update. Workload: {workload}')

    sql = '''
        SELECT * FROM public."fama_french_FFs";
    '''

    fama_french_df = pd.read_sql(sql, con=db.get_bind())

    sql = ''''''

    i = 1
    for idx, ticker in tqdm(enumerate(tickers, 1)):

        try:
            expected_return = calculate_fama_french_expectation(
                db, fama_french_df, ticker)

            if np.isnan(expected_return):
                raise

            sql += f'''
                UPDATE companies_display
                SET fama_french_expectation = {expected_return}
                WHERE ticker = '{ticker}'
                ;
            '''

            i += 1
        except:
            pass

        if i % 20 == 0 or idx == workload:
            if sql != '''''':
                db.execute(sql)
                db.commit()

    sql = '''
        WITH cte AS (
            SELECT 
                ticker, 
                fama_french_expectation,
                PERCENT_RANK() OVER (
                    PARTITION BY fama_french_expectation IS NOT NULL
                    ORDER BY fama_french_expectation
                ) AS fama_french_expectation_ranker
            FROM companies_display
        )

        UPDATE companies_display c
        SET fama_french_expectation_ranker = cte.fama_french_expectation_ranker
        FROM cte
        WHERE cte.ticker = c.ticker
        ;
    '''

    db.execute(sql)
    db.commit()

    print('Finished fama and french expecations update.')
