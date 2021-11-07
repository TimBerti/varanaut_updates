import pandas as pd
import numpy as np
from tqdm import tqdm
from time import time
from sklearn.cluster import KMeans


def sigmoid_scaler(x):
    return 2 / (1 + np.exp(-2 * x)) - 1


def update_clusters(db):
    '''
    Assigns a cluster to each stock based on its Fama and French factors and updates DB.
    '''

    start = time()

    print('Started cluster update...')

    sql = '''
        SELECT 
            ticker,
            SMB_factor, 
            HML_factor, 
            CMA_factor, 
            RMW_factor, 
            excess_market_return_factor 
        from companies_display;
    '''

    df = pd.read_sql(sql, con=db.get_bind())

    factors = ['smb_factor', 'hml_factor', 'cma_factor',
               'rmw_factor', 'excess_market_return_factor']

    df[factors] = (df[factors] - df[factors].median()) / \
        (df[factors].quantile(.75) - df[factors].quantile(.25))
    df[factors] = df[factors].apply(sigmoid_scaler)
    df.dropna(inplace=True)

    kmeans = KMeans(n_clusters=6)
    kmeans.fit(df[factors])
    df['cluster'] = kmeans.predict(df[factors])

    print(f'Workload: {len(df.index)}')

    for _, row in tqdm(df.iterrows()):
        sql = f'''
            UPDATE companies_display
            SET cluster = {row['cluster']}
            WHERE ticker = '{row['ticker']}'
            ;
        '''

        db.execute(sql)
        db.commit()

    end = time()

    print(f'Finished cluster update. ({end - start :.2f}s)')
