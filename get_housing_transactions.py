import calendar
import logging
import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import requests

from secret import ACCESS_KEY
from utils import setup_logger

logger = logging.getLogger(__name__)


def get_token(url):
    headers = {
        "AccessKey": ACCESS_KEY
    }
    resp = requests.get(url, headers=headers)
    token = resp.json()['Result']
    return token


def convert_str_to_date(input_str):
    """ converting dates of 0220 for example to 29-02-2020"""
    dt = datetime.strptime(input_str, '%m%y')
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    dt = dt.replace(day=last_day)
    return dt.strftime('%Y-%m-%d')


def map_type_of_sale(type_of_sale_val):
    mapping = {
        '1': 'New Sale',
        '2': 'Sub Sale',
        '3': 'Resale'
    }
    return mapping.get(type_of_sale_val)


def format_response_to_df(resp):
    logger.info("Formatting response into a DataFrame object")
    result = defaultdict(list)
    for item in resp.json()['Result']:
        for trans in item['transaction']:
            result['y'].append(float(item['y']) if item.get('y') else None)
            result['x'].append(float(item['x']) if item.get('x') else None)
            result['street'].append(item['street'])
            result['project'].append(item['project'])

            result['market_segment'].append(item['marketSegment'])

            area = float(trans['area'])
            area_sqft = area * 10.76391042
            result['area_sqm'].append(area)
            result['area_sqft'].append(area_sqft)

            result['price'].append(float(trans['price']))
            result['nett_price'].append(float(trans['nettPrice']) if trans.get('nettPrice') else None)

            result['unit_price_psm'].append(float(trans['price']) / area)
            result['unit_price_psf'].append(float(trans['price']) / area_sqft)

            result['floor_range'].append(trans['floorRange'])
            result['num_units'].append(int(trans['noOfUnits']))
            result['reference_period'].append(convert_str_to_date(trans['contractDate']))
            result['type_of_sale'].append(map_type_of_sale(trans['typeOfSale']))
            result['property_type'].append(trans['propertyType'])
            result['district'].append(int(trans['district']))
            result['type_of_area'].append(trans['typeOfArea'])
            result['tenure'].append(trans['tenure'])
    df = pd.DataFrame(result)
    # replace all instances of NA with - so that it doesn't mess with pandas NaN when the csv is being read
    df.replace('NA', '-', inplace=True)
    logger.info(f"Retrieved {len(df)} rows of data")
    return df


def get_transaction_data(token_url, data_url, batch_num):
    """
    :param token_url: url to retrieve token from
    :param data_url: url to retrieve data from
    :param batch_num: as required by the API, batch_num ranges from 1 to 4.
    """
    token = get_token(token_url)
    headers = {
        'AccessKey': ACCESS_KEY,
        'Token': token
    }
    params = {
        'service': 'PMI_Resi_Transaction',
        'batch': batch_num,
    }
    resp = requests.get(data_url, headers=headers, params=params)
    df = format_response_to_df(resp)
    return df


def save_df_to_file(file_path, df):
    subset_cols = ['street', 'project', 'market_segment', 'area_sqm', 'price', 'nett_price', 'floor_range', 'num_units',
                   'reference_period', 'type_of_sale', 'property_type', 'district', 'type_of_area', 'tenure']

    if os.path.isfile(file_path):
        # update the dataframe here and save it again
        existing_df = pd.read_csv(file_path)
        logger.info(
            f"Found existing file with {len(existing_df)} rows... Combining the data and dropping duplicates...")

        combined_df = existing_df.append(df)
        combined_df.drop_duplicates(subset=subset_cols, inplace=True)
        combined_df.to_csv(file_path, index=False)
        logger.info(f"Saved combined data with {len(combined_df)} rows.")
    else:
        logger.info(f"No existing file... Dropping duplicates...")
        df.drop_duplicates(subset=subset_cols, inplace=True)
        logger.info(f"Saving {len(df)} rows to {file_path}.")
        df.to_csv(file_path, index=False)


def main():
    setup_logger(logger)
    token_url = 'https://www.ura.gov.sg/uraDataService/insertNewToken.action'
    data_url = 'https://www.ura.gov.sg/uraDataService/invokeUraDS'
    dest_folder = os.path.join('data', 'private_prop_transactions')
    os.makedirs(dest_folder, exist_ok=True)

    df = pd.DataFrame()
    for batch_num in range(1, 5):
        logger.info(f"Getting data for batch {batch_num}")
        df = df.append(get_transaction_data(token_url, data_url, batch_num))
    df['observation_time'] = datetime.now()
    logger.info(f"Total rows retrieved: {len(df)}")
    file_path = os.path.join(dest_folder, 'total_transactions.csv')
    save_df_to_file(file_path, df)


if __name__ == '__main__':
    main()
