import calendar
import os
from datetime import datetime, timedelta, date
from time import sleep

import pandas as pd
import requests

from secret import ACCESS_KEY

from dateutil.relativedelta import relativedelta


def convert_str_to_date(input_dt):
    """ converting dates of Feb-20 for example to date """
    last_day = calendar.monthrange(input_dt.year, input_dt.month)[1]
    dt = input_dt.replace(day=last_day)
    return dt.strftime('%Y-%m-%d')


def format_df(df, cur_dt):
    columns = ['project_name', 'street_name', 'developer', 'property_type', 'locality', 'total_num_units_in_project',
               'cumulative_units_launched_to_date', 'cumulative_units_sold_to_date', 'total_num_unsold_units',
               'cumulative_units_launched_unsold', 'units_launched_month', 'units_sold_month', 'median_price_psf',
               'lowest_price_psf', 'highest_price_psf']
    df.columns = columns
    df['reference_period'] = convert_str_to_date(cur_dt)
    df['median_price_psf'] = df['median_price_psf'].apply(lambda x: x.split(':')[-1].strip())
    return df


def scrape_summary_data(get_url, end_dt, token):
    """
    this function is used to scrape the data from the ura website
     get_url: the starting url to retrieve the cookie from
     post_url: the url to actually post the request
     end_year: a value, e.g. 2020
     end_month: indexed from 1. i.e. jan is 1
    """
    dest_folder = 'data'
    start_dt = datetime(2007, 6, 1)

    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)

    query_param = {
        'service': 'PMI_Resi_Developer_Sales',
        'refPeriod': ''
    }

    headers = {
        'AccessKey': ACCESS_KEY,
        'Token': token
    }

    data = pd.DataFrame()
    while start_dt < end_dt:
        print(f"Scraping for {start_dt.strftime('%Y-%m')}...")

        query_param['refPeriod'] = start_dt.strftime('%m%y')

        resp_json = requests.get(get_url, params=query_param, headers=headers).json()
        if resp_json:
            resp_json['Result']
        try:
            df = pd.read_html(resp.text)[0]
            print(f"Retrieved {len(df)} rows of data!")
        except ValueError as e:
            print(f"Unable to scrape for year {cur_year} and month {cur_month}. Error: {e}. Ignoring...")
            continue
        df = format_df(df, start_dt)
        df.drop(df.tail(2).index, inplace=True)
        data = data.append(df)
        start_dt += relativedelta(months=1)

    dest_path = os.path.join(dest_folder, f'transaction_summary.csv')
    data.to_csv(dest_path, index=False)

def get_token():
    headers = {
        "AccessKey": ACCESS_KEY
    }
    resp = requests.get('https://www.ura.gov.sg/uraDataService/insertNewToken.action', headers=headers)
    return resp.json()['Result']

def main():
    # Information on the preceding month's transactions (e.g. in Apr 2013) will be uploaded on the e-Service on 15th
    # of the following month (e.g. in May 2013). If the scheduled date of update falls on a public holiday,
    # it will be updated on the following working day.
    current_dt = datetime.today()
    if current_dt.day > 20:
        end_dt = current_dt - relativedelta(months=1)
    else:
        end_dt = current_dt - relativedelta(months=2)

    # scrape for condos data
    token = get_token()
    print(f"Scraping transaction summary data until {end_dt}")
    get_url = "https://www.ura.gov.sg/uraDataService/invokeUraDS"
    scrape_summary_data(get_url, end_dt)


if __name__ == '__main__':
    main()
