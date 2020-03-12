import calendar
import os
from datetime import datetime, timedelta, date
from time import sleep

import pandas as pd
import requests

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


def scrape_summary_data(get_url, post_url, end_dt):
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

    form_data = {
        'yearSelect': '',
        'monthSelect': '',
    }

    session = requests.Session()
    session.get(get_url)

    # Sleep is necessary here to somehow allow their backend to persist the cookie first
    sleep(3)

    cookies = requests.utils.dict_from_cookiejar(session.cookies)
    post_url = post_url + ';jsessionid=' + cookies['JSESSIONID']

    data = pd.DataFrame()
    while start_dt < end_dt:
        cur_year = start_dt.year
        cur_month = start_dt.month
        print(f"Scraping for year {cur_year} and month {cur_month}...")

        form_data['yearSelect'] = str(cur_year)
        form_data['monthSelect'] = str(cur_month)

        resp = session.post(post_url, form_data)
        try:
            df = pd.read_html(resp.text)[0]
            print(f"Retrieved {len(df)} rows of data!")
        except ValueError as e:
            print(f"Unable to scrape for year {cur_year} and month {cur_month}. Error: {e}. Ignoring...")
            continue
        format_df(df, start_dt)
        data = data.append(df)
        start_dt += relativedelta(months=1)

    dest_path = os.path.join(dest_folder, f'transaction_summary.csv')
    data.to_csv(dest_path, index=False)


def main():
    current_dt = datetime.today()

    if current_dt.day > 20:
        end_dt = current_dt - relativedelta(months=1)
    else:
        end_dt = current_dt - relativedelta(months=2)

    # scrape for condos data
    print(f"Scraping transaction summary data until {end_dt}")
    post_url = "https://www.ura.gov.sg/realEstateIIWeb/price/submitSearch.action"
    get_url = "https://www.ura.gov.sg/realEstateIIWeb/price/search.action"
    scrape_summary_data(get_url, post_url, end_dt)


if __name__ == '__main__':
    main()
