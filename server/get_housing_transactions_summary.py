import calendar
import logging
import os
from datetime import datetime
from time import sleep, perf_counter

import click
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from config import URA_WEBSITE_TX_SUMMARY_GET_URL, URA_WEBSITE_TX_SUMMARY_POST_URL
from utils import setup_logger

logger = logging.getLogger(__name__)


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
        logger.info(f"Scraping for year {cur_year} and month {cur_month}...")

        form_data['yearSelect'] = str(cur_year)
        form_data['monthSelect'] = str(cur_month)

        resp = session.post(post_url, form_data)
        try:
            df = pd.read_html(resp.text)[0]
            logger.info(f"Retrieved {len(df)} rows of data!")
        except ValueError as e:
            logger.info(f"Unable to scrape for year {cur_year} and month {cur_month}. Error: {e}. Ignoring...")
            continue
        df = format_df(df, start_dt)
        df.drop(df.tail(2).index, inplace=True)
        data = data.append(df)
        start_dt += relativedelta(months=1)
    return data


def save_df_to_csv(dest_path, df):
    logger.info(f"Saving {len(df)} rows of data to {dest_path}")
    df.to_csv(dest_path, index=False)


@click.command()
@click.option('--log-dir', default=None, help='Specify the file path for the log file')
def main(log_dir):
    # Information on the preceding month's transactions (e.g. in Apr 2013) will be uploaded on the e-Service on 15th
    # of the following month (e.g. in May 2013). If the scheduled date of update falls on a public holiday,
    # it will be updated on the following working day.
    start_time = perf_counter()

    setup_logger(logger, log_dir)
    logger.info(f"Starting {__file__} with args: {log_dir}")

    current_dt = datetime.today()
    if current_dt.day > 20:
        end_dt = current_dt - relativedelta(months=1)
    else:
        end_dt = current_dt - relativedelta(months=2)
    logger.info(f"Scraping transaction summary data until {end_dt}")

    dest_folder = os.path.abspath('data')
    dest_file_path = os.path.join(dest_folder, 'transaction_summary.csv')
    os.makedirs(dest_folder, exist_ok=True)

    # We use a web scraping method here because the data here has historical data till 2007
    post_url, get_url = URA_WEBSITE_TX_SUMMARY_POST_URL, URA_WEBSITE_TX_SUMMARY_GET_URL
    df = scrape_summary_data(get_url, post_url, end_dt)
    save_df_to_csv(dest_file_path, df)

    duration = perf_counter() - start_time
    logger.info(f"Program ended. Total duration {duration}")


if __name__ == '__main__':
    main()
