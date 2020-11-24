import logging
import os
from datetime import datetime
from time import perf_counter, sleep

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup

from data_scraping.config import URA_WEBSITE_TX_GET_URL, URA_WEBSITE_TX_POST_URL
from utils import setup_logger

logger = logging.getLogger(__name__)

PROPERTY_TYPE_CODE = {
    'Landed Properties': 'lp',
    'Strata Landed': 'sl',
    'Apartments and Condo': 'ac',
    'Executive Condo': 'ec'
}


def get_district_postal_code_list(soup):
    district_postal_code_list = []
    for item in soup.find(id='select1').children:
        item_string = item.string
        if item_string != '\n':
            district_postal_code_list.append(item_string)
    return district_postal_code_list


def get_start_dt_end_dt(soup):
    start_dt_list = []
    end_dt_list = []

    for item in soup.find(id='searchForm_selectedFromPeriodPostalDistrict').children:
        item_string = item.string
        if item_string != '\n':
            start_dt_list.append(item_string)

    for item in soup.find(id='searchForm_selectedToPeriodPostalDistrict').children:
        item_string = item.string
        if item_string != '\n':
            end_dt_list.append(item_string)

    # get min of start_dt and max of end_dt
    start_dt = datetime.strftime(min([datetime.strptime(dt, '%b %Y') for dt in start_dt_list]), '%b %Y').upper()
    end_dt = datetime.strftime(max([datetime.strptime(dt, '%b %Y') for dt in end_dt_list]), '%b %Y').upper()

    return start_dt, end_dt


def scrape_transaction_data(get_url, post_url):
    """
    this function is used to scrape the data from the ura website
     get_url: the starting url to retrieve the cookie from and the postal district + start date and end date
     post_url: the url to actually post the request
    """
    session = requests.Session()
    resp = session.get(get_url)

    soup = BeautifulSoup(resp.text, 'lxml')

    district_postal_code_list = get_district_postal_code_list(soup)

    start_dt, end_dt = get_start_dt_end_dt(soup)

    # Sleep is necessary here to somehow allow their backend to persist the cookie first
    sleep(2)

    cookies = requests.utils.dict_from_cookiejar(session.cookies)
    post_url = post_url + ';jsessionid=' + cookies['JSESSIONID']

    form_data = {
        'submissionType': 'pd',
        'selectedFromPeriodProjectName': start_dt,
        'selectedToPeriodProjectName': end_dt,
        '__multiselect_selectedProjects1': "",
        'selectedFromPeriodPostalDistrict': start_dt,
        'selectedToPeriodPostalDistrict': end_dt,
        'propertyType': "",
        'postalDistrictList': "28",
        'selectedPostalDistricts1': None,
        '__multiselect_selectedPostalDistricts1': ''
    }
    total_df = pd.DataFrame()
    for property_type, property_code in PROPERTY_TYPE_CODE.items():
        logger.info(f"Retrieving data for property type {property_type}")
        for district in district_postal_code_list:
            logger.info(f"Retrieving data for district: {district}")
            form_data['selectedPostalDistricts1'] = district
            form_data['propertyType'] = property_code
            resp = session.post(post_url, form_data)

            try:
                df = pd.read_html(resp.text)[0]
                logger.info(f"Retrieved {len(df)} rows of data!")
            except ValueError:
                logger.error(
                    f"Unable to scrape for district {district} and start date {start_dt}, end date {end_dt}. Ignoring...")
                continue
            df = format_df(df)
            total_df = total_df.append(df)

            # sleep here to prevent spamming the website cause we're nice people
            sleep(1)
    return total_df


def format_df(df):
    columns_mapping = {
        'Project Name': 'project_name',
        'Street Name': 'street_name',
        'Type': 'property_type',
        'Postal District': 'postal_district',
        'Market Segment': 'market_segment',
        'Tenure': 'tenure',
        'Type of Sale': 'type_of_sale',
        'No. of  Units': 'num_units',
        'Price  ($)': 'price',
        'Nett Price  ($)': 'nett_price',
        'Area (Sqft)¹': 'area_sqft',
        'Type of Area²': 'type_of_area',
        'Floor Level': 'floor',
        'Unit Price ($psf)³': 'unit_price_psf',
        'Date of Sale⁴': 'reference_period',
    }
    df.rename(columns=columns_mapping, inplace=True)
    df['reference_period'] = pd.to_datetime(df['reference_period'], format='%b-%y')
    df['reference_period'] = df['reference_period'] + pd.DateOffset(months=1, days=-1)

    df['area_sqm'] = df['area_sqft'] / 10.764
    df['unit_price_psm'] = df['unit_price_psf'] * 10.764

    return df


def save_df_to_csv(file_path, df):
    subset_cols = ['project_name', 'street_name', 'property_type', 'postal_district', 'market_segment', 'tenure', 'type_of_sale',
                   'num_units', 'price', 'nett_price', 'area_sqft', 'type_of_area', 'floor', 'unit_price_psf']
    logger.info("Saving df to csv...")
    if os.path.isfile(file_path):
        # update the dataframe here and save it again
        existing_df = pd.read_csv(file_path, float_precision='round_trip')
        logger.info(
            f"Found existing file with {len(existing_df)} rows... Combining the data and dropping duplicates...")

        combined_df = existing_df.append(df)
        combined_df.drop_duplicates(subset=subset_cols, inplace=True)
        combined_df.to_csv(file_path, index=False)
        logger.info(f"Saved combined data with {len(combined_df)} rows.")
    else:
        logger.info(f"No existing file... Dropping duplicates from df of length {len(df)}...")
        df.drop_duplicates(subset=subset_cols, inplace=True)
        df.sort_values('reference_period', ascending=False, inplace=True)
        logger.info(f"Saving {len(df)} rows to {file_path}")
        df.to_csv(file_path, index=False)


@click.command()
@click.option('--log-dir', default=None, help='Specify the file path for the log file')
def main(log_dir):
    start_time = perf_counter()
    setup_logger(logger, log_dir)
    logger.info(f"Starting {__file__} with args: {log_dir}")

    # We use the URA API here because it gives us the x y coordinates (more data essentially)
    get_url, post_url = URA_WEBSITE_TX_GET_URL, URA_WEBSITE_TX_POST_URL
    dest_folder = os.path.abspath('../data')
    dest_file_path = os.path.join(dest_folder, 'total_transactions.csv')
    os.makedirs(dest_folder, exist_ok=True)

    df = scrape_transaction_data(get_url, post_url)
    save_df_to_csv(dest_file_path, df)

    duration = perf_counter() - start_time
    logger.info(f"Program ended. Total duration {duration}")


if __name__ == '__main__':
    main()
