import calendar
import os
import sys
from datetime import datetime
from time import sleep

import pandas as pd
import requests

import logging

from dateutil import relativedelta

logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def convert_str_to_date(input_str):
    """ converting dates of Feb-20 for example to date """
    dt = datetime.strptime(input_str, '%b-%y')
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    dt = dt.replace(day=last_day)
    return dt.strftime('%Y-%m-%d')


def format_df(df):
    columns = ['project_name', 'street_name', 'type', 'postal_district', 'market_segment', 'tenure', 'type_of_sale', 'num_units', 'price',
               'nett_price', 'area_sqft', 'type_of_area', 'floor_level', 'unit_price_psf', 'date_of_sale']
    df.columns = columns
    df['date_of_sale'] = df['date_of_sale'].apply(convert_str_to_date)


def scrape_ura_data(get_url, post_url, property_type, start_date, end_date):
    """
    this function is used to scrape the data from the ura website
    :param get_url: the starting url to retrieve the cookie from
    :param post_url: the url to actually post the request
    :param property_type: ac (apartments and condos) or ec (executive condos)
    :param start_date: in the form of MON YEAR. e.g. MAR 2017
    :param end_date: in the form of MON YEAR. e.g. MAR 2017
    :return:
    """
    if property_type == 'ac':
        dest_folder = os.path.join('data', 'apartments_condos')
    elif property_type == 'ec':
        dest_folder = os.path.join('data', 'executive_condos')
    else:
        raise AttributeError("Property type has to be either ec or ac")
    os.makedirs(dest_folder, exist_ok=True)

    max_postal_district_list = 28

    form_data = {
        'submissionType': 'pd',
        'selectedFromPeriodProjectName': start_date,
        'selectedToPeriodProjectName': end_date,
        '__multiselect_selectedProjects1': None,
        'selectedFromPeriodPostalDistrict': start_date,
        'selectedToPeriodPostalDistrict': end_date,
        'propertyType': property_type,
        'postalDistrictList': '28',
        'selectedPostalDistricts1': '01',
        '__multiselect_selectedPostalDistricts1': None
    }
    headers = {
      'Upgrade-Insecure-Requests': '1',
      'Origin': 'https://www.ura.gov.sg',
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    session = requests.Session()
    session.get(get_url, headers=headers)

    # Sleep is necessary here to somehow allow their backend to persist the cookie first
    sleep(5)

    cookies = requests.utils.dict_from_cookiejar(session.cookies)
    post_url = post_url + ';jsessionid=' + cookies['JSESSIONID']
    accumulate_df = pd.DataFrame()
    for i in range(1, max_postal_district_list + 1):
        logger.info(f"Scraping for district {i}...")

        postal_district = f'0{i}' if i < 10 else str(i)
        form_data['selectedPostalDistricts1'] = postal_district
        resp = session.post(post_url, data=form_data, headers=headers)
        try:
            df = pd.read_html(resp.text)[0]
        except ValueError as e:
            logger.info(f"Unable to scrape for district {i}. Error: {e}. Ignoring...")
            continue

        format_df(df)
        dest_path = os.path.join(dest_folder, f'postal_district_{postal_district}.csv')
        if os.path.isfile(dest_path):
            # update the dataframe here and save it again
            existing_df = pd.read_csv(dest_path)
            combined_df = pd.concat([existing_df, df])
            combined_df.drop_duplicates(inplace=True)
            combined_df.to_csv(dest_path, index=False)
            accumulate_df = accumulate_df.append(combined_df)
        else:
            df.to_csv(dest_path, index=False)
            accumulate_df = accumulate_df.append(df)
    accumulate_df.to_csv(os.path.join(dest_folder, 'total.csv'), index=False)

def main():

    # The website only has 3 years worth of data.
    current_dt = datetime.today()
    current_year = current_dt.year
    current_month = current_dt.strftime('%b').upper()
    previous_month = (current_dt - relativedelta.relativedelta(months=1)).strftime('%b').upper()
    start_date_str = f"{current_month} {current_year - 3}"
    end_date_str = f"{previous_month} {current_year}"

    logger.info(f"Scraping from {start_date_str} to {end_date_str} worth of data")
    # scrape for condos data
    logger.info("Scraping condo data")
    ac_post_url = "https://www.ura.gov.sg/realEstateIIWeb/transaction/submitSearch.action"
    ac_get_url = "https://www.ura.gov.sg/realEstateIIWeb/transaction/search.action"
    scrape_ura_data(ac_get_url, ac_post_url, 'ac', start_date_str, end_date_str)


    # scrape for ec data
    logger.info("Scraping EC data")
    ec_post_url = "https://www.ura.gov.sg/realEstateIIWeb/transaction/submitSearch.action"
    ec_get_url = "https://www.ura.gov.sg/realEstateIIWeb/transaction/search.action"
    scrape_ura_data(ec_get_url, ec_post_url, 'ec', start_date_str, end_date_str)


if __name__ == '__main__':
    main()
