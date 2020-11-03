#!/bin/bash
APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"

cd "$APP_ROOT" || exit
TX_LOG_FILE=/home/jamesong/Desktop/Projects/housing/server/logs/%Y/%m/housing_transaction_%Y%m%d.log

env/bin/python data_scraping/get_housing_transactions.py --log-dir=${TX_LOG_FILE}