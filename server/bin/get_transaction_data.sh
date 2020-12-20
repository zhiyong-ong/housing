#!/bin/bash
APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"

cd "$APP_ROOT" || exit
TX_LOG_FILE=/home/jamesong/Projects/housing/server/logs/%Y/%m/housing_transaction_%Y%m%d.log

env/bin/python -m data_scraping.get_housing_transactions --log-dir=${TX_LOG_FILE}