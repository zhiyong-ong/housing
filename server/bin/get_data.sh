#!/bin/bash
APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"

cd "$APP_ROOT" || exit
TX_LOG_FILE=/home/jamesong/Desktop/Projects/housing/server/logs/%Y/%m/housing_transaction_%Y%m%d.log
TX_SUMMARY_LOG_FILE=/home/jamesong/Desktop/Projects/housing/server/logs/%Y/%m/housing_transaction_summary_%Y%m%d.log
exec \
  env/bin/python get_housing_transactions.py --log-dir=${TX_LOG_FILE} && env/bin/python get_housing_transactions_summary.py --log-dir=${TX_SUMMARY_LOG_FILE}