from typing import List

from pydantic import AnyHttpUrl


class Settings:
    PROJECT_NAME: str = 'housing_api'
    API_V1_STR: str = '/api/v1'
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    TRANSACTION_DATA_FILE_PATH = '/home/jamesong/Desktop/Projects/housing/server/data/total_transactions.csv'
    SUMMARY_DATA_FILE_ATH = '/home/jamesong/Desktop/Projects/housing/server/data/transaction_summary.csv'