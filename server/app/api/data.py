import csv
from typing import List

from fastapi import APIRouter
from starlette import status

from app import schemas
from app.config import Settings

router = APIRouter()

BASE_ENDPOINT = ""

TRANSACTION_ENDPOINT = "/transaction"

SUMMARY_ENDPOINT = "/summary"

@router.get(TRANSACTION_ENDPOINT, response_model=List[schemas.TransactionData], status_code=status.HTTP_200_OK)
def get_transaction_data(data_limit: int = 100, pagination: int = 1):
    return_data = []
    with open(Settings.TRANSACTION_DATA_FILE_PATH, 'r') as csvfile:
        data_reader = csv.DictReader(csvfile, delimiter=',')

        for row in data_reader:
            if data_reader.line_num == data_limit:
                break
            return_data.append(row)
    return return_data

