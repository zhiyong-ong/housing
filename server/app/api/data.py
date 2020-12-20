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
def get_transaction_data(limit: int = 100, offset: int = 0):
    print(limit, offset)
    return_data = []
    with open(Settings.TRANSACTION_DATA_FILE_PATH, 'r') as csvfile:
        data_reader = csv.DictReader(csvfile, delimiter=',')

        start_reading = False
        for index, data in enumerate(data_reader):
            if index == limit:
                break
            if index == offset:
                start_reading = True
            if start_reading:
                data['id'] = index
                return_data.append(data)
    return return_data

