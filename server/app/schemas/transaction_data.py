from datetime import date
from typing import Union

from pydantic import BaseModel


class TransactionData(BaseModel):
    project_name: str
    street_name: str
    property_type: str
    postal_district: int
    market_segment: str
    tenure: str
    type_of_sale: str
    num_units: int
    price: int
    nett_price: Union[int, str]
    area_sqft: int
    type_of_area: str
    floor: str
    unit_price_psf: int
    reference_period: date
    area_sqm: float
    unit_price_psm: float