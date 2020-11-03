from fastapi import APIRouter

from app.api import data

api_router = APIRouter()
api_router.include_router(data.router, prefix='/data', tags=["data"])