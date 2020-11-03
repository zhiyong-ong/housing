from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.api.api_router import api_router
from app.config import Settings

app = FastAPI(
    title=Settings.PROJECT_NAME, openapi_url=f"{Settings.API_V1_STR}/openapi.json"
)

# Set all CORS enabled origins
if Settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in Settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


app.include_router(api_router, prefix=Settings.API_V1_STR)

