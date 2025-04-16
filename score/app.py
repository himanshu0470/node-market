import logging
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse

from fastapi.middleware.cors import CORSMiddleware
from score.settings.base import API_VERSION, APP_DESCRIPTION, APP_TITLE
from score.api.routers import router

LOG = logging.getLogger('app')

async def exception_handler(request: Request, exc: Exception):
    LOG.error("Error Occured: %s", exc)
    return JSONResponse({
        "status_code": 500,
        "error": "Couldn't process your request. Please try in some time."
    })

app = FastAPI(
    version=API_VERSION, 
    title=APP_TITLE, 
    description=APP_DESCRIPTION)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_exception_handler(Exception, exception_handler)

app.include_router(router)