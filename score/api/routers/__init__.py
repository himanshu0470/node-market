from fastapi import APIRouter
from fastapi.responses import JSONResponse

from score.api.routers.v1 import router as v1_router

router = APIRouter(prefix="/api")

router.include_router(v1_router) 

@router.get("/keep-alive", tags=["Health Check"])
async def keep_alive():
    """
    Health check endpoint for service router
    """
    return JSONResponse({"status": "Alive"})
