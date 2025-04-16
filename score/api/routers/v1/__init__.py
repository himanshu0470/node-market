from fastapi import APIRouter
from .score import router as score_router

router = APIRouter(prefix="/v1")

router.include_router(score_router) 