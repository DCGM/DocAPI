import logging
from fastapi import APIRouter

logger = logging.getLogger(__name__)

root_router = APIRouter()
admin_router = APIRouter()
debug_router = APIRouter()

from . import user_routes, worker_routes, admin_routes, debug_routes