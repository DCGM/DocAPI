import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)

user_router = APIRouter()
worker_router = APIRouter()
admin_router = APIRouter()
debug_router = APIRouter()
router = APIRouter()

from .user_routes import user_router
from .worker_routes import worker_router
from .admin_routes import admin_router
from .debug_routes import debug_router
