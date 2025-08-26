import hashlib
import json
import logging
import os
from xml.etree.ElementTree import ParseError
from defusedxml import ElementTree as ET

import cv2
import numpy as np
from fastapi import Depends, UploadFile, HTTPException, status

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes.route_guards import challenge_key_access_to_job
from metakat.app.api.authentication import require_api_key
from metakat.app.api.cruds import cruds
from metakat.app.api.database import get_async_session
from metakat.app.api.schemas import base_objects
from metakat.app.db import model
from metakat.app.api.routes import router
from metakat.app.config import config

from typing import List, Optional, Tuple
from uuid import UUID


from schemas.base_objects import MetakatIO, ProarcIO

logger = logging.getLogger(__name__)


# USER
require_user_key = require_api_key()
################################################
@router.get("/me", tags=["User"])
async def me(key: model.Key = Depends(require_user_key)):
    return key.label

# JOB
################################################
@router.post("/job", response_model=base_objects.Job, tags=["User"])
async def create_job(job_definition: cruds.MetakatJobDefinition,
         key: model.Key = Depends(require_user_key),
         db: AsyncSession = Depends(get_async_session)):
    job = await cruds.create_job(db, key.id, job_definition)
    return base_objects.Job.model_validate(job)

@router.post("/proarc_json/{job_id}", tags=["User"])
async def upload_proarc_json(job_id: UUID, proarc_json: ProarcIO,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_key_access_to_job(db, key, job_id)
    db_job = await cruds.get_job(db, job_id)
    if not db_job.proarc_json_required:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "PROARC_NOT_REQUIRED", "message": f"Job '{job_id}' does not require Proarc JSON"},
        )
    if db_job.proarc_json_uploaded:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "PROARC_ALREADY_UPLOADED", "message": f"Job '{job_id}' already has Proarc JSON uploaded"},
        )
    batch_path = os.path.join(config.BATCH_UPLOADED_DIR, str(job_id))
    os.makedirs(batch_path, exist_ok=True)
    proarc_json_path = os.path.join(batch_path, "proarc.json")
    with open(proarc_json_path, "w", encoding="utf-8") as f:
        proarc_json_dict = proarc_json.model_dump(mode="json")
        json.dump(proarc_json_dict, f, ensure_ascii=False, indent=4)
    db_job.proarc_json_uploaded = True
    await db.commit()
    return {"code": "PROARC_UPLOADED", "message": f"Proarc JSON for job '{job_id}' uploaded successfully"}

@router.post("/image/{job_id}/{name}", tags=["User"])
async def upload_image(job_id: UUID, name: str, file: UploadFile,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_key_access_to_job(db, key, job_id)
    db_image = await cruds.get_image_by_job_and_name(db, job_id, name)
    if db_image.image_uploaded:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "IMAGE_ALREADY_UPLOADED", "message": f"Image '{name}' for Job '{job_id}' already uploaded"},
        )
    batch_path = os.path.join(config.BATCH_UPLOADED_DIR, str(job_id))
    os.makedirs(batch_path, exist_ok=True)
    image_path = os.path.join(batch_path, f'{db_image.id}.jpg')

    raw_input = file.file.read()
    contents = np.asarray(bytearray(raw_input), dtype="uint8")
    image = cv2.imdecode(contents, cv2.IMREAD_COLOR)
    if image is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "INVALID_IMAGE", "message": f"Failed to decode image '{file.filename}' for Job '{job_id}', probably not a valid image file"}
        )
    imagehash = hashlib.md5(raw_input).hexdigest()

    cv2.imwrite(image_path, image)

    db_image.imagehash = imagehash
    db_image.image_uploaded = True
    await db.commit()
    return {"code": "IMAGE_UPLOADED", "message": f"Image '{name}' for Job '{job_id}' uploaded successfully"}

ALLOWED_NS = {
    "http://www.loc.gov/standards/alto/ns-v2#",
    "http://www.loc.gov/standards/alto/ns-v3#",
    "http://www.loc.gov/standards/alto/ns-v4#",  # v4.x
}

def _localname(tag: str) -> str:
    # "{ns}alto" -> "alto", "alto" -> "alto"
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def _namespace(tag: str) -> Optional[str]:
    # "{ns}alto" -> "ns", "alto" -> None
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else None

def validate_alto_basic(xml_bytes: bytes) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Returns (ok, namespace, schema_version_attr).
    ok = syntactically valid XML, root is <alto>, ns allowed, has <Layout>.
    """
    try:
        root = ET.fromstring(xml_bytes)  # safe parse (defusedxml)
    except ParseError:
        return False, None, None

    if _localname(root.tag) != "alto":
        return False, None, None

    ns = _namespace(root.tag)
    if ns and ns not in ALLOWED_NS:
        return False, ns, None

    # optional but useful structural check
    layout = root.find(".//{*}Layout")
    if layout is None:
        return False, ns, None

    # many ALTOs have SCHEMAVERSION attribute
    schema_ver = root.attrib.get("SCHEMAVERSION") or root.attrib.get("VERSION")
    return True, ns, schema_ver

@router.post("/alto/{job_id}/{name}", tags=["User"])
async def upload_alto(job_id: UUID,
        name: str,
        file: UploadFile,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_key_access_to_job(db, key, job_id)
    db_image = await cruds.get_image_by_job_and_name(db, job_id, name)
    db_job = await cruds.get_job(db, job_id)
    if not db_job.alto_required:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "ALTO_NOT_REQUIRED", "message": f"Job '{job_id}' does not require ALTO"},
        )
    if db_image.alto_uploaded:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "ALTO_ALREADY_UPLOADED", "message": f"ALTO for image '{name}' for Job '{job_id}' already uploaded"},
        )

    # read once (async) and validate
    data = await file.read()

    ok, ns, schema_ver = validate_alto_basic(data)
    if not ok:
        ns_info = f" (ns: {ns})" if ns else ""
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "INVALID_ALTO_XML", "message": f"Not a valid ALTO XML{ns_info} or missing <Layout>"},
        )

    batch_path = os.path.join(config.BATCH_UPLOADED_DIR, str(job_id))
    os.makedirs(batch_path, exist_ok=True)
    alto_path = os.path.join(batch_path, f"{db_image.id}.xml")

    with open(alto_path, "wb") as f:
        f.write(data)

    db_image.alto_uploaded = True
    await db.commit()

    msg = f"ALTO for image '{name}' for Job '{job_id}' uploaded successfully"
    if schema_ver:
        msg += f" (schema {schema_ver})"
    return {"code": "ALTO_UPLOADED", "message": f"{msg}"}

@router.get("/job/{job_id}", response_model=base_objects.Job, tags=["User"])
async def get_job(job_id: UUID,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_key_access_to_job(db, key, job_id)
    db_job = await cruds.get_job(db, job_id)
    return base_objects.Job.model_validate(db_job)

@router.get("/jobs", response_model=List[base_objects.Job], tags=["User"])
async def get_jobs(
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    return

@router.put("/cancel/{job_id}", tags=["User"])
async def cancel_job(job_id: UUID,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    return

# RESULTS
################################################
@router.get("/result/{job_id}", response_model=MetakatIO, tags=["User"])
async def get_result(job_id: UUID,
        key: model.Key = Depends(require_user_key),
        db: AsyncSession = Depends(get_async_session)):
    return

# ADMIN
require_admin_key = require_api_key(admin=True)
################################################
@router.get("/keys", response_model=List[base_objects.Key], tags=["Admin"])
async def get_keys(
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    db_keys = await cruds.get_keys(db)
    return [base_objects.Key.model_validate(db_key) for db_key in db_keys]

@router.get("/generate_key/{label}", response_model=str, tags=["Admin"])
async def new_key(label: str,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    return await cruds.new_key(db, label)

@router.put("/update_key", tags=["Admin"])
async def update_key(key_update: base_objects.KeyUpdate,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    await cruds.update_key(db, key_update)
