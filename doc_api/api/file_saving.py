"""Coordinate staged file replacements and database metadata updates."""

import asyncio
import fcntl
import logging
import os
import stat
import zipfile
from pathlib import Path
from uuid import uuid4
from contextlib import asynccontextmanager, suppress

import aiofiles
import fastapi
from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
from doc_api.api.guards.user_guards import _challenge_user_access_to_new_job
from doc_api.api.guards.worker_guards import _challenge_worker_access_to_processing_job
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.schemas.responses import AppCode, DocAPIClientErrorException, GENERAL_RESPONSES
from doc_api.db import model


logger = logging.getLogger(__name__)


def temporary_file_path(path, *, purpose):
    """Keep the official filename and explain why this temporary sibling exists."""
    if purpose not in {"upload-staging", "rollback-backup"}:
        raise ValueError(f"Unknown temporary file purpose: {purpose}")
    # Retain the extension at the end for format-sensitive writers such as OpenCV.
    return f"{path}.docapi-tmp-{purpose}-{uuid4().hex}{os.path.splitext(path)[1]}"


class FileSaveTransaction:
    """Keep filesystem and DB recovery under the same destination lock.

    Preparation holds only a filesystem lock. Installation borrows a DB
    connection and locks the job/engine row until commit or rollback. The route
    writes/validates staging first, then installs and updates metadata using db.
    """

    def __init__(self, *, path, db, entity, record_id, key, kind, image_id=None):
        self.path = path
        self.db = db
        self.entity = entity
        self.record_id = record_id
        self.key = key
        self.kind = kind
        self.image_id = image_id
        self.staged = temporary_file_path(path, purpose="upload-staging")
        self.backup = None
        self.installed = False
        self.existed = False
        self.record = None
        self.image = None
        self.lock_descriptor = None
        self.commit_started = False
        self.committed = False

    async def lock_and_check_record(self):
        """Recheck permissions/state under the row lock before replacing bytes."""
        query = select(self.entity).where(self.entity.id == self.record_id).execution_options(populate_existing=True)
        # This is the first DB operation in the save: only now is a pooled
        # connection borrowed. The row lock lasts through commit or rollback.
        query = query.with_for_update()
        self.record = await self.db.scalar(query)
        if self.record is None:
            code = AppCode.JOB_NOT_FOUND if self.entity is model.Job else AppCode.ENGINE_NOT_FOUND
            raise DocAPIClientErrorException(status=fastapi.status.HTTP_404_NOT_FOUND, code=code,
                                             detail=GENERAL_RESPONSES[code]["detail"])
        if self.entity is model.Job:
            if self.kind == "inputs" and self.key.role in {KeyRole.USER, KeyRole.MANAGER}:
                await _challenge_user_access_to_new_job(db=self.db, key=self.key, job_id=self.record_id)
            elif self.kind != "inputs" and self.key.role in {KeyRole.WORKER, KeyRole.MANAGER}:
                await _challenge_worker_access_to_processing_job(db=self.db, key=self.key, job_id=self.record_id)
        if self.image_id is not None:
            self.image = await self.db.get(model.Image, self.image_id, populate_existing=True)
            if self.image is None:
                raise DocAPIClientErrorException(status=fastapi.status.HTTP_404_NOT_FOUND,
                    code=AppCode.IMAGE_NOT_FOUND_FOR_JOB, detail="The specified image was not found for this job.")

    def acquire_lock(self):
        """Reject a competing save without occupying a database connection."""
        destination = Path(self.path)
        # Keep the lock inode stable: unlinking it after release would let two
        # processes lock different inodes for the same destination. The empty
        # sibling is coordination metadata, not a staging or backup file.
        lock_path = destination.with_name(f".{destination.name}.docapi-save.lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            response = GENERAL_RESPONSES[AppCode.FILE_SAVE_IN_PROGRESS]
            raise DocAPIClientErrorException(status=response["status"], code=AppCode.FILE_SAVE_IN_PROGRESS,
                                             detail=response["detail"])
        except BaseException:
            os.close(descriptor)
            raise
        self.lock_descriptor = descriptor

    def prepare(self):
        self.acquire_lock()
        self.create_staging()

    def release_lock(self):
        if self.lock_descriptor is not None:
            # Closing releases flock, including when the process exits.
            os.close(self.lock_descriptor)
            self.lock_descriptor = None

    def create_staging(self):
        destination = Path(self.path)
        backup_pattern = destination.name + ".docapi-tmp-rollback-backup-*"
        backups = list(destination.parent.glob(backup_pattern))
        if backups:
            raise DBError(f"File save requires recovery of rollback backup: {backups[0]}")
        # With the destination lock held, these can only be incomplete uploads
        # abandoned by a previous process. No metadata update preceded install.
        for abandoned in destination.parent.glob(destination.name + ".docapi-tmp-upload-staging-*"):
            abandoned.unlink()
        try:
            previous = os.stat(self.path, follow_symlinks=False)
            mode = stat.S_IMODE(previous.st_mode) if stat.S_ISREG(previous.st_mode) else None
        except FileNotFoundError:
            mode = None
        descriptor = os.open(self.staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        with os.fdopen(descriptor, "wb") as placeholder:
            if mode is not None:
                os.fchmod(placeholder.fileno(), mode)

    async def install(self):
        # Take the row lock only once the file is complete, so slow uploads do
        # not block progress requests. Cleanup cannot remove it during commit.
        await self.lock_and_check_record()
        self.existed = os.path.lexists(self.path)
        if self.existed:
            backup = temporary_file_path(self.path, purpose="rollback-backup")
            os.replace(self.path, backup)
            self.backup = backup
        os.replace(self.staged, self.path)
        self.installed = True

    def restore(self):
        # Rename back over the failed new version. If restoration itself fails,
        # leave the backup on disk for recovery rather than discarding it.
        if self.backup is not None:
            os.replace(self.backup, self.path)
            self.backup = None
        elif self.installed:
            os.remove(self.path)
        self.installed = False

    def discard_backup(self):
        if self.backup is not None:
            try:
                os.remove(self.backup)
            except OSError:
                # The DB has committed; cleanup failure must not undo that data
                # or misleadingly report that the file save failed.
                logger.exception("Committed file save retained rollback backup %s", self.backup)

    async def prepare_staging(self):
        preparing = asyncio.create_task(asyncio.to_thread(self.prepare))
        try:
            await asyncio.shield(preparing)
        except asyncio.CancelledError:
            # Cancelling an await cannot stop its filesystem thread. Wait until
            # it finishes before cleanup removes staging or releases its lock.
            with suppress(Exception):
                await preparing
            raise

    async def commit(self):
        self.commit_started = True
        committing = asyncio.create_task(self.db.commit())
        try:
            await asyncio.shield(committing)
        except asyncio.CancelledError:
            # The DB might already be committing. Resolve its outcome before
            # deciding whether to keep the new file or restore the old one.
            await committing
            self.committed = True
            raise
        self.committed = True

    def _commit_outcome_unknown(self, error):
        # Once commit starts, a lost connection/acknowledgement does not prove
        # rollback. Restoring old bytes could contradict committed DB metadata.
        return self.commit_started and (
            isinstance(error, (OSError, ConnectionError, asyncio.CancelledError)) or
            (isinstance(error, DBAPIError) and error.connection_invalidated)
        )

    async def recover(self, error):
        if self.committed:
            # Cancellation after a confirmed commit must keep the saved data.
            self.discard_backup()
            return

        try:
            if self._commit_outcome_unknown(error):
                logger.exception("File save commit outcome is unknown for %s; retained backup %s",
                                 self.path, self.backup)
            else:
                # Restore BEFORE rollback releases the job row lock. Otherwise,
                # readiness could queue a previously uploaded job while its file
                # still contains bytes from the failed replacement.
                self.restore()
        finally:
            if self.db.in_transaction():
                await self.db.rollback()

    def cleanup(self):
        try:
            with suppress(FileNotFoundError):
                os.remove(self.staged)
        finally:
            self.release_lock()


async def write_zip_to_staging(file: UploadFile, path, *, validate, invalid_code, invalid_detail):
    """Stream and validate a ZIP before the save borrows a DB connection."""
    async with aiofiles.open(path, "wb") as staging:
        while chunk := await file.read(1024 * 1024):
            await staging.write(chunk)

    if validate:
        try:
            with zipfile.ZipFile(path):
                pass
        except zipfile.BadZipFile as error:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                code=invalid_code,
                detail=invalid_detail,
                details={"reason": str(error)},
            ) from error


@asynccontextmanager
async def file_save_transaction(*, db, path, entity, record_id, key, kind, image_id=None):
    """Prepare -> caller writes/validates/installs/updates DB -> commit -> cleanup.

    Creating save_db does not borrow a connection. The first DB query happens
    in install(), after the caller has finished staging. Returning from inside
    the caller's async-with block still runs our commit before returning a
    response; an exception in that block instead resumes here for recovery.
    """
    async with AsyncSession(bind=db.bind, expire_on_commit=False) as save_db:
        save_db.info["file_save_transaction"] = True
        file_save = FileSaveTransaction(
            path=path, db=save_db, entity=entity, record_id=record_id,
            key=key, kind=kind, image_id=image_id,
        )
        try:
            await file_save.prepare_staging()
            yield file_save
            await file_save.commit()
        except BaseException as error:
            # CancelledError inherits BaseException: cancelled requests need the
            # same restoration and lock cleanup as ordinary failures.
            await file_save.recover(error)
            raise
        else:
            file_save.discard_backup()
        finally:
            file_save.cleanup()
