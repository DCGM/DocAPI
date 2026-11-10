"""Running build identity and last schema change; Alembic owns the revision."""
from datetime import datetime, timezone
import logging

from sqlalchemy import inspect, select, update

from doc_api.db.model import DatabaseMetadata

MIGRATION_LOCK = 0x444F43415049
logger = logging.getLogger(__name__)


def get_migration_metadata(connection):
    # Databases predating this feature have no recorded commit or migration time.
    if not inspect(connection).has_table(DatabaseMetadata.__tablename__):
        return None
    return connection.execute(select(DatabaseMetadata.git_commit, DatabaseMetadata.migrated_at)).mappings().one_or_none()


def record_migration_metadata(connection, git_commit, *, update_commit=True):
    if inspect(connection).has_table(DatabaseMetadata.__tablename__):
        values = {"migrated_at": datetime.now(timezone.utc)}
        if update_commit:
            values["git_commit"] = git_commit
        connection.execute(update(DatabaseMetadata).where(DatabaseMetadata.id == 1).values(**values))
        if update_commit and git_commit is None:
            logger.warning("Running Git commit is unknown; database code identity cannot be recorded. Set APP_GIT_COMMIT.")


def validate_database_commit(connection, git_commit, require_matching_commit, *, new_database=False):
    """Check identity before any schema or commit changes in strict mode."""
    metadata = get_migration_metadata(connection)
    if require_matching_commit:
        if git_commit is None:
            raise RuntimeError(
                "DATABASE_REQUIRE_MATCHING_COMMIT=True requires a known running Git commit. "
                "Set APP_GIT_COMMIT or run from a Git checkout."
            )
        # A new database has no previous code identity to match.
        if not new_database:
            stored_commit = metadata["git_commit"] if metadata else None
            if stored_commit != git_commit:
                raise RuntimeError(
                    f"Database Git commit {stored_commit or 'unknown'} differs from running DocAPI "
                    f"commit {git_commit} and DATABASE_REQUIRE_MATCHING_COMMIT=True. "
                    "Run the matching DocAPI image or set DATABASE_REQUIRE_MATCHING_COMMIT=False "
                    "to accept the new code version."
                )
    return metadata


def record_running_commit(connection, git_commit, require_matching_commit, allow_update):
    """Update the code identity without changing the last migration timestamp."""
    metadata = validate_database_commit(connection, git_commit, require_matching_commit)
    if not allow_update:
        return
    if metadata is None:
        logger.warning("Cannot record the running Git commit: database migration metadata is missing.")
        return
    if metadata["git_commit"] != git_commit:
        connection.execute(update(DatabaseMetadata).where(DatabaseMetadata.id == 1).values(git_commit=git_commit))
        logger.info("Updated database Git commit from %s to %s.", metadata["git_commit"] or "unknown", git_commit or "unknown")
    if git_commit is None:
        logger.warning("Running Git commit is unknown; database code identity cannot be recorded. Set APP_GIT_COMMIT.")
