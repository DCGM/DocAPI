"""Insert synthetic finished jobs into an existing, migrated PostgreSQL database.

Run from the repository root with server dependencies installed:
    python -m scripts.seed_dummy_database postgresql://postgres:postgres@localhost:5433/doc_api_db

Only DONE/FAILED jobs are inserted. No files, notifications or runnable work are
created. Dummy engines are inactive and key secrets are unusable by default.
Deleted-file timestamps reflect that the synthetic files do not exist. Each run has its own
label prefix and adds records without modifying existing data. All synthetic job,
key and engine UUIDs begin with d00df00d-. Use --clear to remove these records.
"""
import argparse
import asyncio
import re
from datetime import datetime, timedelta, timezone
import random
import secrets
from uuid import UUID, uuid4

from sqlalchemy import String, cast, delete, insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.db import model


DUMMY_UUID_PREFIX = 'd00df00d'


def dummy_uuid():
    return UUID(DUMMY_UUID_PREFIX + uuid4().hex[8:])


def database_engine(database_url):
    url = make_url(database_url)
    if url.get_backend_name() != 'postgresql':
        raise ValueError('A PostgreSQL database URL is required.')
    return create_async_engine(url.set(drivername='postgresql+asyncpg'))


async def clear_database(database_url):
    """Remove only the UUID namespace reserved for dummy database records."""
    engine = database_engine(database_url)
    def dummy(column):
        return cast(column, String).like(DUMMY_UUID_PREFIX + '-%')
    try:
        async with engine.begin() as connection:
            await connection.execute(delete(model.Image).where(dummy(model.Image.job_id)))
            # Job notifications have ON DELETE CASCADE.
            jobs = await connection.execute(delete(model.Job).where(dummy(model.Job.id)))
            engines = await connection.execute(delete(model.Engine).where(dummy(model.Engine.id)))
            keys = await connection.execute(delete(model.Key).where(dummy(model.Key.id)))
            return dict(jobs=jobs.rowcount, engines=engines.rowcount, keys=keys.rowcount)
    finally:
        await engine.dispose()


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError('Must be at least 1.')
    return value


async def seed_database(database_url, *, jobs=10000, owners=40, workers=20, engines=8,
                        versions=3, days=30, seed=42, prefix=None, usable_keys=False):
    rng = random.Random(seed)
    prefix = prefix or f'dummy-db-{uuid4().hex[:8]}'
    if usable_keys and not re.fullmatch(r'[A-Za-z0-9_-]+', prefix):
        raise ValueError('Usable dummy credentials require a URL-safe label prefix.')
    if usable_keys and max(len(f'{prefix}-{role}-{count:02}') for role, count in [('user', owners), ('worker', workers)]) > 32:
        raise ValueError('Usable dummy key names must fit the 32-character key ID limit; choose a shorter prefix.')
    if usable_keys:
        from doc_api.api.authentication import salted_hmac_sha256_hex
    now = datetime.now(timezone.utc)
    owner_ids = [dummy_uuid() for _ in range(owners)]
    worker_ids = [dummy_uuid() for _ in range(workers)]
    engine_ids = [dummy_uuid() for _ in range(engines * versions)]
    keys = []
    for role, ids in [(KeyRole.USER, owner_ids), (KeyRole.WORKER, worker_ids)]:
        for index, key_id in enumerate(ids):
            keys.append(dict(id=key_id, label=f'{prefix}-{role.value}-{index + 1:02}', role=role,
                             kid=uuid4().hex, salt=secrets.token_hex(32), key_hash=secrets.token_hex(32),
                             active=True, email_notifications=False,
                             created_date=now - timedelta(days=days + 1),
                             last_used=now - timedelta(seconds=rng.randint(5, 3600))))
            if usable_keys:
                key = keys[-1]
                key['kid'] = key['label']
                key['key_hash'] = salted_hmac_sha256_hex(key['label'], key['salt'])
    engine_rows = [dict(id=engine_id, name=f'{prefix}-engine-{index // versions + 1:02}',
                        version=f'{index % versions + 1}.0', description='Synthetic test engine.',
                        definition=None, active=False, default=False)
                   for index, engine_id in enumerate(engine_ids)]
    done = failed = 0
    engine = database_engine(database_url)
    try:
        # All inserts commit together; a failure leaves no partial filler dataset.
        async with engine.begin() as connection:
            await connection.execute(insert(model.Key), keys)
            await connection.execute(insert(model.Engine), engine_rows)
            batch = []
            for index in range(jobs):
                state = ProcessingState.DONE if rng.random() < 0.8 else ProcessingState.FAILED
                done += state == ProcessingState.DONE
                failed += state == ProcessingState.FAILED
                # Uneven group sizes make top-N comparisons meaningful.
                owner_index = int(rng.triangular(0, owners, 0))
                worker_index = int(rng.triangular(0, workers, 0))
                engine_index = int(rng.triangular(0, len(engine_ids), 0))
                image_count = rng.randint(1, 80)
                previous = rng.choices([0, 1, 2, 3, 4], weights=[65, 20, 8, 5, 2])[0]
                runtime = image_count * rng.uniform(0.5, 12) * (previous + 1) + rng.uniform(0, 120)
                finished = now - timedelta(seconds=rng.uniform(0, days * 86400))
                started = finished - timedelta(seconds=runtime)
                attempt = str(previous)
                batch.append(dict(
                    id=dummy_uuid(), owner_key_id=owner_ids[owner_index],
                    worker_key_id=worker_ids[worker_index] if rng.random() > 0.03 else None,
                    engine_id=engine_ids[engine_index] if rng.random() > 0.02 else None,
                    definition={'dummy_records': prefix}, image_count=image_count,
                    state=state, progress=1.0 if state == ProcessingState.DONE else rng.uniform(0, 0.95),
                    previous_attempts=previous, created_date=started - timedelta(seconds=rng.uniform(1, 600)),
                    started_date=started, finished_date=finished, last_change=finished,
                    inputs_deleted_at=now, results_deleted_at=now, artifacts_deleted_at=now,
                    log={attempt: f'Synthetic processing: {image_count} images; attempt {previous + 1}.'},
                    log_user={attempt: 'Synthetic processing log for database previews.'},
                    final_log={attempt: f'Synthetic job finished with state {state.value.upper()}.'},
                    final_log_user={attempt: 'Synthetic output; no files were generated.'},
                ))
                if len(batch) == 1000 or index == jobs - 1:
                    await connection.execute(insert(model.Job), batch)
                    batch.clear()
    finally:
        await engine.dispose()
    return dict(prefix=prefix, jobs=jobs, done=done, failed=failed, owners=owners,
                workers=workers, engines=engines, versions=engines * versions)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('database_url', help='Explicit PostgreSQL URL for an existing migrated database.')
    for name, default in [('jobs', 10000), ('owners', 40), ('workers', 20), ('engines', 8), ('versions', 3), ('days', 30)]:
        parser.add_argument(f'--{name}', type=positive, default=default)
    parser.add_argument('--clear', action='store_true', help='Remove all d00df00d- UUID records instead of inserting.')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducible distributions.')
    parser.add_argument('--prefix', help='Unique label prefix; defaults to dummy-db-<random suffix>.')
    parser.add_argument('--usable-keys', action='store_true', help='Use dummy key names as both key IDs and secrets (PREFIX.name.name); requires the server HMAC_SECRET environment setting.')
    args = parser.parse_args()
    if args.prefix is not None and (not args.prefix.strip() or len(args.prefix) > 220):
        parser.error('--prefix must be nonempty and at most 220 characters.')
    if args.clear and args.usable_keys:
        parser.error('--usable-keys cannot be combined with --clear.')
    if args.usable_keys and args.prefix is not None and not re.fullmatch(r'[A-Za-z0-9_-]+', args.prefix):
        parser.error('--usable-keys requires a prefix containing only letters, digits, underscores or hyphens.')
    try:
        clear = vars(args).pop('clear')
        if clear:
            result = asyncio.run(clear_database(args.database_url))
        else:
            result = asyncio.run(seed_database(**vars(args)))
    except Exception as error:
        # Do not echo the URL or a database exception containing credentials.
        parser.exit(1, f'Database operation failed ({type(error).__name__}); transaction rolled back. Check database connectivity, schema and prefix uniqueness.\n')
    if clear:
        print(f"Removed {result['jobs']} dummy jobs, {result['keys']} dummy keys and {result['engines']} dummy engine versions.")
        return
    print(f"Inserted {result['jobs']} jobs ({result['done']} DONE, {result['failed']} FAILED), "
          f"{result['owners']} owners, {result['workers']} workers, "
          f"{result['engines']} engines / {result['versions']} versions. Prefix: {result['prefix']}")


if __name__ == '__main__':
    main()
