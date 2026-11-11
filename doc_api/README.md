# Database compatibility and migrations

DocAPI initializes an empty database on startup. Existing databases are governed
by four independent settings:

| Setting | Default | Behavior |
| --- | --- | --- |
| `DATABASE_REQUIRE_MATCHING_SCHEMA` | `true` | Reject a schema revision that differs from the code. |
| `DATABASE_ALLOW_SCHEMA_UPDATE` | `false` | Allow automatic schema upgrades when the schema guard is off. |
| `DATABASE_REQUIRE_MATCHING_COMMIT` | `false` | Reject an unknown or different code commit. |
| `DATABASE_ALLOW_COMMIT_UPDATE` | `true` | Update the stored code commit when the commit guard permits startup. |

Guards run first and always take precedence over update permissions. To migrate at
startup, set `DATABASE_REQUIRE_MATCHING_SCHEMA=false` and
`DATABASE_ALLOW_SCHEMA_UPDATE=true`. If the code commit changes, its guard must
also be off. An unknown revision, newer schema or incompatible migration branch
cannot be automatically upgraded; the error shows the actual and expected revisions.
With both schema flags off, DocAPI leaves the schema unchanged and skips that
compatibility requirement. Setting all four flags to `false` skips database
preparation entirely, replacing the old `DATABASE_FORCE` setting.

DocAPI does not automatically back up the database or downgrade its schema.

Before starting a new image that includes migrations, stop the API and workers,
record the old image tag or digest, and create a fresh database backup. Keep a copy
of the uploaded data as well if you need to restore the complete previous state.

The deployment example in `compose.yml` includes a scheduled `db-backup`
service. From `doc_api/`, with that deployment's `.env` and storage paths configured:

```sh
docker compose stop doc-api
docker compose up -d db db-backup
docker compose exec -T db-backup /backup.sh
```

The [backup image supports invoking `/backup.sh` manually](https://github.com/prodrigestivill/docker-postgres-backup-local#manual-backups).
Wait for the command to succeed and verify the new backup under the configured
`/backups` host mount (the example uses `/mnt/zfs1/doc_api/db_backup`). Copy this
backup somewhere outside the scheduled backup rotation if you need to retain it
for rollback. Then start the new API image with the schema guard off and schema updates enabled,
or apply `alembic upgrade head` manually before starting it. Run the appropriate
commands for your deployment; do not start the new API image before backing up.

For a local testing deployment without `db-backup`, use the PostgreSQL tools
already present in the `db` container. From the directory containing that
deployment's Compose file:

```sh
docker compose stop doc-api
mkdir -p "$HOME/data/doc_api_backups"
(umask 077; docker compose exec -T db sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "$HOME/data/doc_api_backups/before-migration-$(date -u +%Y%m%dT%H%M%SZ).dump")
```

Check that the dump command succeeded before rebuilding and starting the API.
A failed command can leave a partial dump; do not use it as a backup.

To roll back, stop the API and workers, restore the backup into a clean database,
and start the previous image. Database writes since the backup will be lost.
Restore the separately backed-up application files too if needed; a database dump
does not contain inputs, results, artifacts, or engine ZIPs.

The database's `database_metadata` row records the Git commit of the running code
and the UTC timestamp of the last successful schema migration. By default,
`DATABASE_ALLOW_COMMIT_UPDATE=true` records the commit at startup even when no
migration is needed; `migrated_at` remains unchanged. Set this flag to `false` to
leave the stored commit unchanged, including during schema migrations.
`APP_GIT_COMMIT` identifies the build; `alembic_version` identifies the schema.

Enable `DATABASE_REQUIRE_MATCHING_COMMIT` to require the running code's commit to
match the DB. A mismatch fails before any updates, even when commit or schema
updates are enabled. A new database has no previous identity to match: initial
creation seeds its commit and migration timestamp, regardless of the update flags.
The commit guard still requires a known build commit for initialization.

Pass `COMMIT_SHA` when building Docker images so the build exports
`APP_GIT_COMMIT`; local Python runs resolve it from the Git checkout. When the
running commit is unknown and commit updates are enabled, the DB stores NULL and
logs a warning. Uncommitted changes are not identified by the commit hash.
Keep all instances sharing a DB on the same code revision: the row records the
latest successful startup or migration that allowed commit updates. Take the
pre-upgrade backup before starting the new image to retain the previous commit.

## Admin operations

An active admin key can use every authenticated endpoint and bypasses ownership,
worker assignment, job-state restrictions, and optional-input requirements. Admin
file downloads use the actual file even when upload flags/timestamps are absent.
Request/file validation, record and file existence checks, database constraints,
and concurrency protections still apply.

Use `PATCH /v1/jobs/{job_id}` for admin edits; the separate
`PATCH /v1/admin/jobs/{job_id}` endpoint has been removed. Admins can edit the full
`JobUpdate` schema. State updates return the corresponding operation success
code. Progress/log updates without a state change return renewed lease data for
both workers and admins. Other admin field edits return `JOB_UPDATED`. Process logs
append unless `append_logs=false`; final logs replace the current attempt and
can be edited without changing state. Activity/finalization defaults still apply,
and explicit admin field values override them. Ordinary user/worker requests keep
their existing workflow rules and responses.

`PUT /debug/jobs/{job_id}/start` manually invokes the normal readiness check. It
still requires a NEW job and complete required inputs. Use PATCH to force a job
state directly instead.

Nullable update fields distinguish omission from an explicit `null`. Admin job
edits can reset `previous_attempts`, clear `started_date`/`finished_date`, or clear
the current attempt’s log entry without removing other attempts. The counter
accepts only null or an integer from 0 to 1000. Worker log updates retain their
append/finalization behavior and cannot clear logs with null. Engine definitions
and key email addresses can also be cleared explicitly.

Progress updates require a finite number between 0 and 1, inclusive. Image order
on job creation must be a non-negative integer fitting a PostgreSQL integer
(maximum 2147483647). Invalid numeric inputs return 422 instead of being clamped.
Date edits and job-listing date filters are normalized to UTC: explicit offsets
preserve the instant, and timezone-free inputs mean UTC. Dates must be representable
in UTC; exact Python minimum/maximum dates reserved by the driver for infinity are
rejected. An edited `last_change` must also leave room for the configured finite
lease duration. Invalid dates return 422 before being used in database queries or updates.
