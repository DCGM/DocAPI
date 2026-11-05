"""Store technical and user logs as JSON arrays indexed by attempt.

Revision ID: e51a7b3d902c
Revises: c42d8f1e6a90
"""
import re

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "e51a7b3d902c"
down_revision = "c42d8f1e6a90"
branch_labels = None
depends_on = None

# Match the entire server-generated delimiter, not arbitrary mentions of ATTEMPT.
HEADER = re.compile(r"\n{4}(ATTEMPT ([0-9]+) STARTED AT [^\n]+ AFTER TIMEOUT/ERROR\.\n\n)")


def split_attempts(value, *, technical):
    if value is None or value == "":
        return None
    entries = []
    index, start, header = 0, 0, ""
    for match in HEADER.finditer(value):
        attempt = int(match[2]) - 1
        if attempt < 1:
            continue
        entries.extend([None] * (index + 1 - len(entries)))
        entries[index] = (header + value[start:match.start()]) or None
        index, start = attempt, match.end()
        header = match[1] if technical else ""
    entries.extend([None] * (index + 1 - len(entries)))
    entries[index] = (header + value[start:]) or None
    return entries if any(entry is not None for entry in entries) else None


def legacy_text(entries, technical_entries):
    if entries is None:
        return None
    parts = [entries[0] or ""]
    for index, entry in enumerate(entries[1:], 1):
        if entry is None:
            continue
        header = f"ATTEMPT {index + 1} STARTED AT UNKNOWN AFTER TIMEOUT/ERROR.\n\n"
        if technical_entries and index < len(technical_entries):
            source = technical_entries[index] or ""
            match = HEADER.match("\n\n\n\n" + source)
            if match:
                header = match[1]
        body = entry or ""
        # Technical entries already contain their header; user entries do not.
        if body.startswith(header):
            body = body[len(header):]
        parts.append(header + body)
    return "\n\n\n\n".join(parts)


def convert_rows(convert):
    connection = op.get_bind()
    jobs = sa.table("jobs", sa.column("id", sa.Uuid()),
                    sa.column("log", JSONB(none_as_null=True)),
                    sa.column("log_user", JSONB(none_as_null=True)),
                    sa.column("previous_attempts", sa.Integer()))
    last_id = None
    while True:
        query = sa.select(jobs).where(sa.or_(jobs.c.log.is_not(None), jobs.c.log_user.is_not(None)))
        if last_id is not None:
            query = query.where(jobs.c.id > last_id)
        rows = connection.execute(query.order_by(jobs.c.id).limit(500)).mappings().all()
        if not rows:
            break
        for row in rows:
            connection.execute(jobs.update().where(jobs.c.id == row["id"]).values(**convert(row)))
        last_id = rows[-1]["id"]


def upgrade():
    # Preserve text as JSON strings first, then split it in bounded batches.
    for field in ("log", "log_user"):
        op.alter_column("jobs", field, type_=JSONB(), existing_type=sa.String(),
                        postgresql_using=f"to_jsonb({field})")
    def convert(row):
        entries = {"log": split_attempts(row["log"], technical=True),
                   "log_user": split_attempts(row["log_user"], technical=False)}
        count = max((row["previous_attempts"] or 0) + 1,
                    *(len(value or []) for value in entries.values()))
        for value in entries.values():
            if value is not None:
                value.extend([None] * (count - len(value)))
        return entries
    convert_rows(convert)


def downgrade():
    # Reintroduce legacy separators in both fields so a later upgrade can
    # recover attempt indexes even when a user-facing attempt had no messages.
    convert_rows(lambda row: {
        "log": legacy_text(row["log"], row["log"]),
        "log_user": legacy_text(row["log_user"], row["log"]),
    })
    for field in ("log", "log_user"):
        op.alter_column("jobs", field, type_=sa.String(), existing_type=JSONB(),
                        postgresql_using=f"{field} #>> '{{}}'")
