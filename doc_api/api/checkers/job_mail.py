"""Persist final notifications and deliver them with bounded, recoverable retries."""
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from threading import Lock
from time import time
from string import Formatter

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import exc, select, exists, func, and_, or_, cast, String
from sqlalchemy.dialects.postgresql import insert

from doc_api.api.database import open_session


from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model
from doc_api.tools.mail.mail_sender import MailSender
from doc_api.tools.secrets import mask_secrets_in_text

logger = logging.getLogger(__name__)
_flood_lock = Lock()
_flood_counts: dict[tuple[str, str, str], tuple[int, int]] = {}


@dataclass(frozen=True)
class JobMail:
    channel: str
    recipients: tuple[str, ...]
    subject: str
    body: str


async def get_final_reports(db: AsyncSession, job: model.Job) -> list[JobMail]:
    """Build snapshots from verified final reports and current recipient settings."""
    if job.state not in {ProcessingState.DONE, ProcessingState.FAILED}:
        return []
    templates = {"EXTERNAL": config.USER_JOB_MAIL_SUBJECT_TEMPLATE,
                 "INTERNAL": config.ADMIN_JOB_MAIL_SUBJECT_TEMPLATE}
    fields = {name for template in templates.values() for _, name, _, _ in Formatter().parse(template)}
    try:
        # A failed notification lookup must not expire the route's committed ORM
        # objects when its read transaction rolls back.
        async with AsyncSession(bind=db.bind, expire_on_commit=False) as mail_db:
            async with mail_db.begin():
                owner = await mail_db.get(model.Key, job.owner_key_id)
                engine = None
                if job.engine_id is not None and fields & {"engine_name", "engine_version"}:
                    engine = await mail_db.get(model.Engine, job.engine_id)
    except exc.SQLAlchemyError:
        logger.exception("Failed preparing final reports for job %s", job.id)
        return []
    values = dict(
        server_name=config.SERVER_NAME, admin_server_name=config.ADMIN_SERVER_NAME,
        status=job.state.value.upper(),
        job_id=str(job.id),
        job_id_short=str(job.id)[:8], image_count=job.image_count,
        attempt=job.previous_attempts + 1 if job.previous_attempts is not None else 0,
        engine_name=engine.name if engine else "", engine_version=engine.version if engine else "",
        owner_label=owner.label if owner else "",
    )
    # Labels may contain line breaks, but an email subject must be a single line.
    subjects = {channel: " ".join(template.format(**values).splitlines())
                for channel, template in templates.items()}
    reports = []
    key = str(0 if job.previous_attempts is None else job.previous_attempts)
    user_report = (job.final_log_user or {}).get(key)
    technical_report = (job.final_log or {}).get(key)
    if (user_report and user_report.strip() and owner is not None and owner.email_notifications
            and owner.email):
        reports.append(JobMail("EXTERNAL", (owner.email,), subjects["EXTERNAL"], user_report))
    recipients = tuple(address for address in config.INTERNAL_MAIL_RECEIVER_MAILS if address.strip())
    if technical_report and technical_report.strip() and recipients:
        # Admins retain history, but an empty current report must not resend old reports.
        history = "\n\n".join(job.final_log[key] for key in sorted(job.final_log, key=int))
        reports.append(JobMail("INTERNAL", recipients, subjects["INTERNAL"], history))
    return reports


def send_final_report(mail: JobMail) -> bool | None:
    """One SMTP attempt without a DB connection; None means rate-limited.

    The checker persists retries. Flood limits are per channel/recipient/subject
    within this server process and count SMTP attempts, including failures.
    """
    limit = getattr(config, f"{mail.channel}_MAIL_FLOOD_LEVEL")
    hour = int(time() // 3600)
    recipients = []
    with _flood_lock:
        for address in dict.fromkeys(mail.recipients):
            key = (mail.channel, address.casefold(), mail.subject)
            bucket, count = _flood_counts.get(key, (hour, 0))
            count = count if bucket == hour else 0
            if limit > 0 and count >= limit:
                logger.warning("Job notification suppressed by the flood limit for %s with subject %r",
                               address, mail.subject)
                continue
            _flood_counts[key] = (hour, count + 1)
            recipients.append(address)
    if not recipients:
        return
    try:
        sender = MailSender(
            server=getattr(config, f"{mail.channel}_MAIL_SERVER"),
            sender_mail=getattr(config, f"{mail.channel}_MAIL_SENDER_MAIL"),
            port=getattr(config, f"{mail.channel}_MAIL_PORT"),
            sender_name=getattr(config, f"{mail.channel}_MAIL_SENDER_NAME"),
            password=getattr(config, f"{mail.channel}_MAIL_PASSWORD"),
            username=getattr(config, f"{mail.channel}_MAIL_USERNAME"),
            security=getattr(config, f"{mail.channel}_MAIL_SECURITY"),
            timeout=getattr(config, f"{mail.channel}_MAIL_TIMEOUT_SECONDS"),
        )
        # Workers are expected to write reports fit to send; masking is opt-in
        # per channel, SECRET_MASKING_INTERNAL_MAIL / SECRET_MASKING_EXTERNAL_MAIL.
        subject, body = mail.subject, mail.body
        if getattr(config, f"SECRET_MASKING_{mail.channel}_MAIL"):
            masking = config.secret_masking()
            subject, body = mask_secrets_in_text(subject, masking), mask_secrets_in_text(body, masking)
        return sender.send(subject, body, recipients)
    except Exception:
        logger.exception("Failed preparing job notification %r", mail.subject)
        return False


async def prepare_job_notifications(db: AsyncSession):
    """Create each job's recipient snapshots once, across API processes."""
    last_id = None
    key = cast(func.coalesce(model.Job.previous_attempts, 0), String)
    has_report = or_(
        func.length(func.btrim(model.Job.final_log.op('->>')(key), ' \t\r\n')) > 0,
        func.length(func.btrim(model.Job.final_log_user.op('->>')(key), ' \t\r\n')) > 0,
    )
    already_prepared = exists(select(model.JobNotification.id).where(
        model.JobNotification.job_id == model.Job.id,
    ))
    while True:
        async with db.begin():
            query = select(model.Job).where(
                model.Job.state.in_([ProcessingState.DONE, ProcessingState.FAILED]),
                has_report, ~already_prepared,
            )
            if last_id is not None:
                query = query.where(model.Job.id > last_id)
            jobs = list(await db.scalars(query.order_by(model.Job.id).limit(100)
                                        .execution_options(populate_existing=True)
                                        .with_for_update(skip_locked=True)))
            if not jobs:
                return
            last_id = jobs[-1].id
            for job in jobs:
                now = datetime.now(timezone.utc)
                rows = []
                for mail in await get_final_reports(db, job):
                    recipients = {address.casefold(): address for address in mail.recipients}
                    for recipient in recipients.values():
                        rows.append(dict(id=uuid4(), job_id=job.id,
                            channel=mail.channel, recipient=recipient, subject=mail.subject, body=mail.body,
                            state='pending', delivery_attempts=0, created_at=now, next_attempt_at=now))
                if rows:
                    await db.execute(insert(model.JobNotification).values(rows).on_conflict_do_nothing(
                        constraint='uq_job_notification_recipient'))


@dataclass(frozen=True)
class ClaimedNotification:
    id: UUID
    token: UUID
    mail: JobMail


async def claim_notification() -> ClaimedNotification | None:
    """Reserve one due delivery, committing and releasing its connection before SMTP."""
    channels = [channel for channel in ('INTERNAL', 'EXTERNAL')
                if getattr(config, f'{channel}_MAIL_SERVER') and getattr(config, f'{channel}_MAIL_SENDER_MAIL')]
    if not channels:
        return None
    while True:
        now = datetime.now(timezone.utc)
        async with open_session() as db:
            async with db.begin():
                row = await db.scalar(select(model.JobNotification).where(
                    model.JobNotification.channel.in_(channels),
                    or_(
                        and_(model.JobNotification.state == 'pending', model.JobNotification.next_attempt_at <= now),
                        and_(model.JobNotification.state == 'processing',
                             model.JobNotification.claimed_at <= now - timedelta(minutes=5)),
                    ),
                ).order_by(model.JobNotification.next_attempt_at, model.JobNotification.id)
                    .with_for_update(skip_locked=True).limit(1))
                if row is None:
                    return None
                if row.delivery_attempts >= config.JOB_MAIL_MAX_ATTEMPTS:
                    row.state = 'failed'
                    row.claimed_at = row.claim_token = None
                    row.last_error = 'Delivery attempt limit reached; an earlier claim may have expired.'
                    continue
                row.state = 'processing'
                row.claimed_at = now
                row.claim_token = uuid4()
                row.delivery_attempts += 1
                return ClaimedNotification(row.id, row.claim_token,
                    JobMail(row.channel, (row.recipient,), row.subject, row.body))


async def finish_notification(claim: ClaimedNotification, result: bool | None):
    now = datetime.now(timezone.utc)
    async with open_session() as db:
        async with db.begin():
            row = await db.scalar(select(model.JobNotification).where(
                model.JobNotification.id == claim.id, model.JobNotification.claim_token == claim.token,
                model.JobNotification.state == 'processing',
            ).with_for_update())
            if row is None:
                return  # An expired claim was recovered elsewhere; its token owns the result.
            row.claimed_at = row.claim_token = None
            if result is True:
                row.state = 'sent'
                row.sent_at = now
                row.last_error = None
            elif result is None:
                row.state = 'pending'
                row.delivery_attempts -= 1  # Flood suppression is not an SMTP attempt.
                row.next_attempt_at = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                row.last_error = 'Deferred by the subject/recipient flood limit.'
            else:
                row.last_error = 'SMTP delivery failed; see the server log for exception details.'
                if row.delivery_attempts >= config.JOB_MAIL_MAX_ATTEMPTS:
                    row.state = 'failed'
                else:
                    row.state = 'pending'
                    delay = min(config.JOB_MAIL_RETRY_DELAY_SECONDS * 2 ** (row.delivery_attempts - 1), 86400)
                    row.next_attempt_at = now + timedelta(seconds=delay)
            logger.info('Notification %s for job %s: %s after %s delivery attempts',
                        row.id, row.job_id, row.state, row.delivery_attempts)


async def deliver_job_notifications():
    while (claim := await claim_notification()) is not None:
        sending = asyncio.create_task(asyncio.to_thread(send_final_report, claim.mail))
        try:
            result = await asyncio.shield(sending)
        except asyncio.CancelledError:
            # A running SMTP thread cannot be cancelled. Finish recording its
            # result during graceful shutdown before releasing this checker.
            result = await sending
            await finish_notification(claim, result)
            raise
        finishing = asyncio.create_task(finish_notification(claim, result))
        try:
            await asyncio.shield(finishing)
        except asyncio.CancelledError:
            await finishing
            raise


async def check_job_notifications():
    async with open_session() as db:
        await prepare_job_notifications(db)
    await deliver_job_notifications()


async def run_job_mail_checks():
    while True:
        try:
            await check_job_notifications()
        except Exception:
            logger.exception('Job mail check failed; pending deliveries will be checked again')
        await asyncio.sleep(config.JOB_MAIL_CHECK_INTERVAL_SECONDS)
