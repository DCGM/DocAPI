"""Delivery policy independent of API route behavior."""
import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID

import pytest
import pytest_asyncio

from doc_api.api.checkers import job_mail
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model
from doc_api.tests.helpers import notification_rows


def test_recipient_flood_limits(job_mail_outbox, monkeypatch):
    monkeypatch.setattr(config, 'EXTERNAL_MAIL_FLOOD_LEVEL', 1)
    hour = [3600]
    monkeypatch.setattr(job_mail, 'time', lambda: hour[0])
    report = job_mail.JobMail('EXTERNAL', ('one@example.com',), 'subject', 'final report')
    job_mail.send_final_report(report)
    job_mail.send_final_report(report)
    job_mail.send_final_report(job_mail.JobMail('EXTERNAL', ('two@example.com',), 'subject', 'other report'))
    assert [receivers for _, receivers, _ in job_mail_outbox] == [['one@example.com'], ['two@example.com']]
    job_mail.send_final_report(job_mail.JobMail('EXTERNAL', ('one@example.com',), 'different subject', 'report'))
    job_mail.send_final_report(job_mail.JobMail('EXTERNAL', ('one@example.com',), 'different subject', 'repeated report'))
    assert len(job_mail_outbox) == 3
    assert job_mail_outbox[-1][2]['Subject'] == 'different subject'
    hour[0] += 3600
    job_mail.send_final_report(report)
    assert len(job_mail_outbox) == 4


def test_invalid_mail_configuration_is_logged(job_mail_outbox, monkeypatch, caplog):
    monkeypatch.setattr(config, 'EXTERNAL_MAIL_PORT', 'invalid')
    job_mail.send_final_report(job_mail.JobMail('EXTERNAL', ('one@example.com',), 'subject', 'report'))
    assert not job_mail_outbox
    assert 'Failed preparing job notification' in caplog.text


@pytest.fixture
def mail_database(monkeypatch):
    def create(*rows):
        db = MagicMock()
        db.get = AsyncMock(side_effect=rows)
        db.__aenter__.return_value = db
        monkeypatch.setattr(job_mail, 'AsyncSession', lambda **kwargs: db)
        return db
    return create


@pytest.mark.asyncio
@pytest.mark.parametrize('template,expected,needs_engine', [
    ('[{server_name}] {state} job {job_id}', '[Server] done job 12345678-0000-0000-0000-000000000001', False),
    ('{server_name}|{state}|{job_id_short}|{image_count}|{attempt}|{engine_name}|{engine_version}|{owner_label}',
     'Server|done|12345678|12|2|OCR|1.7|owner', True),
    ('{state}: {image_count:03d} images', 'done: 012 images', False),
    ('Notification', 'Notification', False),
    ('{server_name}/{admin_server_name}', 'Server/Admin Server', False),
    ('[{server_name}] {state_upper} job {job_id_short}', '[Server] DONE job 12345678', False),
])
async def test_subject_template(job_mail_outbox, mail_database, monkeypatch, template, expected, needs_engine):
    monkeypatch.setattr(config, 'USER_JOB_MAIL_SUBJECT_TEMPLATE', template)
    monkeypatch.setattr(config, 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', 'Admin: ' + template)
    monkeypatch.setattr(config, 'SERVER_NAME', 'Server')
    monkeypatch.setattr(config, 'ADMIN_SERVER_NAME', 'Admin Server')
    owner = SimpleNamespace(label='owner', email='owner@example.com', email_notifications=True)
    engine = SimpleNamespace(name='OCR', version='1.7')
    db = mail_database(owner, engine)
    job = SimpleNamespace(id=UUID('12345678-0000-0000-0000-000000000001'), owner_key_id='owner-id',
        engine_id='engine-id', image_count=12, previous_attempts=1, state=ProcessingState.DONE,
        final_log=[None, 'technical report'], final_log_user=[None, 'user report'])
    reports = await job_mail.get_final_reports(db, job)
    assert [report.subject for report in reports] == [expected, 'Admin: ' + expected]
    assert db.get.await_count == (2 if needs_engine else 1)


@pytest.mark.asyncio
async def test_subject_template_missing_optional_values(job_mail_outbox, mail_database, monkeypatch):
    monkeypatch.setattr(config, 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', '{engine_name}/{engine_version}/{owner_label}/{attempt}')
    db = mail_database(None)
    job = SimpleNamespace(id=UUID(int=1), owner_key_id='owner-id', engine_id=None, image_count=0,
        previous_attempts=None, state=ProcessingState.FAILED, final_log=['report'], final_log_user=None)
    reports = await job_mail.get_final_reports(db, job)
    assert reports[0].subject == '///0'
    db.get.assert_awaited_once()


@pytest.mark.parametrize('setting', ['USER_JOB_MAIL_SUBJECT_TEMPLATE', 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', 'INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE'])
@pytest.mark.parametrize('template', ['{unknown}', '{job_id.__class__}', '{', '', 'a\nb', '{image_count:invalid}'])
def test_subject_template_invalid_configuration(monkeypatch, template, setting):
    monkeypatch.setenv(setting, template)
    with pytest.raises(ValueError, match=f'Invalid {setting}'):
        Config()


def test_mail_configuration_matches_env_example(monkeypatch):
    import os
    import re
    from pathlib import Path

    prefixes = ('INTERNAL_MAIL_', 'EXTERNAL_MAIL_', 'USER_JOB_MAIL_', 'ADMIN_JOB_MAIL_', 'JOB_MAIL_', 'INTERNAL_ERROR_MAIL_')
    for name in list(os.environ):
        if name.startswith(prefixes):
            monkeypatch.delenv(name)
    defaults = Config()
    example = (Path(__file__).parents[1] / '.env_example').read_text()
    documented = {name for name in re.findall(r'^(?:# )?([A-Z_]+)=', example, re.MULTILINE)
                  if name.startswith(prefixes)}
    assert documented == {name for name in vars(defaults) if name.startswith(prefixes)}
    for name, value in re.findall(r'^([A-Z_]+)=(.*)$', example, re.MULTILINE):
        if name not in documented:
            continue
        expected = value.strip().strip('"')
        expected = re.sub(r'\$\{([A-Z_]+)\}', lambda match: str(getattr(defaults, match[1])), expected)
        actual = getattr(defaults, name)
        if isinstance(actual, list):
            assert actual == [address.strip() for address in expected.split(',') if address.strip()]
        else:
            assert str(actual) == expected, name
    assert defaults.INTERNAL_MAIL_RECEIVER_MAILS == []
    assert isinstance(defaults.INTERNAL_MAIL_PORT, int)
    assert isinstance(defaults.EXTERNAL_MAIL_PORT, int)




@pytest_asyncio.fixture
async def finalized_job(job_factory, db_sessions, job_mail_outbox):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.final_log_user = ['user final report']
        owner = await db.get(model.Key, job.owner_key_id)
        owner.email = 'owner@example.com'
        owner.email_notifications = True
    return job_id


@pytest.mark.asyncio
async def test_notification_preparation_and_restart_are_idempotent(finalized_job, db_sessions, job_mail_outbox, monkeypatch):
    for _ in range(2):
        async with db_sessions() as db:
            await job_mail.prepare_job_notifications(db)
    rows = await notification_rows(db_sessions, finalized_job)
    assert len(rows) == 1 and rows[0].state == 'pending' and rows[0].delivery_attempts == 0
    assert not job_mail_outbox
    await job_mail.check_job_notifications()
    monkeypatch.setattr(job_mail, '_flood_counts', {})
    # Reprocessing a finished job does not create another final notification.
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, finalized_job)
        job.previous_attempts = 1
        job.final_log_user = ['user final report', 'report after reprocessing']
    await job_mail.check_job_notifications()
    assert len(job_mail_outbox) == 1
    rows = await notification_rows(db_sessions, finalized_job)
    assert rows[0].state == 'sent' and rows[0].sent_at is not None


@pytest.mark.asyncio
async def test_concurrent_mail_checkers_claim_once(finalized_job, db_sessions, job_mail_outbox):
    async with db_sessions.begin() as db:
        (await db.get(model.Job, finalized_job)).final_log = ['technical final report']
    await asyncio.gather(*(job_mail.check_job_notifications() for _ in range(4)))
    assert len(job_mail_outbox) == 2
    rows = await notification_rows(db_sessions, finalized_job)
    assert len(rows) == 2
    assert all(row.state == 'sent' and row.delivery_attempts == 1 for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize('succeeds', [False, True])
async def test_mail_retry_limit_and_backoff(finalized_job, db_sessions, monkeypatch, succeeds):
    monkeypatch.setattr(config, 'JOB_MAIL_MAX_ATTEMPTS', 3)
    monkeypatch.setattr(config, 'JOB_MAIL_RETRY_DELAY_SECONDS', 60)
    send = Mock(side_effect=[False, False, succeeds])
    monkeypatch.setattr(job_mail, 'send_final_report', send)
    for attempt in range(1, 4):
        before = datetime.now(timezone.utc)
        await job_mail.check_job_notifications()
        [row] = await notification_rows(db_sessions, finalized_job)
        assert row.delivery_attempts == attempt
        if attempt < 3:
            assert row.state == 'pending'
            assert row.next_attempt_at >= before + timedelta(seconds=60 * 2 ** (attempt - 1))
            await job_mail.check_job_notifications()
            assert send.call_count == attempt, 'A delivery must wait for its backoff deadline'
            async with db_sessions.begin() as db:
                (await db.get(model.JobNotification, row.id)).next_attempt_at = before - timedelta(seconds=1)
        else:
            assert row.state == ('sent' if succeeds else 'failed')
            assert (row.sent_at is not None) is succeeds
    await job_mail.check_job_notifications()
    assert send.call_count == 3


@pytest.mark.asyncio
async def test_mail_flood_deferral_does_not_consume_attempt(finalized_job, db_sessions, monkeypatch):
    send = Mock(return_value=None)
    monkeypatch.setattr(job_mail, 'send_final_report', send)
    await job_mail.check_job_notifications()
    [row] = await notification_rows(db_sessions, finalized_job)
    assert row.state == 'pending' and row.delivery_attempts == 0
    assert row.next_attempt_at > datetime.now(timezone.utc)
    assert row.next_attempt_at.minute == row.next_attempt_at.second == 0
    await job_mail.check_job_notifications()
    send.assert_called_once()


@pytest.mark.asyncio
async def test_mail_recovers_expired_claim_and_rejects_stale_result(finalized_job, db_sessions, job_mail_outbox):
    async with db_sessions() as db:
        await job_mail.prepare_job_notifications(db)
    old_claim = await job_mail.claim_notification()
    async with db_sessions.begin() as db:
        row = await db.get(model.JobNotification, old_claim.id)
        assert row.state == 'processing' and row.delivery_attempts == 1
        row.claimed_at = datetime.now(timezone.utc) - timedelta(minutes=6)
    await job_mail.check_job_notifications()
    await job_mail.finish_notification(old_claim, False)
    [row] = await notification_rows(db_sessions, finalized_job)
    assert row.state == 'sent' and row.delivery_attempts == 2
    assert len(job_mail_outbox) == 1


@pytest.mark.asyncio
async def test_mail_shutdown_finishes_inflight_delivery(finalized_job, db_sessions, monkeypatch):
    from threading import Event
    started, release = Event(), Event()
    def send(mail):
        started.set()
        assert release.wait(10)
        return True
    monkeypatch.setattr(job_mail, 'send_final_report', send)
    task = asyncio.create_task(job_mail.check_job_notifications())
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    [row] = await notification_rows(db_sessions, finalized_job)
    assert row.state == 'sent' and row.claim_token is None


@pytest.mark.asyncio
async def test_mail_checker_runs_without_requests(finalized_job, job_mail_outbox, monkeypatch):
    monkeypatch.setattr(config, 'JOB_MAIL_CHECK_INTERVAL_SECONDS', 0.01)
    task = asyncio.create_task(job_mail.run_job_mail_checks())
    try:
        async def wait_for_mail():
            while not job_mail_outbox:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait_for_mail(), timeout=5)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert len(job_mail_outbox) == 1


@pytest.mark.asyncio
async def test_mail_waits_while_smtp_is_disabled(finalized_job, db_sessions, monkeypatch, job_mail_outbox):
    with monkeypatch.context() as patch:
        patch.setattr(config, 'EXTERNAL_MAIL_SERVER', None)
        async with db_sessions() as db:
            await job_mail.prepare_job_notifications(db)
        await job_mail.deliver_job_notifications()
    [row] = await notification_rows(db_sessions, finalized_job)
    assert row.state == 'pending' and row.delivery_attempts == 0
    assert not job_mail_outbox
    await job_mail.deliver_job_notifications()
    assert len(job_mail_outbox) == 1


@pytest.mark.asyncio
async def test_expired_mail_claims_respect_attempt_limit(finalized_job, db_sessions, job_mail_outbox, monkeypatch):
    monkeypatch.setattr(config, 'JOB_MAIL_MAX_ATTEMPTS', 2)
    async with db_sessions() as db:
        await job_mail.prepare_job_notifications(db)
    for _ in range(2):
        claim = await job_mail.claim_notification()
        assert claim is not None
        async with db_sessions.begin() as db:
            (await db.get(model.JobNotification, claim.id)).claimed_at = datetime.now(timezone.utc) - timedelta(minutes=6)
    await job_mail.check_job_notifications()
    [row] = await notification_rows(db_sessions, finalized_job)
    assert row.state == 'failed' and row.delivery_attempts == 2
    assert not job_mail_outbox


@pytest.mark.asyncio
async def test_mail_checker_recovers_after_failure(monkeypatch, caplog):
    check = AsyncMock(side_effect=[OSError('temporary failure'), None])
    sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])
    monkeypatch.setattr(job_mail, 'check_job_notifications', check)
    monkeypatch.setattr(job_mail, 'asyncio', SimpleNamespace(sleep=sleep))
    monkeypatch.setattr(config, 'JOB_MAIL_CHECK_INTERVAL_SECONDS', 11)
    with pytest.raises(asyncio.CancelledError):
        await job_mail.run_job_mail_checks()
    assert check.await_count == 2
    assert [call.args for call in sleep.await_args_list] == [(11,), (11,)]
    assert 'Job mail check failed' in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('template,expected', [
    ('[{admin_server_name}] INTERNAL SERVER ERROR', '[Admin] INTERNAL SERVER ERROR'),
    ('{server_name}/{admin_server_name:.3}', 'Public/Adm'),
    ('Custom error alert', 'Custom error alert'),
])
async def test_internal_error_subject_template(monkeypatch, template, expected):
    from doc_api.api import main
    monkeypatch.setattr(config, 'INTERNAL_MAIL_SERVER', 'smtp.invalid')
    monkeypatch.setattr(config, 'INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE', template)
    monkeypatch.setattr(config, 'SERVER_NAME', 'Public')
    monkeypatch.setattr(config, 'ADMIN_SERVER_NAME', 'Admin')
    mail_logger = Mock()
    monkeypatch.setattr(main, 'internal_mail_logger', mail_logger)
    request = SimpleNamespace(url='http://test/failing', method='GET', client='client')
    response = await main.unhandled(request, RuntimeError('test error'))
    assert response.status_code == 500
    assert mail_logger.critical.call_args.kwargs['extra']['subject'] == expected


@pytest.mark.asyncio
async def test_admin_subject_default_uses_admin_server_name(job_mail_outbox, mail_database, monkeypatch):
    monkeypatch.delenv('ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', raising=False)
    monkeypatch.delenv('INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE', raising=False)
    defaults = Config()
    assert defaults.INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE == '[{admin_server_name}] INTERNAL SERVER ERROR'
    monkeypatch.setattr(config, 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', defaults.ADMIN_JOB_MAIL_SUBJECT_TEMPLATE)
    monkeypatch.setattr(config, 'SERVER_NAME', 'Public')
    monkeypatch.setattr(config, 'ADMIN_SERVER_NAME', 'Admin')
    db = mail_database(SimpleNamespace(label='owner'))
    job = SimpleNamespace(id=UUID('12345678-0000-0000-0000-000000000001'), owner_key_id='owner-id',
        engine_id=None, image_count=1, previous_attempts=0, state=ProcessingState.DONE,
        final_log=['technical report'], final_log_user=None)
    reports = await job_mail.get_final_reports(db, job)
    assert reports[0].subject == '[Admin] DONE job 12345678 (owner)'
