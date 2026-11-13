"""Delivery policy independent of API route behavior."""
import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID, uuid4

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
    ('[{server_name}] {status} job {job_id_short}', '[Server] DONE job 12345678', False),
    ('[{server_name}] {status} job {job_id}', '[Server] DONE job 12345678-0000-0000-0000-000000000001', False),
    ('{server_name}|{status}|{job_id_short}|{image_count}|{attempt}|{engine_name}|{engine_version}|{owner_label}',
     'Server|DONE|12345678|12|2|OCR|1.7|owner', True),
    ('{status}: {image_count:03d} images', 'DONE: 012 images', False),
    ('Notification', 'Notification', False),
    ('{server_name}/{admin_server_name}', 'Server/Admin Server', False),
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
        final_log={'1': 'technical report'}, final_log_user={'1': 'user report'})
    reports = await job_mail.get_final_reports(db, job)
    assert [report.subject for report in reports] == [expected, 'Admin: ' + expected]
    assert db.get.await_count == (2 if needs_engine else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', [ProcessingState.DONE, ProcessingState.FAILED])
async def test_default_subjects(job_mail_outbox, mail_database, monkeypatch, state):
    for setting in ('USER_JOB_MAIL_SUBJECT_TEMPLATE', 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE'):
        monkeypatch.delenv(setting, raising=False)
    defaults = Config()
    monkeypatch.setattr(config, 'USER_JOB_MAIL_SUBJECT_TEMPLATE', defaults.USER_JOB_MAIL_SUBJECT_TEMPLATE)
    monkeypatch.setattr(config, 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', defaults.ADMIN_JOB_MAIL_SUBJECT_TEMPLATE)
    monkeypatch.setattr(config, 'SERVER_NAME', 'Server')
    monkeypatch.setattr(config, 'ADMIN_SERVER_NAME', 'Admin Server')
    owner = SimpleNamespace(label='owner', email='owner@example.com', email_notifications=True)
    db = mail_database(owner)
    job = SimpleNamespace(id=UUID(int=1), owner_key_id='owner-id', engine_id=None, image_count=1,
                          previous_attempts=0, state=state,
                          final_log={'0': 'technical report'}, final_log_user={'0': 'user report'})
    reports = await job_mail.get_final_reports(db, job)
    assert [report.subject for report in reports] == [
        f'[Server] {state.value.upper()} job 00000000',
        f'[Admin Server] {state.value.upper()} job 00000000 (owner)',
    ]


@pytest.mark.asyncio
async def test_subject_template_missing_optional_values(job_mail_outbox, mail_database, monkeypatch):
    monkeypatch.setattr(config, 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', '{engine_name}/{engine_version}/{owner_label}/{attempt}')
    db = mail_database(None)
    job = SimpleNamespace(id=UUID(int=1), owner_key_id='owner-id', engine_id=None, image_count=0,
        previous_attempts=None, state=ProcessingState.FAILED, final_log={'0': 'report'}, final_log_user=None)
    reports = await job_mail.get_final_reports(db, job)
    assert reports[0].subject == '///0'
    db.get.assert_awaited_once()


@pytest.mark.parametrize('setting', ['USER_JOB_MAIL_SUBJECT_TEMPLATE', 'ADMIN_JOB_MAIL_SUBJECT_TEMPLATE', 'INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE'])
@pytest.mark.parametrize('template', ['{unknown}', '{state}', '{state_upper}', '{job_id.__class__}', '{', '', 'a\nb', '{image_count:invalid}'])
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
        job.final_log_user = {'0': 'user final report'}
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
        job.final_log_user = {'0': 'user final report', '1': 'report after reprocessing'}
    await job_mail.check_job_notifications()
    assert len(job_mail_outbox) == 1
    rows = await notification_rows(db_sessions, finalized_job)
    assert rows[0].state == 'sent' and rows[0].sent_at is not None


@pytest.mark.asyncio
async def test_concurrent_mail_checkers_claim_once(finalized_job, db_sessions, job_mail_outbox):
    async with db_sessions.begin() as db:
        (await db.get(model.Job, finalized_job)).final_log = {'0': 'technical final report'}
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
    await asyncio.gather(*main.error_mail_tasks)
    assert response.status_code == 500
    assert mail_logger.critical.call_args.kwargs['extra']['subject'] == expected


@pytest.mark.asyncio
async def test_internal_error_mail_does_not_hold_the_event_loop(monkeypatch):
    import threading
    from doc_api.api import main
    monkeypatch.setattr(config, 'INTERNAL_MAIL_SERVER', 'smtp.invalid')
    release, sent = threading.Event(), []

    def slow_smtp(body, extra):
        release.wait(5)
        sent.append(extra['subject'])
    monkeypatch.setattr(main, 'internal_mail_logger', Mock(critical=slow_smtp))
    request = SimpleNamespace(url='http://test/failing', method='GET', client='client')
    response = await main.unhandled(request, RuntimeError('test error'))
    # The 500 is ready and the loop keeps serving while SMTP is still busy.
    assert response.status_code == 500
    await asyncio.sleep(0.05)
    assert not sent and len(main.error_mail_tasks) == 1
    release.set()
    await asyncio.gather(*main.error_mail_tasks)
    assert len(sent) == 1 and not main.error_mail_tasks


@pytest.mark.asyncio
async def test_internal_error_mail_flood_limit_applies_in_background(monkeypatch):
    from doc_api.api import main
    from doc_api.tools.mail.mail_logger import MailLogger
    sender = Mock()
    mail_logger = MailLogger(logger_name=f'doc_api.tests.error_mail.{uuid4().hex}', sender=sender, flood_level=2)
    monkeypatch.setattr(config, 'INTERNAL_MAIL_SERVER', 'smtp.invalid')
    monkeypatch.setattr(main, 'internal_mail_logger', mail_logger.logger)
    request = SimpleNamespace(url='http://test/failing', method='GET', client='client')
    for _ in range(5):
        await main.unhandled(request, RuntimeError('test error'))
    await asyncio.gather(*main.error_mail_tasks)
    # Two alerts and one notice that further ones are not mailed this hour.
    subjects = [call.args[0] for call in sender.send_message.call_args_list]
    assert len(subjects) == 3 and subjects[-1].endswith('FLOODED')


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
        final_log={'0': 'technical report'}, final_log_user=None)
    reports = await job_mail.get_final_reports(db, job)
    assert reports[0].subject == '[Admin] DONE job 12345678 (owner)'


@pytest.mark.asyncio
@pytest.mark.parametrize('current_report', [None, '', '   ', 'current report'])
async def test_sparse_final_reports_use_current_attempt(job_factory, db_sessions, job_mail_outbox, current_report):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = 10
        entries = {'2': 'middle report', '0': 'first report'}
        if current_report is not None:
            entries['10'] = current_report
        job.final_log = job.final_log_user = entries
        owner = await db.get(model.Key, job.owner_key_id)
        owner.email = 'owner@example.com'
        owner.email_notifications = True
    await job_mail.check_job_notifications()
    rows = await notification_rows(db_sessions, job_id)
    if current_report and current_report.strip():
        assert {row.channel: row.body for row in rows} == {
            'EXTERNAL': current_report,
            'INTERNAL': 'first report\n\nmiddle report\n\n' + current_report,
        }
        assert len(job_mail_outbox) == 2
    else:
        assert not rows and not job_mail_outbox


@pytest.mark.parametrize("channel", ["INTERNAL", "EXTERNAL"])
def test_job_mail_passes_smtp_security_and_username(job_mail_outbox, monkeypatch, channel):
    sender = Mock()
    mail_sender = Mock(return_value=sender)
    monkeypatch.setattr(config, "mail_sender", mail_sender)
    job_mail.send_final_report(job_mail.JobMail(channel, ("one@example.com",), "subject", "report"))
    mail_sender.assert_called_once_with(channel)
    sender.send.assert_called_once_with("subject", "report", ["one@example.com"])


@pytest.mark.parametrize("channel", ["INTERNAL", "EXTERNAL"])
def test_mail_sender_per_channel(monkeypatch, channel):
    monkeypatch.setattr(config, f"{channel}_MAIL_SERVER", "smtp.example.com")
    monkeypatch.setattr(config, f"{channel}_MAIL_SENDER_MAIL", "server@example.com")
    monkeypatch.setattr(config, f"{channel}_MAIL_PORT", 587)
    monkeypatch.setattr(config, f"{channel}_MAIL_SECURITY", "starttls")
    monkeypatch.setattr(config, f"{channel}_MAIL_USERNAME", "smtp-user")
    monkeypatch.setattr(config, f"{channel}_MAIL_TIMEOUT_SECONDS", 12.5)
    monkeypatch.setattr(config, "INTERNAL_MAIL_RECEIVER_MAILS", ["admin@example.com"])
    sender = config.mail_sender(channel)
    assert (sender.server, sender.port, sender.sender_mail, sender.security, sender.username, sender.timeout) == \
           ("smtp.example.com", 587, "server@example.com", "starttls", "smtp-user", 12.5)
    assert list(sender.receiver_mails) == (["admin@example.com"] if channel == "INTERNAL" else [])
    # A channel without a server or a sender address has no sender.
    monkeypatch.setattr(config, f"{channel}_MAIL_SERVER", None)
    assert config.mail_sender(channel) is None
    with pytest.raises(ValueError):
        config.mail_sender("OTHER")


def test_mail_logger_uses_the_internal_sender_and_flood_level(monkeypatch):
    from doc_api.api import main
    factory, sender = Mock(), Mock()
    monkeypatch.setattr(main, "MailLogger", factory)
    monkeypatch.setattr(config, "mail_sender", Mock(return_value=sender))
    monkeypatch.setattr(config, "INTERNAL_MAIL_FLOOD_LEVEL", 7)
    main.internal_mail_logger_from_config()
    config.mail_sender.assert_called_once_with("INTERNAL")
    assert factory.call_args.kwargs["sender"] is sender
    assert factory.call_args.kwargs["flood_level"] == 7


@pytest.mark.parametrize("channel", ["INTERNAL", "EXTERNAL"])
@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "-inf"])
def test_mail_timeout_must_be_finite_positive(monkeypatch, channel, value):
    setting = f"{channel}_MAIL_TIMEOUT_SECONDS"
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError, match=setting):
        Config()
