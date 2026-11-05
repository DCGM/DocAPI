import email
import logging
import uuid
from unittest import mock

import pytest

from doc_api.tools.mail.mail_logger import MailLogger
from doc_api.tools.mail.mail_sender import MailSender
from doc_worker.doc_worker_wrapper import DocWorkerWrapper


def _sender(**kwargs):
    settings = dict(server='smtp.mail.server.cz', sender_mail='worker@mail.server.cz')
    settings.update(kwargs)
    return MailSender(**settings)


def _sent(smtp_class):
    smtp = smtp_class.return_value
    fromaddr, toaddrs, text = smtp.sendmail.call_args.args
    return smtp, fromaddr, toaddrs, email.message_from_string(text)


@mock.patch('smtplib.SMTP')
def test_send_mails_the_given_receivers(smtp_class):
    sender = _sender(receiver_mails=['default@mail.server.cz'], sender_name='Worker')

    assert sender.send('Job done', 'Příliš žluťoučký kůň', ['user@mail.server.cz'])

    smtp_class.assert_called_once_with('smtp.mail.server.cz', 25, timeout=60)
    smtp, fromaddr, toaddrs, message = _sent(smtp_class)
    assert fromaddr == 'worker@mail.server.cz'
    assert toaddrs == ['user@mail.server.cz']
    assert message['To'] == 'user@mail.server.cz'
    assert message['Subject'] == 'Job done'
    assert message['From'] == 'Worker <worker@mail.server.cz>'
    assert message.get_payload(decode=True).decode('utf-8') == 'Příliš žluťoučký kůň'
    smtp.login.assert_not_called()
    smtp.starttls.assert_not_called()


@mock.patch('smtplib.SMTP')
def test_send_defaults_to_the_senders_receivers(smtp_class):
    assert _sender(receiver_mails=['default@mail.server.cz']).send('Job done', 'body')

    assert _sent(smtp_class)[2] == ['default@mail.server.cz']


@mock.patch('smtplib.SMTP')
def test_a_sent_email_is_logged_with_its_sender_and_receivers(smtp_class, caplog):
    with caplog.at_level(logging.INFO, logger='doc_api.tools.mail.mail_sender'):
        _sender(sender_name='Worker').send('Job done', 'First line\nPříliš žluťoučký kůň', ['a@mail.server.cz', 'b@mail.server.cz'])

    assert ("Sent email 'Job done' from Worker <worker@mail.server.cz> "
            "to a@mail.server.cz, b@mail.server.cz") in caplog.text
    assert 'First line' not in caplog.text
    assert 'Příliš žluťoučký kůň' not in caplog.text


@mock.patch('smtplib.SMTP')
def test_starttls_logs_in_as_the_username(smtp_class):
    sender = _sender(port=587, password='secret', username='login', security='starttls')

    assert sender.send('Job done', 'body', ['user@mail.server.cz'])

    smtp_class.assert_called_once_with('smtp.mail.server.cz', 587, timeout=60)
    smtp = _sent(smtp_class)[0]
    smtp.starttls.assert_called_once_with()
    smtp.login.assert_called_once_with('login', 'secret')


@mock.patch('smtplib.SMTP_SSL')
def test_a_password_alone_means_ssl_as_the_sender(smtp_ssl_class):
    assert _sender(port=465, password='secret').send('Job done', 'body', ['user@mail.server.cz'])

    smtp_ssl_class.assert_called_once_with('smtp.mail.server.cz', 465, timeout=60)
    _sent(smtp_ssl_class)[0].login.assert_called_once_with('worker@mail.server.cz', 'secret')


@pytest.mark.parametrize('settings', [dict(security='tls'), dict(server=None), dict(sender_mail='')])
def test_incomplete_or_unknown_settings_are_rejected(settings):
    with pytest.raises(ValueError):
        _sender(**settings)


@mock.patch('smtplib.SMTP')
def test_nothing_is_sent_without_receivers(smtp_class, caplog):
    with caplog.at_level(logging.ERROR):
        assert not _sender().send('Job done', 'body')
    smtp_class.assert_not_called()
    assert 'to no receivers' in caplog.text


@mock.patch('smtplib.SMTP')
def test_a_failed_send_returns_false_and_is_logged(smtp_class, caplog):
    smtp_class.return_value.sendmail.side_effect = OSError('connection refused')

    with caplog.at_level(logging.ERROR):
        assert not _sender().send('Job done', 'First line\nPříliš žluťoučký kůň', ['user@mail.server.cz'])
    assert ("Failed to send email 'Job done' from worker@mail.server.cz "
            "to user@mail.server.cz") in caplog.text
    assert 'First line' not in caplog.text
    assert 'Příliš žluťoučký kůň' not in caplog.text


@mock.patch('smtplib.SMTP')
def test_a_mail_logger_emails_its_records_with_a_sender(smtp_class):
    # A logger keeps its handler between instances of one name.
    mail_logger = MailLogger(logger_name=f'test.mail.{uuid.uuid4().hex}',
                             server='smtp.mail.server.cz', sender_mail='server@mail.server.cz',
                             receiver_mails=['admin@mail.server.cz'])

    mail_logger.logger.critical('Something broke', extra={'subject': 'Server error'})

    _, _, toaddrs, message = _sent(smtp_class)
    assert toaddrs == ['admin@mail.server.cz']
    assert message['Subject'] == 'Server error'
    assert message['X-Log-Level'] == 'CRITICAL'


def test_a_mail_logger_without_a_server_sends_nothing():
    mail_logger = MailLogger(logger_name=f'test.mail.{uuid.uuid4().hex}', server=None,
                             sender_mail='server@mail.server.cz')

    assert mail_logger.mail_handler is None
    mail_logger.logger.critical('Something broke', extra={'subject': 'Server error'})


@mock.patch('smtplib.SMTP')
def test_the_worker_sends_with_its_mail_sender(smtp_class, tmp_path):
    worker = _worker(tmp_path, mail_sender=_sender())

    assert worker.send_mail('Job done', 'body', ['user@mail.server.cz'])
    assert _sent(smtp_class)[2] == ['user@mail.server.cz']


def test_a_worker_without_a_mail_sender_sends_nothing(tmp_path):
    assert not _worker(tmp_path).send_mail('Job done', 'body', ['user@mail.server.cz'])


def _worker(tmp_path, **kwargs):
    class Worker(DocWorkerWrapper):
        def process_job(self, *args, **kwargs):
            raise NotImplementedError

    return Worker(api_url='http://localhost', connector=mock.Mock(), base_dir=str(tmp_path), **kwargs)
