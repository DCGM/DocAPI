import email
import logging
import uuid
from unittest import mock

import pytest

from doc_api.tools.mail.mail_logger import MailLogger
from doc_worker.doc_worker_wrapper import DocWorkerWrapper


def _mail_logger(**kwargs):
    settings = dict(sender_mail='worker@mail.server.cz', server='smtp.mail.server.cz')
    settings.update(kwargs)
    # A logger keeps its handler between instances of one name.
    return MailLogger(logger_name=f'test.mail.{uuid.uuid4().hex}', **settings)


def _sent(smtp_class):
    smtp = smtp_class.return_value
    fromaddr, toaddrs, text = smtp.sendmail.call_args.args
    return smtp, fromaddr, toaddrs, email.message_from_string(text)


@mock.patch('smtplib.SMTP')
def test_send_mails_the_given_receivers(smtp_class):
    mail_logger = _mail_logger(receiver_mails=['default@mail.server.cz'], sender_name='Worker')

    assert mail_logger.send('Job done', 'Příliš žluťoučký kůň', ['user@mail.server.cz'])

    smtp_class.assert_called_once_with('smtp.mail.server.cz', 25)
    smtp, fromaddr, toaddrs, message = _sent(smtp_class)
    assert toaddrs == ['user@mail.server.cz']
    assert message['To'] == 'user@mail.server.cz'
    assert message['Subject'] == 'Job done'
    assert message['From'] == 'Worker <worker@mail.server.cz>'
    assert message.get_payload(decode=True).decode('utf-8') == 'Příliš žluťoučký kůň'
    smtp.login.assert_not_called()
    smtp.starttls.assert_not_called()


@mock.patch('smtplib.SMTP')
def test_send_defaults_to_the_loggers_receivers(smtp_class):
    mail_logger = _mail_logger(receiver_mails=['default@mail.server.cz'])

    assert mail_logger.send('Job done', 'body')

    assert _sent(smtp_class)[2] == ['default@mail.server.cz']


@mock.patch('smtplib.SMTP')
def test_starttls_logs_in_as_the_username(smtp_class):
    mail_logger = _mail_logger(port=587, password='secret', username='login', security='starttls')

    assert mail_logger.send('Job done', 'body', ['user@mail.server.cz'])

    smtp_class.assert_called_once_with('smtp.mail.server.cz', 587)
    smtp = _sent(smtp_class)[0]
    smtp.starttls.assert_called_once_with()
    smtp.login.assert_called_once_with('login', 'secret')


@mock.patch('smtplib.SMTP_SSL')
def test_a_password_alone_means_ssl_as_the_sender(smtp_ssl_class):
    mail_logger = _mail_logger(port=465, password='secret')

    assert mail_logger.send('Job done', 'body', ['user@mail.server.cz'])

    smtp_ssl_class.assert_called_once_with('smtp.mail.server.cz', 465, context=None)
    _sent(smtp_ssl_class)[0].login.assert_called_once_with('worker@mail.server.cz', 'secret')


def test_an_unknown_security_is_rejected():
    with pytest.raises(ValueError):
        _mail_logger(security='tls')


@mock.patch('smtplib.SMTP')
def test_nothing_is_sent_without_a_server_or_receivers(smtp_class):
    assert not _mail_logger(server=None).send('Job done', 'body', ['user@mail.server.cz'])
    assert not _mail_logger().send('Job done', 'body')
    smtp_class.assert_not_called()


@mock.patch('smtplib.SMTP')
def test_a_failed_send_returns_false(smtp_class, caplog):
    smtp_class.return_value.sendmail.side_effect = OSError('connection refused')

    with caplog.at_level(logging.ERROR):
        assert not _mail_logger().send('Job done', 'body', ['user@mail.server.cz'])
    assert 'Failed to send email' in caplog.text


@mock.patch('smtplib.SMTP')
def test_a_logged_record_is_still_mailed(smtp_class):
    mail_logger = _mail_logger(receiver_mails=['admin@mail.server.cz'])

    mail_logger.logger.critical('Something broke', extra={'subject': 'Server error'})

    message = _sent(smtp_class)[3]
    assert message['Subject'] == 'Server error'
    assert message['X-Log-Level'] == 'CRITICAL'


@mock.patch('smtplib.SMTP')
def test_the_worker_sends_with_its_mail_logger(smtp_class, tmp_path):
    worker = _worker(tmp_path, mail_logger=_mail_logger())

    assert worker.send_mail('Job done', 'body', ['user@mail.server.cz'])
    assert _sent(smtp_class)[2] == ['user@mail.server.cz']


def test_a_worker_without_a_mail_logger_sends_nothing(tmp_path):
    assert not _worker(tmp_path).send_mail('Job done', 'body', ['user@mail.server.cz'])


def _worker(tmp_path, **kwargs):
    class Worker(DocWorkerWrapper):
        def process_job(self, *args, **kwargs):
            raise NotImplementedError

    return Worker(api_url='http://localhost', connector=mock.Mock(), base_dir=str(tmp_path), **kwargs)
