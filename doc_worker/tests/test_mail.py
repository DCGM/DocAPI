import email
import logging
import ssl
from unittest import mock

import pytest

from doc_api.tools.mail_sender import MailResult, MailSender, MailStatus
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
def test_send_mails_the_receiver(smtp_class):
    sender = _sender(sender_name='Worker')

    assert sender.send('Job done', 'Příliš žluťoučký kůň', 'user@mail.server.cz')

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
def test_a_sent_email_is_logged_with_its_sender_and_receiver(smtp_class, caplog):
    with caplog.at_level(logging.INFO, logger='doc_api.tools.mail_sender'):
        _sender(sender_name='Worker').send('Job done', 'First line\nPříliš žluťoučký kůň', 'a@mail.server.cz')

    assert "Sent email 'Job done' from Worker <worker@mail.server.cz> to a@mail.server.cz" in caplog.text
    assert 'First line' not in caplog.text
    assert 'Příliš žluťoučký kůň' not in caplog.text


@mock.patch('smtplib.SMTP')
def test_starttls_logs_in_as_the_username(smtp_class):
    sender = _sender(port=587, password='secret', username='login', security='starttls')

    assert sender.send('Job done', 'body', 'user@mail.server.cz')

    smtp_class.assert_called_once_with('smtp.mail.server.cz', 587, timeout=60)
    smtp = _sent(smtp_class)[0]
    smtp.starttls.assert_called_once_with(context=sender._tls_context)
    # The context checks the server's certificate and host name.
    assert sender._tls_context.verify_mode is ssl.CERT_REQUIRED and sender._tls_context.check_hostname
    smtp.login.assert_called_once_with('login', 'secret')


@mock.patch('smtplib.SMTP_SSL')
def test_a_password_alone_means_ssl_as_the_sender(smtp_ssl_class):
    sender = _sender(port=465, password='secret')
    assert sender.send('Job done', 'body', 'user@mail.server.cz')

    smtp_ssl_class.assert_called_once_with('smtp.mail.server.cz', 465, timeout=60, context=sender._tls_context)
    assert sender._tls_context.verify_mode is ssl.CERT_REQUIRED and sender._tls_context.check_hostname
    _sent(smtp_ssl_class)[0].login.assert_called_once_with('worker@mail.server.cz', 'secret')


@pytest.mark.parametrize('settings', [dict(security='tls'), dict(server=None), dict(sender_mail='')])
def test_incomplete_or_unknown_settings_are_rejected(settings):
    with pytest.raises(ValueError):
        _sender(**settings)


@pytest.mark.parametrize('receiver', ['', '  ', None, ['user@mail.server.cz'], 'a@mail.server.cz, b@mail.server.cz'])
@mock.patch('smtplib.SMTP')
def test_nothing_is_sent_unless_the_receiver_is_one_address(smtp_class, receiver, caplog):
    with caplog.at_level(logging.ERROR):
        result = _sender().send('Job done', 'body', receiver)
    assert result.status is MailStatus.FAILED and result.reason == 'the receiver is not one address'
    smtp_class.assert_not_called()
    assert 'not one address' in caplog.text


@mock.patch('smtplib.SMTP')
def test_a_failed_send_returns_false_and_is_logged(smtp_class, caplog):
    smtp_class.return_value.sendmail.side_effect = OSError('connection refused')

    with caplog.at_level(logging.ERROR):
        assert not _sender().send('Job done', 'First line\nPříliš žluťoučký kůň', 'user@mail.server.cz')
    assert ("Failed to send email 'Job done' from worker@mail.server.cz "
            "to user@mail.server.cz") in caplog.text
    assert 'First line' not in caplog.text
    assert 'Příliš žluťoučký kůň' not in caplog.text


@mock.patch('smtplib.SMTP')
def test_the_worker_sends_a_separate_email_to_each_receiver(smtp_class, tmp_path):
    worker = _worker(tmp_path, mail_sender=_sender(), mail_receivers=['a@mail.server.cz', 'b@mail.server.cz'])

    assert worker.send_mail('Job done', 'body')
    assert [call.args[1] for call in smtp_class.return_value.sendmail.call_args_list] == [
        ['a@mail.server.cz'], ['b@mail.server.cz']]
    assert worker.send_mail('Job done', 'body', ['user@mail.server.cz'])
    assert _sent(smtp_class)[2] == ['user@mail.server.cz']


@mock.patch('smtplib.SMTP')
def test_the_worker_reports_a_send_that_misses_any_receiver(smtp_class, tmp_path):
    worker = _worker(tmp_path, mail_sender=_sender(send_receiver_limit=1),
                     mail_receivers=['a@mail.server.cz', 'b@mail.server.cz'])
    assert worker.send_mail('Job done', 'body', ['a@mail.server.cz'])
    # a is over the limit, b still gets the email.
    assert not worker.send_mail('Job done', 'body')
    assert [call.args[1] for call in smtp_class.return_value.sendmail.call_args_list][-1] == ['b@mail.server.cz']


def test_a_worker_without_a_mail_sender_or_receivers_sends_nothing(tmp_path):
    assert not _worker(tmp_path).send_mail('Job done', 'body', ['user@mail.server.cz'])
    assert not _worker(tmp_path, mail_sender=_sender()).send_mail('Job done', 'body')


def _worker(tmp_path, **kwargs):
    class Worker(DocWorkerWrapper):
        def process_job(self, *args, **kwargs):
            raise NotImplementedError

    return Worker(api_url='http://localhost', connector=mock.Mock(), base_dir=str(tmp_path), **kwargs)



def _subjects(smtp_class):
    return [(args.args[1], email.message_from_string(args.args[2])['Subject'])
            for args in smtp_class.return_value.sendmail.call_args_list]


@mock.patch('smtplib.SMTP')
def test_send_results_say_what_happened(smtp_class):
    sender = _sender(send_server_limit=1)
    assert sender.send('Job done', 'body', 'a@mail.server.cz') == MailResult(MailStatus.SENT, 'a@mail.server.cz')
    held = sender.send('Job done', 'body', 'a@mail.server.cz')
    assert held.status is MailStatus.HELD_BACK and held.receiver == 'a@mail.server.cz'
    assert held.retry_at is not None and held.reason == 'send server limit of 1 emails per 3600 s'
    assert not held
    smtp_class.return_value.sendmail.side_effect = OSError('connection refused')
    failed = _sender().send('Job done', 'body', 'a@mail.server.cz')
    assert failed == MailResult(MailStatus.FAILED, 'a@mail.server.cz', reason='OSError: connection refused')
    assert not failed


@mock.patch('smtplib.SMTP')
def test_send_receiver_limit_sends_one_notice_then_holds_back(smtp_class, monkeypatch):
    from doc_api.tools import mail_sender
    now = [1000]
    monkeypatch.setattr(mail_sender, 'time', lambda: now[0])
    sender = _sender(send_receiver_limit=1, send_receiver_window_seconds=600)

    assert sender.send('Alert', 'body', 'a@mail.server.cz')
    assert sender.send('Alert', 'body', 'b@mail.server.cz')
    now[0] = 1300
    # Over the limit: a gets a notice instead, and the result says when to retry.
    held = sender.send('Alert', 'body', 'A@mail.server.cz')
    assert held.status is MailStatus.HELD_BACK and held.retry_at == 1600
    # Only the first email held back in the window sends a notice.
    assert not sender.send('Alert', 'body', 'a@mail.server.cz')
    assert _subjects(smtp_class) == [
        (['a@mail.server.cz'], 'Alert'),
        (['b@mail.server.cz'], 'Alert'),
        (['A@mail.server.cz'], 'Alert - NOT SENT'),
    ]
    notice = email.message_from_string(smtp_class.return_value.sendmail.call_args_list[2].args[2])
    assert notice.get_payload() == ("An email with the subject 'Alert' could not be sent to you: "
                                    "1 emails with this subject were already sent to you within 600 s.")
    # Another subject has its own count.
    assert sender.send('Other', 'body', 'a@mail.server.cz')
    now[0] = 1599
    assert not sender.send('Alert', 'body', 'a@mail.server.cz')
    now[0] = 1600
    assert sender.send('Alert', 'body', 'a@mail.server.cz')
    # A new window may send a new notice.
    now[0] = 2000
    assert not sender.send('Alert', 'body', 'a@mail.server.cz')
    assert _subjects(smtp_class)[-1] == (['a@mail.server.cz'], 'Alert - NOT SENT')


@pytest.mark.parametrize('send_server_limit,reserve', [(1, 0), (2, 1), (10, 1), (11, 2), (100, 10)])
def test_a_tenth_of_the_send_server_limit_is_kept_for_notices(send_server_limit, reserve):
    assert _sender(send_server_limit=send_server_limit).send_server_notice_reserve == reserve


@mock.patch('smtplib.SMTP')
def test_send_server_limit_holds_back_any_email_and_notifies_once_per_window(smtp_class, monkeypatch):
    from doc_api.tools import mail_sender
    now = [1000.0]
    monkeypatch.setattr(mail_sender, 'time', lambda: now[0])
    # Four per minute: three for email, one kept for notices.
    sender = _sender(send_server_limit=4, send_server_window_seconds=60)

    assert sender.send('one', 'body', 'a@mail.server.cz')
    now[0] += 30
    assert sender.send('two', 'body', 'b@mail.server.cz')
    assert sender.send('three', 'body', 'b@mail.server.cz')
    held = sender.send('four', 'body', 'c@mail.server.cz')
    # Notices count too: the window has room for email again once 'two' leaves it.
    assert held.status is MailStatus.HELD_BACK and held.retry_at == 1090
    # c already knows; d gets no notice: the reserve is used up.
    assert sender.send('five', 'body', 'c@mail.server.cz').status is MailStatus.HELD_BACK
    assert sender.send('five', 'body', 'd@mail.server.cz').status is MailStatus.HELD_BACK
    assert _subjects(smtp_class) == [(['a@mail.server.cz'], 'one'), (['b@mail.server.cz'], 'two'),
                                     (['b@mail.server.cz'], 'three'), (['c@mail.server.cz'], 'four - NOT SENT')]
    notice = email.message_from_string(smtp_class.return_value.sendmail.call_args.args[2])
    assert notice.get_payload() == ("An email with the subject 'four' could not be sent to you: "
                                    "the sender reached its send server limit of 4 emails per 60 s.")
    now[0] = 1089
    assert sender.send('four', 'body', 'c@mail.server.cz').status is MailStatus.HELD_BACK
    now[0] = 1090
    assert sender.send('four', 'body', 'c@mail.server.cz')
    # Fill the new window; it notifies c again.
    assert sender.send('six', 'body', 'e@mail.server.cz')
    assert sender.send('seven', 'body', 'e@mail.server.cz')
    assert sender.send('eight', 'body', 'c@mail.server.cz').status is MailStatus.HELD_BACK
    assert _subjects(smtp_class)[-1] == (['c@mail.server.cz'], 'eight - NOT SENT')


@mock.patch('smtplib.SMTP')
def test_a_failed_send_counts_against_the_limits(smtp_class):
    smtp_class.return_value.sendmail.side_effect = OSError('connection refused')
    sender = _sender(send_server_limit=1)
    assert sender.send('one', 'body', 'a@mail.server.cz').status is MailStatus.FAILED
    assert sender.send('two', 'body', 'a@mail.server.cz').status is MailStatus.HELD_BACK


@mock.patch('smtplib.SMTP')
def test_notices_never_exceed_the_send_server_limit(smtp_class):
    sender = _sender(send_receiver_limit=1, send_server_limit=3)
    assert sender.send('Alert', 'body', 'a@mail.server.cz')
    assert sender.send('Alert', 'body', 'b@mail.server.cz')
    # a gets the send receiver limit notice from the reserve: the window is full.
    assert sender.send('Alert', 'body', 'a@mail.server.cz').status is MailStatus.HELD_BACK
    assert sender.send('Other', 'body', 'c@mail.server.cz').status is MailStatus.HELD_BACK
    assert _subjects(smtp_class) == [(['a@mail.server.cz'], 'Alert'), (['b@mail.server.cz'], 'Alert'),
                                     (['a@mail.server.cz'], 'Alert - NOT SENT')]


@mock.patch('smtplib.SMTP')
def test_held_back_email_is_logged(smtp_class, caplog):
    sender = _sender(send_server_limit=1, send_server_window_seconds=60)
    assert sender.send('one', 'body', 'a@mail.server.cz')
    with caplog.at_level(logging.WARNING, logger='doc_api.tools.mail_sender'):
        assert not sender.send('two', 'body', 'a@mail.server.cz')
    assert ("Email 'two' from worker@mail.server.cz to a@mail.server.cz held back by the "
            "send server limit of 1 emails per 60 s") in caplog.text
    smtp_class.return_value.sendmail.assert_called_once()


@pytest.mark.parametrize('settings', [dict(send_receiver_limit=-1), dict(send_receiver_window_seconds=0),
                                      dict(send_receiver_window_seconds=float('nan')), dict(send_server_limit=-1),
                                      dict(send_server_window_seconds=0), dict(send_server_window_seconds=float('inf'))])
def test_invalid_limits_are_rejected(settings):
    with pytest.raises(ValueError):
        _sender(**settings)


@pytest.mark.parametrize('security,password', [('none', None), ('starttls', None), ('starttls', 'secret'),
                                               ('ssl', None), ('ssl', 'secret')])
def test_a_round_trip_through_a_local_smtp_server(local_smtp, security, password):
    server = local_smtp(security, login='login', password=password)
    sender = MailSender(server=server.host, port=server.port, security=security, sender_mail='worker@mail.server.cz',
                        sender_name='Worker', username='login', password=password, ca_file=server.ca_file,
                        timeout=5)

    result = sender.send('Job done', 'Příliš žluťoučký kůň', 'a@mail.server.cz')

    assert result == MailResult(MailStatus.SENT, 'a@mail.server.cz')
    [mail] = server.messages
    assert mail.mail_from == 'worker@mail.server.cz'
    assert mail.rcpt_tos == ['a@mail.server.cz']
    assert mail.login == ('login' if password else None)
    message = mail.message
    assert message['Subject'] == 'Job done'
    assert message['From'] == 'Worker <worker@mail.server.cz>'
    assert message['To'] == 'a@mail.server.cz'
    assert message['X-Mailer'] == 'DocAPI MailSender'
    assert message['Message-ID'] and message['Date']
    assert message.get_content_type() == 'text/plain' and message.get_content_charset() == 'utf-8'
    assert message.get_content() == 'Příliš žluťoučký kůň'


@pytest.mark.parametrize('security,password', [('starttls', 'wrong'), ('none', 'secret')])
def test_a_local_smtp_server_refusing_the_login_fails_the_send(local_smtp, security, password):
    # A wrong password, and a login without TLS, which the server does not offer.
    server = local_smtp('starttls' if security == 'starttls' else 'none', login='login', password='secret')
    sender = MailSender(server=server.host, port=server.port, security=security, sender_mail='worker@mail.server.cz',
                        username='login', password=password, ca_file=server.ca_file, timeout=5)

    result = sender.send('Job done', 'body', 'a@mail.server.cz')

    assert result.status is MailStatus.FAILED and result.reason.startswith('SMTP')
    assert not server.messages


@pytest.mark.parametrize('security', ['starttls', 'ssl'])
def test_a_server_certificate_the_sender_does_not_trust_fails_the_send(local_smtp, security, caplog):
    server = local_smtp(security, login='login', password='secret')
    # The server's self-signed certificate is not among the system's CAs.
    sender = MailSender(server=server.host, port=server.port, security=security, sender_mail='worker@mail.server.cz',
                        username='login', password='secret', timeout=5)

    result = sender.send('Job done', 'body', 'a@mail.server.cz')

    assert result.status is MailStatus.FAILED and 'CERTIFICATE_VERIFY_FAILED' in result.reason
    assert not server.messages


def test_an_unreadable_ca_file_is_rejected(tmp_path):
    with pytest.raises(OSError):
        _sender(ca_file=str(tmp_path / 'missing.pem'))


def test_the_send_receiver_and_server_limits_hold_against_a_local_smtp_server(local_smtp):
    server = local_smtp()
    sender = MailSender(server=server.host, port=server.port, sender_mail='worker@mail.server.cz',
                        send_receiver_limit=1, send_server_limit=4, timeout=5)

    statuses = [sender.send('Alert', 'body', 'a@mail.server.cz').status for _ in range(3)]
    statuses += [sender.send(subject, 'body', 'b@mail.server.cz').status for subject in ('Other', 'Third')]

    # Alert, its send receiver limit notice, then Other uses the last regular slot and Third
    # gets the notice kept in reserve.
    assert statuses == [MailStatus.SENT, MailStatus.HELD_BACK, MailStatus.HELD_BACK, MailStatus.SENT,
                        MailStatus.HELD_BACK]
    assert [mail.message['Subject'] for mail in server.messages] == [
        'Alert', 'Alert - NOT SENT', 'Other', 'Third - NOT SENT']
    assert [mail.rcpt_tos for mail in server.messages][-1] == ['b@mail.server.cz']
