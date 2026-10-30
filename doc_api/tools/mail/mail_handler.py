import datetime
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from logging import Formatter, LogRecord, CRITICAL
from logging.handlers import SMTPHandler
from socket import gethostname
from ssl import SSLContext
from typing import Mapping, Optional, Sequence, Union

'''
Modified version of MailingLogger for ScribbleSense

Based on https://github.com/simplistix/mailinglogger

Copyright (c) 2015-2020 Chris Withers

Copyright (c) 2004-2015 Simplistix Ltd

Copyright (c) 2001-2003 New Information Paradigms Ltd

Permission is hereby granted, free of charge, to any person 
obtaining a copy of this software and associated documentation 
files (the "Software"), to deal in the Software without restriction, 
including without limitation the rights to use, copy, modify, merge, 
publish, distribute, sublicense, and/or sell copies of the Software, 
and to permit persons to whom the Software is furnished to do so, 
subject to the following conditions:

The above copyright notice and this permission notice shall be 
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS 
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN 
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE.
'''

flood_template = ("Too Many Log Entries"
                  "\n\n"
                  "More than %s entries have been logged that would have resulted in\n"
                  "emails being sent."
                  "\n\n"
                  "No further emails will be sent for log entries generated between\n"
                  "%s and %i:00:00"
                  "\n\n"
                  "Please consult any other configured logs, such as a File Logger,\n"
                  "that may contain important entries that have not been emailed.")


x_mailer = 'DocAPI MailHandler'


class SubjectFormatter(Formatter):
    """Format a subject from a log record.

    Besides the record's own attributes, ``%(line)s`` gives the first line of
    the message and ``%(hostname)s`` the host name.
    """

    def format(self, record):
        record.message = record.getMessage()
        if '%(line)' in self._fmt:
            record.line = record.message.split('\n')[0]
        if '%(asctime)' in self._fmt:
            record.asctime = self.formatTime(record, self.datefmt)
        if '%(hostname)' in self._fmt:
            record.hostname = gethostname()
        return self._fmt % record.__dict__


class MailHandler(SMTPHandler):

    now = datetime.datetime.now

    def __init__(self,
                 fromaddr,
                 toaddrs,
                 mailhost='localhost',
                 subject='%(line)s',
                 send_empty_entries=False,
                 flood_level=10,
                 username=None,
                 password=None,
                 headers=None,
                 template=None,
                 charset='utf-8',
                 content_type='text/plain',
                 secure: Union[bool, SSLContext] = None,
                 starttls: bool = False):
        """
        secure: connect over SSL (an SSLContext to use one of your own)
        starttls: upgrade a plain connection with STARTTLS
        The server is logged in to whenever both username and password are given.
        """
        SMTPHandler.__init__(self, mailhost, fromaddr, list(toaddrs or []), subject)
        self.subject_formatter = SubjectFormatter(subject)
        self.send_empty_entries = send_empty_entries
        self.flood_level = flood_level
        self.hour = self.now().hour
        self.sent = 0
        self.username = username
        self.password = password
        self.headers = headers or {}
        self.template = template
        self.charset = charset
        self.content_type = content_type
        self.secure: Union[bool, SSLContext] = secure
        self.starttls = starttls
        if secure and starttls:
            raise ValueError('secure and starttls are mutually exclusive')
        if not self.mailport:
            self.mailport = smtplib.SMTP_SSL_PORT if secure else smtplib.SMTP_PORT

    def getSubject(self, record):
        return self.subject_formatter.format(record)

    def send_message(self,
                     subject: str,
                     body: str,
                     toaddrs: Optional[Sequence[str]] = None,
                     headers: Optional[Mapping[str, str]] = None) -> None:
        """Send one email now, raising on failure.

        toaddrs default to the handler's own receivers. Unlike a logged
        message, it does not count towards the flood level.
        """
        toaddrs = self.toaddrs if toaddrs is None else list(toaddrs)
        if not toaddrs:
            raise ValueError('No receiver mails to send the email to')

        subtype = self.content_type.split('/')[-1]
        email = MIMEText(body, subtype, 'us-ascii' if body.isascii() else self.charset)
        for header, value in {**self.headers, **(headers or {})}.items():
            email[header] = value
        email['Subject'] = subject
        email['From'] = self.fromaddr
        email['To'] = ', '.join(toaddrs)
        email['X-Mailer'] = x_mailer
        email['Date'] = formatdate()
        email['Message-ID'] = make_msgid('MailingLogger')

        if self.secure:
            context = self.secure if isinstance(self.secure, SSLContext) else None
            smtp = smtplib.SMTP_SSL(self.mailhost, self.mailport, context=context)
        else:
            smtp = smtplib.SMTP(self.mailhost, self.mailport)
        with smtp:
            if self.starttls:
                smtp.starttls()
            if self.username and self.password:
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, toaddrs, email.as_string())

    def emit(self, record):
        msg = record.getMessage()
        if not self.send_empty_entries and not msg.strip():
            return

        current_time = self.now()
        current_hour = current_time.hour
        if current_hour != self.hour:
            self.hour = current_hour
            self.sent = 0
        if self.flood_level > 0 and self.sent == self.flood_level:
            # send critical error
            subject = record.subject
            record = LogRecord(
                name='flood',
                level=CRITICAL,
                pathname='',
                lineno=0,
                msg=flood_template % (self.sent,
                                      current_time.strftime('%H:%M:%S'),
                                      current_hour + 1),
                args=(),
                exc_info=None)
            record.subject = f'{subject} - FLOODED'
        elif self.flood_level > 0 and self.sent > self.flood_level:
            # do nothing, we've sent too many emails already
            return
        self.sent += 1

        # actually send the mail
        try:
            msg = self.format(record)
            if self.template is not None:
                msg = self.template % msg
            self.send_message(self.getSubject(record), msg, headers={'X-Log-Level': record.levelname})
        except Exception:
            self.handleError(record)
