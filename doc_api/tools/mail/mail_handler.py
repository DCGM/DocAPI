import datetime
from logging import Formatter, Handler, LogRecord, CRITICAL
from socket import gethostname

from doc_api.tools.mail.mail_sender import MailSender


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


class MailHandler(Handler):
    """Emails log records with a MailSender, to the sender's receivers.

    At most flood_level records are emailed per hour (0 for no limit); the
    first one over the limit is replaced by a notice that further ones are
    not emailed.
    """

    now = datetime.datetime.now

    def __init__(self,
                 sender: MailSender,
                 subject='%(line)s',
                 send_empty_entries=False,
                 flood_level=10,
                 template=None):
        Handler.__init__(self)
        self.sender = sender
        self.subject_formatter = SubjectFormatter(subject)
        self.send_empty_entries = send_empty_entries
        self.flood_level = flood_level
        self.hour = self.now().hour
        self.sent = 0
        self.template = template

    def getSubject(self, record):
        return self.subject_formatter.format(record)

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
            self.sender.send_message(self.getSubject(record), msg, headers={'X-Log-Level': record.levelname})
        except Exception:
            self.handleError(record)
