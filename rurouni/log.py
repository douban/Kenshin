# coding: utf-8

import time
from zope.interface import implements
from sys import stdout, stderr

from twisted.python.log import startLoggingWithObserver, textFromEventDict, msg, err, ILogObserver
from twisted.python.syslog import SyslogObserver
from twisted.python.logfile import DailyLogFile



class RurouniLogObserver(object):
    implements(ILogObserver)

    def __call__(self, event):
        return self.observer(event)

    def logToDir(self, logdir):
        self.logdir = logdir
        self.console_logfile = DailyLogFile('console.log', logdir)
        self.custom_logs = {}
        self.observer = self.logdirObserver

    def logToSyslog(self, prefix):
        observer = SyslogObserver(prefix).emit
        def log(event):
            event['system'] = event.get('type', 'console')
            observer(event)
        self.observer = log

    def stdoutObserver(self, event):
        stdout.write(formatEvent(event, includeType=True) + '\n')
        stdout.flush()

    def logdirObserver(self, event):
        msg = formatEvent(event)
        log_type = event.get('type')

        if log_type is not None and log_type not in self.custom_logs:
            self.custom_logs[log_type] = DailyLogFile(log_type + '.log', self.logdir)

        logfile = self.custom_logs.get(log_type, self.console_logfile)
        logfile.write(msg + '\n')
        logfile.flush()

    observer = stdoutObserver  # default to stdout


def formatEvent(event, includeType=False):
    event['isError'] = 'failure' in event
    msg = textFromEventDict(event)

    if includeType:
        type_tag = '[%s] ' % event.get('type', 'console')
    else:
        type_tag = ''

    timestamp = time.strftime("%d/%m/%Y %H:%M:%S")
    return "%s\t%s\t%s" % (timestamp, type_tag, msg)


rurouniLogObserver = RurouniLogObserver()
logToDir = rurouniLogObserver.logToDir
logToSyslog = rurouniLogObserver.logToSyslog
logToStdout = lambda: startLoggingWithObserver(rurouniLogObserver)


def cache(message, **context):
    context['type'] = 'cache'
    msg(message, **context)

def clients(message, **context):
    context['type'] = 'clients'
    msg(message, **context)

def creates(message, **context):
    context['type'] = 'creates'
    msg(message, **context)

def updates(message, **context):
    context['type'] = 'updates'
    msg(message, **context)

def listener(message, **context):
    context['type'] = 'listener'
    msg(message, **context)

def relay(message, **context):
    context['type'] = 'relay'
    msg(message, **context)

def aggregator(message, **context):
    context['type'] = 'aggregator'
    msg(message, **context)

def query(message, **context):
    context['type'] = 'query'
    msg(message, **context)

def debug(message, **context):
    if debugEnabled:
        msg(message, **context)

debugEnabled = False
def setDebugEnabled(enabled):
    global debugEnabled
    debugEnabled = enabled
