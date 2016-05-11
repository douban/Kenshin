# coding: utf-8
from twisted.python.failure import Failure

from rurouni import state, log
from rurouni.state import instrumentation


class Event(object):

    def __init__(self, name, default_handler=None):
        self.name = name
        self.handlers = [default_handler] if default_handler else []

    def addHandler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def removeHandler(self, handler):
        if handler in self.handlers:
            self.handlers.remove(handler)

    def __call__(self, *args, **kwargs):
        for h in self.handlers:
            try:
                h(*args, **kwargs)
            except Exception as e:
                log.err(None,
                        "Exception %s in %s event handler: args=%s, kwargs=%s"
                        % (e, self.name, args, kwargs))


metricReceived = Event('metricReceived',
                       lambda *a, **ka: instrumentation.incr('metricReceived'))

cacheFull = Event('cacheFull')
cacheFull.addHandler(lambda *a, **ka: instrumentation.incr('cacheOverflow'))
cacheFull.addHandler(lambda *a, **ka: setattr(state, 'cacheTooFull', True))
