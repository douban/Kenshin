# coding: utf-8
import os
from time import time
from os.path import dirname, basename, abspath, splitext
from rurouni.fnv1a import get_int32_hash


def run_twistd_plugin(filename):
    from twisted.scripts.twistd import runApp
    from twisted.scripts.twistd import ServerOptions
    from rurouni.conf import get_parser

    bin_dir = dirname(abspath(filename))
    root_dir = dirname(bin_dir)
    os.environ.setdefault('GRAPHITE_ROOT', root_dir)

    program = splitext(basename(filename))[0]
    parser = get_parser()
    (options, args) = parser.parse_args()

    if not args:
        parser.print_usage()
        return

    twistd_options = []
    try:
        from twisted.internet import epollreactor
        twistd_options.append('--reactor=epoll')
    except:
        pass

    if options.debug or options.nodaemon:
        twistd_options.append('--nodaemon')
    if options.pidfile:
        twistd_options.extend(['--pidfile', options.pidfile])
    if options.umask:
        twistd_options.extend(['--umask', options.umask])

    twistd_options.append(program)

    if options.debug:
        twistd_options.append('--debug')
    for name, value in vars(options).items():
        if (value is not None and
                name not in ('debug', 'nodaemon', 'pidfile', 'umask')):
            twistd_options.extend(["--%s" % name.replace("_", '-'),
                                  value])

    twistd_options.extend(args)
    config = ServerOptions()
    config.parseOptions(twistd_options)
    runApp(config)


class TokenBucket(object):
    ''' Token Bucket algorithm for rate-limiting.
    URL: https://en.wikipedia.org/wiki/Token_bucket

    >>> bucket = TokenBucket(60, 1)
    >>> print bucket.consume(6)
    True
    >>> print bucket.consume(54)
    True
    >>> print bucket.consume(1)
    False
    >>> import time
    >>> time.sleep(1)
    >>> print bucket.consume(1)
    True
    '''
    def __init__(self, capacity, fill_rate):
        '''
        @capacity: total number of tokens in the bucket.
        @fill_rate: the rate in tokens/second that the bucket will be refilled.
        '''
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self.fill_rate = float(fill_rate)
        self.timestamp = time()

    def consume(self, tokens):
        ''' Consume tokens from the bucket.

        Return True if there were sufficient tokens otherwise False.
        '''
        if tokens <= self.tokens:
            self._tokens -= tokens
            return True
        else:
            return False

    @property
    def tokens(self):
        ''' Return the current number of tokens in the bucket. '''
        if self._tokens < self.capacity:
            now = time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens

    def __repr__(self):
        return '<%s %.2f %.2f>' % (
            self.__class__.__name__, self.capacity, self.fill_rate)


def get_instance_of_metric(metric, num_all_instance):
    return get_int32_hash(metric) % num_all_instance


if __name__ == '__main__':
    import doctest
    doctest.testmod()
