# coding: utf-8
import os
import sys
import errno
from os.path import join, normpath, expanduser, dirname, exists, isdir
from ConfigParser import ConfigParser
from optparse import OptionParser
from twisted.python import usage

from rurouni.exceptions import RurouniException, ConfigException
from rurouni import log


defaults = dict(
    CACHE_QUERY_PORT = '7002',
    CACHE_QUERY_INTERFACE = '0.0.0.0',

    LINE_RECEIVER_PORT = '2003',
    LINE_RECEIVER_INTERFACE = '0.0.0.0',

    PICKLE_RECEIVER_PORT = '2004',
    PICKLE_RECEIVER_INTERFACE = '0.0.0.0',

    DEFAULT_WAIT_TIME = 10,
    RUROUNI_METRIC_INTERVAL = 60,
    RUROUNI_METRIC = 'rurouni',

    LOG_UPDATES = True,
    CONF_DIR = None,
    LOCAL_DATA_DIR = None,
    LOCAL_LINK_DIR = None,
    PID_DIR = None,

    MAX_CREATES_PER_MINUTE=float('inf'),
)


class Settings(dict):
    __getattr__ = dict.__getitem__

    def __init__(self):
        dict.__init__(self)
        self.update(defaults)

    def readFrom(self, path, section):
        parser = ConfigParser()
        if not parser.read(path):
            raise RurouniException("Failed to read config: %s" % path)

        if not parser.has_section(section):
            return

        for key, val in parser.items(section):
            key = key.upper()
            val_typ = type(defaults[key]) if key in defaults else str

            if val_typ is list:
                val = [v.strip() for v in val.split(',')]
            elif val_typ is bool:
                val = parser.getboolean(section, key)
            else:
                # attempt to figure out numeric types automatically
                try:
                    val = int(val)
                except:
                    try:
                        val = float(val)
                    except:
                        pass
            self[key] = val


settings = Settings()


class OrderedConfigParser(ConfigParser):
    """
    Ordered Config Parser.

    http://stackoverflow.com/questions/1134071/keep-configparser-output-files-sorted.

    Acturally, from python 2.7 the ConfigParser default dict is `OrderedDict`,
    So we just rewrite the read method to check config file.
    """
    def read(self, path):
        if not os.access(path, os.R_OK):
            raise RurouniException(
                "Missing config file or wrong perm on %s" % path)
        return ConfigParser.read(self, path)


class RurouniOptions(usage.Options):

    optFlags = [
        ["debug", "", "run in debug mode."],
    ]

    optParameters = [
        ['config', 'c', None, 'use the given config file.'],
        ['instance', '', 'a', 'manage a specific rurouni instance.'],
        ['logdir', '', None, 'write logs to the given directory.'],
    ]

    def postOptions(self):
        global settings
        pidfile = self.parent['pidfile']
        if pidfile.endswith('twistd.pid'):
            pidfile = None
        self['pidfile'] = pidfile

        # Enforce a default umask of '022' if none was set.
        if not self.parent.has_key('umask') or self.parent['umask'] is None:
            self.parent['umask'] = 022

        program = self.parent.subCommand
        settings['program'] = program
        program_settings = read_config(program, self)
        settings.update(program_settings)

        # normalize and expand path
        variables = ['STORAGE_DIR', 'LOCAL_DATA_DIR', 'LOCAL_LINK_DIR',
                     'PID_DIR', 'LOG_DIR', 'pidfile', 'INDEX_FILE']
        for var in variables:
            settings[var] = normpath(expanduser(settings[var]))

        storage_schemas = join(settings['CONF_DIR'], 'storage-schemas.conf')
        if not exists(storage_schemas):
            print 'Error missing config %s' % storage_schemas
            sys.exit(1)

        self.parent['pidfile'] = settings['pidfile']

        if not 'action' in self:
            self['action'] = 'start'
        self.handleAction()

        if self['debug']:
            log.setDebugEnabled(True)
        else:
            if self.parent.get('syslog', None):
                log.logToSyslog(self.parent['prefix'])
            elif not self.parent['nodaemon']:
                if not isdir(settings.LOG_DIR):
                    os.makedirs(settings.LOG_DIR)
                log.logToDir(settings.LOG_DIR)

    @staticmethod
    def _normpath(path):
        return normpath(expanduser(path))

    def parseArgs(self, *action):
        if len(action) == 1:
            self["action"] = action[0]

    def handleAction(self):
        action = self['action']
        pidfile = self.parent["pidfile"]
        program = settings['program']
        instance = self['instance']

        if action == 'stop':
            if not exists(pidfile):
                print 'pidfile %s does not exist' % pidfile
                raise SystemExit(0)
            with open(pidfile) as f:
                pid = int(f.read().strip())
            print 'sending kill signal to pid %d' % pid
            try:
                os.kill(pid, 15)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    print 'no process with pid %d running' % pid
                else:
                    raise
            raise SystemExit(0)

        elif action == 'start':
            if exists(pidfile):
                with open(pidfile) as f:
                    pid = int(f.read().strip())
                if _process_alive(pid):
                    print ('%s (instance %s) is already running with pid %d' %
                           (program, instance, pid))
                    raise SystemExit(1)
                else:
                    print 'removing stale pidfile %s' % pidfile
                    try:
                        os.unlink(pidfile)
                    except:
                        print 'could not remove pidfile %s' % pidfile
            else:
                if not os.path.exists(settings['PID_DIR']):
                    try:
                        os.makedirs(settings['PID_DIR'])
                    except OSError as e:
                        if e.errno == errno.EEXIST and os.path.isdir(settings['PID_DIR']):
                            pass

        elif action == 'status':
            if not exists(pidfile):
                print '%s (instance %s) is not running' % (program, instance)
                raise SystemExit(0)
            with open(pidfile) as f:
                pid = int(f.read().strip())

            if _process_alive(pid):
                print ('%s (instance %s) is running with pid %d' %
                       (program, instance, pid))
                raise SystemExit(0)
            else:
                print "%s (instance %s) is not running" % (program, instance)
                raise SystemExit(1)


def get_parser(usage="%prog [options] <start|stop|status>"):
    "Create a parser for command line options."
    parser = OptionParser(usage=usage)
    parser.add_option(
        "--debug", action="store_true",
        help="Run in the foreground, log to stdout")
    parser.add_option(
        "--nodaemon", action="store_true",
        help='Run in the foreground')
    parser.add_option(
        "--pidfile", default=None,
        help='Write pid to the given file')
    parser.add_option(
        "--umask", default=None,
        help="Use the given umask when creating files")
    parser.add_option(
        '--config', default=None,
        help="Use the given config file")
    parser.add_option(
        "--instance", default="a",
        help="Manage a specific rurouni instance")
    return parser


def read_config(program, options):
    """
    Read settings for 'program' from configuration file specified by
    'options["config"]', with missing values provide by 'defaults'.
    """
    settings = Settings()

    # os environ variables
    graphite_root = os.environ.get('GRAPHITE_ROOT')
    if graphite_root is None:
        raise ConfigException('GRAPHITE_ROOT needs to be provided.')
    settings['STORAGE_DIR'] = os.environ.get(
        'STORAGE_DIR', join(graphite_root, 'storage'))
    settings['CONF_DIR'] = os.environ.get(
        'CONF_DIR', join(graphite_root, 'conf'))

    # set default config variables
    settings['LOCAL_DATA_DIR'] = join(settings['STORAGE_DIR'], 'data')
    settings['LOCAL_LINK_DIR'] = join(settings['STORAGE_DIR'], 'link')
    settings['PID_DIR'] = join(settings['STORAGE_DIR'], 'run')
    settings['LOG_DIR'] = join(settings['STORAGE_DIR'], 'log', program)

    if options['config'] is None:
        options['config'] = join(settings['CONF_DIR'], 'rurouni.conf')
    else:
        settings['CONF_DIR'] = dirname(normpath(options['config']))

    # read configuration options from program-specific section.
    section = program[len('rurouni-'):]
    config = options['config']
    if not exists(config):
        raise ConfigException('Error: missing required config %s' % config)

    instance = options['instance']
    settings['instance'] = instance

    # read configuration file
    settings.readFrom(config, section)
    settings.readFrom(config, '%s:%s' % (section, instance))

    settings['pidfile'] = (
        options['pidfile'] or
        join(settings['PID_DIR'], '%s-%s.pid' % (program, instance))
    )
    settings['LOG_DIR'] = (
        options['logdir'] or
        join(settings['LOG_DIR'], '%s-%s' % (program, instance))
    )

    settings['INDEX_FILE'] = join(settings['LOCAL_DATA_DIR'],
                                    '%s.idx' % instance)
    return settings


def _process_alive(pid):
    if exists('/proc'):
        return exists('/proc/%d' % pid)
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except OSError as e:
            return e.errno == errno.EPERM
