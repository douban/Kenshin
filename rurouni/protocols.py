# coding: utf-8
import cPickle as pickle

from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from twisted.internet.error import ConnectionDone

from rurouni.state import events
from rurouni import log
from rurouni.cache import MetricCache


### metric receiver

class MetricReceiver:
    """ Base class for all metric receive protocols.
    """
    def connectionMade(self):
        self.peerName = self.getPeerName()

    def getPeerName(self):
        if hasattr(self.transport, 'getPeer'):
            peer = self.transport.getPeer()
            return '%s:%d' % (peer.host, peer.port)
        else:
            return 'peer'

    def metricReceived(self, metric, datapoint):
        events.metricReceived(metric, datapoint)


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
    delimiter = '\n'

    def lineReceived(self, line):
        try:
            metric, value, timestamp = line.strip().split()
            datapoint = (int(timestamp), float(value))
        except:
            log.msg('invalid line (%s) received from client %s' %
                    (line, self.peerName))
            return
        self.metricReceived(metric, datapoint)


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
    MAX_LENGTH = 2<<20  # 2M

    def connectionMade(self):
        MetricReceiver.connectionMade(self)

    def stringReceived(self, data):
        try:
            datapoints = pickle.loads(data)
        except:
            log.listener("invalid pickle received from %s, ignoring"
                         % self.peerName)
        for metric, (timestamp, value) in datapoints:
            try:
                datapoint = int(timestamp), float(value)
            except Exception as e:
                continue
            self.metricReceived(metric, datapoint)


class CacheManagementHandler(Int32StringReceiver):
    MAX_LENGTH = 3<<20  # 3M

    def connectionMade(self):
        peer = self.transport.getPeer()
        self.peerAddr = "%s:%s" % (peer.host, peer.port)
        log.query("%s connected" % self.peerAddr)

    def connectionLost(self, reason):
        if reason.check(ConnectionDone):
            log.query("%s disconnected" % self.peerAddr)
        else:
            log.query("%s connection lost: %s" % (self.peerAddr, reason.value))

    def stringReceived(self, rawRequest):
        request = pickle.loads(rawRequest)
        datapoints = MetricCache.get(request['metric'])
        rs = dict(datapoints=datapoints)
        response = pickle.dumps(rs, protocol=-1)
        self.sendString(response)
