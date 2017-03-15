"""

An abstraction layer for metadata fetchers. Supports both syncronous and asyncronous fetchers with cache.

"""

from __future__ import absolute_import, unicode_literals
from .logs import log
import os
import requests
from requests_file import FileAdapter
from .constants import config, EVENT_TX_COMPLETE, EVENT_TX_ABANDON, EVENT_TX_RETRY
from .samlmd import entities_list, entitiesdescriptor, parse_metadata
from .utils import parse_xml, dumptree, Observable
from datetime import datetime, timedelta
from collections import deque, namedtuple
from multiprocess import Queue, Process, Pool
from UserDict import DictMixin
from time import sleep

try:
    from cStringIO import StringIO
except ImportError:  # pragma: no cover
    print(" *** install cStringIO for better performance")
    from StringIO import StringIO


class TxIDGenerator(object):
    def __init__(self):
        self._i = 1

    def next(self):
        n = self._i
        n += 1
        self._i = n
        return n


tx_id = TxIDGenerator()


class ResourceTxN(object):

    def __init__(self, name):
        self.tx_id = tx_id.next()
        self.info = None
        self.ttl = None
        self.xml = None
        self.name = name
        self.ex = None

    def __str__(self):
        return "ResourceTxN[%s (%s)]" % (self.name, self.tx_id)

    def __eq__(self, other):
        return self.tx_id == other.tx_id

    def __le__(self, other):
        self.tx_id == other.tx_id or (self.name == other.name and self.tx_id < other.tx_id)

    def __ge__(self, other):
        self.tx_id == other.tx_id or (self.name == other.name and self.tx_id > other.tx_id)


transaction = ResourceTxN


class ResourceException(Exception):
    def __init__(self, msg, wrapped=None, data=None):
        self._wraped = wrapped
        self._data = data
        super(self.__class__, self).__init__(msg)

    def raise_wraped(self):
        raise self._wraped


class TxSet():

    def __init__(self):
        self._set = list()

    def add(self, other):
        if other not in self:
            self._set.append(other)

    def __contains__(self, item):
        for i in self._set:
            if item >= i: # more recent and covering the same name
                return True
        return False


class UpdateNotification(object):

    def __init__(self, watch, callback, *args, **kwargs):
        self._tx_watch = watch
        self._seen = TxSet()
        self._callback = callback
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        if 'type' in kwargs and 'tx' in kwargs:
            tx = kwargs['tx']
            et = kwargs['type']
            if et is EVENT_TX_COMPLETE:
                self._seen.add(tx)
                if all([tx in self._seen for tx in self._tx_watch]):
                    del kwargs['tx']
                    self._callback(self._args, self._kwargs)
                    return True
            if et is EVENT_TX_ABANDON and tx in self._tx_watch:
                return False
        return None


class ResourceManager(DictMixin, Observable):

    def __init__(self):
        super(ResourceManager, self).__init__()
        self._resources = dict()
        self._pool = Pool(config.worker_pool_size)
        self._okq = Queue()
        self._errq = Queue()
        self._errh = Process(self._error_handler)
        self._okh = Process(self._result_handler)
        self.shutdown = False

    def _result_handler(self):
        while not self.shutdown:
            tx = self._okq.get(block=True)
            r = self[tx.name]
            r.update(tx)
            log.info('Successful update of %s (%s)' % (str(r), r.tx_id))
            if tx.ok():
                log.debug('Transaction %s is complete' % tx.tx_id)
                self.event(type=EVENT_TX_COMPLETE, tx=tx)

    def _error_handler(self):
        while not self.shutdown:
            tx = self._errq.get(block=True)
            r = self[tx.name]
            if r.tx_id < tx.tx_id:  # stil current... lets retry!
                self._pool.apply_async(r.fetch, (tx, self._okq, self._errq))
                self.event(type=EVENT_TX_RETRY, tx=tx)
                log.error("Scheduled retry of (%s) after getting %s" % (tx.tx_id, tx.ex))
            else:
                self.event(type=EVENT_TX_ABANDON, tx=tx)
                log.error("Giving up on (%s) after getting %s - later update succeeded" % (tx.tx_id, tx.ex))

    def __setitem__(self, key, value):
        if not isinstance(value, Resource):
            raise ValueError("I can only store Resources")
        self._resources[key] = value

    def __getitem__(self, key):
        return self._resources[key]

    def __delitem__(self, key):
        if key in self:
            del self._resources[key]

    def keys(self):
        return self._resources.keys()

    def iterkeys(self):
        return self._resources.iterkeys()

    def itervalues(self):
        return self._resources.itervalues()

    def iteritems(self):
        return self._resources.iteritems()

    def add(self, r):
        if not isinstance(r, Resource):
            raise ValueError("I can only store Resources")
        self[r.name] = r

    def __contains__(self, item):
        return item in self._resources

    def reload(self, name_or_url, callback=None, *args, **kwargs):
        if name_or_url not in self:
            raise ResourceException("Unknown resource named '%s'" % name_or_url)

        r = self[name_or_url]
        tx = transaction(name_or_url)
        self._pool.apply_async(r.fetch, (tx, self._okq, self._errq))
        if callback is not None:
            self.onlyonce(UpdateNotification([tx], callback, *args, **kwargs))
        return tx

    def reload_all(self, callback=None, *args, **kwargs):
        txn = []
        for r in self.keys():
            txn.append(self.reload(r))

        if callback is not None:
            self.onlyonce(UpdateNotification(txn, callback, *args, **kwargs)) # XXX race condition


class Resource(object):
    def __init__(self, url, post, **kwargs):
        self.url = url
        self.post = post
        self.opts = kwargs
        self.t = None
        self.expire_time = None
        self._infos = deque(maxlen=config.info_buffer_size)

    def __str__(self):
        return "Resource %s (%s) expires at %s using " % (self.url, self.tx_id, self.expire_time) + \
               ",".join(["%s=%s" % (k, v) for k, v in self.opts.iteritems()])

    def is_valid(self):
        return self.t is not None

    def add_info(self, info):
        self._infos.append(info)

    @property
    def name(self):
        if 'as' in self.opts:
            return self.opts['as']
        else:
            return self.url

    @property
    def info(self):
        return self._infos[0]

    def fetch(self, tx, okq, errq):
        try:
            if "://" in self.url:
                self.fetch_url(tx)
            elif os.path.exists(self.url):
                if os.path.isdir(self.url):
                    self.fetch_dir(tx)
                elif os.path.isfile(self.url):
                    self.url = "file://%s" % self.url
                    self.fetch_url(tx)

            if tx.ok():
                okq.put(tx)
            else:
                errq.put(tx)
        except Exception as ex:
            errq.put(tx)

    def update(self, tx):
        """
        Updates a resource object with the result of a successful background reload process. The update is
        discarded if the tx_id indicates this is an out-of-order update.

        :param tx: A ResourceResult object (named tuple) containing the result of a reload process
        :return: nothing
        """
        if tx.tx_id > self.tx_id:  # protect against out-of-order update results
            self.t = self.post(parse_xml(StringIO(tx.xml), base_url=self.url))
            self.expire_time = datetime.now() + tx.ttl
            self.add_info(tx.info)
            self.tx_id = tx.tx_id
        else:
            log.warning("Ignoring out-of order update (%s) for %s" % (tx.tx_id, str(self)))

    def fetch_url(self, tx):
        s = requests.session()
        s.mount('file://', FileAdapter())
        r = requests.get(self.url)
        info = dict()
        info['Response Headers'] = r.headers
        info['Validation Errors'] = dict()

        if r.ok and len(r.text) > 0:
            t, expire_time_offset = parse_metadata(StringIO(r.text),
                                                   key=self.opts['verify'],
                                                   base_url=self.url,
                                                   fail_on_error=self.opts['fail_on_error'],
                                                   filter_invalid=self.opts['filter_invalid'],
                                                   validate=self.opts['validate'],
                                                   validation_errors=info['Validation Errors'])

            ttl = config.default_cache_duration
            expired = False
            if expire_time_offset is not None:
                expire_time = datetime.now() + expire_time_offset
                ttl = expire_time_offset.total_seconds()
                info['Expiration Time'] = str(expire_time)
                info['Expiration TTL'] = str(ttl)

            if ttl < 0:
                raise ResourceException("Resource at %s has expired" % r.url)

            tx.info = info
            tx.xml = dumptree(t)

            return tx

        raise ResourceException("Got status=%d (%s) while fetching %s" % (r.status_code, r.text, r.url))

    def fetch_dir(self, tx, ext=".xml"):
        directory = self.url
        description = "Entities from %s" % directory
        entities = []
        for top, dirs, files in os.walk(directory):
            for dn in dirs:
                if dn.startswith("."):
                    dirs.remove(dn)
            for nm in files:
                if nm.endswith(ext):
                    log.debug("parsing from file %s" % nm)
                    fn = os.path.join(top, nm)
                    try:
                        validation_errors = dict()
                        t, expire_time_offset = parse_metadata(fn,  # TODO: generate better expiration for this case
                                                               base_url=self.url,
                                                               fail_on_error=self.opts['fail_on_error'],
                                                               filter_invalid=self.opts['filter_invalid'],
                                                               validate=self.opts['validate'],
                                                               validation_errors=validation_errors)
                        entities.extend(entities_list(t))  # local metadata is assumed to be ok
                        for (eid, error) in validation_errors.iteritems():
                            log.error(error)

                        if expire_time_offset is None:
                            expire_time_offset = timedelta(seconds=config.default_cache_duration)
                            # XXX: this may have bad effects - will cause md to not reload for x seconds
                    except Exception as ex:
                        log.error(ex)
                        if self.opts['fail_on_error']:
                            raise ex

        tx.info = dict(Description=description, Size=str(len(entities)))
        tx.ttl = expire_time_offset
        tx.xml = dumptree(entitiesdescriptor(entities, self.url, validate=self.opts['validate'], copy=False))

        return tx
