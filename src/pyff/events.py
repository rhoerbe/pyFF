"""

A simple asynchronous singleton event queue

"""

from Queue import Queue, Empty, Full
import threading
from .constants import config
import time
from datetime import datetime
import uuid
from .logs import log
import cherrypy
from cherrypy.process import wspbus, plugins

class Manager(threading.Thread):
    def __init__(self, size=10, name="pyFF event manager"):
        threading.Thread.__init__(self, name=name)
        self.callbacks = dict()
        self.queue = Queue(size)
        self.halt = False

    def subscribe(self, t, cb):
        self.callbacks.setdefault(t, [])
        self.callbacks[t].append(cb)

    def stop(self):
        self.halt = True

    def run(self):
        while not self.halt:
            try:
                e = self.queue.get(timeout=1)
                self.callbacks.setdefault(e.type, [])
                for fn in self.callbacks[e.type]:
                    try:
                        fn(e)  # TODO: execute in a thread and do timeout handling
                    except Exception as ex:
                        from traceback import print_exc
                        print_exc(ex)
                        log.error("An error occured while processing an event: %s" % ex)
            except Empty:
                pass


class Emit(threading.Thread):
    def __init__(self, manager, event_factory, frequency, name="pyFF event emitter"):
        threading.Thread.__init__(self, name=name)
        self.manager = manager
        self.event_factory = event_factory
        self.frequency = frequency
        self.halt = False

    def stop(self):
        self.halt = True

    def run(self):

        def _emit():
            self.manager.queue.put(self.event_factory(), block=False)

        _emit()
        t = None
        while not self.halt:
            if t is None or not t.isAlive():
                try:
                    log.debug("emitting new tick in %d seconds" % self.frequency)
                    t = threading.Timer(self.frequency, _emit)
                    t.start()
                except Full:
                    pass
            else:
                t.join(1)

        if t is not None:
            t.cancel()
            t.join()


class Event(object):
    def __init__(self, typ, o, **kwargs):
        self.type = typ
        self.object = o
        self.ctx = kwargs


_manager = Manager(size=config.event_manager_queue_size)


def subscribe(t, cb):
    _manager.subscribe(t, cb)


def publish(t, o, **kwargs):
    e = Event(t, o, **kwargs)
    _manager.queue.put(e)


def every(frequency, o, cb):
    t = uuid.uuid4()

    subscribe(t, cb)

    def _tick():
        log.debug("Emitting an instance (frequency=%d) of %s on %s" % (frequency, t, o))
        return Event(t, o, timestamp=datetime.now())

    thread = Emit(_manager, _tick, frequency)
    thread.start()


class PyffEvents(plugins.SimplePlugin):
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)

    def start(self):
        if not any(isinstance(thread, Manager) for thread in threading.enumerate()):
            _manager.start()

    def stop(self):
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread and (isinstance(t, Manager) or isinstance(t, Emit)):
                log.debug("Asking %s to stop" % t)
                t.stop()
                t.join()


