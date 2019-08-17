# -*- coding: utf-8 -*-

from __future__ import print_function

import threading

try:
    from queue import Queue, Full, Empty
except ImportError:
    from Queue import Queue, Full, Empty

from fluent import sender
from fluent.sender import EventTime

__all__ = ["EventTime", "FluentSender"]

DEFAULT_QUEUE_MAXSIZE = 100

_TOMBSTONE = object()

_global_sender = None


def _set_global_sender(sender):  # pragma: no cover
    """ [For testing] Function to set global sender directly
    """
    global _global_sender
    _global_sender = sender


def setup(tag, **kwargs):  # pragma: no cover
    global _global_sender
    _global_sender = FluentSender(tag, **kwargs)


def get_global_sender():  # pragma: no cover
    return _global_sender


def close():  # pragma: no cover
    get_global_sender().close()


class FluentSender(sender.FluentSender):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 nanosecond_precision=False,
                 msgpack_kwargs=None,
                 queue_maxsize=DEFAULT_QUEUE_MAXSIZE,
                 **kwargs):
        """
        :param kwargs: This kwargs argument is not used in __init__. This will be removed in the next major version.
        """
        super(FluentSender, self).__init__(tag=tag, host=host, port=port, bufmax=bufmax, timeout=timeout,
                                           verbose=verbose, buffer_overflow_handler=buffer_overflow_handler,
                                           nanosecond_precision=nanosecond_precision,
                                           msgpack_kwargs=msgpack_kwargs,
                                           **kwargs)
        self._queue_maxsize = queue_maxsize

        self._thread_guard = threading.Event()  # This ensures visibility across all variables
        self._closed = False

        self._queue = Queue(maxsize=queue_maxsize)
        self._send_thread = threading.Thread(target=self._send_loop,
                                             name="AsyncFluentSender %d" % id(self))
        self._send_thread.daemon = True
        self._send_thread.start()

    def close(self, flush=True):
        with self.lock:
            if self._closed:
                return
            self._closed = True
            if not flush:
                while True:
                    try:
                        self._queue.get(block=False)
                    except Empty:
                        break
            self._queue.put(_TOMBSTONE)
            self._send_thread.join()

    @property
    def queue_maxsize(self):
        return self._queue_maxsize

    def _send(self, bytes_, record):
        with self.lock:
            if self._closed:
                return False
            if self._queue.full():
                self._call_buffer_overflow_handler(bytes_, records=[record])
            else:
                try:
                    self._queue.put((bytes_, record), block=False)
                except Full:    # pragma: no cover
                    return False    # this actually can't happen

            return True

    def _send_loop(self):
        send_internal = super(FluentSender, self)._send_internal

        try:
            while True:
                bytes_, record = self._queue.get(block=True)
                if bytes_ is _TOMBSTONE:
                    break

                send_internal(bytes_, record)
        finally:
            self._close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
