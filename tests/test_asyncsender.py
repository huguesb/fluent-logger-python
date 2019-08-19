# -*- coding: utf-8 -*-

from __future__ import print_function

import socket
import unittest

import msgpack

import fluent.asyncsender
from tests import mockserver


class TestSetup(unittest.TestCase):
    def tearDown(self):
        from fluent.asyncsender import _set_global_sender
        _set_global_sender(None)

    def test_no_kwargs(self):
        fluent.asyncsender.setup("tag")
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "localhost")
        self.assertEqual(actual.port, 24224)
        self.assertEqual(actual.timeout, 3.0)
        actual.close()

    def test_host_and_port(self):
        fluent.asyncsender.setup("tag", host="myhost", port=24225)
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 3.0)
        actual.close()

    def test_tolerant(self):
        fluent.asyncsender.setup("tag", host="myhost", port=24225, timeout=1.0)
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 1.0)
        actual.close()


class TestSender(unittest.TestCase):
    def setUp(self):
        super(TestSender, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_decorator_simple(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_nanosecond(self):
        with self._sender as sender:
            sender.nanosecond_precision = True
            sender.emit('foo', {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)

    def test_nanosecond_coerce_float(self):
        time_ = 1490061367.8616468906402588
        with self._sender as sender:
            sender.nanosecond_precision = True
            sender.emit_with_time('foo', time_, {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)
        eq(data[0][1].data, b'X\xd0\x8873[\xb0*')

    def test_no_last_error_on_successful_emit(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'}, None)

        self.assertEqual(sender.last_error, None)

    def test_last_error_property(self):
        EXCEPTION_MSG = "custom exception for testing last_error property"
        self._sender.last_error = socket.error(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)

    def test_clear_last_error(self):
        EXCEPTION_MSG = "custom exception for testing clear_last_error"
        self._sender.last_error = socket.error(EXCEPTION_MSG)
        self._sender.clear_last_error()

        self.assertEqual(self._sender.last_error, None)

    @unittest.skip("This test failed with 'TypeError: catching classes that do not "
                   "inherit from BaseException is not allowed' so skipped")
    def test_connect_exception_during_sender_init(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a sender init socket connect() exception"
        mock_connect.side_effect = socket.error(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)

    def test_sender_without_flush(self):
        with self._sender as sender:
            sender._queue.put((fluent.asyncsender._TOMBSTONE, None))  # This closes without closing
            sender._send_thread.join()
            for x in range(1, 10):
                sender._queue.put(x, None)
            sender.close(False)
            self.assertIs(sender._queue.get(False)[0], fluent.asyncsender._TOMBSTONE)


class TestSenderDefaultProperties(unittest.TestCase):
    def setUp(self):
        super(TestSenderDefaultProperties, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def test_default_properties(self):
        with self._sender as sender:
            self.assertTrue(isinstance(sender.queue_maxsize, int))
            self.assertTrue(sender.queue_maxsize > 0)


class TestSenderWithTimeout(unittest.TestCase):
    def setUp(self):
        super(TestSenderWithTimeout, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port,
                                                       queue_timeout=0.04)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_simple_with_timeout_props(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'}, None)

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))


class TestEventTime(unittest.TestCase):
    def test_event_time(self):
        time = fluent.asyncsender.EventTime(1490061367.8616468906402588)
        self.assertEqual(time.code, 0)
        self.assertEqual(time.data, b'X\xd0\x8873[\xb0*')


class TestSenderWithTimeoutAndOverflow(unittest.TestCase):
    Q_SIZE = 3

    def setUp(self):
        super(TestSenderWithTimeoutAndOverflow, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port,
                                                       queue_maxsize=self.Q_SIZE)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        overflows = []
        def overflow_handler(buf, records=None):
            overflows.append((buf, records))

        self._sender.buffer_overflow_handler = overflow_handler
        with self._sender as sender:
            self.assertEqual(self._sender.queue_maxsize, self.Q_SIZE)

            ok = sender.emit('foo1', {'bar': 'baz1'}, 1)
            self.assertTrue(ok)
            ok = sender.emit('foo2', {'bar': 'baz2'}, 2)
            self.assertTrue(ok)
            ok = sender.emit('foo3', {'bar': 'baz3'}, 3)
            self.assertTrue(ok)
            ok = sender.emit('foo4', {'bar': 'baz4'}, 4)
            self.assertTrue(ok)
            ok = sender.emit('foo5', {'bar': 'baz5'}, 5)
            self.assertTrue(ok)

        data = self.get_data()
        eq = self.assertEqual
        # with the logging interface, we can't be sure to have filled up the queue, so we can
        # test only for a cautelative condition here
        self.assertTrue(len(data) >= self.Q_SIZE)
        eq(3, len(data[0]))
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

        eq(3, len(data[2]))
        self.assertTrue(data[2][1])
        self.assertTrue(isinstance(data[2][1], int))

        self.assertEqual(2, len(overflows))
        for overflow in overflows:
            self.assertEqual(1, len(overflow[1]))
        self.assertEqual(4, overflows[0][1][0])
        self.assertEqual(5, overflows[1][1][0])

class TestSenderUnlimitedSize(unittest.TestCase):
    Q_SIZE = 3

    def setUp(self):
        super(TestSenderUnlimitedSize, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port,
                                                       queue_timeout=0.04,
                                                       queue_maxsize=0)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        with self._sender as sender:
            self.assertEqual(self._sender.queue_maxsize, 0)

            NUM = 1000
            for i in range(1, NUM + 1):
                ok = sender.emit("foo{}".format(i), {'bar': "baz{}".format(i)}, None)
                self.assertTrue(ok)

        data = self.get_data()
        eq = self.assertEqual
        eq(NUM, len(data))
        el = data[0]
        eq(3, len(el))
        eq('test.foo1', el[0])
        eq({'bar': 'baz1'}, el[2])
        self.assertTrue(el[1])
        self.assertTrue(isinstance(el[1], int))

        el = data[NUM - 1]
        eq(3, len(el))
        eq("test.foo{}".format(NUM), el[0])
        eq({'bar': "baz{}".format(NUM)}, el[2])
