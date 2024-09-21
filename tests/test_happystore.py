# Copyright 2024 Joseph P McAnulty. All rights reserved.
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from functools import partial

import happystore

class BasicInOutTestsMixin:
    def test_basic_in_out_scenario(self):
        self.store.set('a', 5)

        res = self.store.get('a')
        self.assertEqual(res, 5)

        res = self.store.has('a')
        self.assertEqual(res, True)

        res = self.store.delete('a')
        self.assertEqual(res, True)

        res = self.store.delete('a')
        self.assertEqual(res, False)

        res = self.store.has('a')
        self.assertEqual(res, False)

        with self.assertRaises(LookupError):
            self.store.get('a')

    def test_basic_bulk_in_out_scenario(self):
        self.store.bulk_set([('a', 5), ('b', 7), ('c', 9)])

        res = self.store.bulk_get(['a', 'b', 'd'])
        self.assertEqual(res[0], 5)
        self.assertEqual(res[1], 7)
        self.assertEqual(type(res[2]), LookupError)

        res = self.store.bulk_delete(['a', 'd'])
        self.assertEqual(res, [True, False])

        res = self.store.has('a')
        self.assertEqual(res, False)

    def test_query_by_keyprefix(self):
        self.store.set('a', 1)
        self.store.set('ab', 2)
        self.store.set('c', 3)

        res = self.store.query(keyprefix='a')
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(keyprefix='c')
        self.assertEqual(
            res,
            [('c', 3)]
        )

        res = self.store.query(keyprefix='d')
        self.assertEqual(
            res,
            []
        )

        res = self.store.query(keyprefix='ac')
        self.assertEqual(
            res,
            []
        )

        res = self.store.query(keyprefix='')
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2), ('c', 3)]
        )

    def test_query_by_min_max_range(self):
        self.store.set('a', 1)
        self.store.set('ab', 2)
        self.store.set('c', 3)

        res = self.store.query(start='a', end='ab')
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(start='c', end='c')
        self.assertEqual(
            res,
            [('c', 3)]
        )

        res = self.store.query(start='d', end='e')
        self.assertEqual(
            res,
            []
        )

        res = self.store.query(start='ab', end='c')
        self.assertEqual(
            res,
            [('ab', 2), ('c', 3)]
        )

        res = self.store.query(start='', end='z')
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2), ('c', 3)]
        )

    def test_query_by_min_and_limit(self):
        self.store.set('a', 1)
        self.store.set('ab', 2)
        self.store.set('c', 3)

        res = self.store.query(start='a', limit=2)
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(start='a', limit=2, reverse=True)
        self.assertEqual(
            res,
            [('c', 3), ('ab', 2)]
        )

        res = self.store.query(start='', limit=2)
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(start='d', limit=2, reverse=True)
        self.assertEqual(
            res,
            []
        )

    def test_query_by_max_and_limit(self):
        self.store.set('a', 1)
        self.store.set('ab', 2)
        self.store.set('c', 3)

        res = self.store.query(end='c', limit=2)
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(end='c', limit=2, reverse=True)
        self.assertEqual(
            res,
            [('c', 3), ('ab', 2)]
        )

        res = self.store.query(end='e', limit=2)
        self.assertEqual(
            res,
            [('a', 1), ('ab', 2)]
        )

        res = self.store.query(end='e', limit=2, reverse=True)
        self.assertEqual(
            res,
            [('c', 3), ('ab', 2)]
        )

    def test_scan(self):
        self.store.set('a', 1)
        self.store.set('b', 2)
        self.store.set('c', 3)

        res = list(self.store.scan(pagesize=1))
        self.assertEqual(
            res,
            [('a', 1), ('b', 2), ('c', 3)]
        )

    def test_transaction_commit(self):
        with self.store.transaction():
            self.store.set('a', 1)
            self.store.set('b', 2)

        self.assertEqual(self.store.has('a'), True)
        self.assertEqual(self.store.has('b'), True)

    def test_transaction_rollback_explicit_abort(self):
        with self.store.transaction():
            self.store.set('a', 1)
            raise happystore.AbortionError()

        self.assertEqual(self.store.has('a'), False)

    def test_transaction_rollback_unexpected_exception(self):
        with self.assertRaises(Exception):
            with self.store.transaction():
                self.store.set('a', 1)
                raise Exception()

        self.assertEqual(self.store.has('a'), False)

    def test_nested_transactions(self):
        # set up a complex scenario without a bubbling up exception
        with self.store.transaction():
            self.store.set('a', 1)
            with self.store.transaction():
                self.store.set('b', 2)
                raise happystore.AbortionError()
            with self.store.transaction():
                with self.store.transaction():
                    self.store.set('c', 3)
            self.store.set('d', 4)

        self.assertEqual(self.store.has('a'), True)
        self.assertEqual(self.store.has('b'), False)
        self.assertEqual(self.store.has('c'), True)
        self.assertEqual(self.store.has('d'), True)

        # set up a complex scenario with a bubbling up exception
        with self.assertRaises(Exception):
            with self.store.transaction():
                self.store.set('e', 1)
                with self.store.transaction():
                    self.store.set('f', 2)
                    raise Exception()
                with self.store.transaction():
                    with self.store.transaction():
                        self.store.set('g', 3)
                self.store.set('h', 4)

        self.assertEqual(self.store.has('e'), False)
        self.assertEqual(self.store.has('f'), False)
        self.assertEqual(self.store.has('g'), False)
        self.assertEqual(self.store.has('h'), False)


class InterfaceErrorTestsMixin:  # type errors, value errors, runtime errors, etc 
    pass  # TODO write these eventually


class StressTestsMixin:
    def test_1000_threads_incrementing_one_key(self):
        self.store.set('a', 0)

        def incr_a():
            time.sleep(1)
            with self.store.transaction():
                a_val = self.store.get('a')
                self.store.set('a', a_val + 1)

        for i in range(1000):
            t = threading.Thread(target=incr_a)
            t.start()

        time.sleep(2)  # give the 1000 threads a clear head start
        self.assertEqual(self.store.get('a'), 1000)

    def test_1000_threads_inserting_their_own_key(self):
        for i in range(1000):
            t = threading.Thread(target=self.store.set, args=(str(i), i))
            t.start()
        time.sleep(1)  # give the 1000 threads a head start
        self.assertEqual(self.store.get('0'), 0)
        self.assertEqual(self.store.get('999'), 999)

    def test_scan_1000_keys(self):
        for i in range(1000):
            self.store.set(str(i), i)

        self.assertEqual(len(list(self.store.scan(pagesize=1))), 1000)

    def test_long_running_transaction_contention(self):
        self.store.set('a', 0)

        def incr_a():
            with self.store.transaction():
                a_val = self.store.get('a')
                time.sleep(7)
                self.store.set('a', a_val + 1)

        for i in range(2):
            t = threading.Thread(target=incr_a)
            t.start()

        time.sleep(1)  # make sure the transactions above get a change to 'start'
        self.assertEqual(self.store.get('a'), 2)


class BaiscInOutTestsInMemory(
    unittest.TestCase,
    BasicInOutTestsMixin,
    InterfaceErrorTestsMixin,
    StressTestsMixin
):
    def setUp(self):
        self.store = happystore.HappyStore(
            ':memory:',
            serializer=happystore.PickleSerializer()
        )
        self.addCleanup(self.store.close)


class BaiscInOutTestsOnDisk(
    unittest.TestCase,
    BasicInOutTestsMixin,
    InterfaceErrorTestsMixin,
    StressTestsMixin
):
    def setUp(self):
        # make the store
        self.store = happystore.HappyStore(
            'test_db.dat',
            serializer=happystore.PickleSerializer()
        )
        self.addCleanup(partial(os.remove, 'test_db.dat'))
        self.addCleanup(self.store.close)


class SerializationAnomaliesThreadingTests(unittest.TestCase):  # ensure multi-threading is actually safe
    def setUp(self):
        # first, i need a store object to use
        self.store = happystore.HappyStore(':memory:', serializer=happystore.PickleSerializer())
        # then, i need to share it between two threads
        self.thread_executor = ThreadPoolExecutor(max_workers=2)
        self.addCleanup(self.thread_executor.shutdown)

    def test_no_dirty_reads_with_transactions(self):
        # create ideal conditions for dirty read
        def fast_reader():
            with self.store.transaction():
                # try to read a somewhat quickly,
                # but it shouldn't be here yet
                # since it wasn't commited
                # by slow_writer
                time.sleep(1)
                result = self.store.has('a')
                return result

        def slow_writer():
            with self.store.transaction():
                # quickly set a value
                self.store.set('a', 1)
                # but wait a bit to 'commit'
                time.sleep(2)

        
        fast_reader_future = self.thread_executor.submit(fast_reader)
        slow_writer_future = self.thread_executor.submit(slow_writer)

        wait([fast_reader_future, slow_writer_future])

        # fast_reader shouldn't have seen the 'a' key since
        # even though it was set in a seperate transaction,
        # it wasn't commited yet
        self.assertEqual(fast_reader_future.result(), False)
        # but now our new get should see the new value
        self.assertEqual(self.store.get('a'), 1)

    def test_no_non_repeatable_reads_with_transactions(self):
        # create ideal conditions for non-repeatable read
        def slow_reader():
            with self.store.transaction():
                time.sleep(2)
                result = self.store.has('a')
                return result

        def fast_writer():
            with self.store.transaction():
                time.sleep(1)
                self.store.set('a', 1)
        
        slow_reader_future = self.thread_executor.submit(slow_reader)
        fast_writer_future = self.thread_executor.submit(fast_writer)

        wait([slow_reader_future, fast_writer_future])

        # the slow reader transaction shouldn't see the fast_writer
        # value because it should only see the 'snapshot' of when
        # it was started (snapshot isolation property)
        self.assertEqual(slow_reader_future.result(), False)
        # but now our new get should see the new value
        self.assertEqual(self.store.get('a'), 1)

    def test_no_lost_updates_with_transactions(self):
        # create ideal conditions for lost update
        self.store.set('a', 1)

        def slow_incr():
            with self.store.transaction():
                val = self.store.get('a')
                self.store.set('a', val + 1)
                time.sleep(2)

        def fast_incr():
            with self.store.transaction():
                val = self.store.get('a')
                self.store.set('a', val + 1)
        
        slow_incr_future = self.thread_executor.submit(slow_incr)
        fast_incr_future = self.thread_executor.submit(fast_incr)

        wait([slow_incr_future, fast_incr_future])

        # 'a' should be three if the transactions are truly
        # serialized. a 'lost update would make it only 2
        self.assertEqual(self.store.get('a'), 3)

    def test_no_write_skew_with_transactions(self):
        self.store.set('a', 1)
        self.store.set('b', 1)

        # race to delete either a or b, but make
        # sure at least a or b exists at end

        def x():
            with self.store.transaction():
                if self.store.has('b'):
                    time.sleep(1)  # without serilization, y changes the condition
                    self.store.delete('a')

        def y():
            with self.store.transaction():
                if self.store.has('a'):
                    time.sleep(1)  # without serilization, x changes the condition
                    self.store.delete('b')
        
        x_future = self.thread_executor.submit(x)
        y_future = self.thread_executor.submit(y)

        wait([slow_incr_future, fast_incr_future])

        # if we don't allow write skew, then either a or b should
        # exist. write skew would cause them to both not exist
        self.assertEqual(
            self.store.has('a') or self.store.has('b'),
            True
        )


class SerializationAnomaliesMultiProcessingTests(unittest.TestCase):
    def setUp(self):
        self.store = happystore.HappyStore(
            'test_db.dat',
            serializer=happystore.PickleSerializer()
        )
        self.addCleanup(partial(os.remove, 'test_db.dat'))
        # then, i need to share it between two processes
        self.proc_executor = ProcessPoolExecutor(max_workers=2)
        self.addCleanup(self.proc_executor.shutdown)

    def test_no_dirty_reads_with_transactions(self):
        # create ideal conditions for dirty read
        
        fast_reader_future = self.proc_executor.submit(fast_reader)
        slow_writer_future = self.proc_executor.submit(slow_writer)

        wait([fast_reader_future, slow_writer_future])

        # fast_reader shouldn't have seen the 'a' key since
        # even though it was set in a seperate transaction,
        # it wasn't commited yet
        self.assertEqual(fast_reader_future.result(), False)
        # but now our new get should see the new value
        self.assertEqual(self.store.get('a'), 1)

    def test_no_non_repeatable_reads_with_transactions(self):
        # create ideal conditions for non-repeatable read
        
        slow_reader_future = self.proc_executor.submit(slow_reader)
        fast_writer_future = self.proc_executor.submit(fast_writer)

        wait([slow_reader_future, fast_writer_future])

        # the slow reader transaction shouldn't see the fast_writer
        # value because it should only see the 'snapshot' of when
        # it was started (snapshot isolation property)
        self.assertEqual(slow_reader_future.result(), False)
        # but now our new get should see the new value
        self.assertEqual(self.store.get('a'), 1)

    def test_no_lost_updates_with_transactions(self):
        # create ideal conditions for lost update
        self.store.set('a', 1)
        
        slow_incr_future = self.proc_executor.submit(slow_incr)
        fast_incr_future = self.proc_executor.submit(fast_incr)

        wait([slow_incr_future, fast_incr_future])

        # 'a' should be three if the transactions are truly
        # serialized. a 'lost update would make it only 2
        self.assertEqual(self.store.get('a'), 3)


def slow_incr():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        val = store.get('a')
        store.set('a', val + 1)
        time.sleep(2)
    store.close()

def fast_incr():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        val = store.get('a')
        store.set('a', val + 1)
    store.close()

def slow_reader():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        time.sleep(2)
        result = store.has('a')
        return result
    store.close()

def fast_writer():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        time.sleep(1)
        store.set('a', 1)
    store.close()

def fast_reader():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        # try to read a somewhat quickly,
        # but it shouldn't be here yet
        # since it wasn't commited
        # by slow_writer
        time.sleep(1)
        result = store.has('a')
        return result
    store.close()

def slow_writer():
    store = happystore.HappyStore(
        'test_db.dat',
        serializer=happystore.PickleSerializer()
    )
    with store.transaction():
        # quickly set a value
        store.set('a', 1)
        # but wait a bit to 'commit'
        time.sleep(2)
    store.close()
