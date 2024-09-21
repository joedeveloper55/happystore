# Copyright 2024 Joseph P McAnulty. All rights reserved.
"""
HappyStore is a simple yet feature rich, pure python, embedded key-value database with a tiny api.

If you need something sort of like shelve from the python standard library, but it's feature set,
performance, and robustness just doesn't quite cut it for you, Happystore is the python object 
persistance tool you need.

Major features include:
  * stores arbitrary python objects as values. serialization and deserialization is trivially pluggable.
  * all the expected get, set, delete, and has operations expected of a key-value databse
  * a means to efficiently query a "range" or collection of keys at once, rather than just one at a time
  * a few more convenience operations for optimizing performance (bulk_get, bulk_set, bulk_delete)
  * full thread safety and process safety
  * strictly serializable transactions suppored as a core feature
  * nestable transactions supported as a core feature
  * runs in memory or on disk
  * zero dependencies, pure python

A HappyStore db is a thread-safe, process-safe collection of "key-value pairs". Keys are always utf-8
strings and values can be arbitrary Python objects.

A HappyStore db can be ran on disk or in memory, just like sqlite if you're familiar with it;
Actually, behind the scenes it is backed by sqlite.

To connect to a HappyStore, you must explicitly provide a "serializer object" to the constructor
that controls the serialization and deserialization of python objects to and from bytes (for your
convenience, this module comes with some 'batteries included' serializers in the forms of
PickleSerializer and JsonSerializer. You can create your own by extending the Serializer abstract
base class).

>>> HappyStore('/tmp/happy_store_db_file.dat', serializer=PickleSerializer())  # doctest: +ELLIPSIS
<happystore.HappyStore object at 0x...>

To run a HappyStore in memory, just use the special ":memory:" string in place of a filename

>>> HappyStore(':memory:', serializer=PickleSerializer())  # doctest: +ELLIPSIS
<happystore.HappyStore object at 0x...>

so now that you can see how to construct various kinds of HappyStores, let's explore how to
work with one

>>> store = HappyStore(':memory:', serializer=PickleSerializer())

A key value pair is added into the store via the "set" method

>>> store.set('a', 5)

Keys must be strings

>>> store.set(0, 5)
Traceback (most recent call last):
...
TypeError: key must be a str, not <class 'int'>

but values can be any kind of python object (as long as it's 
one your serialzer class can turn into bytes)

>>> store.set('b', {'k': [1, 2]})

To get a value back out of the store, you use the 'get' method

>>> store.get('a')
5

Trying to get a value that doesn't exist results in a LookupError

>>> store.get('c')
Traceback (most recent call last):
...
LookupError

alternatively, you may check for the existance of a key-value pair with the 'has' method

>>> store.has('a')
True
>>> store.has('c')
False

To remove a key-value pair you use the 'delete' method

>>> store.delete('b')
True

It returns true if the key existed and was deleted and False if it never existed

With the current operations we've shown you (set, get, has, and delete), there's no way to establish
and search for a "group" or "range" of key-value pairs. Happystore provides a way for you to do this
with its "query" method.

first we'll set up some keys to search through

>>> store.set('a', 5)

>>> store.set('ab', 10)

>>> store.set('abc', 15)

You can then search with the 'query' method

>>> store.query(keyprefix='a') == [('a', 5), ('ab', 10), ('abc', 15)]  # it returns key-value pairs
True

Instead of searching by key prefix, you can also search by range (always inclusive)
>>> store.query(start='a', end='ab') == [('a', 5), ('ab', 10)]
True

You can also iterate through all key-value pairs in the database with the scan method

>>> list(store.scan())
[('a', 5), ('ab', 10), ('abc', 15)]

Happystore also provides methods for efficiently performing 'bulk' get, set, and delete operations

>>> store.bulk_set([('1', 'bacon'), ('2', 'eggs'), ('3', 'cheese')])

>>> store.bulk_get(['1', '2', '3'])
['bacon', 'eggs', 'cheese']
>>> store.bulk_delete(['1', '2', '3'])
[True, True, True]

Finally, and perhaps the most important feature of all in HappyStore, is it's support for "transactions".

The HappyStore library was designed with strictly serializable transactions down to its very core;
In fact, every single operation is implicitly executed in a transaction if not explicitly placed in one.

To explicitly begin a transaction, you use the "transaction" method to get a context manager.

>>> with store.transaction():  # doctest: +SKIP
...     value = store.get('a')
...     store.set('a', value + 1)
... 

The above implements an atomic thread-safe and process-safe incrrement operation on the 
integer stored at 'a'.

Happystore Transactions can be explicitly aborted from within by raising an AbortionError,
and they are implicitly aborted if an exception is thrown inside a trasaction.

>>> with store.transaction():  # doctest: +SKIP
...     value = store.get('a')
...     store.delete('a')
...     if value < 12:
...         raise AbortionError()
...
>>> store.get('a')
5

The above conditionally deletes the key-value pair at 'a' if it's value is over 12

It is worth noting that you are also allowed to nest transactions with the expected
semantics

Such strictly serializable transactions are an incredibly powerful primitive, but caution and discipline is required
when using them. Remember, Happystore only executes one of these transactions at a time, so
any other threads or processes must 'wait'; A long running transaction has the potential to
absoultely destroy performance. Transaction blocks should almost always be kept short, containing
only basic, fast maniulations of python objects and HappyStore method calls.

Once you're done working with a happystore database, you'll want to close it

>>> store.close()

If you've read up to this point, you've now more or less learned the entire api. See the api docs
below for some more information.
"""


import abc as _abc
import contextlib as _contextlib
import io as _io
import pickle as _pickle
import threading as _threading
import sqlite3 as _sqlite3


class AbortionError(Exception):
    """An exception to raise inside a transaction to abort
       it and roll it back. A transaction context manager
       will not re-raise it, unlike other exceptions thrown
       inside it.
    """
    pass


class SerializationError(Exception):
    """An exception raised in Serializer implementations
       when some object couldn't be serialized.
    """
    pass


class DeserializationError(Exception):
    """An exception raised in Serializer implementations
    when some object couldn't be deserialized.
    """
    pass


class Serializer(_abc.ABC):
    """ Abstract base class used to define your own custom serialization
    and deserialization of objects. For example, the below implements
    application level encryption of pickles

    >>> import cryptography
    >>> import pickle
    >>> class EncryptedPickleSerializer(Serializer):
    ...    def __init__(self, sym_key):
    ...        self._cipher = cryptography.fernet.Fernet(sym_key)
    ...
    ...    def serailaze(self, value):
    ...        try:
    ...            return self._cipher.encrypt(pickle.dumps(value))
    ...        except Exception:
    ...            raise SerializationError('couldn\\'t serialize value')
    ...
    ...    def deserialize(self, bytess):
    ...        try:
    ...            return pickle.loads(self._cipher.decrypt(value))
    ...        except Exception:
    ...            raise DeserializationError('couldn\\'t deserialize value')
    ...

    """
    @_abc.abstractmethod
    def serialize(self, value):
        """Serialize an object

        Args:
            - value: any kind of object

        Returns:
            - bytes: the object serialized to bytes

        Raises:
            - SerializationError: if the value couldn't be serialized

        """
        pass

    @_abc.abstractmethod
    def deserialize(self, bytess):
        """Deserialize some bytes

        Args:
            - value (bytes): the bytes to deserialize

        Returns:
            - the deserialized object

        Raises:
            - DeserializationError: if the value couldn't be deserialized

        """
        pass


class PickleSerializer(Serializer):
    """A serializer implementation that uses the python pickle
    module to serialize and deserialize values. Custom
    Pickler and Unpickler classes can be passed to the constructor
    to customize behavior further.

    """
    def __init__(
        self,
        pickler_factory=_pickle.Pickler,
        unpickler_factory=_pickle.Unpickler
    ):
        self.pickler_factory = pickler_factory
        self.unpickler_factory = unpickler_factory

    def serialize(self, value):
        """Serialize an object with pickler_factory

        Args:
            - value: any kind of object

        Returns:
            - bytes: the object serialized to bytes

        Raises:
            - SerializationError: if the value couldn't be serialized
        """

        try:
            f = _io.BytesIO()
            p = self.pickler_factory(f)
            p.dump(value)
            return f.getvalue()
        except Exception:
            raise SerializationError('couldn\'t serialize value')

    def deserialize(self, bytess):
        """Deserialize some bytes with unpickler_factory

        Args:
            - value (bytes): the bytes to deserialize

        Returns:
            - the deserialized object

        Raises:
            - DeserializationError: if the value couldn't be deserialized

        """
        try:
            f = _io.BytesIO(bytess)
            return self.unpickler_factory(f).load()
        except Exception:
            raise DeserializationError('couldn\'t deserialize value')

class JsonSerializer(Serializer):
    """A serializer implementation that uses the python json module
    to serialize and deserialize values. expects utf-8 encoded json.

    """

    def serialize(self, value):
        """Serialize an object to utf-8 json

        Args:
            - value: any kind of object

        Returns:
            - bytes: the object serialized to bytes

        Raises:
            - SerializationError: if the value couldn't be serialized

        """
        try:
            return json.dumps(value).encode('utf-8')
        except Exception:
            raise SerializationError('couldn\'t serialize value')

    def deserialize(self, bytess):
        """Deserialize some bytes from utf-8 json

        Args:
            - value (bytes): the bytes to deserialize

        Returns:
            - the deserialized object

        Raises:
            - DeserializationError: if the value couldn't be deserialized

        """
        try:
            return json.loads(bytes.decode('utf-8'))
        except Exception:
            raise DeserializationError('couldn\'t deserialize value')


class RawSerializer(Serializer):
    """A serializer implementation that doesn't do any serialization
       or deserialization. Its serialize method just takes bytes
       and returns them as they are, and it's deserialize method does
       the same.

    """

    def serialize(self, value):
        """Serialize pure bytes

        Args:
            - value (bytes):

        Returns:
            - bytes: the object serialized to bytes

        Raises:
            - SerializationError: if the value couldn't be serialized

        """
        if type(value) is bytes:
            return value
        else:
            raise SerializationError('couldn\'t serialize value')

    def deserialize(self, bytess):
        """Deserialize pure bytes

        Args:
            - value (bytes): the bytes to deserialize

        Returns:
            - the deserialized object

        Raises:
            - DeserializationError: if the value couldn't be deserialized

        """
        if type(bytes) is bytes:
            return value
        else:
            raise SerializationError('couldn\'t serialize value')


class HappyStore:
    """
    The class for connection to a HappyStore database.
    """
    def __init__(self, database, serializer, timeout=None):
        """make and connect to a new HappyStore, or connect to an existing one

        Args:
            - database: a path-like object, often just a string denoting the
                file the Happystore is located in, or just ':memory:' if it's in memory
            - serializer (:obj:`Serializer`): A subclass of the abstract Serializer
                class, used for turning arbitrary Python objects into bytes and
                vice versa for persistance. It is possible for this object to raise
                SerializationError and DeserializationError from its methods.
            - timeout (float): time in seconds to wait on an operation until
              raising an exception from sqlite. passed through to the underlying
              sqlite3 connection

        Returns:
            - A new HappyStore object for interacting with the database
        """
        self._happy_store_impl = _SqlLiteHappyStore(database, serializer, timeout)

    def close(self):
        """close the connection to the HappyStore database"""
        self._happy_store_impl.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def get(self, key):
        """try to get the key-value pair by key

        Args:
            - key (str):

        Returns:
            - object: The value of the key value pair

        Raises:
            - LookupError: If the key-value pair isn't in the HappyStore
            - DeserializationError: if the value couldn't be deserialized
        """
        if type(key) is not str:
            raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.get(key)

    def bulk_get(self, keys):
        """get a bunch of key-value pairs at once

        Args:
            - keys: a list of the keys to get

        Returns:
            - a list of values and/or LookupErrors
        """
        for key in keys:
            if type(key) is not str:
                raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.bulk_get(keys)

    def has(self, key):
        """test if the key is in the HappyStore

        Args:
            - key (str):

        Returns:
            - bool: The value of the key value pair
        """
        if type(key) is not str:
            raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.has(key)

    def set(self, key, value):
        """try to set the key-value pair by key

        Args:
            - key (str):
            - value (object):

        Raises:
            - SerializationError: if the value couldn't be serialized
        """
        if type(key) is not str:
            raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.set(key, value)

    def bulk_set(self, key_value_pairs):
        """set a bunch of key-value pairs at once

        Args:
            - key_value_pairs: a list of tuples (first item is key, second is value)

        Raises:
            - SerializationError: if the value couldn't be serialized
        """
        for key, val in key_value_pairs:
            if type(key) is not str:
                raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.bulk_set(key_value_pairs)

    def delete(self, key):
        """remove the key-value pair from the HappyStore

        Args:
            - key (str):

        Returns:
            - bool: True if the key found and deleted, False if it didn't exist
        """
        if type(key) is not str:
            raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.delete(key)

    def bulk_delete(self, keys):
        """remove a bunch of key-value pairs at once

        Args:
            - keys: a list of the keys to remove

        Returns:
            - a list of boolean values for each key removed
        """
        for key in keys:
            if type(key) is not str:
                raise TypeError('key must be a str, not %s' % type(key))
        return self._happy_store_impl.bulk_delete(keys)

    def query(self, keyprefix=None, start=None, end=None, limit=None, reverse=False):
        """search for a range of key-value pairs in the happystore.
           It is efficient and will locate your range in approximately
           O(log(n)) time complexity.

           For the arguments, you must supply only either keyprefix,
           or start and end. You can't supply all three.

            Args:
                - keyprefix (str): search for all keys with prefix
                - start (str): search for all keys lexicogrphically greater than or equal to start
                - end (str): search for all keys lexicogrphically less than or equal to end
                - limit (int): only return at most the specified number of key-value pairs
                - reverse (bool): if True, search the range in reverse order (max to min).
                  defaults to False (min to max)

            Returns:
                - list: a list of tuples containing the found key value pairs   
        """
        if type(keyprefix) not in (str, type(None)):
            raise TypeError('keyprefix must be a str, not %s' % type(key))
        if type(start) not in (str, type(None)):
            raise TypeError('start must be a str, not %s' % type(key))
        if type(end) not in (str, type(None)):
            raise TypeError('end must be a str, not %s' % type(key))
        # either only keyprefix of min/max are allowed
        if keyprefix is not None and (start is not None or end is not None):
            raise RuntimeError('only supply keyprefix, or start and end')
        return self._happy_store_impl.query(keyprefix, start, end, limit, reverse)

    def scan(self, pagesize=100):
        """iterate through key-value pairs in the happystore, one page
           at a time. pagesize can be adjusted to fine tune the performance.
           larger page sizes consume more memory but perform less io.

            Args:
                - pagesize(int): number of key-value pairs per page

            Returns:
                - an iterator of of key-value pairs
        """
        if type(pagesize) is not int:
            raise TypeError('pagesize must be int, not %s' % type(key))
        return self._happy_store_impl.scan(pagesize)

    def transaction(self):
        """returns a context manager for executing operations
           inside a transaction. All operations are strictly
           serializable.

           An AbortionError can be raised inside a transaction
           block to explicitly rollback the transaction. It is
           never re-raised from the context manger.

           Any other exceptions raised inside the transaction
           block will implicitly rollback the transaction and
           be re-raised.

           The changes are comited at the end of the block if no
           exceptions happened.

           Transactions can be nested.

        Returns:
            - A context manager for wrapping a transaction
        """
        return self._happy_store_impl.transaction()


class _SqlLiteHappyStore:
    def __init__(self, database, serializer, timeout):
        # serializer registration
        self._serializer = serializer

        # txn/thread control primitives
        self._txn_rlock = _threading.RLock()
        self._is_already_in_txn = False
        self._txn_nesting_level = 0

        # interface into main db
        self._sqlite_connection = _sqlite3.connect(
            database,
            check_same_thread=False,
            isolation_level='EXCLUSIVE',
            timeout=(timeout or 5.0)
        )
        self._sqlite_connection_is_already_closed = False
        self._sqlite_connection.executescript("""
            CREATE TABLE IF NOT EXISTS database (
                key              TEXT PRIMARY KEY,
                value            BLOB
            )
        """)

    def close(self):
        with self._txn_rlock:
            if not self._sqlite_connection_is_already_closed:
                self._sqlite_connection_is_already_closed = True
                self._sqlite_connection.close()

    def get(self, key):
        value_bytes = self._ensure_execution_in_txn(
            self._get_impl,
            [key]
        )
        desrialized_value = self._serializer.deserialize(value_bytes)
        return desrialized_value

    def _get_impl(self, key):
        curs = self._sqlite_connection.execute(
            """
            SELECT value
              FROM database
             WHERE key = ?;
            """,
            [key]
        )
        results = curs.fetchall()
        if len(results) != 0:
            value_bytes = results[0][0]
            return value_bytes
        else:
            raise LookupError(key)

    def bulk_get(self, keys):
        values_bytes = self._ensure_execution_in_txn(
            self._bulk_get_impl,
            [keys]
        )  # Note theat 'values_bytes' list may also contain LookupError for items that don't exist
        deserialized_values = [
            self._serializer.deserialize(value_bytes) 
                    if type(value_bytes) == bytes
                    else value_bytes
            for value_bytes in values_bytes
        ]
        return deserialized_values
    
    def _bulk_get_impl(self, keys):
        curs = self._sqlite_connection.execute(
            f"""
            SELECT key, value
              FROM database
             WHERE key IN ({','.join(['?']*len(keys))});
            """,
            keys
        )
        results = curs.fetchall()
        found_kv_pairs = {result[0]: result[1] for result in results}

        return [
            found_kv_pairs[key] if key in found_kv_pairs else LookupError(key)
            for key in keys
        ]

    def has(self, key):
        return self._ensure_execution_in_txn(
            self._has_impl,
            [key]
        )

    def _has_impl(self, key):
        curs = self._sqlite_connection.execute(
            """
            SELECT 1
              FROM database
             WHERE key = ?
             LIMIT 1;
            """,
            [key]
        )
        results = curs.fetchall()
        if len(results) != 0:
            return True
        else:
            return False

    def set(self, key, value):
        value_bytes = self._serializer.serialize(value)
        return self._ensure_execution_in_txn(
            self._set_impl,
            [key, value_bytes]
        )

    def _set_impl(self, key, value):
        self._sqlite_connection.execute(
            """
            REPLACE INTO database
            VALUES (?, ?)
            """,
            [key, value]
        )

    def bulk_set(self, key_value_pairs):
        key_serialized_value_pairs = [
            (k, self._serializer.serialize(v))
            for k, v in key_value_pairs
        ]
        return self._ensure_execution_in_txn(
            self._bulk_set_impl,
            [key_serialized_value_pairs]
        )

    def _bulk_set_impl(self, key_serialized_value_pairs):
        self._sqlite_connection.execute(
            f"""
            REPLACE INTO database
            VALUES {','.join(['(?, ?)']*len(key_serialized_value_pairs))}
            """,
            _flatten(key_serialized_value_pairs)
        )

    def delete(self, key):
        return self._ensure_execution_in_txn(
            self._delete_impl,
            [key]
        )

    def _delete_impl(self, key):
        curs = self._sqlite_connection.execute(
            """
            DELETE FROM database
             WHERE key = ?;
            """,
            [key]
        )
        return curs.rowcount > 0

    def bulk_delete(self, keys):
        return self._ensure_execution_in_txn(
            self._bulk_delete_impl,
            [keys]
        )

    def _bulk_delete_impl(self, keys):
        curs = self._sqlite_connection.execute(
            f"""
            SELECT key
              FROM database
             WHERE key IN ({','.join(['?']*len(keys))});
            """,
            keys
        )
        results = curs.fetchall()
        found_keys = {result[0] for result in results}

        curs = self._sqlite_connection.execute(
            f"""
            DELETE FROM database
             WHERE key IN ({','.join(['?']*len(keys))});
            """,
            keys
        )

        return [
            (key in found_keys) 
            for key in keys
        ]

    def query(self, keyprefix, start, end, limit, reverse):
        kv_pairs_with_bytes_values = self._ensure_execution_in_txn(
            self._query_impl,
            [keyprefix, start, end, limit, reverse]
        )
        kv_pairs = [
            (k, self._serializer.deserialize(v))
            for k, v in kv_pairs_with_bytes_values
        ]
        return kv_pairs

    def _query_impl(self, keyprefix, start, end, limit, reverse):
        if keyprefix is not None:
            sql = f"""
                SELECT key, value
                FROM database
                WHERE key LIKE ?
             ORDER BY key
             {'' if limit is None else 'LIMIT ?'};
            """
            params = [keyprefix + '%'] if limit is None else [keyprefix + '%', limit]
        elif start is not None and end is not None:
            sql = f"""
                SELECT key, value
                FROM database
                WHERE key >= ? AND key <= ?
             ORDER BY key {'ASC' if not reverse else 'DESC'}
             {'' if limit is None else 'LIMIT ?'};
            """
            params = [start, end] if limit is None else [start, end, limit]
        elif start is not None and end is None:
            sql = f"""
                SELECT key, value
                FROM database
                WHERE key >= ?
             ORDER BY key {'ASC' if not reverse else 'DESC'}
             {'' if limit is None else 'LIMIT ?'};
            """
            params = [start] if limit is None else [start, limit]
        elif start is None and end is not None:
            sql = f"""
                SELECT key, value
                FROM database
                WHERE key <= ?
             ORDER BY key {'ASC' if not reverse else 'DESC'}
             {'' if limit is None else 'LIMIT ?'};
            """
            params = [end] if limit is None else [end, limit]

        curs = self._sqlite_connection.execute(sql, params)
        return curs.fetchall()

    def scan(self, pagesize=100):
        next_key = ''  # the 'smallest' string to start
        while True:
            sql = """
                SELECT key, value
                  FROM database
                 WHERE key >= ?
              ORDER BY key
                 LIMIT ?
            """
            params = [next_key, pagesize + 1]
            with self.transaction():
                curs = self._sqlite_connection.execute(sql, params)
                page = curs.fetchall()
            if len(page) < pagesize + 1:   # we're at the end
                page = [
                    (k, self._serializer.deserialize(v))
                    for k, v in page
                ]
                for item in page:
                    yield item
                break
            next_key = page[-1][0]
            page.pop()
            page = [
                (k, self._serializer.deserialize(v))
                for k, v in page
            ]
            for item in page:
                yield item

    @_contextlib.contextmanager
    def transaction(self):
        with self._txn_rlock:
            try:
                if self._is_already_in_txn:
                    self._txn_nesting_level += 1
                    self._sqlite_connection.execute('SAVEPOINT txn;')
                else:
                    self._is_already_in_txn = True
                    self._sqlite_connection.execute('BEGIN EXCLUSIVE TRANSACTION;')
                yield
            except AbortionError:
                if self._txn_nesting_level > 0:
                    self._sqlite_connection.execute('ROLLBACK TO txn;')
                else:
                    self._sqlite_connection.execute('ROLLBACK;')
            except Exception:
                if self._txn_nesting_level > 0:
                    self._sqlite_connection.execute('ROLLBACK TO txn;')
                else:
                    self._sqlite_connection.execute('ROLLBACK;')
                raise
            else:
                if self._txn_nesting_level > 0:
                    self._sqlite_connection.execute('RELEASE SAVEPOINT txn;')
                else:
                    self._sqlite_connection.execute('COMMIT;')
            finally:
                if self._txn_nesting_level == 0:
                    self._is_already_in_txn = False
                else:
                    self._txn_nesting_level -= 1


    def _ensure_execution_in_txn(self, f, args=[], kwargs={}):
        with self._txn_rlock:
            if self._is_already_in_txn:
                return f(*args, **kwargs)
            else:
                with self.transaction():
                    return f(*args, **kwargs)


def _flatten(xss):
        return [x for xs in xss for x in xs]
