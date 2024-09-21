# happystore
## Overview

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

```python
>>> HappyStore('/tmp/happy_store_db_file.dat', serializer=PickleSerializer())  
<happystore.HappyStore object at 0x...>

```
To run a HappyStore in memory, just use the special ":memory:" string in place of a filename

```python
>>> HappyStore(':memory:', serializer=PickleSerializer())  
<happystore.HappyStore object at 0x...>

```
so now that you can see how to construct various kinds of HappyStores, let's explore how to
work with one

```python
>>> store = HappyStore(':memory:', serializer=PickleSerializer())

```
A key value pair is added into the store via the "set" method

```python
>>> store.set('a', 5)

```
Keys must be strings

```python
>>> store.set(0, 5)
Traceback (most recent call last):
...
TypeError: key must be a str, not <class 'int'>

```
but values can be any kind of python object (as long as it's 
one your serialzer class can turn into bytes)

```python
>>> store.set('b', {'k': [1, 2]})

```
To get a value back out of the store, you use the 'get' method

```python
>>> store.get('a')
5

```
Trying to get a value that doesn't exist results in a LookupError

```python
>>> store.get('c')
Traceback (most recent call last):
...
LookupError

```
alternatively, you may check for the existance of a key-value pair with the 'has' method

```python
>>> store.has('a')
True
>>> store.has('c')
False

```
To remove a key-value pair you use the 'delete' method

```python
>>> store.delete('b')
True

```
It returns true if the key existed and was deleted and False if it never existed

With the current operations we've shown you (set, get, has, and delete), there's no way to establish
and search for a "group" or "range" of key-value pairs. Happystore provides a way for you to do this
with its "query" method.

first we'll set up some keys to search through

```python
>>> store.set('a', 5)

```
```python
>>> store.set('ab', 10)

```
```python
>>> store.set('abc', 15)

```
You can then search with the 'query' method

```python
>>> store.query(keyprefix='a') == [('a', 5), ('ab', 10), ('abc', 15)]  # it returns key-value pairs
True

```
Instead of searching by key prefix, you can also search by range (always inclusive)
```python
>>> store.query(start='a', end='ab') == [('a', 5), ('ab', 10)]
True

```
You can also iterate through all key-value pairs in the database with the scan method

```python
>>> list(store.scan())
[('a', 5), ('ab', 10), ('abc', 15)]

```
Happystore also provides methods for efficiently performing 'bulk' get, set, and delete operations

```python
>>> store.bulk_set([('1', 'bacon'), ('2', 'eggs'), ('3', 'cheese')])

```
```python
>>> store.bulk_get(['1', '2', '3'])
['bacon', 'eggs', 'cheese']
>>> store.bulk_delete(['1', '2', '3'])
[True, True, True]

```
Finally, and perhaps the most important feature of all in HappyStore, is it's support for "transactions".

The HappyStore library was designed with strictly serializable transactions down to its very core;
In fact, every single operation is implicitly executed in a transaction if not explicitly placed in one.

To explicitly begin a transaction, you use the "transaction" method to get a context manager.

```python
>>> with store.transaction():  
...     value = store.get('a')
...     store.set('a', value + 1)
... 

```
The above implements an atomic thread-safe and process-safe incrrement operation on the 
integer stored at 'a'.

Happystore Transactions can be explicitly aborted from within by raising an AbortionError,
and they are implicitly aborted if an exception is thrown inside a trasaction.

```python
>>> with store.transaction():  
...     value = store.get('a')
...     store.delete('a')
...     if value < 12:
...         raise AbortionError()
...
>>> store.get('a')
5

```
The above conditionally deletes the key-value pair at 'a' if it's value is over 12

It is worth noting that you are also allowed to nest transactions with the expected
semantics

Such strictly serializable transactions are an incredibly powerful primitive, but caution and discipline is required
when using them. Remember, Happystore only executes one of these transactions at a time, so
any other threads or processes must 'wait'; A long running transaction has the potential to
absoultely destroy performance. Transaction blocks should almost always be kept short, containing
only basic, fast maniulations of python objects and HappyStore method calls.

Once you're done working with a happystore database, you'll want to close it

```python
>>> store.close()

```
If you've read up to this point, you've now more or less learned the entire api. See the api docs
below for some more information.

## API Reference
### **class** AbortionError
An exception to raise inside a transaction to abort
it and roll it back. A transaction context manager
will not re-raise it, unlike other exceptions thrown
inside it.


### **class** SerializationError
An exception raised in Serializer implementations
when some object couldn't be serialized.


### **class** DeserializationError
An exception raised in Serializer implementations
when some object couldn't be deserialized.


### **class** Serializer
Abstract base class used to define your own custom serialization
and deserialization of objects. For example, the below implements
application level encryption of pickles

```python
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
...            raise SerializationError('couldn\'t serialize value')
...
...    def deserialize(self, bytess):
...        try:
...            return pickle.loads(self._cipher.decrypt(value))
...        except Exception:
...            raise DeserializationError('couldn\'t deserialize value')
...

```

#### *abstract method* serialize(self, value)
Serialize an object  
  
Args:  
- value: any kind of object  
  
Returns:  
- bytes: the object serialized to bytes  
  
Raises:  
- SerializationError: if the value couldn't be serialized  
  


#### *abstract method* deserialize(self, bytess)
Deserialize some bytes  
  
Args:  
- value (bytes): the bytes to deserialize  
  
Returns:  
- the deserialized object  
  
Raises:  
- DeserializationError: if the value couldn't be deserialized  
  


### **class** PickleSerializer
A serializer implementation that uses the python pickle
module to serialize and deserialize values. Custom
Pickler and Unpickler classes can be passed to the constructor
to customize behavior further.


#### *method* \_\_init\_\_(self, pickler_factory=<class '_pickle.Pickler'>, unpickler_factory=<class '_pickle.Unpickler'>)


#### *method* serialize(self, value)
Serialize an object with pickler_factory  
  
Args:  
- value: any kind of object  
  
Returns:  
- bytes: the object serialized to bytes  
  
Raises:  
- SerializationError: if the value couldn't be serialized  


#### *method* deserialize(self, bytess)
Deserialize some bytes with unpickler_factory  
  
Args:  
- value (bytes): the bytes to deserialize  
  
Returns:  
- the deserialized object  
  
Raises:  
- DeserializationError: if the value couldn't be deserialized  
  


### **class** JsonSerializer
A serializer implementation that uses the python json module
to serialize and deserialize values. expects utf-8 encoded json.


#### *method* serialize(self, value)
Serialize an object to utf-8 json  
  
Args:  
- value: any kind of object  
  
Returns:  
- bytes: the object serialized to bytes  
  
Raises:  
- SerializationError: if the value couldn't be serialized  
  


#### *method* deserialize(self, bytess)
Deserialize some bytes from utf-8 json  
  
Args:  
- value (bytes): the bytes to deserialize  
  
Returns:  
- the deserialized object  
  
Raises:  
- DeserializationError: if the value couldn't be deserialized  
  


### **class** RawSerializer
A serializer implementation that doesn't do any serialization
or deserialization. Its serialize method just takes bytes
and returns them as they are, and it's deserialize method does
the same.


#### *method* serialize(self, value)
Serialize pure bytes  
  
Args:  
- value (bytes):  
  
Returns:  
- bytes: the object serialized to bytes  
  
Raises:  
- SerializationError: if the value couldn't be serialized  
  


#### *method* deserialize(self, bytess)
Deserialize pure bytes  
  
Args:  
- value (bytes): the bytes to deserialize  
  
Returns:  
- the deserialized object  
  
Raises:  
- DeserializationError: if the value couldn't be deserialized  
  


### **class** HappyStore

The class for connection to a HappyStore database.

#### *method* \_\_init\_\_(self, database, serializer, timeout=None)
make and connect to a new HappyStore, or connect to an existing one  
  
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


#### *method* close(self)
close the connection to the HappyStore database

#### *method* \_\_enter\_\_(self)


#### *method* \_\_exit\_\_(self, exc_type, exc_val, exc_tb)


#### *method* get(self, key)
try to get the key-value pair by key  
  
Args:  
- key (str):  
  
Returns:  
- object: The value of the key value pair  
  
Raises:  
- LookupError: If the key-value pair isn't in the HappyStore  
- DeserializationError: if the value couldn't be deserialized  


#### *method* bulk_get(self, keys)
get a bunch of key-value pairs at once  
  
Args:  
- keys: a list of the keys to get  
  
Returns:  
- a list of values and/or LookupErrors  


#### *method* has(self, key)
test if the key is in the HappyStore  
  
Args:  
- key (str):  
  
Returns:  
- bool: The value of the key value pair  


#### *method* set(self, key, value)
try to set the key-value pair by key  
  
Args:  
- key (str):  
- value (object):  
  
Raises:  
- SerializationError: if the value couldn't be serialized  


#### *method* bulk_set(self, key_value_pairs)
set a bunch of key-value pairs at once  
  
Args:  
- key_value_pairs: a list of tuples (first item is key, second is value)  
  
Raises:  
- SerializationError: if the value couldn't be serialized  


#### *method* delete(self, key)
remove the key-value pair from the HappyStore  
  
Args:  
- key (str):  
  
Returns:  
- bool: True if the key found and deleted, False if it didn't exist  


#### *method* bulk_delete(self, keys)
remove a bunch of key-value pairs at once  
  
Args:  
- keys: a list of the keys to remove  
  
Returns:  
- a list of boolean values for each key removed  


#### *method* query(self, keyprefix=None, start=None, end=None, limit=None, reverse=False)
search for a range of key-value pairs in the happystore.  
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


#### *method* scan(self, pagesize=100)
iterate through key-value pairs in the happystore, one page  
at a time. pagesize can be adjusted to fine tune the performance.  
larger page sizes consume more memory but perform less io.  
  
Args:  
- pagesize(int): number of key-value pairs per page  
  
Returns:  
- an iterator of of key-value pairs  


#### *method* transaction(self)
returns a context manager for executing operations  
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




## authors/contributors
 * Joeph P McAnulty
