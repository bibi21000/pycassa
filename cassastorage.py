# -*- coding: utf-8 -*-

__license__ = """
    This file is part of BigD.

    BigD is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    BigD is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with BigD.  If not, see <http://www.gnu.org/licenses/>.

    Storage backend for Whoosh 2.6 using pycassa/cassandra

    LIMITATIONS :
        - only ONE writer is available. Got errors when trying 3. More tests are neeeded.
        - is cassandra a good choice for whoosh ??? :
            In logs :
                WARN [ReadStage:80] 2014-01-23 00:21:12,910 SliceQueryFilter.java (line 209) Read 1 live and 3729 tombstoned cells (see tombstone_warn_threshold)
                WARN [ReadStage:89] 2014-01-23 00:21:26,384 SliceQueryFilter.java (line 209) Read 1 live and 3730 tombstoned cells (see tombstone_warn_threshold)
            Cassandra don't like creation/deletions. Seems that segments in Whoose use a lot of ...
            Read : http://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets
        - committing : merge or not, optimize ?
            Actually I use : merge=False and Optimize=True every 75 updates

    Inspired from https://bitbucket.org/mtasic85/whoosh/src/c483499e015171ba40e200777f0ffa4325739cd7/src/whoosh/filedb/filestore.py?at=cassandra-2.4x
    Inspired from https://github.com/samuraisam/padlock/blob/master/padlock/distributed/cassandra.py

    How to use it :

    - Copy this file in your project

    - And use it :

    from cassastorage import PycassaStorage
    ...

    #create a systemManager
    cassa_sys = SystemManager('localhost')

    #and a pool
    pool = ConnectionPool('keyspace',['localhost'])

    #And define a storage for whoosh 2.6
    storage = PycassaStorage("Whoosh", pool, cassa_sys=cassa_sys, keyspace='keyspace', cache_dir="/tmp/whoosh-uniq", readonly=False, supports_mmap=False)

    #Create it if needed
    storage.create()

    #Define a schema
    schema = Schema(page=ID(stored=True,unique=True),
                title=TEXT(stored=True),
                content=TEXT,
                tags=KEYWORD)

    #and create index if needed
    storage.create_index(schema)
    #And open it
    idx=storage.open_index()
    ...

    - write, search in your whoosh index

"""
__copyright__ = "2013 : (c) Sébastien GALLET aka bibi21000"
__author__ = 'Sébastien GALLET aka bibi21000'
__email__ = 'bibi21000@gmail.com'

__all__ = ['PycassaStorage','BusyLockException','StaleLockException']

import os
import sys
from StringIO import StringIO
import thrift
#from pycassa import *
from pycassa import ConsistencyLevel, ColumnFamily
from pycassa.system_manager import SystemManager
from pycassa.pool import ConnectionPool
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
from pycassa.columnfamily import *
from pycassa.index import *
from pycassa.types import UTF8Type,IntegerType, BytesType
from pycassa.util import *
from pycassa.batch import *
import calendar
import datetime
from time_uuid import TimeUUID
from whoosh.index import _DEF_INDEX_NAME, EmptyIndexError
from whoosh.filedb.structfile import StructFile
from whoosh.filedb.filestore import Storage, FileStorage
from whoosh.util import random_name
import logging

_cf_args = [
    'read_consistency_level',
    'write_consistency_level',
    'autopack_names',
    'autopack_values',
    'autopack_keys',
    'column_class_name',
    'super_column_name_class',
    'default_validation_class',
    'column_validators',
    'key_validation_class',
    'dict_class',
    'buffer_size',
    'column_bufer_size',
    'timestamp'
]

class IRetryPolicy(object):
    def duplicate(self):
        """
        Return a new instance of this class with the same startup properties but otherwise clean state"""

    def allow_retry(self):
        """Determines whether or not a retry should be allowed at this time. This may internally sleep..."""

class RunOncePolicy(IRetryPolicy):
    """
    A RetryPolicy that runs only once
    """
    def duplicate(self):
        return self.__class__()

    def allow_retry(self):
        return False

class RunOnceRetryManyPolicy(RunOncePolicy):
    """
    A RetryPolicy that runs only once but allows retry
    """
    def allow_retry(self):
        return True

class BusyLockException(Exception):
    """
    Raised when a lock is already taken on this row.
    """

class StaleLockException(Exception):
    """
    Raised old, stale locks exist on a row and we don't want them to have existed.
    """

# This is pretty much directly lifted from the excellent Astynax cassandra clibrary from Netflix
#
# Here is their copyright:
#
#    Copyright 2011 Netflix
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

class PycassaDistributedRowLock(object):
    """
    A lock that is implemented in a row of a Cassandra column family. It's good to use this type of lock when you want
    to lock a single row in cassandra for some purpose in a scenario where there will not be a lot of lock contention.

    Shamelessly lifted from: Netflix's `Astynax library <https://github.com/Netflix/astyanax>`_. Take a `look <https://github.com/Netflix/astyanax/blob/master/src/main/java/com/netflix/astyanax/recipes/locks/ColumnPrefixDistributedRowLock.java>`_ at the implementation (in Java).

    Importantly, note that this in no way a transaction for a cassandra row!

    :param pool: A pycassa ConnectionPool. It will be used to facilitate communication with cassandra.
    :type pool: pycassa.pool.ConnectionPool
    :param column_family: Either a `string` (which will then be made into a `pycassa.column_family.ColumnFamily` instance) or
        an already configured instance of `ColumnFamily` (which will be used directly).
    :type column_family: string
    :param key: The row key for this lock. The lock can co-exist with other columns on an existing row if desired.
    :type key: string

    The following paramters are optional and all come with defaults:

    :param prefix: The column prefix. Defaults to `_lock_`
    :type prefix: str
    :param lock_id: A unique string, should probably be a UUIDv1 if provided at all. Defaults to a UUIDv1 provided by `time-uuid <http://github.com/samuraisam/time_uuid>`_
    :type lock_id: str
    :param fail_on_stale_lock: Whether or not to fail when stale locks are found. Otherwise they'll just be cleaned up.
    :type fail_on_stale_lock: bool
    :param timeout: How long to wait until the lock is considered stale. You should set this to as much time as you think the work will take using the lock.
    :type timeout: float
    :param ttl: How many seconds until cassandra will automatically clean up stale locks. It must be greater than `timeout`.
    :type ttl: float
    :param backoff_policy: a :py:class:`padlock.distributed.retry_policy.IRetryPolicy` instance. Governs the retry policy of acquiring the lock.
    :type backoff_policy: IRetryPolicy
    :param allow_retry: Whether or not to allow retry. Defaults to `True`
    :type allow_retry: bool

    You can also provide the following keyword arguments which will be passed directly to the `ColumnFamily` constructor
    if you didn't provide the instance yourself:

        * **read_consistency_level**
        * **write_consistency_level**
        * **autopack_names**
        * **autopack_values**
        * **autopack_keys**
        * **column_class_name**
        * **super_column_name_class**
        * **default_validation_class**
        * **column_validators**
        * **key_validation_class**
        * **dict_class**
        * **buffer_size**
        * **column_bufer_size**
        * **timestamp**
    """

    def __init__(self, pool, column_family, key, backoff_policy=RunOncePolicy(), **kwargs):
        #print "Create PycassaDistributedRowLock instance"
        self.pool = pool
        if isinstance(column_family, ColumnFamily):
            self.column_family = column_family
        else:
            cf_kwargs = {k: kwargs.get(k) for k in _cf_args if k in kwargs}
            self.column_family = ColumnFamily(self.pool, column_family, **cf_kwargs)
        self.key = key
        self.consistency_level = kwargs.get('consistency_level', ConsistencyLevel.LOCAL_QUORUM)
        self.prefix = kwargs.get('prefix', '_lock_')
        self.lock_id = kwargs.get('lock_id', str(TimeUUID.with_utcnow()))
        self.fail_on_stale_lock = kwargs.get('fail_on_stale_lock', False)
        self.timeout = kwargs.get('timeout', 60.0)  # seconds
        self.ttl = kwargs.get('ttl', None)
        self.backoff_policy = backoff_policy
        self.allow_retry = kwargs.get('allow_retry', True)
        self.locks_to_delete = set()
        self.lock_column = None

    def acquire(self):
        """
        Acquire the lock on this row. It will then read immediatly from cassandra, potentially retrying, potentially
        sleeping the executing thread.
        """
        logging.debug("try to acquire lock")
        if self.ttl is not None:
            if self.timeout > self.ttl:
                raise ValueError("Timeout {} must be less than TTL {}".format(self.timeout, self.ttl))

        retry = self.backoff_policy.duplicate()
        retry_count = 0

        while True:
            try:
                cur_time = self.utcnow()
                mutation = self.column_family.batch()
                self.fill_lock_mutation(mutation, cur_time, self.ttl)
                mutation.send()
                self.verify_lock(cur_time)
                self.acquire_time = self.utcnow()
                logging.debug( "lock acquired")
                return True
            except BusyLockException, e:
                logging.warning("lock failed")
                self.release()
                if not retry.allow_retry():
                    raise e
                logging.warning("Retry #%s"%retry_count)
                retry_count += 1
        return False

    def release(self):
        """
        Allow this row to be locked by something (or someone) else. Performs a single write (round trip) to Cassandra.
        """
        logging.debug( "release lock")
        if not len(self.locks_to_delete) or self.lock_column is not None:
            mutation = self.column_family.batch()
            self.fill_release_mutation(mutation, False)
            mutation.send()

    def verify_lock(self, cur_time):
        """
        Whether or not the lock can be verified by reading the row and ensuring the paramters of the lock
        according to the current :py:class:`CassandraDistributedRowLock` instance's configuration is valid.

        This must only be called after :py:meth:`acquire` is called, or else you will get a :py:class:`ValueError`

        :param cur_time: The current time in microseconds
        :type cur_time: long
        :rtype: None
        """
        if self.lock_column is None:
            raise ValueError("verify_lock() called without attempting to take the lock")
        cols = self.read_lock_columns()
        for k, v in cols.iteritems():
            if v != 0 and cur_time > v:
                if self.fail_on_stale_lock:
                    raise StaleLockException("Stale lock on row '{}'. Manual cleanup required.".format(self.key))
                self.locks_to_delete.add(k)
            elif k != self.lock_column:
                raise BusyLockException("Lock already acquired for row '{}' with lock column '{}'".format(self.key, k))

    def read_lock_columns(self):
        """
        Return all columns in this row with the timeout value deserialized into a long

        :rtype: dict
        """
        res = {}
        try:
            cols = self.column_family.get(self.key, column_count=1e9)
        except NotFoundException:
            cols = {}
        for k, v in cols.iteritems():
            res[k] = self.read_timeout_value(v)
        return res

    def release_locks(self, force=False):
        """
        Clean up after ourselves. Removes all lock columns (everything returned by :py:meth:`read_lock_columns`)
        that is not stale.

        :param force: Remove even non-stale locks
        """
        locks = self.read_lock_columns()
        cols_to_remove = []
        now = self.utcnow()
        for k, v in locks.iteritems():
            if force or (v > 0 and v < now):
                cols_to_remove.add(k)

        self.column_family.batch().remove(self.key, cols_to_remove).send()

        return locks

    def utcnow(self):
        """
        Used internally - return the current time, as microseconds from the unix epoch (Jan 1 1970 UTC)

        :rtype: long
        """
        d = datetime.datetime.utcnow()
        return long(calendar.timegm(d.timetuple())*1e6) + long(d.microsecond)

    def fill_lock_mutation(self, mutation, time, ttl):
        """
        Used internally - fills out `pycassa.batch.CfMutator` with the necessary steps to acquire the lock.
        """
        if self.lock_column is not None:
            if self.lock_column != (self.prefix + self.lock_id):
                raise ValueError("Can't change prefix or lock_id after acquiring the lock")
        else:
            self.lock_column = self.prefix + self.lock_id
        if time is None:
            timeout_val = 0
        else:
            timeout_val = time + long(self.timeout * 1e6) # convert self.timeout to microseconds

        kw = {}
        if ttl is not None:
            kw['ttl'] = ttl

        mutation.insert(self.key, {self.lock_column: self.generate_timeout_value(timeout_val)}, **kw)

        return self.lock_column

    def generate_timeout_value(self, timeout_val):
        """
        Used internally - serialize a timeout value (a `long`) to be inserted into a cassandra as a `string`.
        """
        return repr(timeout_val)

    def read_timeout_value(self, col):
        """
        Used internally - deserialize a timeout value that was stored in cassandra as a `string` back into a `long`.
        """
        return long(col)

    def fill_release_mutation(self, mutation, exclude_current_lock=False):
        """
        Used internally - used to fill out a `pycassa.batch.CfMutator` with the necessary steps to release the lock.
        """
        cols_to_delete = []

        for lock_col_name in self.locks_to_delete:
            cols_to_delete.append(lock_col_name)

        if not exclude_current_lock and self.lock_column is not None:
            cols_to_delete.append(self.lock_column)

        mutation.remove(self.key, cols_to_delete)

        self.locks_to_delete.clear()
        self.lock_column = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

class PycassaStorage(Storage):
    """An implementation of :class:`whoosh.store.Storage` that stores files in
    the Cassandra database.
    """

    def __init__(self, path, pool, cassa_sys=None, keyspace="WhooshTest", cache_dir=None, tmp_dir="/tmp", readonly=False, supports_mmap=False):
        """

        :param cache_dir: None or a path to a directory. If a path is given, it is used as a cache directory.
        :param supports_mmap: if True (the default), use the ``mmap`` module to
            open memory mapped files. You can open the storage object with
            ``supports_mmap=False`` to force Whoosh to open files normally
            instead of with ``mmap``.
        :param readonly: If ``True``, the object will raise an exception if you
            attempt to create or rename a file.
        """
        # index_column_family
        self.path = path
        self.keyspace = keyspace
        if isinstance(pool, ConnectionPool) :
            self.pool = pool
        else :
            raise ValueError('Bad ConnectionPool object')
        if cassa_sys!=None and isinstance(cassa_sys, SystemManager) :
            self.cassa_sys = cassa_sys
        elif cassa_sys==None :
            self.cassa_sys = None
        else:
            raise ValueError('Bad SystemManager object')
        # cache dirctory
        self._tmp_dir = tmp_dir
        self._cache_dir = cache_dir
        # readonly
        self._readonly = readonly
        # supports_mmap
        self._supports_mmap = supports_mmap
        self._index_column_family = None

    def create(self):
        """Creates this storage object's directory path using ``os.makedirs`` if
        it doesn't already exist.

        >>> from whoosh.filedb.filestore import FileStorage
        >>> st = FileStorage("indexdir")
        >>> st.create()

        This method returns ``self``, you can say::

            st = FileStorage("indexdir").create()

        Note that you can simply create handle the creation of the directory
        yourself and open the storage object using the initializer::

            dirname = "indexdir"
            os.mkdir(dirname)
            st = FileStorage(dirname)

        However, using the ``create()`` method allows you to potentially swap in
        other storage implementations more easily.

        :return: a :class:`Storage` instance.
        """

        if (self.cassa_sys != None) :
            try :
                self.cassa_sys.create_column_family(self.keyspace, self.path, key_validation_class=UTF8Type())
            except InvalidRequestException :
                print("ColumnFamily %s exists" % self.path)
            self.cassa_sys.alter_column(self.keyspace, self.path, 'content', BytesType())
            self.cassa_sys.alter_column(self.keyspace, self.path, 'size', IntegerType())
            try :
                self.cassa_sys.create_column_family(self.keyspace, "%s_Locks" % self.path, key_validation_class=UTF8Type())
            except InvalidRequestException :
                print("ColumnFamily %s_Locks exists" % self.path)
            return self

    def create_index(self, schema, indexname=_DEF_INDEX_NAME, indexclass=None):
        """Creates a new index in this storage.

        >>> from whoosh import fields
        >>> from whoosh.filedb.filestore import FileStorage
        >>> schema = fields.Schema(content=fields.TEXT)
        >>> # Create the storage directory
        >>> st = FileStorage.create("indexdir")
        >>> # Create an index in the storage
        >>> ix = st.create_index(schema)

        :param schema: the :class:`whoosh.fields.Schema` object to use for the
            new index.
        :param indexname: the name of the index within the storage object. You
            can use this option to store multiple indexes in the same storage.
        :param indexclass: an optional custom ``Index`` sub-class to use to
            create the index files. The default is
            :class:`whoosh.index.FileIndex`. This method will call the
            ``create`` class method on the given class to create the index.
        :return: a :class:`whoosh.index.Index` instance.
        """
        if self._index_column_family == None :
            self._index_column_family=ColumnFamily(self.pool, self.path)
        return Storage.create_index(self, indexname=indexname, schema=schema, indexclass=indexclass)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None, indexclass=None):
        """Opens an existing index (created using :meth:`create_index`) in this storage.

        :param indexname: the name of the index within the storage object. You
            can use this option to store multiple indexes in the same storage.
        :param schema: if you pass in a :class:`whoosh.fields.Schema` object
            using this argument, it will override the schema that was stored
            with the index.
        :param indexclass: an optional custom ``Index`` sub-class to use to
            open the index files. The default is
            :class:`whoosh.index.FileIndex`. This method will instantiate the
            class with this storage object.
        :return: a :class:`whoosh.index.Index` instance.
        """
        if self._index_column_family == None :
            try :
                self._index_column_family=ColumnFamily(self.pool, self.path)
            except NotFoundException:
                raise EmptyIndexError()
        return Storage.open_index(self, indexname=indexname, schema=schema, indexclass=indexclass)

    def list(self):
        """Returns a list of file names in this storage.

        :return: a list of strings
        """
        try :
            if self._index_column_family == None :
                    self._index_column_family=ColumnFamily(self.pool, self.path)
            items = self._index_column_family.get_range(
                columns = ['size'],
                read_consistency_level = ConsistencyLevel.QUORUM,
                row_count = None,
                filter_empty = True,
            )
            # NOTE: keys are whoosh filenames
            keys = [key for key, row in items]
            return keys
        except InvalidRequestException:
            raise EmptyIndexError()

    def lock(self, name):
        """Return a named lock object (implementing ``.acquire()`` and
        ``.release()`` methods). Different storage implementations may use
        different lock types with different guarantees. For example, the
        RamStorage object uses Python thread locks, while the FileStorage
        object uses filesystem-based locks that are valid across different
        processes.

        :param name: a name for the lock.
        :return: a lock-like object.
        """
        return PycassaDistributedRowLock(pool=self.pool, column_family="%s_Locks" % self.path,key=name, backoff_plicy=RunOnceRetryManyPolicy())

    def temp_storage(self, name=None):
        """Creates a new storage object for temporary files. You can call
        :meth:`Storage.destroy` on the new storage when you're finished with
        it.

        :param name: a name for the new storage. This may be optional or
            required depending on the storage implementation.
        :rtype: :class:`Storage`
        """
        name = name or "%s.tmp" % random_name()
        path = os.path.join(os.path.join(self._tmp_dir, self.path), name)
        tempstore = FileStorage(path)
        return tempstore.create()

    def create_file(self, name, *args, **kwargs):
        """Creates a file with the given name in this storage.

        :param name: the name for the new file.
        :return: a :class:`whoosh.filedb.structfile.StructFile` instance.
        """
        def onclose(sfile):
            pos = sfile.file.tell()
            sfile.file.seek(0, 2)
            size = sfile.file.tell()
            sfile.file.seek(pos, 0)
            self._index_column_family.insert(
                key = name,
                columns = {
                    'content': sfile.file.getvalue(),
                    'size': size,
                },
                write_consistency_level = ConsistencyLevel.QUORUM,
            )

        return StructFile(
            StringIO(),
            name = name,
            onclose = onclose,
        )

    def open_file(self, name, *args, **kwargs):
        """Opens a file with the given name in this storage.

        :param name: the name for the new file.
        :return: a :class:`whoosh.filedb.structfile.StructFile` instance.
        """
        def get(name):
            try:
                row = self._index_column_family.get(
                    key = name,
                    columns = ['content', 'size'],
                    read_consistency_level = ConsistencyLevel.QUORUM,
                )
            except Exception as e:
                raise NameError('File "%r" does not exist' % name)
            return row

        def onclose(sfile):
            if isinstance(sfile.file, StringIO):
                sfile.file.seek(0, 2)
                size = sfile.file.tell()
                content = sfile.file.getvalue()
            else:
                sfile.file.seek(0, 0)
                content = sfile.file.read()
                size = sfile.file.tell()
            self._index_column_family.insert(
                key = name,
                columns = {
                    'content': content,
                    'size': size,
                },
                write_consistency_level = ConsistencyLevel.QUORUM,
                )

        if self._cache_dir:
            # create cache directory if one does not exist
            if not os.path.exists(self._cache_dir):
                os.makedirs(self._cache_dir)
            path = os.path.join(self._cache_dir, name)
            if os.path.exists(path):
                f = open(path, 'rb+')
            else:
                row = get(name)
                content = row['content']
                f = open(path, 'wb')
                f.write(content)
                f.flush()
                f.close()
                f = None
                f = open(path, 'rb+')
        else:
            row = get(name)
            content = row['content']
            f = StringIO(content)

        return StructFile(
            f,
            name = name,
            onclose = onclose,
            *args,
            **kwargs
        )

    def delete_file(self, name):
        """Removes the given file from this storage.

        :param name: the name to delete.
        """
        try:
            row = self._index_column_family.remove(
                key = name,
                write_consistency_level = ConsistencyLevel.QUORUM,
            )
        except Exception as e:
            raise NameError('File %r does not exist' % name)

    def rename_file(self, frm, to, safe=False):
        """Renames a file in this storage.

        :param frm: The current name of the file.
        :param to: The new name for the file.
        :param safe: if True, raise an exception if a file with the new name
            already exists.
        """
        try:
            row = self._index_column_family.get(
                key = frm,
                read_consistency_level = ConsistencyLevel.QUORUM,
            )
        except Exception as e:
            raise NameError('File %r does not exist' % frm)

        if safe:
            try:
                self._index_column_family.get(
                    key = to,
                    columns = ['size'],
                    read_consistency_level = ConsistencyLevel.QUORUM,
                )
            except Exception as e:
                pass
            else:
                raise NameError('File %r exists' % to)

        self._index_column_family.remove(
            key = frm,
            write_consistency_level = ConsistencyLevel.QUORUM,
        )

        self._index_column_family.insert(
            key = to,
            columns = row,
            write_consistency_level = ConsistencyLevel.QUORUM,
        )

    def file_exists(self, name):
        """Returns True if the given file exists in this storage.

        :param name: the name to check.
        :rtype: bool
        """
        try:
            self._index_column_family.get(
                key = name,
                columns = ['size'],
                read_consistency_level = ConsistencyLevel.QUORUM,
            )
        except Exception as e:
            return False

        return True

    def file_modified(self, name):
        """Returns the last-modified time of the given file in this storage (as
        a "ctime" UNIX timestamp).

        :param name: the name to check.
        :return: a "ctime" number.
        """
        try:
            row = self._index_column_family.get(
                key = name,
                columns = ['size'],
                include_timestamp = True,
                read_consistency_level = ConsistencyLevel.QUORUM,
            )
        except Exception as e:
            raise NameError('File %r does not exist' % name)

        content, timestamp = row['size']
        return timestamp

    def file_length(self, name):
        """Returns the size (in bytes) of the given file in this storage.

        :param name: the name to check.
        :rtype: int
        """
        try:
            row = self._index_column_family.get(
                key = name,
                columns = ['size'],
                read_consistency_level = ConsistencyLevel.QUORUM,
            )
        except Exception as e:
            raise NameError('File %r does not exist' % name)

        return row['size']

    def flush(self):
        """Flushes the buffer of the wrapped file. This is a no-op if the
        wrapped file does not have a flush method.
        """

        if hasattr(self.file, "flush"):
            self.file.flush()

    def optimize(self):
        """Optimizes the storage object. The meaning and cost of "optimizing"
        will vary by implementation. For example, a database implementation
        might run a garbage collection procedure on the underlying database.
        """
        pass
