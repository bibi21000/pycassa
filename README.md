pycassa extensions for Flask, Whoosh, ...
=========================================

pycassa/cassandra backend for Flask sessions :
----------------------------------------------

How to use it :

- Copy this file in your project

- Create the column_family in your 'keyspace': 'Sessions' (ie)

- And use it :

    from cassasession import PycassaSessionInterface
    ...

    #Define a secret key
    app.secret_key = 'secret'

    #and create a pool
    pool = ConnectionPool('keyspace',['localhost'])

    #Define a column family for 'Sessions'
    session_family = ColumnFamily(dbpool, 'Sessions')

    #And define the session interface or your app
    app.session_interface = PycassaSessionInterface(session_family)

- That's all folk !!!

pycassa/cassandra backend for Whoosh :
--------------------------------------

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
    storage = PycassaStorage("Whoosh", pool, cassa_sys=cassa_sys, keyspace='keyspace', readonly=False, supports_mmap=False)

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

- Update, search, ... your Whoosh index

