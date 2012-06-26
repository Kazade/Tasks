.. _quickstart:

Downloads and Quickstart guide
###############################

How to start working with the u1db Python implementation.

Getting u1db
------------

Download
^^^^^^^^

This is the recommended version of u1db to use for your Python application.

Download the latest release from `the U1DB download page <http://launchpad.net/u1db/+download>`_.

Use from source control
^^^^^^^^^^^^^^^^^^^^^^^

u1db is `maintained in bazaar in Launchpad <http://launchpad.net/u1db/>`_. To fetch the latest version,
`bzr branch lp:u1db`.

Starting u1db
-------------

.. doctest ::

    >>> import u1db, json, tempfile
    >>> db = u1db.open(":memory:", create=True)
    
    >>> content = json.dumps({"name": "Alan Hansen"}) # create a document
    >>> doc = db.create_doc(content)
    >>> print doc.content
    {"name": "Alan Hansen"}
    >>> doc.content = json.dumps({"name": "Alan Hansen", "position": "defence"}) # update the document's content
    >>> rev = db.put_doc(doc)
    
    >>> content = json.dumps({"name": "John Barnes", "position": "forward"}) # create more documents
    >>> doc2 = db.create_doc(content)
    >>> content = json.dumps({"name": "Ian Rush", "position": "forward"})
    >>> doc2 = db.create_doc(content)
    
    >>> db.create_index("by-position", ("position",)) # create an index by passing an index expression
    
    >>> results = db.get_from_index("by-position", [("forward",)]) # query that index by passing a list of tuples of queries
    >>> len(results)
    2
    >>> data = [json.loads(result.content) for result in results]
    >>> names = [item["name"] for item in data]
    >>> sorted(names)
    [u'Ian Rush', u'John Barnes']
    
Running a server
----------------

The reference implementation comes with a command-line client and a server. The
command-line client covers the basic operations on a database.

.. code-block:: bash

    ~/u1db/trunk$ ./u1db-client init-db example.u1db
    ~/u1db/trunk$ echo '{"key": "value"}' | ./u1db-client create example.u1db # add a document to our database
    id: D-cf8a96bea58b4b5ab2ce1ab9c1bfa053
    rev: f6657904254d474d9a333585928726df:1
    ~/u1db/trunk$ ./u1db-client get example.u1db D-cf8a96bea58b4b5ab2ce1ab9c1bfa053 # fetch it
    {"key": "value"}
    rev: f6657904254d474d9a333585928726df:1
    ~/u1db/trunk$ ./u1db-client delete example.u1db D-cf8a96bea58b4b5ab2ce1ab9c1bfa053 f6657904254d474d9a333585928726df:1 # and delete it
    rev: f6657904254d474d9a333585928726df:2

    ~/u1db/trunk$ ./u1db-serve --verbose # run the server, and you can now use http://127.0.0.1:43632/example.u1db as a sync URL
    listening on: 127.0.0.1:43632

Syncing to other databases
--------------------------

.. code-block:: python

    >>> import u1db
    >>> db = u1db.open(":memory:", create=True)
    >>> generation = db.sync("http://127.0.0.1:43632/example.u1db")
    
or from the command line

.. code-block:: bash

    ~/u1db/trunk$ ./u1db-client init-db someother.u1db
    ~/u1db/trunk$ ./u1db-client sync someother.u1db http://127.0.0.1:43632/example.u1db

    
