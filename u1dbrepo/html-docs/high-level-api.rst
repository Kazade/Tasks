.. _high-level-api:

The high-level API
##################

The U1DB API has three separate sections: document storage and retrieval,
querying, and sync. Here we describe the high-level API. Remember that you
will need to choose an implementation, and exactly how this API is defined
is implementation-specific, in order that it fits with the language's 
conventions.

Document storage and retrieval
##############################

U1DB stores documents. A document is a set of nested key-values; basically,
anything you can express with JSON. Implementations are likely to provide a 
Document object "wrapper" for these documents; exactly how the wrapper works
is implementation-defined.

Creating and editing documents
------------------------------

To create a document, use ``create_doc()``. Code examples below are from 
:ref:`reference-implementation` in Python.

.. testcode ::

    import json, u1db
    db = u1db.open(":memory:", create=True)
    doc = db.create_doc(json.dumps({"key": "value"}), doc_id="testdoc")
    print doc.content
    print doc.doc_id

.. testoutput ::

    {"key": "value"}
    testdoc

Editing an *existing* document is done with ``put_doc()``. This is separate from
``create_doc()`` so as to avoid accidental overwrites. ``put_doc()`` takes a
``Document`` object, because the object encapsulates revision information for
a particular document.

.. testcode ::

    import json, u1db
    db = u1db.open(":memory:", create=True)
    doc1 = db.create_doc(json.dumps({"key1": "value1"}), doc_id="doc1")
    # the next line should fail because it's creating a doc that already exists
    try:
        doc1fail = db.create_doc(json.dumps({"key1fail": "value1fail"}), doc_id="doc1")
    except u1db.errors.RevisionConflict:
        print "There was a conflict when creating the doc!"
    print "Now editing the doc with the doc object we got back..."
    data = json.loads(doc1.content)
    data["key1"] = "edited"
    doc1.content = json.dumps(data)
    db.put_doc(doc1)

.. testoutput ::

    There was a conflict when creating the doc!
    Now editing the doc with the doc object we got back...

Finally, deleting a document is done with ``delete_doc()``.

.. testcode ::

    import json, u1db
    db = u1db.open(":memory:", create=True)
    doc = db.create_doc(json.dumps({"key": "value"}))
    db.delete_doc(doc)

.. testoutput ::


Retrieving documents
--------------------

The simplest way to retrieve documents from a u1db is by ``doc_id``.

.. testcode ::

    import json, u1db
    db = u1db.open(":memory:", create=True)
    doc = db.create_doc(json.dumps({"key": "value"}), doc_id="testdoc")
    doc1 = db.get_doc("testdoc")
    print doc1.content
    print doc1.doc_id

.. testoutput ::

    {"key": "value"}
    testdoc

And it's also possible to retrieve many documents by ``doc_id``.

.. testcode ::

    import json, u1db
    db = u1db.open(":memory:", create=True)
    doc1 = db.create_doc(json.dumps({"key": "value"}), doc_id="testdoc1")
    doc2 = db.create_doc(json.dumps({"key": "value"}), doc_id="testdoc2")
    for doc in db.get_docs(["testdoc2","testdoc1"]):
        print doc.doc_id

.. testoutput ::

    testdoc2
    testdoc1

Note that ``get_docs()`` returns the documents in the order specified.

Document functions
^^^^^^^^^^^^^^^^^^

 * create_doc(JSON string, optional_doc_id)
 * put_doc(Document)
 * get_doc(doc_id)
 * get_docs(list_of_doc_ids)
 * delete_doc(Document)
 * whats_changed(generation)

Querying
--------

To retrieve documents other than by ``doc_id``, you query the database.
Querying a U1DB is done by means of an index. To retrieve only some documents
from the database based on certain criteria, you must first create an index,
and then query that index.

An index is created from ''index expressions''. An index expression names one
or more fields in the document. A simple example follows: view many more
examples here.

Given a database with the following documents::

    {"firstname": "John", "surname", "Barnes", "position": "left wing"} ID jb
    {"firstname": "Jan", "surname", "Molby", "position": "midfield"} ID jm
    {"firstname": "Alan", "surname", "Hansen", "position": "defence"} ID ah
    {"firstname": "John", "surname", "Wayne", "position": "filmstar"} ID jw

an index expression of ``["firstname"]`` will create an index that looks 
(conceptually) like this

 ====================== ===========
 index expression value document id
 ====================== ===========
 Alan                   ah
 Jan                    jm
 John                   jb
 John                   jw
 ====================== ===========

and that index is created with ``create_index("by-firstname", ["firstname"])`` - that is,
create an index with a name and a list of index expressions. (Exactly how to
pass the name and the list of index expressions is something specific to
each implementation.)

Index expressions
^^^^^^^^^^^^^^^^^

An index expression describes how to get data from a document; you can think
of it as describing a function which, when given a document, returns a value,
which is then used as the index key.

**Name a field.** A basic index expression is a dot-delimited list of nesting
fieldnames, so the index expression ``field.sub1.sub2`` applied to a document 
with ID ``doc1`` and content::

  {
      "field": { 
          "sub1": { 
              "sub2": "hello"
              "sub3": "not selected"
          }
      }
  }

gives the index key "hello", and therefore an entry in the index of

 ========= ======
 Index key doc_id
 ========= ======
 hello     doc1
 ========= ======

**Name a list.** If an index expression names a field whose contents is a list
of strings, the doc will have multiple entries in the index, one per entry in
the list. So, the index expression ``field.tags`` applied to a document with 
ID "doc2" and content::

  {
      "field": { 
          "tags": [ "tag1", "tag2", "tag3" ]
      }
  }

gives index entries

 ========= ======
 Index key doc_id
 ========= ======
 tag1      doc2
 tag2      doc2
 tag3      doc2
 ========= ======

**Transformation functions.** An index expression may be wrapped in any number of
transformation functions. A function transforms the result of the contained
index expression: for example, if an expression ``name.firstname`` generates 
"John" when applied to a document, then ``lower(name.firstname)`` generates 
"john".

Available transformation functions are:

 * ``lower(index_expression)`` - lowercase the value
 * ``splitwords(index_expression)`` - split the value on whitespace; will act like a 
   list and add multiple entries to the index
 * ``is_null(index_expression)`` - True if value is null or not a string or the field 
   is absent, otherwise false

So, the index expression ``splitwords(lower(field.name))`` applied to a document with 
ID "doc3" and content::

  {
      "field": { 
          "name": "Bruce David Grobbelaar"
      }
  }

gives index entries

 ========== ======
 Index key  doc_id
 ========== ======
 bruce      doc3
 david      doc3
 grobbelaar doc3
 ========== ======


Querying an index
-----------------

Pass a list of tuples of index keys to ``get_from_index``; the last index key in
each tuple (and *only* the last one) can end with an asterisk, which matches 
initial substrings. So, querying our ``by-firstname`` index from above::

    get_from_index(
        "by-firstname",                     # name of index
            [                               # begin the list of index keys
                ("John", )                  # an index key
            ]                               # end the list
    )


will return ``[ 'jw', 'jb' ]`` - that is, a list of document IDs.

``get_from_index("by_firstname", [("J*")])`` will match all index keys beginning
with "J", and so will return ``[ 'jw', 'jb', 'jm' ]``.

``get_from_index("by_firstname", [("Jan"), ("Alan")])`` will match both the
queried index keys, and so will return ``[ 'jm', 'ah' ]``.


Index functions
^^^^^^^^^^^^^^^

 * create_index(name, index_expressions_list)
 * delete_index(name)
 * get_from_index(name, list_of_index_key_tuples)
 * get_keys_from_index(name)
 * list_indexes()

Syncing
#######

U1DB is a syncable database. Any U1DB can be synced with any U1DB server; most
U1DB implementations are capable of being run as a server. Syncing brings
both the server and the client up to date with one another; save data into a
local U1DB whether online or offline, and then sync when online.

Pass an HTTP URL to sync with that server.

Syncing databases which have been independently changed may produce conflicts.
Read about the U1DB conflict policy and more about syncing at :ref:`conflicts`.

Running your own U1DB server is implementation-specific. :ref:`reference-implementation` 
is able to be run as a server.

Dealing with conflicts
----------------------

Syncing a database can result in conflicts; if your user changes the same 
document in two different places and then syncs again, that document will be
''in conflict'', meaning that it has incompatible changes. If this is the case,
``doc.has_conflicts`` will be true, and put_doc to a conflicted doc will give a
``ConflictedDoc`` error. To get a list of conflicted versions of the
document, do ``get_doc_conflicts(doc_id)``. Deciding what the final unconflicted
document should look like is obviously specific to the user's application; once
decided, call ``resolve_doc(doc, list_of_conflicted_revisions)`` to resolve and
set the final resolved content.

Syncing functions
^^^^^^^^^^^^^^^^^

 * sync(URL)
 * resolve_doc(self, Document, conflicted_doc_revs)
 * get_doc_conflicts(doc_id)
 * resolve_doc(doc, list_of_conflicted_revisions)

