.. _reference-implementation:

The reference implementation
#############################

The u1db reference implementation is written in Python, with a SQLite back end.
It can be used as a real working implementation by Python code. It is also used 
to document and test how u1db should work; it has a comprehensive test suite. 
Implementation authors should port the u1db reference test suite in order to 
test that their implementation is correct; in particular, sync conformance is 
defined as being able to sync with the reference implementation.

Fetch with ``bzr branch lp:u1db`` or from `Launchpad <http://launchpad.net/u1db>`_.

To open a new database, use ``u1db.open``:

.. autofunction:: u1db.open

Opening returns a ``Database`` object:

.. autoclass:: u1db.Database
    :members:

