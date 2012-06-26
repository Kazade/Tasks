.. _conflicts:

Conflicts, syncing, and revisions
#################################


Conflicts
-------------

If two u1dbs are synced, and then the same document is changed in different ways
in each u1db, and then they are synced again, there will be a *conflict*. This
does not block synchronisation: the document is registered as being in conflict,
and resolving that is up to the u1db-using application.

Importantly, **conflicts are not synced**. If *machine A* initiates a sync with
*machine B*, and this sync results in a conflict, the conflict **only registers
on machine A**. This policy is sometimes called "other wins": the machine you
synced *to* wins conflicts, and the document will have machine B's content on
both machine A and machine B. However, on machine A the document is marked
as having conflicts, and must be resolved there:

.. testsetup ::

    import u1db, json
    db=u1db.open(':memory:', True)
    docFromA=u1db.Document('test','machineA:1',json.dumps({'camefrom':'machineA'}))
    db.put_doc_if_newer(docFromA, save_conflict=True)
    docFromB=u1db.Document('test','machineB:1',json.dumps({'camefrom':'machineB'}))
    db.put_doc_if_newer(docFromB, save_conflict=True)

.. doctest ::

    >>> docFromB
    Document(test, machineB:1, conflicted, '{"camefrom": "machineB"}')
    >>> docFromB.has_conflicts # the document is in conflict
    True
    >>> conflicts = db.get_doc_conflicts(docFromB.doc_id)
    >>> print conflicts
    [(u'machineB:1', u'{"camefrom": "machineB"}'), (u'machineA:1', u'{"camefrom": "machineA"}')]
    >>> db.resolve_doc(docFromB, [x[0] for x in conflicts]) # resolve in favour of B
    >>> doc_is_now = db.get_doc("test")
    >>> doc_is_now.content # the content has been updated to doc's content
    u'{"camefrom": "machineB"}'
    >>> doc_is_now.has_conflicts # and is no longer in conflict
    False

Note that ``put_doc`` will fail because we got conflicts from a sync, but it
may also fail for another reason. If you acquire a document before a sync and 
then sync, and the sync updates that document, then re-putting that document 
with modified content will also fail, because the revision is not the current 
one. This will raise a ``RevisionConflict`` error.

Revisions
----------

As an app developer, you should treat a ``Document``'s ``revision`` as an opaque
cookie; do not try and deconstruct it or edit it. It is for your u1db 
implementation's use. You can therefore ignore the rest of this section.

If you are writing a new u1db implementation, understanding revisions is 
important, and this is where you find out about them.

To keep track of document revisions u1db uses vector versions. Each
synchronized instance of the same database is called a replica and has
a unique identifier (``replica uid``) assigned to it (currently the
reference implementation by default uses UUID4s for that); a
revision is a mapping between ``replica uids`` and ``edit numbers``: ``rev =
<replica_uid:edit_num...>``, or using a functional notation
``rev(replica_uid) = edit_num``. The current concrete format is a string
built out of each ``replica_uid`` concatenated with ``':'`` and with its edit
number in decimal, sorted lexicographically by ``replica_uid`` and then
all joined with ``'|'``, for example: ``'replicaA:1|replicaB:3'`` . Absent
``replica uids`` in a revision mapping are implicitly mapped to edit
number 0.

The new revision of a document modified locally in a replica, is the
modification of the old revision where the edit number mapped for the
editing ``replica uid`` is increased by 1.

When syncing one needs to establish whether an incoming revision is
newer than the current one or in conflict. A revision 

``rev1 = <replica_1i:edit_num1i|i=1..n>``

is newer than a different 

``rev2 = <replica_2j:edit_num2j|j=1..m>``

if for all ``i=1..n``, ``rev2(replica_1i) <= edit_num1i`` 

and for all ``j=1..m``, ``rev1(replica_2j) >= edit_num2j``. 

Two revisions which are not equal nor one newer than the
other are in conflict.

When resolving a conflict locally in a replica ``replica_resol``, starting from 
``rev1...revN`` in conflict, the resulting revision ``rev_resol`` is obtained by:

     ``R`` is the set the of all replicas explicitly mentioned in ``rev1..revN``

     ``rev_resol(r) = max(rev1(r)...revN(r))`` for all ``r`` in ``R``, with ``r != rev_resol``

     ``rev_resol(replica_resol) = max(rev1(replica_resol)...revN(replica_resol))+1``


Syncing
-------
