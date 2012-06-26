# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""The backend class for U1DB. This deals with hiding storage details."""

import simplejson
from u1db import (
    DocumentBase,
    errors,
    tests,
    vectorclock,
    )

simple_doc = tests.simple_doc
nested_doc = tests.nested_doc

from u1db.tests.test_remote_sync_target import (
    http_server_def,
    oauth_http_server_def,
)

from u1db.remote import (
    http_database,
    )

try:
    from u1db.tests import c_backend_wrapper
except ImportError:
    c_backend_wrapper = None


def http_create_database(test, replica_uid, path='test'):
    test.startServer()
    test.request_state._create_database(replica_uid)
    return http_database.HTTPDatabase(test.getURL(path))


def oauth_http_create_database(test, replica_uid):
    http_db = http_create_database(test, replica_uid, '~/test')
    http_db.set_oauth_credentials(tests.consumer1.key, tests.consumer1.secret,
                                  tests.token1.key, tests.token1.secret)
    return http_db


class TestAlternativeDocument(DocumentBase):
    """A (not very) alternative implementation of Document."""


class AllDatabaseTests(tests.DatabaseBaseTests, tests.TestCaseWithServer):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + [
        ('http', {'do_create_database': http_create_database,
                  'make_document': tests.create_doc,
                  'server_def': http_server_def}),
        ('oauth_http', {'do_create_database': oauth_http_create_database,
                        'make_document': tests.create_doc,
                        'server_def': oauth_http_server_def})
        ] + tests.C_DATABASE_SCENARIOS

    def test_close(self):
        self.db.close()

    def test_create_doc_allocating_doc_id(self):
        doc = self.db.create_doc(simple_doc)
        self.assertNotEqual(None, doc.doc_id)
        self.assertNotEqual(None, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_create_doc_different_ids_same_db(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertNotEqual(doc1.doc_id, doc2.doc_id)

    def test_create_doc_with_id(self):
        doc = self.db.create_doc(simple_doc, doc_id='my-id')
        self.assertEqual('my-id', doc.doc_id)
        self.assertNotEqual(None, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_create_doc_existing_id(self):
        doc = self.db.create_doc(simple_doc)
        new_content = '{"something": "else"}'
        self.assertRaises(errors.RevisionConflict, self.db.create_doc,
                          new_content, doc.doc_id)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_put_doc_creating_initial(self):
        doc = self.make_document('my_doc_id', None, simple_doc)
        new_rev = self.db.put_doc(doc)
        self.assertIsNot(None, new_rev)
        self.assertGetDoc(self.db, 'my_doc_id', new_rev, simple_doc, False)

    def test_put_doc_space_in_id(self):
        doc = self.make_document('my doc id', None, simple_doc)
        new_rev = self.db.put_doc(doc)
        self.assertIsNot(None, new_rev)
        self.assertGetDoc(self.db, 'my doc id', new_rev, simple_doc, False)

    def test_put_doc_update(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        orig_rev = doc.rev
        doc.set_json('{"updated": "stuff"}')
        new_rev = self.db.put_doc(doc)
        self.assertNotEqual(new_rev, orig_rev)
        self.assertGetDoc(self.db, 'my_doc_id', new_rev,
                          '{"updated": "stuff"}', False)
        self.assertEqual(doc.rev, new_rev)

    def test_put_non_ascii_key(self):
        content = simplejson.dumps({u'key\xe5': u'val'})
        doc = self.db.create_doc(content, doc_id='my_doc')
        self.assertGetDoc(self.db, 'my_doc', doc.rev, content, False)

    def test_put_non_ascii_value(self):
        content = simplejson.dumps({'key': u'\xe5'})
        doc = self.db.create_doc(content, doc_id='my_doc')
        self.assertGetDoc(self.db, 'my_doc', doc.rev, content, False)

    def test_put_doc_refuses_no_id(self):
        doc = self.make_document(None, None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)
        doc = self.make_document("", None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_doc_refuses_slashes(self):
        doc = self.make_document('a/b', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)
        doc = self.make_document(r'\b', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_doc_refuses_non_existing_old_rev(self):
        doc = self.make_document('doc-id', 'test:4', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, doc)

    def test_put_doc_refuses_non_ascii_doc_id(self):
        doc = self.make_document('d\xc3\xa5c-id', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_fails_with_bad_old_rev(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        old_rev = doc.rev
        bad_doc = self.make_document(doc.doc_id, 'other:1',
                                     '{"something": "else"}')
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, bad_doc)
        self.assertGetDoc(self.db, 'my_doc_id', old_rev, simple_doc, False)

    def test_create_succeeds_after_delete(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.db.delete_doc(doc)
        deleted_doc = self.db.get_doc('my_doc_id', include_deleted=True)
        deleted_vc = vectorclock.VectorClockRev(deleted_doc.rev)
        new_doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertGetDoc(self.db, 'my_doc_id', new_doc.rev, simple_doc, False)
        new_vc = vectorclock.VectorClockRev(new_doc.rev)
        self.assertTrue(
            new_vc.is_newer(deleted_vc),
            "%s does not supersede %s" % (new_doc.rev, deleted_doc.rev))

    def test_put_succeeds_after_delete(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.db.delete_doc(doc)
        deleted_doc = self.db.get_doc('my_doc_id', include_deleted=True)
        deleted_vc = vectorclock.VectorClockRev(deleted_doc.rev)
        doc2 = self.make_document('my_doc_id', None, simple_doc)
        self.db.put_doc(doc2)
        self.assertGetDoc(self.db, 'my_doc_id', doc2.rev, simple_doc, False)
        new_vc = vectorclock.VectorClockRev(doc2.rev)
        self.assertTrue(
            new_vc.is_newer(deleted_vc),
            "%s does not supersede %s" % (doc2.rev, deleted_doc.rev))

    def test_get_doc_after_put(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertGetDoc(self.db, 'my_doc_id', doc.rev, simple_doc, False)

    def test_get_doc_nonexisting(self):
        self.assertIs(None, self.db.get_doc('non-existing'))

    def test_get_doc_deleted(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.db.delete_doc(doc)
        self.assertIs(None, self.db.get_doc('my_doc_id'))

    def test_get_doc_include_deleted(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.db.delete_doc(doc)
        self.assertGetDocIncludeDeleted(
            self.db, doc.doc_id, doc.rev, None, False)

    def test_handles_nested_content(self):
        doc = self.db.create_doc(nested_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, nested_doc, False)

    def test_handles_doc_with_null(self):
        doc = self.db.create_doc('{"key": null}')
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, '{"key": null}', False)

    def test_delete_doc(self):
        doc = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)
        orig_rev = doc.rev
        self.db.delete_doc(doc)
        self.assertNotEqual(orig_rev, doc.rev)
        self.assertGetDocIncludeDeleted(
            self.db, doc.doc_id, doc.rev, None, False)
        self.assertIs(None, self.db.get_doc(doc.doc_id))

    def test_delete_doc_non_existent(self):
        doc = self.make_document('non-existing', 'other:1', simple_doc)
        self.assertRaises(errors.DocumentDoesNotExist, self.db.delete_doc, doc)

    def test_delete_doc_already_deleted(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertRaises(errors.DocumentAlreadyDeleted,
                          self.db.delete_doc, doc)
        self.assertGetDocIncludeDeleted(
            self.db, doc.doc_id, doc.rev, None, False)

    def test_delete_doc_bad_rev(self):
        doc1 = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)
        doc2 = self.make_document(doc1.doc_id, 'other:1', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.delete_doc, doc2)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_delete_doc_sets_content_to_None(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertIs(None, doc.get_json())

    def test_delete_doc_rev_supersedes(self):
        doc = self.db.create_doc(simple_doc)
        doc.set_json(nested_doc)
        self.db.put_doc(doc)
        doc.set_json('{"fishy": "content"}')
        self.db.put_doc(doc)
        old_rev = doc.rev
        self.db.delete_doc(doc)
        cur_vc = vectorclock.VectorClockRev(old_rev)
        deleted_vc = vectorclock.VectorClockRev(doc.rev)
        self.assertTrue(deleted_vc.is_newer(cur_vc),
                "%s does not supersede %s" % (doc.rev, old_rev))

    def test_delete_then_put(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertGetDocIncludeDeleted(
            self.db, doc.doc_id, doc.rev, None, False)
        doc.set_json(nested_doc)
        self.db.put_doc(doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, nested_doc, False)


class LocalDatabaseTests(tests.DatabaseBaseTests):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_create_doc_different_ids_diff_db(self):
        doc1 = self.db.create_doc(simple_doc)
        db2 = self.create_database('other-uid')
        doc2 = db2.create_doc(simple_doc)
        self.assertNotEqual(doc1.doc_id, doc2.doc_id)

    def test_put_doc_refuses_slashes_picky(self):
        doc = self.make_document('/a', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_get_docs(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual([doc1, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id]))

    def test_get_docs_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.db.delete_doc(doc1)
        self.assertEqual([doc2], self.db.get_docs([doc1.doc_id, doc2.doc_id]))

    def test_get_docs_include_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.db.delete_doc(doc1)
        self.assertEqual(
            [doc1, doc2],
            self.db.get_docs([doc1.doc_id, doc2.doc_id], include_deleted=True))

    def test_get_docs_request_ordered(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual([doc1, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id]))
        self.assertEqual([doc2, doc1],
                         self.db.get_docs([doc2.doc_id, doc1.doc_id]))

    def test_get_docs_empty_list(self):
        self.assertEqual([], self.db.get_docs([]))

    def test_get_all_docs_empty(self):
        self.assertEqual([], self.db.get_all_docs()[1])

    def test_get_all_docs(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual(
            sorted([doc1, doc2]), sorted(self.db.get_all_docs()[1]))

    def test_get_all_docs_exclude_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.db.delete_doc(doc2)
        self.assertEqual([doc1], self.db.get_all_docs()[1])

    def test_get_all_docs_include_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.db.delete_doc(doc2)
        self.assertEqual(
            sorted([doc1, doc2]),
            sorted(self.db.get_all_docs(include_deleted=True)[1]))

    def test_get_all_docs_generation(self):
        self.db.create_doc(simple_doc)
        self.db.create_doc(nested_doc)
        self.assertEqual(2, self.db.get_all_docs()[0])

    def test_simple_put_doc_if_newer(self):
        doc = self.make_document('my-doc-id', 'test:1', simple_doc)
        state_at_gen = self.db._put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual(('inserted', 1), state_at_gen)
        self.assertGetDoc(self.db, 'my-doc-id', 'test:1', simple_doc, False)

    def test_simple_put_doc_if_newer_deleted(self):
        self.db.create_doc('{}', doc_id='my-doc-id')
        doc = self.make_document('my-doc-id', 'test:2', None)
        state_at_gen = self.db._put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual(('inserted', 2), state_at_gen)
        self.assertGetDocIncludeDeleted(
            self.db, 'my-doc-id', 'test:2', None, False)

    def test_put_doc_if_newer_already_superseded(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        doc1_rev1 = doc1.rev
        doc1.set_json(simple_doc)
        self.db.put_doc(doc1)
        doc1_rev2 = doc1.rev
        # Nothing is inserted, because the document is already superseded
        doc = self.make_document(doc1.doc_id, doc1_rev1, orig_doc)
        state, _ = self.db._put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual('superseded', state)
        self.assertGetDoc(self.db, doc1.doc_id, doc1_rev2, simple_doc, False)

    def test_put_doc_if_newer_autoresolve(self):
        doc1 = self.db.create_doc(simple_doc)
        rev = doc1.rev
        doc = self.make_document(doc1.doc_id, "whatever:1", doc1.get_json())
        state, _ = self.db._put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual('superseded', state)
        doc2 = self.db.get_doc(doc1.doc_id)
        v2 = vectorclock.VectorClockRev(doc2.rev)
        self.assertTrue(v2.is_newer(vectorclock.VectorClockRev("whatever:1")))
        self.assertTrue(v2.is_newer(vectorclock.VectorClockRev(rev)))

    def test_put_doc_if_newer_already_converged(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        state_at_gen = self.db._put_doc_if_newer(doc1, save_conflict=False)
        self.assertEqual(('converged', 1), state_at_gen)

    def test_put_doc_if_newer_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        # Nothing is inserted, the document id is returned as would-conflict
        alt_doc = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        state, _ = self.db._put_doc_if_newer(alt_doc, save_conflict=False)
        self.assertEqual('conflicted', state)
        # The database wasn't altered
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_put_doc_if_newer_newer_generation(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        doc = self.make_document('doc_id', 'other:2', simple_doc)
        state, _ = self.db._put_doc_if_newer(
            doc, save_conflict=False, replica_uid='other', replica_gen=2,
            replica_trans_id='T-irrelevant')
        self.assertEqual('inserted', state)

    def test_put_doc_if_newer_same_generation_same_txid(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        doc = self.make_document('doc_id', 'other:2', simple_doc)
        state, _ = self.db._put_doc_if_newer(
            doc, save_conflict=False, replica_uid='other', replica_gen=1,
            replica_trans_id='T-sid')
        self.assertEqual('superseded', state)

    def test_put_doc_if_newer_wrong_transaction_id(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        doc = self.make_document('doc_id', 'other:1', simple_doc)
        self.assertRaises(
            errors.InvalidTransactionId,
            self.db._put_doc_if_newer, doc, save_conflict=False,
            replica_uid='other', replica_gen=1, replica_trans_id='T-sad')

    def test_put_doc_if_newer_old_generation_older_doc(self):
        orig_doc = '{"new": "doc"}'
        doc = self.db.create_doc(orig_doc)
        doc_rev1 = doc.rev
        doc.set_json(simple_doc)
        self.db.put_doc(doc)
        self.db._set_sync_info('other', 5, 'T-sid')
        older_doc = self.make_document(doc.doc_id, doc_rev1, simple_doc)
        state, _ = self.db._put_doc_if_newer(
            older_doc, save_conflict=False, replica_uid='other', replica_gen=3,
            replica_trans_id='T-irrelevant')
        self.assertEqual('superseded', state)

    def test_put_doc_if_newer_old_generation_newer_doc(self):
        self.db._set_sync_info('other', 5, 'T-sid')
        doc = self.make_document('doc_id', 'other:1', simple_doc)
        self.assertRaises(
            errors.InvalidGeneration,
            self.db._put_doc_if_newer, doc, save_conflict=False,
            replica_uid='other', replica_gen=1, replica_trans_id='T-sad')

    def test_validate_gen_and_trans_id(self):
        self.db.create_doc(simple_doc)
        gen, trans_id = self.db._get_generation_info()
        self.db.validate_gen_and_trans_id(gen, trans_id)

    def test_validate_gen_and_trans_id_invalid_txid(self):
        self.db.create_doc(simple_doc)
        gen, _ = self.db._get_generation_info()
        self.assertRaises(
            errors.InvalidTransactionId,
            self.db.validate_gen_and_trans_id, gen, 'wrong')

    def test_validate_gen_and_trans_id_invalid_txid(self):
        self.db.create_doc(simple_doc)
        gen, trans_id = self.db._get_generation_info()
        self.assertRaises(
            errors.InvalidGeneration,
            self.db.validate_gen_and_trans_id, gen + 1, trans_id)

    def test_validate_source_gen_and_trans_id_same(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        v1 = vectorclock.VectorClockRev('other:1|self:1')
        v2 = vectorclock.VectorClockRev('other:1|self:1')
        self.assertEqual(
            'superseded',
            self.db._validate_source('other', 1, 'T-sid', v1, v2))

    def test_validate_source_gen_newer(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        v1 = vectorclock.VectorClockRev('other:1|self:1')
        v2 = vectorclock.VectorClockRev('other:2|self:2')
        self.assertEqual(
            'ok',
            self.db._validate_source('other', 2, 'T-whatevs', v1, v2))

    def test_validate_source_wrong_txid(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        v1 = vectorclock.VectorClockRev('other:1|self:1')
        v2 = vectorclock.VectorClockRev('other:2|self:2')
        self.assertRaises(
            errors.InvalidTransactionId,
            self.db._validate_source, 'other', 1, 'T-sad', v1, v2)

    def test_validate_source_gen_older_and_vcr_older(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        self.db._set_sync_info('other', 2, 'T-sod')
        v1 = vectorclock.VectorClockRev('other:1|self:1')
        v2 = vectorclock.VectorClockRev('other:2|self:2')
        self.assertEqual(
            'superseded',
            self.db._validate_source('other', 1, 'T-sid', v2, v1))

    def test_validate_source_gen_older_vcr_newer(self):
        self.db._set_sync_info('other', 1, 'T-sid')
        self.db._set_sync_info('other', 2, 'T-sod')
        v1 = vectorclock.VectorClockRev('other:1|self:1')
        v2 = vectorclock.VectorClockRev('other:2|self:2')
        self.assertRaises(
            errors.InvalidGeneration,
            self.db._validate_source, 'other', 1, 'T-sid', v1, v2)

    def test_put_doc_if_newer_replica_uid(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db._set_sync_info('other', 1, 'T-sid')
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|other:1',
                                  nested_doc)
        self.assertEqual('inserted',
            self.db._put_doc_if_newer(doc2, save_conflict=False,
                                      replica_uid='other', replica_gen=2,
                                      replica_trans_id='T-id2')[0])
        self.assertEqual((2, 'T-id2'), self.db._get_sync_gen_info('other'))
        # Compare to the old rev, should be superseded
        doc2 = self.make_document(doc1.doc_id, doc1.rev, nested_doc)
        self.assertEqual('superseded',
            self.db._put_doc_if_newer(doc2, save_conflict=False,
                                      replica_uid='other', replica_gen=3,
                                      replica_trans_id='T-id3')[0])
        self.assertEqual((3, 'T-id3'), self.db._get_sync_gen_info('other'))
        # A conflict that isn't saved still records the sync gen, because we
        # don't need to see it again
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|fourth:1',
                                  '{}')
        self.assertEqual('conflicted',
            self.db._put_doc_if_newer(doc2, save_conflict=False,
                                      replica_uid='other', replica_gen=4,
                                      replica_trans_id='T-id4')[0])
        self.assertEqual((4, 'T-id4'), self.db._get_sync_gen_info('other'))

    def test__get_sync_gen_info(self):
        self.assertEqual((0, ''), self.db._get_sync_gen_info('other-db'))
        self.db._set_sync_info('other-db', 2, 'T-transaction')
        self.assertEqual((2, 'T-transaction'),
                         self.db._get_sync_gen_info('other-db'))

    def test_put_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        self.assertTransactionLog([doc.doc_id], self.db)
        doc.set_json('{"something": "else"}')
        self.db.put_doc(doc)
        self.assertTransactionLog([doc.doc_id, doc.doc_id], self.db)
        last_trans_id = self.getLastTransId(self.db)
        self.assertEqual((2, last_trans_id, [(doc.doc_id, 2, last_trans_id)]),
                         self.db.whats_changed())

    def test_delete_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        db_gen, _, _ = self.db.whats_changed()
        self.db.delete_doc(doc)
        last_trans_id = self.getLastTransId(self.db)
        self.assertEqual((2, last_trans_id, [(doc.doc_id, 2, last_trans_id)]),
                         self.db.whats_changed(db_gen))

    def test_whats_changed_initial_database(self):
        self.assertEqual((0, '', []), self.db.whats_changed())

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc = self.db.create_doc(simple_doc)
        doc.set_json('{"new": "contents"}')
        self.db.put_doc(doc)
        last_trans_id = self.getLastTransId(self.db)
        self.assertEqual((2, last_trans_id, [(doc.doc_id, 2, last_trans_id)]),
                         self.db.whats_changed())
        self.assertEqual((2, last_trans_id, []), self.db.whats_changed(2))

    def test_whats_changed_returns_last_edits_ascending(self):
        doc = self.db.create_doc(simple_doc)
        doc1 = self.db.create_doc(simple_doc)
        doc.set_json('{"new": "contents"}')
        self.db.delete_doc(doc1)
        delete_trans_id = self.getLastTransId(self.db)
        self.db.put_doc(doc)
        put_trans_id = self.getLastTransId(self.db)
        self.assertEqual((4, put_trans_id,
                          [(doc1.doc_id, 3, delete_trans_id),
                           (doc.doc_id, 4, put_trans_id)]),
                         self.db.whats_changed())

    def test_whats_changed_doesnt_include_old_gen(self):
        self.db.create_doc(simple_doc)
        self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        last_trans_id = self.getLastTransId(self.db)
        self.assertEqual((3, last_trans_id, [(doc2.doc_id, 3, last_trans_id)]),
                         self.db.whats_changed(2))


class LocalDatabaseWithConflictsTests(tests.DatabaseBaseTests):
    # test supporting/functionality around storing conflicts

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_get_docs_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertEqual([doc2], self.db.get_docs([doc1.doc_id]))

    def test_get_docs_conflicts_ignored(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        alt_doc = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(alt_doc, save_conflict=True)
        no_conflict_doc = self.make_document(doc1.doc_id, 'alternate:1',
                                             nested_doc)
        self.assertEqual([no_conflict_doc, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id],
                                          check_for_conflicts=False))

    def test_get_doc_conflicts(self):
        doc = self.db.create_doc(simple_doc)
        alt_doc = self.make_document(doc.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(alt_doc, save_conflict=True)
        self.assertEqual([alt_doc, doc],
                         self.db.get_doc_conflicts(doc.doc_id))

    def test_get_doc_conflicts_unconflicted(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_doc_conflicts(doc.doc_id))

    def test_get_doc_conflicts_no_such_id(self):
        self.assertEqual([], self.db.get_doc_conflicts('doc-id'))

    def test_resolve_doc(self):
        doc = self.db.create_doc(simple_doc)
        alt_doc = self.make_document(doc.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(alt_doc, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc.doc_id,
            [('alternate:1', nested_doc), (doc.rev, simple_doc)])
        orig_rev = doc.rev
        self.db.resolve_doc(doc, [alt_doc.rev, doc.rev])
        self.assertNotEqual(orig_rev, doc.rev)
        self.assertFalse(doc.has_conflicts)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)
        self.assertGetDocConflicts(self.db, doc.doc_id, [])

    def test_resolve_doc_picks_biggest_vcr(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, simple_doc)])
        orig_doc1_rev = doc1.rev
        self.db.resolve_doc(doc1, [doc2.rev, doc1.rev])
        self.assertFalse(doc1.has_conflicts)
        self.assertNotEqual(orig_doc1_rev, doc1.rev)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        vcr_1 = vectorclock.VectorClockRev(orig_doc1_rev)
        vcr_2 = vectorclock.VectorClockRev(doc2.rev)
        vcr_new = vectorclock.VectorClockRev(doc1.rev)
        self.assertTrue(vcr_new.is_newer(vcr_1))
        self.assertTrue(vcr_new.is_newer(vcr_2))

    def test_resolve_doc_partial_not_winning(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, simple_doc)])
        content3 = '{"key": "valin3"}'
        doc3 = self.make_document(doc1.doc_id, 'third:1', content3)
        self.db._put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [(doc3.rev, content3),
             (doc1.rev, simple_doc),
             (doc2.rev, nested_doc)])
        self.db.resolve_doc(doc1, [doc2.rev, doc1.rev])
        self.assertTrue(doc1.has_conflicts)
        self.assertGetDoc(self.db, doc1.doc_id, doc3.rev, content3, True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [(doc3.rev, content3),
             (doc1.rev, simple_doc)])

    def test_resolve_doc_partial_winning(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        content3 = '{"key": "valin3"}'
        doc3 = self.make_document(doc1.doc_id, 'third:1', content3)
        self.db._put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc3.rev, content3),
                                    (doc1.rev, simple_doc),
                                    (doc2.rev, nested_doc)])
        self.db.resolve_doc(doc1, [doc3.rev, doc1.rev])
        self.assertTrue(doc1.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc1.rev, simple_doc),
                                    (doc2.rev, nested_doc)])

    def test_resolve_doc_with_delete_conflict(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, None)])
        self.db.resolve_doc(doc2, [doc1.rev, doc2.rev])
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, nested_doc, False)

    def test_resolve_doc_with_delete_to_delete(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, None)])
        self.db.resolve_doc(doc1, [doc1.rev, doc2.rev])
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        self.assertGetDocIncludeDeleted(
            self.db, doc1.doc_id, doc1.rev, None, False)

    def test_put_doc_if_newer_save_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        # Document is inserted as a conflict
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        state, _ = self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertEqual('conflicted', state)
        # The database was updated
        self.assertGetDoc(self.db, doc1.doc_id, doc2.rev, nested_doc, True)

    def test_force_doc_conflict_supersedes_properly(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', '{"b": 1}')
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        doc3 = self.make_document(doc1.doc_id, 'altalt:1', '{"c": 1}')
        self.db._put_doc_if_newer(doc3, save_conflict=True)
        doc22 = self.make_document(doc1.doc_id, 'alternate:2', '{"b": 2}')
        self.db._put_doc_if_newer(doc22, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:2', doc22.get_json()),
             ('altalt:1', doc3.get_json()),
             (doc1.rev, simple_doc)])

    def test_put_doc_if_newer_save_conflict_was_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertTrue(doc2.has_conflicts)
        self.assertGetDoc(
            self.db, doc1.doc_id, 'alternate:1', nested_doc, True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:1', nested_doc), (doc1.rev, None)])

    def test_put_doc_if_newer_propagates_full_resolution(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        resolved_vcr = vectorclock.VectorClockRev(doc1.rev)
        vcr_2 = vectorclock.VectorClockRev(doc2.rev)
        resolved_vcr.maximize(vcr_2)
        resolved_vcr.increment('alternate')
        doc_resolved = self.make_document(doc1.doc_id, resolved_vcr.as_str(),
                                '{"good": 1}')
        state, _ = self.db._put_doc_if_newer(doc_resolved, save_conflict=True)
        self.assertEqual('inserted', state)
        self.assertFalse(doc_resolved.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        doc3 = self.db.get_doc(doc1.doc_id)
        self.assertFalse(doc3.has_conflicts)

    def test_put_doc_if_newer_propagates_partial_resolution(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'altalt:1', '{}')
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        doc3 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:1', nested_doc), ('test:1', simple_doc),
             ('altalt:1', '{}')])
        resolved_vcr = vectorclock.VectorClockRev(doc1.rev)
        vcr_3 = vectorclock.VectorClockRev(doc3.rev)
        resolved_vcr.maximize(vcr_3)
        resolved_vcr.increment('alternate')
        doc_resolved = self.make_document(doc1.doc_id, resolved_vcr.as_str(),
                                          '{"good": 1}')
        state, _ = self.db._put_doc_if_newer(doc_resolved, save_conflict=True)
        self.assertEqual('inserted', state)
        self.assertTrue(doc_resolved.has_conflicts)
        doc4 = self.db.get_doc(doc1.doc_id)
        self.assertTrue(doc4.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:2|test:1', '{"good": 1}'), ('altalt:1', '{}')])

    def test_put_doc_if_newer_replica_uid(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db._set_sync_info('other', 1, 'T-id')
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|other:1',
                                  nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True,
                                  replica_uid='other', replica_gen=2,
                                  replica_trans_id='T-id2')
        # Conflict vs the current update
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|third:3',
                                  '{}')
        self.assertEqual('conflicted',
            self.db._put_doc_if_newer(doc2, save_conflict=True,
                replica_uid='other', replica_gen=3,
                replica_trans_id='T-id3')[0])
        self.assertEqual((3, 'T-id3'), self.db._get_sync_gen_info('other'))

    def test_put_doc_if_newer_autoresolve_2(self):
        # this is an ordering variant of _3, but that already works
        # adding the test explicitly to catch the regression easily
        doc_a1 = self.db.create_doc(simple_doc)
        doc_a2 = self.make_document(doc_a1.doc_id, 'test:2', "{}")
        doc_a1b1 = self.make_document(doc_a1.doc_id, 'test:1|other:1',
                                      '{"a":"42"}')
        doc_a3 = self.make_document(doc_a1.doc_id, 'test:2|other:1', "{}")
        state, _ = self.db._put_doc_if_newer(doc_a2, save_conflict=True)
        self.assertEqual(state, 'inserted')
        state, _ = self.db._put_doc_if_newer(doc_a1b1, save_conflict=True)
        self.assertEqual(state, 'conflicted')
        state, _ = self.db._put_doc_if_newer(doc_a3, save_conflict=True)
        self.assertEqual(state, 'inserted')
        self.assertFalse(self.db.get_doc(doc_a1.doc_id).has_conflicts)

    def test_put_doc_if_newer_autoresolve_3(self):
        doc_a1 = self.db.create_doc(simple_doc)
        doc_a1b1 = self.make_document(doc_a1.doc_id, 'test:1|other:1', "{}")
        doc_a2 = self.make_document(doc_a1.doc_id, 'test:2',  '{"a":"42"}')
        doc_a3 = self.make_document(doc_a1.doc_id, 'test:3', "{}")
        state, _ = self.db._put_doc_if_newer(doc_a1b1, save_conflict=True)
        self.assertEqual(state, 'inserted')
        state, _ = self.db._put_doc_if_newer(doc_a2, save_conflict=True)
        self.assertEqual(state, 'conflicted')
        state, _ = self.db._put_doc_if_newer(doc_a3, save_conflict=True)
        self.assertEqual(state, 'superseded')
        doc = self.db.get_doc(doc_a1.doc_id, True)
        self.assertFalse(doc.has_conflicts)
        rev = vectorclock.VectorClockRev(doc.rev)
        rev_a3 = vectorclock.VectorClockRev('test:3')
        rev_a1b1 = vectorclock.VectorClockRev('test:1|other:1')
        self.assertTrue(rev.is_newer(rev_a3))
        self.assertTrue(rev.is_newer(rev_a1b1))

    def test_put_doc_if_newer_autoresolve_4(self):
        doc_a1 = self.db.create_doc(simple_doc)
        doc_a1b1 = self.make_document(doc_a1.doc_id, 'test:1|other:1', None)
        doc_a2 = self.make_document(doc_a1.doc_id, 'test:2',  '{"a":"42"}')
        doc_a3 = self.make_document(doc_a1.doc_id, 'test:3', None)
        state, _ = self.db._put_doc_if_newer(doc_a1b1, save_conflict=True)
        self.assertEqual(state, 'inserted')
        state, _ = self.db._put_doc_if_newer(doc_a2, save_conflict=True)
        self.assertEqual(state, 'conflicted')
        state, _ = self.db._put_doc_if_newer(doc_a3, save_conflict=True)
        self.assertEqual(state, 'superseded')
        doc = self.db.get_doc(doc_a1.doc_id, True)
        self.assertFalse(doc.has_conflicts)
        rev = vectorclock.VectorClockRev(doc.rev)
        rev_a3 = vectorclock.VectorClockRev('test:3')
        rev_a1b1 = vectorclock.VectorClockRev('test:1|other:1')
        self.assertTrue(rev.is_newer(rev_a3))
        self.assertTrue(rev.is_newer(rev_a1b1))

    def test_put_refuses_to_update_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        content2 = '{"key": "altval"}'
        doc2 = self.make_document(doc1.doc_id, 'altrev:1', content2)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDoc(self.db, doc1.doc_id, doc2.rev, content2, True)
        content3 = '{"key": "local"}'
        doc2.set_json(content3)
        self.assertRaises(errors.ConflictedDoc, self.db.put_doc, doc2)

    def test_delete_refuses_for_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'altrev:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, nested_doc, True)
        self.assertRaises(errors.ConflictedDoc, self.db.delete_doc, doc2)


class DatabaseIndexTests(tests.DatabaseBaseTests):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_create_index(self):
        self.db.create_index('test-idx', 'name')
        self.assertEqual([('test-idx', ['name'])],
                         self.db.list_indexes())

    def test_create_index_on_non_ascii_field_name(self):
        doc = self.db.create_doc(simplejson.dumps({u'\xe5': 'value'}))
        self.db.create_index('test-idx', u'\xe5')
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'value'))

    def test_list_indexes_with_non_ascii_field_names(self):
        self.db.create_index('test-idx', u'\xe5')
        self.assertEqual(
            [('test-idx', [u'\xe5'])], self.db.list_indexes())

    def test_create_index_evaluates_it(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'value'))

    def test_wildcard_matches_unicode_value(self):
        doc = self.db.create_doc(simplejson.dumps({"key": u"valu\xe5"}))
        self.db.create_index('test-idx', 'key')
        self.assertEqual([doc], self.db.get_from_index('test-idx', '*'))

    def test_retrieve_unicode_value_from_index(self):
        doc = self.db.create_doc(simplejson.dumps({"key": u"valu\xe5"}))
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc], self.db.get_from_index('test-idx', u"valu\xe5"))

    def test_create_index_fails_if_name_taken(self):
        self.db.create_index('test-idx', 'key')
        self.assertRaises(errors.IndexNameTakenError,
                          self.db.create_index,
                          'test-idx', 'stuff')

    def test_create_index_does_not_fail_if_name_taken_with_same_index(self):
        self.db.create_index('test-idx', 'key')
        self.db.create_index('test-idx', 'key')
        self.assertEqual([('test-idx', ['key'])], self.db.list_indexes())

    def test_create_index_after_deleting_document(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc2)
        self.db.create_index('test-idx', 'key')
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'value'))

    def test_delete_index(self):
        self.db.create_index('test-idx', 'key')
        self.assertEqual([('test-idx', ['key'])], self.db.list_indexes())
        self.db.delete_index('test-idx')
        self.assertEqual([], self.db.list_indexes())

    def test_create_adds_to_index(self):
        self.db.create_index('test-idx', 'key')
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'value'))

    def test_get_from_index_unmatched(self):
        self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertEqual([], self.db.get_from_index('test-idx', 'novalue'))

    def test_create_index_multiple_exact_matches(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', 'value')))

    def test_get_from_index(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'value'))

    def test_get_from_index_multi(self):
        content = '{"key": "value", "key2": "value2"}'
        doc = self.db.create_doc(content)
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc], self.db.get_from_index('test-idx', 'value', 'value2'))

    def test_get_from_index_multi_ordered(self):
        doc1 = self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value3"}')
        doc3 = self.db.create_doc('{"key": "value2", "key2": "value2"}')
        doc4 = self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc4, doc3, doc2, doc1],
            self.db.get_from_index('test-idx', 'v*', '*'))

    def test_get_range_from_index_start_end(self):
        doc1 = self.db.create_doc('{"key": "value3"}')
        doc2 = self.db.create_doc('{"key": "value2"}')
        self.db.create_doc('{"key": "value4"}')
        self.db.create_doc('{"key": "value1"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc2, doc1],
            self.db.get_range_from_index('test-idx', 'value2', 'value3'))

    def test_get_range_from_index_start(self):
        doc1 = self.db.create_doc('{"key": "value3"}')
        doc2 = self.db.create_doc('{"key": "value2"}')
        doc3 = self.db.create_doc('{"key": "value4"}')
        self.db.create_doc('{"key": "value1"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc2, doc1, doc3],
            self.db.get_range_from_index('test-idx', 'value2'))

    def test_get_range_from_index_end(self):
        self.db.create_doc('{"key": "value3"}')
        doc2 = self.db.create_doc('{"key": "value2"}')
        self.db.create_doc('{"key": "value4"}')
        doc4 = self.db.create_doc('{"key": "value1"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc4, doc2],
            self.db.get_range_from_index('test-idx', None, 'value2'))

    def test_get_wildcard_range_from_index_start(self):
        doc1 = self.db.create_doc('{"key": "value4"}')
        doc2 = self.db.create_doc('{"key": "value23"}')
        doc3 = self.db.create_doc('{"key": "value2"}')
        doc4 = self.db.create_doc('{"key": "value22"}')
        self.db.create_doc('{"key": "value1"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc3, doc4, doc2, doc1],
            self.db.get_range_from_index('test-idx', 'value2*'))

    def test_get_wildcard_range_from_index_end(self):
        self.db.create_doc('{"key": "value4"}')
        doc2 = self.db.create_doc('{"key": "value23"}')
        doc3 = self.db.create_doc('{"key": "value2"}')
        doc4 = self.db.create_doc('{"key": "value22"}')
        doc5 = self.db.create_doc('{"key": "value1"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc5, doc3, doc4, doc2],
            self.db.get_range_from_index('test-idx', None, 'value2*'))

    def test_get_wildcard_range_from_index_start_end(self):
        self.db.create_doc('{"key": "a"}')
        doc2 = self.db.create_doc('{"key": "boo3"}')
        doc3 = self.db.create_doc('{"key": "catalyst"}')
        doc4 = self.db.create_doc('{"key": "whaever"}')
        doc5 = self.db.create_doc('{"key": "zerg"}')
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            [doc3, doc4],
            self.db.get_range_from_index('test-idx', 'cat*', 'zap*'))

    def test_get_range_from_index_multi_column_start_end(self):
        self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value3"}')
        doc3 = self.db.create_doc('{"key": "value2", "key2": "value2"}')
        self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc3, doc2],
            self.db.get_range_from_index(
                'test-idx', ('value2', 'value2'), ('value2', 'value3')))

    def test_get_range_from_index_multi_column_start(self):
        doc1 = self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value3"}')
        self.db.create_doc('{"key": "value2", "key2": "value2"}')
        self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc2, doc1],
            self.db.get_range_from_index('test-idx', ('value2', 'value3')))

    def test_get_range_from_index_multi_column_end(self):
        self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value3"}')
        doc3 = self.db.create_doc('{"key": "value2", "key2": "value2"}')
        doc4 = self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc4, doc3, doc2],
            self.db.get_range_from_index(
                'test-idx', None, ('value2', 'value3')))

    def test_get_wildcard_range_from_index_multi_column_start(self):
        doc1 = self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value23"}')
        doc3 = self.db.create_doc('{"key": "value2", "key2": "value2"}')
        self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc3, doc2, doc1],
            self.db.get_range_from_index('test-idx', ('value2', 'value2*')))

    def test_get_wildcard_range_from_index_multi_column_end(self):
        self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value23"}')
        doc3 = self.db.create_doc('{"key": "value2", "key2": "value2"}')
        doc4 = self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc4, doc3, doc2],
            self.db.get_range_from_index(
                'test-idx', None, ('value2', 'value2*')))

    def test_get_glob_range_from_index_multi_column_start(self):
        doc1 = self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value23"}')
        self.db.create_doc('{"key": "value1", "key2": "value2"}')
        self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc2, doc1],
            self.db.get_range_from_index('test-idx', ('value2', '*')))

    def test_get_glob_range_from_index_multi_column_end(self):
        self.db.create_doc('{"key": "value3", "key2": "value4"}')
        doc2 = self.db.create_doc('{"key": "value2", "key2": "value23"}')
        doc3 = self.db.create_doc('{"key": "value1", "key2": "value2"}')
        doc4 = self.db.create_doc('{"key": "value1", "key2": "value1"}')
        self.db.create_index('test-idx', 'key', 'key2')
        self.assertEqual(
            [doc4, doc3, doc2],
            self.db.get_range_from_index('test-idx', None, ('value2', '*')))

    def test_get_range_from_index_illegal_wildcard_order(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_range_from_index, 'test-idx', ('*', 'v2'))

    def test_get_range_from_index_illegal_glob_after_wildcard(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_range_from_index, 'test-idx', ('*', 'v*'))

    def test_get_range_from_index_illegal_wildcard_order_end(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_range_from_index, 'test-idx', None, ('*', 'v2'))

    def test_get_range_from_index_illegal_glob_after_wildcard_end(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_range_from_index, 'test-idx', None, ('*', 'v*'))

    def test_get_from_index_fails_if_no_index(self):
        self.assertRaises(
            errors.IndexDoesNotExist, self.db.get_from_index, 'foo')

    def test_get_index_keys_fails_if_no_index(self):
        self.assertRaises(errors.IndexDoesNotExist,
                          self.db.get_index_keys,
                          'foo')

    def test_get_index_keys_works_if_no_docs(self):
        self.db.create_index('test-idx', 'key')
        self.assertEqual([], self.db.get_index_keys('test-idx'))

    def test_put_updates_index(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        new_content = '{"key": "altval"}'
        doc.set_json(new_content)
        self.db.put_doc(doc)
        self.assertEqual([], self.db.get_from_index('test-idx', 'value'))
        self.assertEqual([doc], self.db.get_from_index('test-idx', 'altval'))

    def test_delete_updates_index(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertEqual(
            sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', 'value')))
        self.db.delete_doc(doc)
        self.assertEqual([doc2], self.db.get_from_index('test-idx', 'value'))

    def test_get_from_index_illegal_number_of_entries(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidValueForIndex, self.db.get_from_index, 'test-idx')
        self.assertRaises(
            errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', 'v1')
        self.assertRaises(
            errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', 'v1', 'v2', 'v3')

    def test_get_from_index_illegal_wildcard_order(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_from_index, 'test-idx', '*', 'v2')

    def test_get_from_index_illegal_glob_after_wildcard(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_from_index, 'test-idx', '*', 'v*')

    def test_get_all_from_index(self):
        self.db.create_index('test-idx', 'key')
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        # This one should not be in the index
        self.db.create_doc('{"no": "key"}')
        diff_value_doc = '{"key": "diff value"}'
        doc4 = self.db.create_doc(diff_value_doc)
        # This is essentially a 'prefix' match, but we match every entry.
        self.assertEqual(
            sorted([doc1, doc2, doc4]),
            sorted(self.db.get_from_index('test-idx', '*')))

    def test_get_all_from_index_ordered(self):
        self.db.create_index('test-idx', 'key')
        doc1 = self.db.create_doc('{"key": "value x"}')
        doc2 = self.db.create_doc('{"key": "value b"}')
        doc3 = self.db.create_doc('{"key": "value a"}')
        doc4 = self.db.create_doc('{"key": "value m"}')
        # This is essentially a 'prefix' match, but we match every entry.
        self.assertEqual(
            [doc3, doc2, doc4, doc1], self.db.get_from_index('test-idx', '*'))

    def test_put_updates_when_adding_key(self):
        doc = self.db.create_doc("{}")
        self.db.create_index('test-idx', 'key')
        self.assertEqual([], self.db.get_from_index('test-idx', '*'))
        doc.set_json(simple_doc)
        self.db.put_doc(doc)
        self.assertEqual([doc], self.db.get_from_index('test-idx', '*'))

    def test_get_from_index_empty_string(self):
        self.db.create_index('test-idx', 'key')
        doc1 = self.db.create_doc(simple_doc)
        content2 = '{"key": ""}'
        doc2 = self.db.create_doc(content2)
        self.assertEqual([doc2], self.db.get_from_index('test-idx', ''))
        # Empty string matches the wildcard.
        self.assertEqual(
            sorted([doc1, doc2]),
            sorted(self.db.get_from_index('test-idx', '*')))

    def test_get_from_index_not_null(self):
        self.db.create_index('test-idx', 'key')
        doc1 = self.db.create_doc(simple_doc)
        self.db.create_doc('{"key": null}')
        self.assertEqual([doc1], self.db.get_from_index('test-idx', '*'))

    def test_get_partial_from_index(self):
        content1 = '{"k1": "v1", "k2": "v2"}'
        content2 = '{"k1": "v1", "k2": "x2"}'
        content3 = '{"k1": "v1", "k2": "y2"}'
        # doc4 has a different k1 value, so it doesn't match the prefix.
        content4 = '{"k1": "NN", "k2": "v2"}'
        doc1 = self.db.create_doc(content1)
        doc2 = self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        self.db.create_doc(content4)
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertEqual(
            sorted([doc1, doc2, doc3]),
            sorted(self.db.get_from_index('test-idx', "v1", "*")))

    def test_get_glob_match(self):
        # Note: the exact glob syntax is probably subject to change
        content1 = '{"k1": "v1", "k2": "v1"}'
        content2 = '{"k1": "v1", "k2": "v2"}'
        content3 = '{"k1": "v1", "k2": "v3"}'
        # doc4 has a different k2 prefix value, so it doesn't match
        content4 = '{"k1": "v1", "k2": "ZZ"}'
        self.db.create_index('test-idx', 'k1', 'k2')
        doc1 = self.db.create_doc(content1)
        doc2 = self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        self.db.create_doc(content4)
        self.assertEqual(
            sorted([doc1, doc2, doc3]),
            sorted(self.db.get_from_index('test-idx', "v1", "v*")))

    def test_nested_index(self):
        doc = self.db.create_doc(nested_doc)
        self.db.create_index('test-idx', 'sub.doc')
        self.assertEqual(
            [doc], self.db.get_from_index('test-idx', 'underneath'))
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual(
            sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', 'underneath')))

    def test_nested_nonexistent(self):
        self.db.create_doc(nested_doc)
        # sub exists, but sub.foo does not:
        self.db.create_index('test-idx', 'sub.foo')
        self.assertEqual([], self.db.get_from_index('test-idx', '*'))

    def test_nested_nonexistent2(self):
        self.db.create_doc(nested_doc)
        # sub exists, but sub.foo does not:
        self.db.create_index('test-idx', 'sub.foo.bar.baz.qux.fnord')
        self.assertEqual([], self.db.get_from_index('test-idx', '*'))

    def test_index_list1(self):
        self.db.create_index("index", "name")
        content = '{"name": ["foo", "bar"]}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "bar")
        self.assertEqual([doc], rows)

    def test_index_list2(self):
        self.db.create_index("index", "name")
        content = '{"name": ["foo", "bar"]}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_get_from_index_case_sensitive(self):
        self.db.create_index('test-idx', 'key')
        doc1 = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_from_index('test-idx', 'V*'))
        self.assertEqual([doc1], self.db.get_from_index('test-idx', 'v*'))

    def test_get_from_index_illegal_glob_before_value(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_from_index, 'test-idx', 'v*', 'v2')

    def test_get_from_index_illegal_glob_after_glob(self):
        self.db.create_index('test-idx', 'k1', 'k2')
        self.assertRaises(
            errors.InvalidGlobbing,
            self.db.get_from_index, 'test-idx', 'v*', 'v*')

    def test_get_from_index_with_sql_wildcards(self):
        self.db.create_index('test-idx', 'key')
        content1 = '{"key": "va%lue"}'
        content2 = '{"key": "value"}'
        content3 = '{"key": "va_lue"}'
        doc1 = self.db.create_doc(content1)
        self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        # The '%' in the search should be treated literally, not as a sql
        # globbing character.
        self.assertEqual([doc1], self.db.get_from_index('test-idx', 'va%*'))
        # Same for '_'
        self.assertEqual([doc3], self.db.get_from_index('test-idx', 'va_*'))

    def test_get_from_index_with_lower(self):
        self.db.create_index("index", "lower(name)")
        content = '{"name": "Foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_get_from_index_with_lower_matches_same_case(self):
        self.db.create_index("index", "lower(name)")
        content = '{"name": "foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_index_lower_doesnt_match_different_case(self):
        self.db.create_index("index", "lower(name)")
        content = '{"name": "Foo"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", "Foo")
        self.assertEqual([], rows)

    def test_index_lower_doesnt_match_other_index(self):
        self.db.create_index("index", "lower(name)")
        self.db.create_index("other_index", "name")
        content = '{"name": "Foo"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", "Foo")
        self.assertEqual(0, len(rows))

    def test_index_split_words_match_first(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": "foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_index_split_words_match_second(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": "foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "bar")
        self.assertEqual([doc], rows)

    def test_index_split_words_match_both(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": "foo foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_index_split_words_double_space(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": "foo  bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "bar")
        self.assertEqual([doc], rows)

    def test_index_split_words_leading_space(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": " foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "foo")
        self.assertEqual([doc], rows)

    def test_index_split_words_trailing_space(self):
        self.db.create_index("index", "split_words(name)")
        content = '{"name": "foo bar "}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "bar")
        self.assertEqual([doc], rows)

    def test_get_from_index_with_number(self):
        self.db.create_index("index", "number(foo, 5)")
        content = '{"foo": 12}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "00012")
        self.assertEqual([doc], rows)

    def test_get_from_index_with_number_bigger_than_padding(self):
        self.db.create_index("index", "number(foo, 5)")
        content = '{"foo": 123456}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "123456")
        self.assertEqual([doc], rows)

    def test_number_mapping_ignores_non_numbers(self):
        self.db.create_index("index", "number(foo, 5)")
        content = '{"foo": 56}'
        doc1 = self.db.create_doc(content)
        content = '{"foo": "this is not a maigret painting"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", "*")
        self.assertEqual([doc1], rows)

    def test_get_from_index_with_bool(self):
        self.db.create_index("index", "bool(foo)")
        content = '{"foo": true}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "1")
        self.assertEqual([doc], rows)

    def test_get_from_index_with_bool_false(self):
        self.db.create_index("index", "bool(foo)")
        content = '{"foo": false}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", "0")
        self.assertEqual([doc], rows)

    def test_get_from_index_with_non_bool(self):
        self.db.create_index("index", "bool(foo)")
        content = '{"foo": 42}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", "*")
        self.assertEqual([], rows)

    def test_get_index_keys_from_index(self):
        self.db.create_index('test-idx', 'key')
        content1 = '{"key": "value1"}'
        content2 = '{"key": "value2"}'
        content3 = '{"key": "value2"}'
        self.db.create_doc(content1)
        self.db.create_doc(content2)
        self.db.create_doc(content3)
        self.assertEqual(
            [('value1',), ('value2',)],
            sorted(self.db.get_index_keys('test-idx')))

    def test_get_index_keys_from_multicolumn_index(self):
        self.db.create_index('test-idx', 'key1', 'key2')
        content1 = '{"key1": "value1", "key2": "val2-1"}'
        content2 = '{"key1": "value2", "key2": "val2-2"}'
        content3 = '{"key1": "value2", "key2": "val2-2"}'
        content4 = '{"key1": "value2", "key2": "val3"}'
        self.db.create_doc(content1)
        self.db.create_doc(content2)
        self.db.create_doc(content3)
        self.db.create_doc(content4)
        self.assertEqual([
            ('value1', 'val2-1'),
            ('value2', 'val2-2'),
            ('value2', 'val3')],
            sorted(self.db.get_index_keys('test-idx')))


class PythonBackendTests(tests.DatabaseBaseTests):

    def test_create_doc_with_factory(self):
        self.db.set_document_factory(TestAlternativeDocument)
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertTrue(isinstance(doc, TestAlternativeDocument))

    def test_get_doc_after_put_with_factory(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.db.set_document_factory(TestAlternativeDocument)
        result = self.db.get_doc('my_doc_id')
        self.assertTrue(isinstance(result, TestAlternativeDocument))
        self.assertEqual(doc.doc_id, result.doc_id)
        self.assertEqual(doc.rev, result.rev)
        self.assertEqual(doc.get_json(), result.get_json())
        self.assertEqual(False, result.has_conflicts)

    def test_get_doc_nonexisting_with_factory(self):
        self.db.set_document_factory(TestAlternativeDocument)
        self.assertIs(None, self.db.get_doc('non-existing'))

    def test_get_all_docs_with_factory(self):
        self.db.set_document_factory(TestAlternativeDocument)
        self.db.create_doc(simple_doc)
        self.assertTrue(isinstance(
            self.db.get_all_docs()[1][0], TestAlternativeDocument))

    def test_get_docs_conflicted_with_factory(self):
        self.db.set_document_factory(TestAlternativeDocument)
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db._put_doc_if_newer(doc2, save_conflict=True)
        self.assertTrue(
            isinstance(
                self.db.get_docs([doc1.doc_id])[0], TestAlternativeDocument))

    def test_get_from_index_with_factory(self):
        self.db.set_document_factory(TestAlternativeDocument)
        self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        self.assertTrue(
            isinstance(
                self.db.get_from_index('test-idx', 'value')[0],
                TestAlternativeDocument))

    def test_sync_exchange_updates_indexes(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', 'key')
        new_content = '{"key": "altval"}'
        other_rev = 'test:1|z:2'
        st = self.db.get_sync_target()

        def ignore(doc_id, doc_rev, doc):
            pass

        doc_other = self.make_document(doc.doc_id, other_rev, new_content)
        docs_by_gen = [(doc_other, 10, 'T-sid')]
        st.sync_exchange(
            docs_by_gen, 'other-replica', last_known_generation=0,
            return_doc_cb=ignore)
        self.assertGetDoc(self.db, doc.doc_id, other_rev, new_content, False)
        self.assertEqual(
            [doc_other], self.db.get_from_index('test-idx', 'altval'))
        self.assertEqual([], self.db.get_from_index('test-idx', 'value'))


# Use a custom loader to apply the scenarios at load time.
load_tests = tests.load_with_scenarios
