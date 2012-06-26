# Copyright 2011-2012 Canonical Ltd.
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

"""Tests for the remote sync targets"""

from wsgiref import simple_server
import cStringIO
#from paste import httpserver

from u1db import (
    errors,
    tests,
    )
from u1db.remote import (
    http_app,
    http_target,
    oauth_middleware,
    )


class TestHTTPSyncTargetBasics(tests.TestCase):

    def test_parse_url(self):
        remote_target = http_target.HTTPSyncTarget('http://127.0.0.1:12345/')
        self.assertEqual('http', remote_target._url.scheme)
        self.assertEqual('127.0.0.1', remote_target._url.hostname)
        self.assertEqual(12345, remote_target._url.port)
        self.assertEqual('/', remote_target._url.path)


class TestParsingSyncStream(tests.TestCase):

    def test_wrong_start(self):
        tgt = http_target.HTTPSyncTarget("http://foo/foo")

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "{}\r\n]", None)

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "\r\n{}\r\n]", None)

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "", None)

    def test_wrong_end(self):
        tgt = http_target.HTTPSyncTarget("http://foo/foo")

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "[\r\n{}", None)

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "[\r\n", None)

    def test_missing_comma(self):
        tgt = http_target.HTTPSyncTarget("http://foo/foo")

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream,
                          '[\r\n{}\r\n{"id": "i", "rev": "r", '
                          '"content": "c", "gen": 3}\r\n]', None)

    def test_extra_comma(self):
        tgt = http_target.HTTPSyncTarget("http://foo/foo")

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream, "[\r\n{},\r\n]", None)

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream,
                          '[\r\n{},\r\n{"id": "i", "rev": "r", '
                          '"content": "{}", "gen": 3, "trans_id": "T-sid"}'
                          ',\r\n]',
                          lambda doc, gen, trans_id: None)

    def test_error_in_stream(self):
        tgt = http_target.HTTPSyncTarget("http://foo/foo")

        self.assertRaises(errors.Unavailable,
                          tgt._parse_sync_stream,
                          '[\r\n{"new_generation": 0},'
                          '\r\n{"error": "unavailable"}\r\n', None)

        self.assertRaises(errors.Unavailable,
                          tgt._parse_sync_stream,
                          '[\r\n{"error": "unavailable"}\r\n', None)

        self.assertRaises(errors.BrokenSyncStream,
                          tgt._parse_sync_stream,
                          '[\r\n{"error": "?"}\r\n', None)


def http_server_def():

    def make_server(host_port, handler, state):
        application = http_app.HTTPApp(state)
        srv = simple_server.WSGIServer(host_port, handler)
        srv.set_app(application)
        #srv = httpserver.WSGIServerBase(application,
        #                                host_port,
        #                                handler
        #                                )
        return srv

    class req_handler(simple_server.WSGIRequestHandler):
        def log_request(*args):
            pass  # suppress

    #rh = httpserver.WSGIHandler
    return make_server, req_handler, "shutdown", "http"


def http_sync_target(test, path):
    return http_target.HTTPSyncTarget(test.getURL(path))


def oauth_http_server_def():

    def make_server(host_port, handler, state):
        app = http_app.HTTPApp(state)
        application = oauth_middleware.OAuthMiddleware(app, None)
        application.get_oauth_data_store = lambda: tests.testingOAuthStore
        srv = simple_server.WSGIServer(host_port, handler)
        # patch the value in
        application.base_url = "http://%s:%s" % srv.server_address
        srv.set_app(application)
        return srv

    class req_handler(simple_server.WSGIRequestHandler):
        def log_request(*args):
            pass  # suppress

    return make_server, req_handler, "shutdown", "http"


def oauth_http_sync_target(test, path):
    st = http_sync_target(test, '~/' + path)
    st.set_oauth_credentials(tests.consumer1.key, tests.consumer1.secret,
                             tests.token1.key, tests.token1.secret)
    return st


class TestRemoteSyncTargets(tests.TestCaseWithServer):

    scenarios = [
        ('http', {'server_def': http_server_def,
                  'make_document': tests.create_doc,
                  'sync_target': http_sync_target}),
        ('oauth_http', {'server_def': oauth_http_server_def,
                        'make_document': tests.create_doc,
                        'sync_target': oauth_http_sync_target}),
        ]

    def getSyncTarget(self, path=None):
        if self.server is None:
            self.startServer()
        return self.sync_target(self, path)

    def test_get_sync_info(self):
        self.startServer()
        db = self.request_state._create_database('test')
        db._set_sync_info('other-id', 1, 'T-transid')
        remote_target = self.getSyncTarget('test')
        self.assertEqual(('test', 0, 1, 'T-transid'),
                         remote_target.get_sync_info('other-id'))

    def test_record_sync_info(self):
        self.startServer()
        db = self.request_state._create_database('test')
        remote_target = self.getSyncTarget('test')
        remote_target.record_sync_info('other-id', 2, 'T-transid')
        self.assertEqual((2, 'T-transid'), db._get_sync_gen_info('other-id'))

    def test_sync_exchange_send(self):
        self.startServer()
        db = self.request_state._create_database('test')
        remote_target = self.getSyncTarget('test')
        other_docs = []

        def receive_doc(doc):
            other_docs.append((doc.doc_id, doc.rev, doc.get_json()))

        doc = self.make_document('doc-here', 'replica:1', '{"value": "here"}')
        new_gen, trans_id = remote_target.sync_exchange(
                [(doc, 10, 'T-sid')],
                'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertGetDoc(
            db, 'doc-here', 'replica:1', '{"value": "here"}', False)

    def test_sync_exchange_send_failure_and_retry_scenario(self):
        self.startServer()

        def blackhole_getstderr(inst):
            return cStringIO.StringIO()

        self.patch(self.server.RequestHandlerClass, 'get_stderr',
                   blackhole_getstderr)
        db = self.request_state._create_database('test')
        _put_doc_if_newer = db._put_doc_if_newer
        trigger_ids = ['doc-here2']

        def bomb_put_doc_if_newer(doc, save_conflict,
                                  replica_uid=None, replica_gen=None,
                                  replica_trans_id=None):
            if doc.doc_id in trigger_ids:
                raise Exception
            return _put_doc_if_newer(doc, save_conflict=save_conflict,
                replica_uid=replica_uid, replica_gen=replica_gen,
                replica_trans_id=replica_trans_id)
        self.patch(db, '_put_doc_if_newer', bomb_put_doc_if_newer)
        remote_target = self.getSyncTarget('test')
        other_changes = []

        def receive_doc(doc, gen, trans_id):
            other_changes.append(
                (doc.doc_id, doc.rev, doc.get_json(), gen, trans_id))

        doc1 = self.make_document('doc-here', 'replica:1', '{"value": "here"}')
        doc2 = self.make_document('doc-here2', 'replica:1',
                                  '{"value": "here2"}')
        self.assertRaises(errors.HTTPError, remote_target.sync_exchange,
                [(doc1, 10, 'T-sid'),
                 (doc2, 11, 'T-sud')
                 ], 'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertGetDoc(db, 'doc-here', 'replica:1', '{"value": "here"}',
                          False)
        self.assertEqual((10, 'T-sid'), db._get_sync_gen_info('replica'))
        self.assertEqual([], other_changes)
        # retry
        trigger_ids = []
        new_gen, trans_id = remote_target.sync_exchange(
                [(doc2, 11, 'T-sud')
                 ], 'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertGetDoc(db, 'doc-here2', 'replica:1', '{"value": "here2"}',
                          False)
        self.assertEqual((11, 'T-sud'), db._get_sync_gen_info('replica'))
        self.assertEqual(2, new_gen)
        # bounced back to us
        self.assertEqual(
            ('doc-here', 'replica:1', '{"value": "here"}', 1),
            other_changes[0][:-1])

    def test_sync_exchange_in_stream_error(self):
        self.startServer()

        def blackhole_getstderr(inst):
            return cStringIO.StringIO()

        self.patch(self.server.RequestHandlerClass, 'get_stderr',
                   blackhole_getstderr)
        db = self.request_state._create_database('test')
        doc = db.create_doc('{"value": "there"}')

        def bomb_get_docs(doc_ids, check_for_conflicts=None,
                          include_deleted=False):
            yield doc
            # delayed failure case
            raise errors.Unavailable

        self.patch(db, 'get_docs', bomb_get_docs)
        remote_target = self.getSyncTarget('test')
        other_changes = []

        def receive_doc(doc, gen, trans_id):
            other_changes.append(
                (doc.doc_id, doc.rev, doc.get_json(), gen, trans_id))

        self.assertRaises(errors.Unavailable, remote_target.sync_exchange,
                          [], 'replica', last_known_generation=0,
                          return_doc_cb=receive_doc)
        self.assertEqual(
            (doc.doc_id, doc.rev, '{"value": "there"}', 1),
            other_changes[0][:-1])

    def test_sync_exchange_receive(self):
        self.startServer()
        db = self.request_state._create_database('test')
        doc = db.create_doc('{"value": "there"}')
        remote_target = self.getSyncTarget('test')
        other_changes = []

        def receive_doc(doc, gen, trans_id):
            other_changes.append(
                (doc.doc_id, doc.rev, doc.get_json(), gen, trans_id))

        new_gen, trans_id = remote_target.sync_exchange(
                        [], 'replica', last_known_generation=0,
                        return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertEqual(
            (doc.doc_id, doc.rev, '{"value": "there"}', 1),
            other_changes[0][:-1])


load_tests = tests.load_with_scenarios
