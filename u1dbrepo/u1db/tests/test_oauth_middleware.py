# Copyright 2012 Canonical Ltd.
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

"""Test OAuth wsgi middleware"""
import paste.fixture
from oauth import oauth
import simplejson
import time

from u1db import tests

from u1db.remote.oauth_middleware import OAuthMiddleware


BASE_URL = 'https://u1db.net'


class TestAuthMiddleware(tests.TestCase):

    def setUp(self):
        super(TestAuthMiddleware, self).setUp()
        self.got = []
        def witness_app(environ, start_response):
            start_response("200 OK", [("content-type", "text/plain")])
            self.got.append((environ['token_key'], environ['PATH_INFO'],
                             environ['QUERY_STRING']))
            return ["ok"]
        class MyOAuthMiddleware(OAuthMiddleware):
            get_oauth_data_store = lambda self: tests.testingOAuthStore

            def verify(self, environ, oauth_req):
                consumer, token = super(MyOAuthMiddleware, self).verify(
                    environ, oauth_req)
                environ['token_key'] = token.key
        self.oauth_midw = MyOAuthMiddleware(witness_app, BASE_URL)
        self.app = paste.fixture.TestApp(self.oauth_midw)

    def test_expect_tilde(self):
        url = BASE_URL + '/foo/doc/doc-id'
        resp = self.app.delete(url, expect_errors=True)
        self.assertEqual(400, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual('{"error": "bad request"}', resp.body)

    def test_missing_oauth(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        resp = self.app.delete(url, expect_errors=True)
        self.assertEqual(401, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"error": "unauthorized",
                          "message": "Missing OAuth."},
                         simplejson.loads(resp.body))

    def test_oauth_in_query_string(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        params = {'old_rev': 'old-rev'}
        oauth_req = oauth.OAuthRequest.from_consumer_and_token(
            tests.consumer1,
            tests.token1,
            parameters=params,
            http_url=url,
            http_method='DELETE'
            )
        oauth_req.sign_request(tests.sign_meth_HMAC_SHA1,
                               tests.consumer1, tests.token1)
        resp = self.app.delete(oauth_req.to_url())
        self.assertEqual(200, resp.status)
        self.assertEqual([(tests.token1.key,
                           '/foo/doc/doc-id', 'old_rev=old-rev')], self.got)

    def test_oauth_invalid(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        params = {'old_rev': 'old-rev'}
        oauth_req = oauth.OAuthRequest.from_consumer_and_token(
            tests.consumer1,
            tests.token3,
            parameters=params,
            http_url=url,
            http_method='DELETE'
            )
        oauth_req.sign_request(tests.sign_meth_HMAC_SHA1,
                               tests.consumer1, tests.token3)
        resp = self.app.delete(oauth_req.to_url(),
                               expect_errors=True)
        self.assertEqual(401, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        err = simplejson.loads(resp.body)
        self.assertEqual({"error": "unauthorized",
                          "message": err['message']},
                         err)

    def test_oauth_in_header(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        params = {'old_rev': 'old-rev'}
        oauth_req = oauth.OAuthRequest.from_consumer_and_token(
            tests.consumer2,
            tests.token2,
            parameters=params,
            http_url=url,
            http_method='DELETE'
            )
        url = oauth_req.get_normalized_http_url() + '?' + (
            '&'.join("%s=%s" % (k, v) for k, v in params.items()))
        oauth_req.sign_request(tests.sign_meth_HMAC_SHA1,
                               tests.consumer2, tests.token2)
        resp = self.app.delete(url, headers=oauth_req.to_header())
        self.assertEqual(200, resp.status)
        self.assertEqual([(tests.token2.key,
                           '/foo/doc/doc-id', 'old_rev=old-rev')], self.got)

    def test_oauth_plain_text(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        params = {'old_rev': 'old-rev'}
        oauth_req = oauth.OAuthRequest.from_consumer_and_token(
            tests.consumer1,
            tests.token1,
            parameters=params,
            http_url=url,
            http_method='DELETE'
            )
        oauth_req.sign_request(tests.sign_meth_PLAINTEXT,
                               tests.consumer1, tests.token1)
        resp = self.app.delete(oauth_req.to_url())
        self.assertEqual(200, resp.status)
        self.assertEqual([(tests.token1.key,
                           '/foo/doc/doc-id', 'old_rev=old-rev')], self.got)

    def test_oauth_timestamp_threshold(self):
        url = BASE_URL + '/~/foo/doc/doc-id'
        params = {'old_rev': 'old-rev'}
        oauth_req = oauth.OAuthRequest.from_consumer_and_token(
            tests.consumer1,
            tests.token1,
            parameters=params,
            http_url=url,
            http_method='DELETE'
            )
        oauth_req.set_parameter('oauth_timestamp', int(time.time()) - 5)
        oauth_req.sign_request(tests.sign_meth_PLAINTEXT,
                               tests.consumer1, tests.token1)
        # tweak threshold
        self.oauth_midw.timestamp_threshold = 1
        resp = self.app.delete(oauth_req.to_url(), expect_errors=True)
        self.assertEqual(401, resp.status)
        err = simplejson.loads(resp.body)
        self.assertIn('Expired timestamp', err['message'])
        self.assertIn('threshold 1', err['message'])
