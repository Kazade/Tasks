/*
 * Copyright 2012 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "u1db/u1db_http_internal.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <json/json.h>
#include <curl/curl.h>
#include <oauth.h>

#ifndef max
#define max(a, b) ((a) > (b) ? (a) : (b))
#endif // max

struct _http_state;
struct _http_request;

static int st_http_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen,
        char **trans_id);

static int st_http_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen, const char *trans_id);

static int st_http_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange);
static int st_http_sync_exchange(u1db_sync_target *st,
                                 const char *source_replica_uid, int n_docs,
                                 u1db_document **docs, int *generations,
                                 const char **trans_ids, int *target_gen,
                                 char **target_trans_id, void *context,
                                 u1db_doc_gen_callback cb);
static int st_http_sync_exchange_doc_ids(u1db_sync_target *st,
                                         u1database *source_db, int n_doc_ids,
                                         const char **doc_ids,
                                         int *generations,
                                         const char **trans_ids,
                                         int *target_gen,
                                         char **target_trans_id, void *context,
                                         u1db_doc_gen_callback cb);
static void st_http_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange);
static int st_http_set_trace_hook(u1db_sync_target *st,
                             void *context, u1db__trace_callback cb);
static void st_http_finalize(u1db_sync_target *st);
static int initialize_curl(struct _http_state *state);
static int simple_set_curl_data(CURL *curl, struct _http_request *header,
                     struct _http_request *body, struct _http_request *put);


struct _http_state {
    char is_http[4];
    char *base_url;
    char *replica_uid;
    CURL *curl;
    char *consumer_key;
    char *consumer_secret;
    char *token_key;
    char *token_secret;
};

static const char is_http[4] = "HTTP";
static const char auth_header_prefix[] =
    "Authorization: OAuth realm=\"\", ";

// Do a safe cast from implementation into the http state
static int
impl_as_http_state(void *impl, struct _http_state **state)
{
    struct _http_state *maybe_state;
    if (impl == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    maybe_state = (struct _http_state *)impl;
    if (memcmp(maybe_state->is_http, is_http, sizeof(is_http)) != 0) {
        return U1DB_INVALID_PARAMETER;
    }
    *state = maybe_state;
    return U1DB_OK;
}


struct _http_request {
    struct _http_state *state;
    int num_header_bytes;
    int max_header_bytes;
    char *header_buffer;
    int num_body_bytes;
    int max_body_bytes;
    char *body_buffer;
    int num_put_bytes;
    const char *put_buffer;
    struct _http_sync_response_state *response_state;
};


int
u1db__create_oauth_http_sync_target(const char *url,
    const char *consumer_key, const char *consumer_secret,
    const char *token_key, const char *token_secret,
    u1db_sync_target **target)
{
    int status = U1DB_OK;
    int url_len;
    struct _http_state *state = NULL;
    u1db_sync_target *new_target;

    if (url == NULL || target == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    new_target = (u1db_sync_target *)calloc(1, sizeof(u1db_sync_target));
    if (new_target == NULL) { goto oom; }
    state = (struct _http_state *)calloc(1, sizeof(struct _http_state));
    if (state == NULL) { goto oom; }
    memcpy(state->is_http, is_http, sizeof(is_http));
    status = initialize_curl(state);
    if (status != U1DB_OK) { goto fail; }
    // Copy the url, but ensure that it ends in a '/'
    url_len = strlen(url);
    if (url[url_len-1] == '/') {
        state->base_url = strdup(url);
        if (state->base_url == NULL) { goto oom; }
    } else {
        state->base_url = (char*)calloc(url_len+2, sizeof(char));
        if (state->base_url == NULL) { goto oom; }
        memcpy(state->base_url, url, url_len);
        state->base_url[url_len] = '/';
        state->base_url[url_len+1] = '\0';
    }
    if (consumer_key != NULL) {
        state->consumer_key = strdup(consumer_key);
    }
    if (consumer_secret != NULL) {
        state->consumer_secret = strdup(consumer_secret);
    }
    if (token_key != NULL) {
        state->token_key = strdup(token_key);
    }
    if (token_secret != NULL) {
        state->token_secret = strdup(token_secret);
    }
    new_target->implementation = state;
    new_target->get_sync_info = st_http_get_sync_info;
    new_target->record_sync_info = st_http_record_sync_info;
    new_target->sync_exchange = st_http_sync_exchange;
    new_target->sync_exchange_doc_ids = st_http_sync_exchange_doc_ids;
    new_target->get_sync_exchange = st_http_get_sync_exchange;
    new_target->finalize_sync_exchange = st_http_finalize_sync_exchange;
    new_target->_set_trace_hook = st_http_set_trace_hook;
    new_target->finalize = st_http_finalize;
    *target = new_target;
    return status;
oom:
    status = U1DB_NOMEM;
fail:
    if (state != NULL) {
        if (state->base_url != NULL) {
            free(state->base_url);
            state->base_url = NULL;
        }
        if (state->curl != NULL) {
            curl_easy_cleanup(state->curl);
            state->curl = NULL;
        }
        free(state);
        state = NULL;
    }
    if (new_target != NULL) {
        free(new_target);
        new_target = NULL;
    }
    return status;
}


int
u1db__create_http_sync_target(const char *url, u1db_sync_target **target)
{
    return u1db__create_oauth_http_sync_target(url, NULL, NULL, NULL, NULL, target);
}


static size_t
recv_header_bytes(const char *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    int needed_bytes;
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    // Note: curl says that CURLOPT_HEADERFUNCTION is called 1 time for each
    //       header, with exactly the header contents. So we should be able to
    //       change this into something that parses the header content itself,
    //       without separately buffering the raw bytes.
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    if (req->state != NULL && total_bytes > 9 && strncmp(ptr, "HTTP/", 5) == 0)
    {
        if (strncmp(ptr, "HTTP/1.0 ", 9) == 0) {
            // The server is an HTTP 1.0 server (like in the test suite). Tell
            // curl to treat it as such from now on. I don't understand why
            // curl isn't doing this already, because it has seen that the
            // server is v1.0
            curl_easy_setopt(req->state->curl, CURLOPT_HTTP_VERSION,
                             CURL_HTTP_VERSION_1_0);
        } else if (strncmp(ptr, "HTTP/1.1 ", 9) == 0) {
            curl_easy_setopt(req->state->curl, CURLOPT_HTTP_VERSION,
                             CURL_HTTP_VERSION_1_0);
        }
    }
    needed_bytes = req->num_header_bytes + total_bytes + 1;
    if (needed_bytes >= req->max_header_bytes) {
        req->max_header_bytes = max((req->max_header_bytes * 2), needed_bytes);
        req->max_header_bytes += 100;
        req->header_buffer = realloc(req->header_buffer, req->max_header_bytes);
        if (req->header_buffer == NULL) {
            return 0;
        }
    }
    memcpy(req->header_buffer + req->num_header_bytes, ptr, total_bytes);
    req->num_header_bytes += total_bytes;
    req->header_buffer[req->num_header_bytes] = '\0';
    return total_bytes;
}


static size_t
recv_body_bytes(const char *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    int needed_bytes;
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    needed_bytes = req->num_body_bytes + total_bytes + 1;
    if (needed_bytes >= req->max_body_bytes) {
        req->max_body_bytes = max((req->max_body_bytes * 2), needed_bytes);
        req->max_body_bytes += 100;
        req->body_buffer = realloc(req->body_buffer, req->max_body_bytes);
        if (req->body_buffer == NULL) {
            return 0;
        }
    }
    memcpy(req->body_buffer + req->num_body_bytes, ptr, total_bytes);
    req->num_body_bytes += total_bytes;
    req->body_buffer[req->num_body_bytes] = '\0';
    return total_bytes;
}


static size_t
send_put_bytes(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    if (total_bytes > (size_t) req->num_put_bytes) {
        total_bytes = req->num_put_bytes;
    }
    memcpy(ptr, req->put_buffer, total_bytes);
    req->num_put_bytes -= total_bytes;
    req->put_buffer += total_bytes;
    return total_bytes;
}


static int
initialize_curl(struct _http_state *state)
{
    int status;

    state->curl = curl_easy_init();
    if (state->curl == NULL) { goto oom; }
    // All conversations are done without CURL generating progress bars.
    status = curl_easy_setopt(state->curl, CURLOPT_NOPROGRESS, 1L);
    if (status != CURLE_OK) { goto fail; }
    /// status = curl_easy_setopt(state->curl, CURLOPT_VERBOSE, 1L);
    /// if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_HEADERFUNCTION,
                              recv_header_bytes);
    if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_WRITEFUNCTION,
                              recv_body_bytes);
    if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_READFUNCTION,
                              send_put_bytes);
    if (status != CURLE_OK) { goto fail; }
    return status;
oom:
    status = U1DB_NOMEM;
fail:
    if (state->curl != NULL) {
        curl_easy_cleanup(state->curl);
        state->curl = NULL;
    }
    return status;
}


// If we have oauth credentials, sign the URL and set the Authorization:
// header
static int
maybe_sign_url(u1db_sync_target *st, const char *http_method,
               const char *url, struct curl_slist **headers)
{
    int status;
    struct _http_state *state;
    char *authorization = NULL;
    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }
    if (state->consumer_key == NULL || state->consumer_secret == NULL) {
        return U1DB_OK; // Shortcut, do nothing, no OAuth creds to use
    }
    status = u1db__get_oauth_authorization(st, http_method, url,
        &authorization);
    if (status != U1DB_OK) {
        return status;
    }
    *headers = curl_slist_append(*headers, authorization);
    if (authorization != NULL) {
        // curl_slist_append already copies the data, so we don't need it now
        free(authorization);
    }
    return U1DB_OK;
}


static int
st_http_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen,
        char **trans_id)
{
    struct _http_state *state;
    struct _http_request req = {0};
    char *url = NULL;
    const char *tmp = NULL;
    int status;
    long http_code;
    struct curl_slist *headers = NULL;

    json_object *json = NULL, *obj = NULL;

    if (st == NULL || source_replica_uid == NULL || st_replica_uid == NULL
            || st_gen == NULL || source_gen == NULL
            || st->implementation == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }

    headers = curl_slist_append(NULL, "Content-Type: application/json");
    if (headers == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    req.state = state;
    status = u1db__format_sync_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPGET, 1L);
    if (status != CURLE_OK) { goto finish; }
    // status = curl_easy_setopt(state->curl, CURLOPT_USERAGENT, "...");
    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    req.body_buffer = req.header_buffer = NULL;
    status = simple_set_curl_data(state->curl, &req, &req, NULL);
    if (status != CURLE_OK) { goto finish; }
    status = maybe_sign_url(st, "GET", url, &headers);
    if (status != U1DB_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, headers);
    if (status != CURLE_OK) { goto finish; }
    // Now do the GET
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200) { // 201 for created? shouldn't happen on GET
        status = http_code;
        goto finish;
    }
    if (req.body_buffer == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    json = json_tokener_parse(req.body_buffer);
    if (json == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    obj = json_object_object_get(json, "target_replica_uid");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    if (state->replica_uid == NULL) {
        // we cache this on the state object, because the api for get_sync_info
        // asserts that callers do not have to free the returned string.
        // This isn't a functional problem, because if the sync target ever
        // changed its replica uid we'd be seriously broken anyway.
        state->replica_uid = strdup(json_object_get_string(obj));
    } else {
        if (strcmp(state->replica_uid, json_object_get_string(obj)) != 0) {
            // Our http target changed replica_uid, this would be a really
            // strange bug
            status = U1DB_INVALID_HTTP_RESPONSE;
            goto finish;
        }
    }
    *st_replica_uid = state->replica_uid;
    if (*st_replica_uid == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    obj = json_object_object_get(json, "target_replica_generation");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    *st_gen = json_object_get_int(obj);
    obj = json_object_object_get(json, "source_replica_generation");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    *source_gen = json_object_get_int(obj);
    obj = json_object_object_get(json, "source_transaction_id");
    if (obj == NULL) {
        *trans_id = NULL;
    } else {
        tmp = json_object_get_string(obj);
        if (tmp == NULL) {
            *trans_id = NULL;
        } else {
            *trans_id = strdup(tmp);
            if (*trans_id == NULL) {
                status = U1DB_NOMEM;
            }
        }
    }
finish:
    if (req.header_buffer != NULL) {
        free(req.header_buffer);
    }
    if (req.body_buffer != NULL) {
        free(req.body_buffer);
    }
    if (json != NULL) {
        json_object_put(json);
    }
    if (url != NULL) {
        free(url);
    }
    curl_slist_free_all(headers);
    return status;
}


// Use the default send_put_bytes, recv_body_bytes, and recv_header_bytes. Only
// set the functions if the associated data is not NULL
static int
simple_set_curl_data(CURL *curl, struct _http_request *header,
                     struct _http_request *body, struct _http_request *put)
{
    int status;
    status = curl_easy_setopt(curl, CURLOPT_HEADERDATA, header);
    if (status != CURLE_OK) { goto finish; }
    if (header == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                                  recv_header_bytes);
    }
    status = curl_easy_setopt(curl, CURLOPT_WRITEDATA, body);
    if (status != CURLE_OK) { goto finish; }
    if (body == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                                  recv_body_bytes);
    }
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_READDATA, put);
    if (status != CURLE_OK) { goto finish; }
    if (put == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_READFUNCTION,
                                  send_put_bytes);
    }
finish:
    return status;
}


static int
st_http_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen, const char *trans_id)
{
    struct _http_state *state;
    struct _http_request req = {0};
    char *url = NULL;
    int status;
    long http_code;
    json_object *json = NULL;
    const char *raw_body = NULL;
    int raw_len;
    struct curl_slist *headers = NULL;

    if (st == NULL || source_replica_uid == NULL || st->implementation == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }

    status = u1db__format_sync_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    json = json_object_new_object();
    if (json == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    json_object_object_add(json, "generation", json_object_new_int(source_gen));
    json_object_object_add(json, "transaction_id",
                           json_object_new_string(trans_id));
    raw_body = json_object_to_json_string(json);
    raw_len = strlen(raw_body);
    req.state = state;
    req.put_buffer = raw_body;
    req.num_put_bytes = raw_len;

    headers = curl_slist_append(headers, "Content-Type: application/json");
    // We know the message is going to be short, no reason to wait for server
    // confirmation of the post.
    headers = curl_slist_append(headers, "Expect:");

    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, headers);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_UPLOAD, 1L);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_PUT, 1L);
    if (status != CURLE_OK) { goto finish; }
    status = simple_set_curl_data(state->curl, &req, &req, &req);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_INFILESIZE_LARGE,
                              (curl_off_t)req.num_put_bytes);
    if (status != CURLE_OK) { goto finish; }
    status = maybe_sign_url(st, "PUT", url, &headers);
    if (status != U1DB_OK) { goto finish; }

    // Now actually send the data
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200 && http_code != 201) {
        status = http_code;
        goto finish;
    }
finish:
    if (req.header_buffer != NULL) {
        free(req.header_buffer);
    }
    if (req.body_buffer != NULL) {
        free(req.body_buffer);
    }
    if (json != NULL) {
        json_object_put(json);
    }
    if (url != NULL) {
        free(url);
    }
    if (headers != NULL) {
        curl_slist_free_all(headers);
    }
    return status;
}


// Setup the CURL handle for doing the POST for sync exchange
// @param headers   (OUT) Pass in a handle for curl_slist, callers must call
//                  curl_slist_free_all themselves
// @param req       The request state will be attached to this object
// @param fd        This handle should have all data written to it. We will use
//                  ftell to determine content length, then seek to the
//                  beginning to do the upload
static int
setup_curl_for_sync(CURL *curl, struct curl_slist **headers,
                    struct _http_request *req, FILE *fd)
{
    int status;
    curl_off_t size;
    *headers = curl_slist_append(*headers,
            "Content-Type: application/x-u1db-sync-stream");
    if (*headers == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    status = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_POST, 1L);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, NULL);
    if (status != CURLE_OK) { goto finish; }

    status = curl_easy_setopt(curl, CURLOPT_HEADERDATA, req);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                              recv_header_bytes);
    status = curl_easy_setopt(curl, CURLOPT_WRITEDATA, req);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                              recv_body_bytes);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_READDATA, fd);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_READFUNCTION, fread);
    if (status != CURLE_OK) { goto finish; }
    size = ftell(fd);
    fseek(fd, 0, 0);
    status = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, size);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, size);
    if (status != CURLE_OK) { goto finish; }
finish:
    return status;
}


static int
doc_to_tempfile(u1db_document *doc, int gen, const char *trans_id, FILE *fd)
{
    int status = U1DB_OK;
    json_object *json = NULL;
    fputs(",\r\n", fd);
    json = json_object_new_object();
    if (json == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    json_object_object_add(json, "id", json_object_new_string(doc->doc_id));
    json_object_object_add(json, "rev", json_object_new_string(doc->doc_rev));
    json_object_object_add(
        json, "content", doc->json?json_object_new_string(doc->json):NULL);
    json_object_object_add(json, "gen", json_object_new_int(gen));
    json_object_object_add(json, "trans_id", json_object_new_string(trans_id));
    fputs(json_object_to_json_string(json), fd);
finish:
    if (json != NULL) {
        json_object_put(json);
    }
    return status;
}

static FILE *
make_tempfile(char tmpname[1024])
{
    const char tmp_template[] = "tmp-u1db-sync-XXXXXX";
    const char *env_temp[] = {"TMP", "TEMP", "TMPDIR"};
    int i, fd;
    FILE *ret;
    const char *tmpdir = NULL;

    for (i = 0; i < sizeof(env_temp); ++i) {
        tmpdir = getenv(env_temp[0]);
        if (tmpdir != NULL && tmpdir[0] != '\0') break;
    } 
    if (tmpdir == NULL || tmpdir[0] == '\0') {
        tmpdir = ".";
    }
    snprintf(tmpname, 1024, "%s/%s", tmpdir, tmp_template);
    fd = mkstemp(tmpname);
    if (fd == -1) {
        return NULL;
    }
    ret = fdopen(fd, "wb+");
    if (ret == NULL) {
        close(fd);
        unlink(tmpname);
        tmpname[0] = '\0';
    }
    return ret;
}


static int
init_temp_file(char tmpname[], FILE **temp_fd, int target_gen)
{
    int status = U1DB_OK;
    *temp_fd = make_tempfile(tmpname);
    if (*temp_fd == NULL) {
        status = errno;
        if (status == 0) {
            status = U1DB_INTERNAL_ERROR;
        }
        goto finish;
    }
    // Spool all of the documents to a temporary file, so that it we can
    // determine Content-Length before we start uploading the data.
    fprintf(*temp_fd, "[\r\n{\"last_known_generation\": %d}", target_gen);
finish:
    return status;
}


static int
finalize_and_send_temp_file(u1db_sync_target *st, FILE *temp_fd,
                            const char *source_replica_uid,
                            struct _http_request *req)
{
    int status;
    long http_code;
    char *url = NULL;
    struct _http_state *state;
    struct curl_slist *headers = NULL;

    fputs("\r\n]", temp_fd);
    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }
    status = u1db__format_sync_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    status = setup_curl_for_sync(state->curl, &headers, req, temp_fd);
    if (status != CURLE_OK) { goto finish; }
    status = maybe_sign_url(st, "POST", url, &headers);
    if (status != U1DB_OK) { goto finish; }
    // Now send off the messages, and handle the returned content.
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200 && http_code != 201) {
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
finish:
    if (url != NULL) {
        free(url);
    }
    if (headers != NULL) {
        curl_slist_free_all(headers);
    }
    return status;
}


static int
process_response(u1db_sync_target *st, void *context, u1db_doc_gen_callback cb,
                 char *response, int *target_gen, char **target_trans_id)
{
    int status = U1DB_OK;
    int i, doc_count;
    json_object *json = NULL, *obj = NULL, *attr = NULL;
    const char *doc_id, *content, *rev;
    const char *tmp = NULL;
    int gen;
    const char *trans_id = NULL;
    u1db_document *doc;

    json = json_tokener_parse(response);
    if (json == NULL || !json_object_is_type(json, json_type_array)) {
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
    doc_count = json_object_array_length(json);
    if (doc_count < 1) {
        // the first response is the new_generation info, so it must exist
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
    obj = json_object_array_get_idx(json, 0);
    attr = json_object_object_get(obj, "new_generation");
    if (attr == NULL) {
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
    *target_gen = json_object_get_int(attr);
    attr = json_object_object_get(obj, "new_transaction_id");
    if (attr == NULL) {
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
    tmp = json_object_get_string(attr);
    if (tmp == NULL) {
        status = U1DB_BROKEN_SYNC_STREAM;
        goto finish;
    }
    *target_trans_id = strdup(tmp);
    if (*target_trans_id == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }

    for (i = 1; i < doc_count; ++i) {
        obj = json_object_array_get_idx(json, i);
        attr = json_object_object_get(obj, "id");
        doc_id = json_object_get_string(attr);
        attr = json_object_object_get(obj, "rev");
        rev = json_object_get_string(attr);
        attr = json_object_object_get(obj, "content");
        content = json_object_get_string(attr);
        attr = json_object_object_get(obj, "gen");
        gen = json_object_get_int(attr);
        attr = json_object_object_get(obj, "trans_id");
        trans_id = json_object_get_string(attr);
        status = u1db__allocate_document(doc_id, rev, content, 0, &doc);
        if (status != U1DB_OK)
            goto finish;
        if (doc == NULL) {
            status = U1DB_NOMEM;
            goto finish;
        }
        status = cb(context, doc, gen, trans_id);
        if (status != U1DB_OK) { goto finish; }
    }
finish:
    if (json != NULL) {
        json_object_put(json);
    }
    return status;
}

static void

cleanup_temp_files(char tmpname[], FILE *temp_fd, struct _http_request *req)
{
    if (temp_fd != NULL) {
        fclose(temp_fd);
    }
    if (req != NULL) {
        if (req->body_buffer != NULL) {
            free(req->body_buffer);
            req->body_buffer = NULL;
        }
        if (req->header_buffer != NULL) {
            free(req->header_buffer);
            req->header_buffer = NULL;
        }
    }
    if (tmpname[0] != '\0') {
        unlink(tmpname);
    }
}

static int
st_http_sync_exchange(u1db_sync_target *st, const char *source_replica_uid,
                      int n_docs, u1db_document **docs, int *generations,
                      const char **trans_ids, int *target_gen,
                      char **target_trans_id, void *context,
                      u1db_doc_gen_callback cb)
{
    int status, i;
    FILE *temp_fd = NULL;
    struct _http_request req = {0};
    char tmpname[1024] = {0};

    if (st == NULL || generations == NULL || target_gen == NULL
            || target_trans_id == NULL || cb == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    if (n_docs > 0 && (docs == NULL || generations == NULL)) {
        return U1DB_INVALID_PARAMETER;
    }
    status = init_temp_file(tmpname, &temp_fd, *target_gen);
    if (status != U1DB_OK) { goto finish; }
    for (i = 0; i < n_docs; ++i) {
        status = doc_to_tempfile(
            docs[i], generations[i], trans_ids[i], temp_fd);
        if (status != U1DB_OK) { goto finish; }
    }
    status = finalize_and_send_temp_file(st, temp_fd, source_replica_uid, &req);
    if (status != U1DB_OK) { goto finish; }
    status = process_response(st, context, cb, req.body_buffer, target_gen,
                              target_trans_id);
finish:
    cleanup_temp_files(tmpname, temp_fd, &req);
    return status;
}


struct _get_doc_to_tempfile_context {
    int offset;
    int num;
    int *generations;
    const char **trans_ids;
    FILE *temp_fd;
};


static int
get_docs_to_tempfile(void *context, u1db_document *doc)
{
    int status = U1DB_OK;
    struct _get_doc_to_tempfile_context *state;

    state = (struct _get_doc_to_tempfile_context *)context;
    if (state->offset >= state->num) {
        status = U1DB_INTERNAL_ERROR;
    } else {
        status = doc_to_tempfile(doc, state->generations[state->offset],
                                 state->trans_ids[state->offset],
                                 state->temp_fd);
    }
    u1db_free_doc(&doc);
    return status;
}


int u1db_get_docs(u1database *db, int n_doc_ids, const char **doc_ids,
                  int check_for_conflicts, int include_deleted, void *context,
                  u1db_doc_callback cb);

static int
st_http_sync_exchange_doc_ids(u1db_sync_target *st, u1database *source_db,
                              int n_doc_ids, const char **doc_ids,
                              int *generations, const char **trans_ids,
                              int *target_gen, char **target_trans_id,
                              void *context, u1db_doc_gen_callback cb)
{
    int status;
    FILE *temp_fd = NULL;
    struct _http_request req = {0};
    char tmpname[1024] = {0};
    const char *source_replica_uid = NULL;
    struct _get_doc_to_tempfile_context state = {0};

    if (st == NULL || generations == NULL || target_gen == NULL
            || target_trans_id == NULL || cb == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    if (n_doc_ids > 0 && (doc_ids == NULL || generations == NULL)) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_get_replica_uid(source_db, &source_replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = init_temp_file(tmpname, &temp_fd, *target_gen);
    if (status != U1DB_OK) { goto finish; }
    state.num = n_doc_ids;
    state.generations = generations;
    state.trans_ids = trans_ids;
    state.temp_fd = temp_fd;
    status = u1db_get_docs(source_db, n_doc_ids, doc_ids, 0, 1,
            &state, get_docs_to_tempfile);
    if (status != U1DB_OK) { goto finish; }
    status = finalize_and_send_temp_file(st, temp_fd, source_replica_uid, &req);
    if (status != U1DB_OK) { goto finish; }
    status = process_response(st, context, cb, req.body_buffer, target_gen,
                              target_trans_id);
finish:
    cleanup_temp_files(tmpname, temp_fd, &req);
    return status;
}


static int
st_http_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange)
{
    // Intentionally not implemented
    return U1DB_NOT_IMPLEMENTED;
}


static void
st_http_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange)
{
    // Intentionally a no-op
}


static int
st_http_set_trace_hook(u1db_sync_target *st, void *context,
                       u1db__trace_callback cb)
{
    // We can't trace a remote database
    return U1DB_NOT_IMPLEMENTED;
}


static void
st_http_finalize(u1db_sync_target *st)
{
    if (st->implementation != NULL) {
        struct _http_state *state;
        state = (struct _http_state *)st->implementation;
        if (state->base_url != NULL) {
            free(state->base_url);
            state->base_url = NULL;
        }
        if (state->replica_uid != NULL) {
            free(state->replica_uid);
            state->replica_uid = NULL;
        }
        if (state->curl != NULL) {
            curl_easy_cleanup(state->curl);
            state->curl = NULL;
        }
        if (state->consumer_key != NULL) {
            free(state->consumer_key);
            state->consumer_key = NULL;
        }
        if (state->consumer_secret != NULL) {
            free(state->consumer_secret);
            state->consumer_secret = NULL;
        }
        if (state->token_key != NULL) {
            free(state->token_key);
            state->token_key = NULL;
        }
        if (state->token_secret != NULL) {
            free(state->token_secret);
            state->token_secret = NULL;
        }
        free(st->implementation);
        st->implementation = NULL;
    }
}


int
u1db__format_sync_url(u1db_sync_target *st,
                      const char *source_replica_uid, char **sync_url)
{
    int status, url_len;
    struct _http_state *state;
    char *tmp;

    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }

    url_len = strlen(state->base_url) + 1;
    url_len += strlen("sync-from/");
    tmp = curl_easy_escape(state->curl, source_replica_uid, 0);
    url_len += strlen(tmp);

    *sync_url = (char *)calloc(url_len+1, sizeof(char));
    snprintf(*sync_url, url_len, "%ssync-from/%s", state->base_url, tmp);
    curl_free(tmp);

    return U1DB_OK;
}


int
u1db__get_oauth_authorization(u1db_sync_target *st,
    const char *http_method, const char *url,
    char **oauth_authorization)
{
    int status = U1DB_OK;
    struct _http_state *state;
    char *oauth_data = NULL;
    char *http_hdr = NULL;
    int argc = 0;
    int hdr_size = 0, oauth_size = 0;
    char **argv = NULL;

    status = impl_as_http_state(st->implementation, &state);
    if (status != U1DB_OK) {
        return status;
    }
    if (state->consumer_key == NULL || state->consumer_secret == NULL
        || state->token_key == NULL || state->token_secret == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    argc = oauth_split_url_parameters(url, &argv);
    oauth_sign_array2_process(&argc, &argv, NULL, OA_HMAC, http_method,
        state->consumer_key, state->consumer_secret,
        state->token_key, state->token_secret);
    oauth_data = oauth_serialize_url_sep(argc, 1, argv, ", ", 6);
    if (oauth_data == NULL) {
        status = U1DB_INTERNAL_ERROR;
        goto finish;
    }
    oauth_size = strlen(oauth_data);
    // sizeof(auth_header_prefix) includes the trailing null, so we don't
    // need to add 1
    hdr_size = sizeof(auth_header_prefix) + oauth_size;
    http_hdr = (char *)calloc(hdr_size, 1);
    if (http_hdr == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    memcpy(http_hdr, auth_header_prefix, sizeof(auth_header_prefix));
    memcpy(http_hdr + sizeof(auth_header_prefix)-1, oauth_data, oauth_size);
finish:
    if (oauth_data != NULL) {
        free(oauth_data);
    }
    oauth_free_array(&argc, &argv);
    if (status == U1DB_OK) {
        *oauth_authorization = http_hdr;
    } else if (http_hdr != NULL) {
        free(http_hdr);
    }
    return status;
}
