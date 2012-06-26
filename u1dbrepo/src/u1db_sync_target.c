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

#include "u1db/u1db_internal.h"
#include <string.h>
#include <json/linkhash.h>


static int st_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen,
        char **source_trans_id);

static int st_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen, const char *trans_id);

static int st_sync_exchange(u1db_sync_target *st,
                          const char *source_replica_uid, int n_docs,
                          u1db_document **docs, int *generations,
                          const char **trans_ids, int *target_gen,
                          char **target_trans_id, void *context,
                          u1db_doc_gen_callback cb);
static int st_sync_exchange_doc_ids(u1db_sync_target *st,
                                    u1database *source_db, int n_doc_ids,
                                    const char **doc_ids, int *generations,
                                    const char **trans_ids, int *target_gen,
                                    char **target_trans_id, void *context,
                                    u1db_doc_gen_callback cb);
static int st_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange);

static void st_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange);
static int st_set_trace_hook(u1db_sync_target *st,
                             void *context, u1db__trace_callback cb);
static void st_finalize(u1db_sync_target *st);
static void se_free_seen_id(struct lh_entry *e);


struct _get_docs_to_doc_gen_context {
    int doc_offset;
    void *user_context;
    u1db_doc_gen_callback user_cb;
    int *gen_for_doc_ids;
    const char **trans_ids_for_doc_ids;
    int free_when_done;
};

// A wrapper to change a 'u1db_doc_callback' into a 'u1db_doc_gen_callback'.
static int get_docs_to_gen_docs(void *context, u1db_document *doc);

int
u1db__get_sync_target(u1database *db, u1db_sync_target **sync_target)
{
    int status = U1DB_OK;

    if (db == NULL || sync_target == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    *sync_target = (u1db_sync_target *)calloc(1, sizeof(u1db_sync_target));
    if (*sync_target == NULL) {
        return U1DB_NOMEM;
    }
    (*sync_target)->implementation = db;
    (*sync_target)->get_sync_info = st_get_sync_info;
    (*sync_target)->record_sync_info = st_record_sync_info;
    (*sync_target)->sync_exchange = st_sync_exchange;
    (*sync_target)->sync_exchange_doc_ids = st_sync_exchange_doc_ids;
    (*sync_target)->get_sync_exchange = st_get_sync_exchange;
    (*sync_target)->finalize_sync_exchange = st_finalize_sync_exchange;
    (*sync_target)->_set_trace_hook = st_set_trace_hook;
    (*sync_target)->finalize = st_finalize;
    return status;
}


void
u1db__free_sync_target(u1db_sync_target **sync_target)
{
    if (sync_target == NULL || *sync_target == NULL) {
        return;
    }
    (*sync_target)->finalize(*sync_target);
    free(*sync_target);
    *sync_target = NULL;
}


static int
st_get_sync_info(u1db_sync_target *st, const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen,
        char **source_trans_id)
{
    int status = U1DB_OK;
    u1database *db;
    if (st == NULL || source_replica_uid == NULL || st_replica_uid == NULL
            || st_gen == NULL || source_gen == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    // TODO: This really feels like it should be done inside some sort of
    //       transaction, so that the sync information is consistent with the
    //       current db generation. (at local generation X we are synchronized
    //       with remote generation Y.)
    //       At the very least, though, we check the sync generation *first*,
    //       so that we should only be getting the same data again, if for some
    //       reason we are currently synchronizing with the remote object.
    db = (u1database *)st->implementation;
    status = u1db_get_replica_uid(db, st_replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__get_sync_gen_info(
        db, source_replica_uid, source_gen, source_trans_id);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__get_generation(db, st_gen);
finish:
    return status;
}


static int
st_record_sync_info(u1db_sync_target *st, const char *source_replica_uid,
                    int source_gen, const char *trans_id)
{
    int status;
    u1database *db;
    if (st == NULL || source_replica_uid == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (st->trace_cb) {
        status = st->trace_cb(st->trace_context, "record_sync_info");
        if (status != U1DB_OK) { goto finish; }
    }
    db = (u1database *)st->implementation;
    status = u1db__set_sync_info(db, source_replica_uid, source_gen, trans_id);
finish:
    return status;
}


static int
st_get_sync_exchange(u1db_sync_target *st, const char *source_replica_uid,
                     int target_gen_known_by_source,
                     u1db_sync_exchange **exchange)
{
    u1db_sync_exchange *tmp;
    if (st == NULL || source_replica_uid == NULL || exchange == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    tmp = (u1db_sync_exchange *)calloc(1, sizeof(u1db_sync_exchange));
    if (tmp == NULL) {
        return U1DB_NOMEM;
    }
    tmp->db = (u1database *)st->implementation;
    tmp->source_replica_uid = source_replica_uid;
    tmp->target_gen = target_gen_known_by_source;
    // Note: lh_table is overkill for what we need. We only need a set, not a
    //       mapping, and we don't need the prev/next pointers. But it is
    //       already available, and doesn't require us to implement and debug
    //       another set() implementation.
    tmp->seen_ids = lh_kchar_table_new(100, "seen_ids",
            se_free_seen_id);
    tmp->trace_context = st->trace_context;
    tmp->trace_cb = st->trace_cb;
    *exchange = tmp;
    return U1DB_OK;
}


static void
st_finalize_sync_exchange(u1db_sync_target *st, u1db_sync_exchange **exchange)
{
    int i;
    if (exchange == NULL || *exchange == NULL) {
        return;
    }
    if ((*exchange)->seen_ids != NULL) {
        lh_table_free((*exchange)->seen_ids);
        (*exchange)->seen_ids = NULL;
    }
    if ((*exchange)->doc_ids_to_return != NULL) {
        for (i = 0; i < (*exchange)->num_doc_ids; ++i) {
            free((*exchange)->doc_ids_to_return[i]);
        }
        free((*exchange)->doc_ids_to_return);
        (*exchange)->doc_ids_to_return = NULL;
        (*exchange)->num_doc_ids = 0;
    }
    if ((*exchange)->gen_for_doc_ids != NULL) {
        free((*exchange)->gen_for_doc_ids);
        (*exchange)->gen_for_doc_ids = NULL;
    }
    if ((*exchange)->trans_ids_for_doc_ids != NULL) {
        free((*exchange)->trans_ids_for_doc_ids);
        (*exchange)->trans_ids_for_doc_ids = NULL;
    }
    if ((*exchange)->target_trans_id != NULL) {
        free((*exchange)->target_trans_id);
        (*exchange)->target_trans_id = NULL;
    }
    free(*exchange);
    *exchange = NULL;
}


static int
st_set_trace_hook(u1db_sync_target *st, void *context, u1db__trace_callback cb)
{
    st->trace_context = context;
    st->trace_cb = cb;
    return U1DB_OK;
}


static void
st_finalize(u1db_sync_target *st)
{
    return;
}


static void
se_free_seen_id(struct lh_entry *e)
{
    if (e == NULL) {
        return;
    }
    if (e->k != NULL) {
        free((void *)e->k);
        e->k = NULL;
    }
    /* v is a (void*)int */
}


int
u1db__sync_exchange_seen_ids(u1db_sync_exchange *se, int *n_ids,
                             const char ***doc_ids)
{
    int i;
    struct lh_entry *entry;
    if (se == NULL || n_ids == NULL || doc_ids == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (se->seen_ids == NULL || se->seen_ids->count == 0) {
        *n_ids = 0;
        *doc_ids = NULL;
        return U1DB_OK;
    }
    *n_ids = se->seen_ids->count;
    (*doc_ids) = (const char **)calloc(*n_ids, sizeof(char *));
    i = 0;
    lh_foreach(se->seen_ids, entry) {
        if (entry->k != NULL) {
            if (i >= (*n_ids)) {
                // TODO: Better error? For some reason we found more than
                //       'count' valid entries
                return U1DB_INVALID_PARAMETER;
            }
            (*doc_ids)[i] = entry->k;
            i++;
        }
    }
    return U1DB_OK;
}

int
u1db__sync_exchange_insert_doc_from_source(u1db_sync_exchange *se,
        u1db_document *doc, int source_gen, const char *trans_id)
{
    int status = U1DB_OK;
    int insert_state;
    int at_gen;
    if (se == NULL || se->db == NULL || doc == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    // fprintf(stderr, "Inserting %s from source\n", doc->doc_id);
    status = u1db__put_doc_if_newer(se->db, doc, 0, se->source_replica_uid,
                                    source_gen, trans_id, &insert_state,
                                    &at_gen);
    if (insert_state == U1DB_INSERTED || insert_state == U1DB_CONVERGED) {
        lh_table_insert(se->seen_ids, strdup(doc->doc_id),
        (void *)(intptr_t)at_gen);
    } else {
        // state should be either U1DB_SUPERSEDED or U1DB_CONFLICTED, in either
        // case, we don't count this as a 'seen_id' because we will want to be
        // returning a document with this identifier back to the user.
        // fprintf(stderr, "Not inserting %s, %d\n", doc->doc_id, insert_state);
    }
    return status;
}


struct _whats_changed_doc_ids_state {
    int num_doc_ids;
    int max_doc_ids;
    struct lh_table *exclude_ids;
    char **doc_ids_to_return;
    int *gen_for_doc_ids;
    const char **trans_ids_for_doc_ids;
};

// Callback for whats_changed to map the callback into the sync_exchange
// doc_ids_to_return array.
static int
whats_changed_to_doc_ids(void *context, const char *doc_id, int gen,
                         const char *trans_id)
{
    struct lh_entry *e;
    struct _whats_changed_doc_ids_state *state;
    state = (struct _whats_changed_doc_ids_state *)context;
    if (state->exclude_ids != NULL
        && (e = lh_table_lookup_entry(state->exclude_ids, doc_id)) != NULL
        && (intptr_t)e->v >= gen)
    {
        // This document was already seen at this gen,
        // so we don't need to return it
        return 0;
    }
    if (state->num_doc_ids >= state->max_doc_ids) {
        state->max_doc_ids = (state->max_doc_ids * 2) + 10;
        if (state->doc_ids_to_return == NULL) {
            state->doc_ids_to_return = (char **)calloc(
                state->max_doc_ids, sizeof(char*));
            state->gen_for_doc_ids = (int *)calloc(
                state->max_doc_ids, sizeof(int));
            state->trans_ids_for_doc_ids = (const char **)calloc(
                state->max_doc_ids, sizeof(char*));
        } else {
            state->doc_ids_to_return = (char **)realloc(
                state->doc_ids_to_return, state->max_doc_ids * sizeof(char*));
            state->gen_for_doc_ids = (int *)realloc(
                state->gen_for_doc_ids, state->max_doc_ids * sizeof(int));
            state->trans_ids_for_doc_ids = (const char **)realloc(
                state->gen_for_doc_ids, state->max_doc_ids * sizeof(char*));
        }
        if (state->doc_ids_to_return == NULL || state->gen_for_doc_ids == NULL
                || state->trans_ids_for_doc_ids == NULL)
        {
            return U1DB_NOMEM;
        }
    }
    state->doc_ids_to_return[state->num_doc_ids] = strdup(doc_id);
    state->gen_for_doc_ids[state->num_doc_ids] = gen;
    state->trans_ids_for_doc_ids[state->num_doc_ids] = trans_id;
    state->num_doc_ids++;
    return 0;
}


int
u1db__sync_exchange_find_doc_ids_to_return(u1db_sync_exchange *se)
{
    int status;
    struct _whats_changed_doc_ids_state state = {0};
    char *target_trans_id = NULL;
    if (se == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "before whats_changed");
        if (status != U1DB_OK) { goto finish; }
    }
    state.exclude_ids = se->seen_ids;
    status = u1db_whats_changed(se->db, &se->target_gen, &se->target_trans_id,
            (void*)&state, whats_changed_to_doc_ids);
    if (status != U1DB_OK) {
        free(state.doc_ids_to_return);
        free(state.gen_for_doc_ids);
        free(state.trans_ids_for_doc_ids);
        goto finish;
    }
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "after whats_changed");
        if (status != U1DB_OK) { goto finish; }
    }
    se->num_doc_ids = state.num_doc_ids;
    se->doc_ids_to_return = state.doc_ids_to_return;
    se->gen_for_doc_ids = state.gen_for_doc_ids;
    se->trans_ids_for_doc_ids = state.trans_ids_for_doc_ids;
finish:
    if (target_trans_id != NULL) {
        free(target_trans_id);
    }
    return status;
}


// A wrapper to change a 'u1db_doc_callback' into a 'u1db_doc_gen_callback'.
static int
get_docs_to_gen_docs(void *context, u1db_document *doc)
{
    struct _get_docs_to_doc_gen_context *ctx;
    int status;
    ctx = (struct _get_docs_to_doc_gen_context *)context;
    // Note: using doc_offset in this way assumes that u1db_get_docs will
    //       always return them in exactly the order we requested. This is
    //       probably true, though.
    status = ctx->user_cb(
        ctx->user_context, doc, ctx->gen_for_doc_ids[ctx->doc_offset],
        ctx->trans_ids_for_doc_ids[ctx->doc_offset]);
    ctx->doc_offset++;
    if (ctx->free_when_done) {
        u1db_free_doc(&doc);
    }
    return status;
}


int
u1db__sync_exchange_return_docs(u1db_sync_exchange *se, void *context,
                                int (*cb)(void *context, u1db_document *doc,
                                          int gen, const char *trans_id))
{
    int status = U1DB_OK;
    struct _get_docs_to_doc_gen_context state = {0};
    if (se == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    state.user_context = context;
    state.user_cb = cb;
    state.doc_offset = 0;
    state.gen_for_doc_ids = se->gen_for_doc_ids;
    state.trans_ids_for_doc_ids = se->trans_ids_for_doc_ids;
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "before get_docs");
        if (status != U1DB_OK) { goto finish; }
    }
    if (se->num_doc_ids > 0) {
        status = u1db_get_docs(se->db, se->num_doc_ids,
                (const char **)se->doc_ids_to_return,
                0, 1, &state, get_docs_to_gen_docs);
    }
finish:
    return status;
}

struct _return_doc_state {
    u1database *db;
    const char *target_uid;
    int num_inserted;
};

static int
return_doc_to_insert_from_target(void *context, u1db_document *doc, int gen,
                                 const char *trans_id)
{
    int status, insert_state;
    struct _return_doc_state *state;
    state = (struct _return_doc_state *)context;

    status = u1db__put_doc_if_newer(
        state->db, doc, 1, state->target_uid, gen, trans_id, &insert_state,
        NULL);
    u1db_free_doc(&doc);
    if (status == U1DB_OK) {
        if (insert_state == U1DB_INSERTED || insert_state == U1DB_CONFLICTED) {
            // Either it was directly inserted, or it was saved as a conflict
            state->num_inserted++;
        }
    } else {
    }
    return status;
}


static int
get_and_insert_docs(u1database *source_db, u1db_sync_exchange *se,
                    int n_doc_ids, const char **doc_ids, int *generations,
                    const char **trans_ids)
{
    struct _get_docs_to_doc_gen_context get_doc_state = {0};

    get_doc_state.free_when_done = 1;
    get_doc_state.user_context = se;
    // Note: user_cb takes a 'void *' as the first parameter, so we cast the
    //       u1db__sync_exchange_insert_doc_from_source to avoid the warning
    get_doc_state.user_cb =
        (u1db_doc_gen_callback)u1db__sync_exchange_insert_doc_from_source;
    get_doc_state.gen_for_doc_ids = generations;
    get_doc_state.trans_ids_for_doc_ids = trans_ids;
    return u1db_get_docs(source_db, n_doc_ids, doc_ids,
            0, 1, &get_doc_state, get_docs_to_gen_docs);
}


static int
st_sync_exchange(u1db_sync_target *st, const char *source_replica_uid,
                 int n_docs, u1db_document **docs, int *generations,
                 const char **trans_ids, int *target_gen,
                 char **target_trans_id, void *context,
                 u1db_doc_gen_callback cb)
{
    int status, i;

    u1db_sync_exchange *exchange = NULL;
    if (st == NULL || generations == NULL || target_gen == NULL
            || target_trans_id == NULL || cb == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    if (n_docs > 0 && (docs == NULL || generations == NULL)) {
        return U1DB_INVALID_PARAMETER;
    }
    status = st->get_sync_exchange(st, source_replica_uid,
                                   *target_gen, &exchange);
    if (status != U1DB_OK) { goto finish; }
    for (i = 0; i < n_docs; ++i) {
        status = u1db__sync_exchange_insert_doc_from_source(
            exchange, docs[i], generations[i], trans_ids[i]);
        if (status != U1DB_OK) { goto finish; }
    }
    status = u1db__sync_exchange_find_doc_ids_to_return(exchange);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__sync_exchange_return_docs(exchange, context, cb);
    if (status != U1DB_OK) { goto finish; }
    if (status == U1DB_OK) {
        *target_gen = exchange->target_gen;
        *target_trans_id = exchange->target_trans_id;
        // We set this to NULL, because the caller is now responsible for it
        exchange->target_trans_id = NULL;
    }
finish:
    st->finalize_sync_exchange(st, &exchange);
    return status;
}


static int
st_sync_exchange_doc_ids(u1db_sync_target *st, u1database *source_db,
        int n_doc_ids, const char **doc_ids, int *generations,
        const char **trans_ids, int *target_gen, char **target_trans_id,
        void *context, u1db_doc_gen_callback cb)
{
    int status;
    const char *source_replica_uid = NULL;
    u1db_sync_exchange *exchange = NULL;
    if (st == NULL || source_db == NULL || target_gen == NULL
        || target_trans_id == NULL || cb == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    if (n_doc_ids > 0 && (doc_ids == NULL || generations == NULL)) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_get_replica_uid(source_db, &source_replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = st->get_sync_exchange(st, source_replica_uid,
                                   *target_gen, &exchange);
    if (status != U1DB_OK) { goto finish; }
    if (n_doc_ids > 0) {
        status = get_and_insert_docs(source_db, exchange,
            n_doc_ids, doc_ids, generations, trans_ids);
        if (status != U1DB_OK) { goto finish; }
    }
    status = u1db__sync_exchange_find_doc_ids_to_return(exchange);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__sync_exchange_return_docs(exchange, context, cb);
    if (status != U1DB_OK) { goto finish; }
    *target_gen = exchange->target_gen;
    if (status == U1DB_OK) {
        *target_gen = exchange->target_gen;
        *target_trans_id = exchange->target_trans_id;
        // We set this to NULL, because the caller is now responsible for it
        exchange->target_trans_id = NULL;
    }
finish:
    st->finalize_sync_exchange(st, &exchange);
    return status;
}


int
u1db__sync_db_to_target(u1database *db, u1db_sync_target *target,
                        int *local_gen_before_sync)
{
    int status;
    struct _whats_changed_doc_ids_state to_send_state = {0};
    struct _return_doc_state return_doc_state = {0};
    const char *target_uid, *local_uid;
    char *local_trans_id = NULL;
    char *local_target_trans_id = NULL;
    char *target_trans_id_known_by_local = NULL;
    char *local_trans_id_known_by_target = NULL;
    int target_gen, local_gen;
    int local_gen_known_by_target, target_gen_known_by_local;

    // fprintf(stderr, "Starting\n");
    if (db == NULL || target == NULL || local_gen_before_sync == NULL) {
        // fprintf(stderr, "DB, target, or local are NULL\n");
        status = U1DB_INVALID_PARAMETER;
        goto finish;
    }

    status = u1db_get_replica_uid(db, &local_uid);
    if (status != U1DB_OK) { goto finish; }
    // fprintf(stderr, "Local uid: %s\n", local_uid);
    status = target->get_sync_info(target, local_uid, &target_uid, &target_gen,
                   &local_gen_known_by_target, &local_trans_id_known_by_target);
    if (status != U1DB_OK) { goto finish; }
    status = u1db_validate_gen_and_trans_id(
            db, local_gen_known_by_target, local_trans_id_known_by_target);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__get_sync_gen_info(db, target_uid,
        &target_gen_known_by_local, &target_trans_id_known_by_local);
    if (status != U1DB_OK) { goto finish; }
    local_target_trans_id = target_trans_id_known_by_local;
    local_gen = local_gen_known_by_target;

    // Before we start the sync exchange, get the list of doc_ids that we want
    // to send. We have to do this first, so that local_gen_before_sync will
    // match exactly the list of doc_ids we send
    status = u1db_whats_changed(
        db, &local_gen, &local_trans_id, (void*)&to_send_state,
        whats_changed_to_doc_ids);
    if (status != U1DB_OK) { goto finish; }
    if (local_gen == local_gen_known_by_target
        && target_gen == target_gen_known_by_local)
    {
        // We know status == U1DB_OK, and we can shortcut the rest of the
        // logic, no need to look for more information.
        goto finish;
    }
    *local_gen_before_sync = local_gen;
    return_doc_state.db = db;
    return_doc_state.target_uid = target_uid;
    return_doc_state.num_inserted = 0;
    status = target->sync_exchange_doc_ids(target, db,
        to_send_state.num_doc_ids,
        (const char**)to_send_state.doc_ids_to_return,
        to_send_state.gen_for_doc_ids, to_send_state.trans_ids_for_doc_ids,
        &target_gen_known_by_local, &target_trans_id_known_by_local,
        &return_doc_state, return_doc_to_insert_from_target);
    if (status != U1DB_OK) { goto finish; }
    if (local_trans_id != NULL) {
        free(local_trans_id);
    }
    status = u1db__get_generation_info(db, &local_gen, &local_trans_id);
    if (status != U1DB_OK) { goto finish; }
    // Now we successfully sent and received docs, make sure we record the
    // current remote generation
    status = u1db__set_sync_info(
        db, target_uid, target_gen_known_by_local,
        target_trans_id_known_by_local);
    if (status != U1DB_OK) { goto finish; }
    if (return_doc_state.num_inserted > 0 &&
        ((*local_gen_before_sync + return_doc_state.num_inserted)
         == local_gen))
    {
        status = target->record_sync_info(
            target, local_uid, local_gen, local_trans_id);
        if (status != U1DB_OK) { goto finish; }
    }
finish:
    if (local_trans_id != NULL) {
        free(local_trans_id);
    }
    if (local_trans_id_known_by_target != NULL) {
        free(local_trans_id_known_by_target);
    }
    if (local_target_trans_id != NULL) {
        if (target_trans_id_known_by_local == local_target_trans_id) {
            // Don't double free
            target_trans_id_known_by_local = NULL;
        }
        free(local_target_trans_id);
        local_target_trans_id = NULL;
    }
    if (target_trans_id_known_by_local != NULL) {
        free(target_trans_id_known_by_local);
        target_trans_id_known_by_local = NULL;
    }
    if (to_send_state.doc_ids_to_return != NULL) {
        int i;

        for (i = 0; i < to_send_state.num_doc_ids; ++i) {
            free(to_send_state.doc_ids_to_return[i]);
        }
        free(to_send_state.doc_ids_to_return);
    }
    if (to_send_state.gen_for_doc_ids != NULL) {
        free(to_send_state.gen_for_doc_ids);
    }
    if (to_send_state.trans_ids_for_doc_ids != NULL) {
        free(to_send_state.trans_ids_for_doc_ids);
    }
    return status;
}
