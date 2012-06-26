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

#ifndef U1DB_INTERNAL_H
#define U1DB_INTERNAL_H

#include <stdarg.h>
#include "u1db/u1db.h"
#include "u1db/compat.h"
#include "u1db/u1db_vectorclock.h"

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;

struct _u1database
{
    sqlite3 *sql_handle;
    char *replica_uid;
};

struct _u1query {
    const char *index_name;
    int num_fields;
    char **fields;
};

typedef struct _u1db_sync_exchange u1db_sync_exchange;

typedef struct _u1db_sync_target u1db_sync_target;

typedef int (*u1db__trace_callback)(void *context, const char *state);

struct _u1db_sync_target {
    void *trace_context;
    u1db__trace_callback trace_cb;
    void *implementation;

    /**
     * Get the information for synchronization about another replica.
     *
     * @param st    Pass this sync_target to the function,
     *              eg st->get_sync_info(st, ...)
     * @param source_replica_uid    The unique identifier for the source we
     *                              want to synchronize from.
     * @param st_replica_uid    (OUT) The replica uid for the database this
     *                          SyncTarget is attached to.
     *                          Note that this is const char and memory will be
     *                          managed by the sync_target, so it should *not*
     *                          be freed.
     * @param st_get            (OUT) The database generation for this sync
     *                          target, matches st_replica_uid
     * @param source_gen        (OUT) The last generation of source_replica_uid
     *                          that st has synchronized with.
     * @param trans_id          (OUT) The transaction id associated with the
     *                          source generation, the memory must be freed by
     *                          the caller.
     */
    int (*get_sync_info)(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen,
        char **trans_id);
    /**
     * Set the synchronization information about another replica.
     *
     * @param st    Pass this sync_target to the function,
     *              eg st->get_sync_info(st, ...)
     * @param source_replica_uid    The unique identifier for the source we
     *                              want to synchronize from.
     * @param source_gen        The last generation of source_replica_uid
     *                          that st has synchronized with.
     * @param trans_id          The transaction id associated with source_gen
     */
    int (*record_sync_info)(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen, const char *trans_id);

    /**
     * Send documents to the target, and receive the response.
     *
     * @param st        The target to sync with
     * @param source_db The database we will get docs from
     * @param n_doc_ids The number of document ids and generations in the
     *                  following arrays.
     * @param doc_ids   An array of document ids indicating the documents we
     *                  want to send to the sync target
     * @param generations   An array of generations. Each generation
     *                      corresponds to a doc_id.
     * @param trans_ids   An array of transaction ids. Each transaction id
     *                      corresponds to a doc_id.
     * @param target_gen    (IN/OUT) Seed this with the generation of the
     *                      target that source_db has last seen, it will then
     *                      be filled with the final generation of the target
     *                      database from the returned document stream.
     * @param target_trans_id (IN/OUT) This is associated with the generation
     *                        of the target that the source has seen. Will be
     *                        filled in with the final generation of the
     *                        target. Note the value passed in and the value
     *                        returned should be freed by the caller.
     * @param context   Passed to cb.
     * @param cb        After sending the requested documents, we read the
     *                  response stream. For each document in the stream, we
     *                  will trigger a callback.
     */
    int (*sync_exchange_doc_ids)(u1db_sync_target *st, u1database *source_db,
            int n_doc_ids, const char **doc_ids, int *generations,
            const char **trans_ids, int *target_gen, char **target_trans_id,
            void *context, u1db_doc_gen_callback cb);

    /**
     * Same as sync_exchange, only using document objects.
     */
    int (*sync_exchange)(u1db_sync_target *st,
                         const char *source_replica_uid, int n_docs,
                         u1db_document **docs, int *generations,
                         const char **trans_ids, int *target_gen,
                         char **target_trans_id,
                         void *context, u1db_doc_gen_callback cb);
    /**
     * Create a sync_exchange state object.
     *
     * This encapsulates the state during a single document exchange.
     * The returned u1db_sync_exchange object should be freed with
     * finalize_sync_exchange.
     */
    int (*get_sync_exchange)(u1db_sync_target *st,
                             const char *source_replica_uid,
                             int target_gen_known_by_source,
                             u1db_sync_exchange **exchange);

    void (*finalize_sync_exchange)(u1db_sync_target *st,
                                   u1db_sync_exchange **exchange);
    /**
     * Set a trace hook.
     *
     * At various points during sync, the callback will be invoked indicating
     * the current state of the sync. This is used during the test suite to
     * allow concurrency, etc, testing.
     *
     * @param context   Will be passed to the callback along with the current
     *                  state.
     * @param cb        If the callback returns anything other that U1DB_OK, it
     *                  will be considered an error, and the sync functionality
     *                  will return.
     * @return U1DB_OK if the hook was set, might return U1DB_NOT_IMPLEMENTED
     */
    int (*_set_trace_hook)(u1db_sync_target *st,
                           void *context, u1db__trace_callback cb);
    /**
     * The sync target is being cleaned up. Release resources.
     */
    void (*finalize)(u1db_sync_target *st);
};


struct _u1db_sync_exchange {
    u1database *db;
    const char *source_replica_uid;
    int target_gen;
    char *target_trans_id;
    struct lh_table *seen_ids;
    int num_doc_ids;
    int *gen_for_doc_ids;
    const char **trans_ids_for_doc_ids;
    char **doc_ids_to_return;
    void *trace_context;
    u1db__trace_callback trace_cb;
};

/**
 * Update content if the revision is newer than what is already present.
 *
 * @param doc (IN/OUT) The document we want added to the database. If
 *            save_conflict is true and this conflicts, we will set
 *            doc->has_conflicts
 * @param save_conflict If 1, when a document would conflict, it is saved as
 *                      the current version and marked as a conflict.
 *                      Otherwise the document is just rejected as not newer.
 * @param replica_uid Used during synchronization to indicate what replica
 *                    this document came from. (Can be NULL)
 * @param replica_gen Generation of the replica. Only meaningful if
 *                    replica_uid is set.
 * @param replica_trans_id Transaction id associated with generation.
 * @param state (OUT) Return one of:
 *  U1DB_INSERTED   The document is newer than what we have
 *  U1DB_SUPERSEDED We already have a newer document than what was passed
 *  U1DB_CONVERGED  We have exactly the same document
 *  U1DB_CONFLICTED Neither document is strictly newer than the other. If
 *                  save_conflict is false, then we will ignore the document.
 * @param at_gen (OUT) For INSERTED or CONVERGED states used to return
 *                     the insertion/current generation. Ignored if NULL.
 */
int u1db__put_doc_if_newer(u1database *db, u1db_document *doc,
                           int save_conflict, const char *replica_uid,
                           int replica_gen, const char *replica_trans_id,
                           int *state, int *at_gen);

/**
 * Validate source generation and transaction id.
 *
 * @param replica_uid uid of source replica at the time of the
 *     change we are syncing.
 * @param replica_gen Generation of the replica at the time of the
 *     change we are syncing.
 * @param replica_trans_id Transaction id of the replica at the time of the
 *     change we are syncing.
 * @param cur_vcr Vector clock for the document in the database.
 * @param other_vcr Vector clock of the document being put.
 * @param state (OUT) 0 for success, U1DB_SUPERSEDED if the document is
 *     superseded.
 */
int u1db__validate_source(u1database *db, const char *replica_uid,
                          int replica_gen, const char *replica_trans_id,
                          u1db_vectorclock *cur_vcr,
                          u1db_vectorclock *other_vcr, int *state);

/**
 * Internal API, Get the global database rev.
 */
int u1db__get_generation(u1database *db, int *generation);

/**
 * Internal API, Get the global database rev and transaction id.
 */
int u1db__get_generation_info(u1database *db, int *generation,
                              char **trans_id);

/**
 * Internal API, Validate generation and transaction id.
 */
int u1db_validate_gen_and_trans_id(u1database *db, int generation,
                                   const char *trans_id);
/**
 * Internal API, Allocate a new document id, for cases when callers do not
 * supply their own. Callers of this API are expected to free the result.
 */
char *u1db__allocate_doc_id(u1database *db);

/**
 * Internal api, close the underlying sql instance.
 */
int u1db__sql_close(u1database *db);

/**
 * Internal api, check to see if the underlying SQLite handle has been closed.
 */
int u1db__sql_is_open(u1database *db);

/**
 * Check if a doc_id is valid.
 *
 * Returns U1DB_OK if everything is ok, otherwise U1DB_INVALID_DOC_ID.
 */
int u1db__is_doc_id_valid(const char *doc_id);

/**
 * Internal api, run an SQL query directly.
 */
typedef struct _u1db_row {
    struct _u1db_row *next;
    int num_columns;
    int *column_sizes;
    unsigned char **columns;
} u1db_row;

typedef struct _u1db_table {
    int status;
    u1db_row *first_row;
} u1db_table;

u1db_table *u1db__sql_run(u1database *db, const char *sql, size_t n);
void u1db__free_table(u1db_table **table);


/**
 * Get the list of everything that has changed that we've recorded.
 */
int u1db__get_transaction_log(u1database *db, void *context,
                              u1db_trans_info_callback cb);

/**
 * Get the known generation we synchronized with another implementation.
 *
 * @param replica_uid The identifier for the other database
 * @param generation  (OUT) The last generation that we know we synchronized
 *                    with the other database.
 * @param trans_id    (OUT) The transaction id associated with the generation.
 *                    Callers must free the data.
 */
int u1db__get_sync_gen_info(u1database *db, const char *replica_uid,
                            int *generation, char **trans_id);

/**
 * Set the known sync generation for another replica.
 *
 */
int u1db__set_sync_info(u1database *db, const char *replica_uid,
                        int generation, const char *trans_id);

/**
 * Internal sync api, get the stored information about another machine.
 */
int u1db__sync_get_machine_info(u1database *db, const char *other_replica_uid,
                            int *other_db_rev, char **my_replica_uid,
                            int *my_db_rev);

/**
 * Internal sync api, store information about another machine.
 */
int u1db__sync_record_machine_info(u1database *db, const char *replica_uid,
                                   int db_rev);

const char **u1db__schema;
const int u1db__schema_len;

typedef struct _u1db_record {
    struct _u1db_record *next;
    char *doc_id;
    char *doc_rev;
    char *doc;
} u1db_record;

/**
 * Internal sync api, exchange sync records.
 */
int u1db__sync_exchange(u1database *db, const char *from_replica_uid,
                        int from_db_rev, int last_known_rev,
                        u1db_record *from_records, u1db_record **new_records,
                        u1db_record **conflict_records);

/**
 * Allocate a new u1db_record, and copy all records over.
 */
u1db_record *u1db__create_record(const char *doc_id, const char *doc_rev,
                                 const char *doc);

u1db_record *u1db__copy_record(u1db_record *src);

/**
 * Free a linked list of records. All linked records will be freed, including
 * all memory referenced from them.
 */
void u1db__free_records(u1db_record **record);

/**
 * Create a new u1db_document object. This should be freed
 */
int u1db__allocate_document(const char *doc_id, const char *revision,
                            const char *content, int has_conflicts,
                            u1db_document **result);

/**
 * Generate a unique id.
 *
 * @param uuid A buffer to put the id, must be 32 bytes long.
 */
int u1db__generate_hex_uuid(char *uuid);


/**
 * Format a given query.
 *
 * @param n_fields The number of fields being passed in. (The number of values
 *     in values)
 * @param values Array of values being passed.
 * @param buf (OUT) The character array. This will be dynamically allocated,
 *     and callers must free() it.
 * @param wildcard (IN/OUT) Any array indicating a wildcard type for this
 *     argument. A 0 indicates this is an exact match, a 1
 *     indicates this is a pure wildcard (eg, "*") and a 2
 *     indicates this is a glob (eg "f*").
 *     This must point to an array at least n_fields wide.
 */
int u1db__format_query(int n_fields, const char **values, char **buf,
                       int *wildcard);

/**
 * Format a given range query.
 *
 * @param n_fields The number of fields being passed in. (The number of values
 *     in start_values and end_values)
 * @param start_values Array of values used to define the lower bound of the
 *     query.
 * @param end_values Array of values used to define the upper bound of the
 *     query.
 * @param buf (OUT) The character array. This will be dynamically allocated,
 *     and callers must free() it.
 */
int u1db__format_range_query(int n_fields, const char **start_values,
                             const char **end_values, char **buf,
                             int *start_wildcard, int *end_wildcard);

/**
 * Format an index keys query
 *
 * @param n_fields  The number of fields being passed in, (the number of args
 *     in argp)
 * @param buf (OUT) The character array. This will be dynamically allocated,
 *     and callers must free() it.
 */
int u1db__format_index_keys_query(int n_fields, char **buf);

/**
 * Given this document content, update the indexed fields in the db.
 */
int u1db__update_indexes(u1database *db, const char *doc_id,
                         const char *content);

/**
 * Find what expressions do not already exist in the database.
 *
 * @param n_unique (OUT) The number of unique expressions found
 * @param unique_expressions (OUT) An array holding pointers to the strings in
 *                       expressions, must be freed by the caller if there
 *                       isn't an error.
 */
int u1db__find_unique_expressions(u1database *db,
                              int n_expressions, const char **expressions,
                              int *n_unique, const char ***unique_expressions);

/**
 * Add the given field expressions to be indexed.
 *
 * This will iterate over all documents, and request that they be indexed.
 */
int u1db__index_all_docs(u1database *db, int n_expressions,
                         const char **expressions);


/**
 * Create an object for synchronizing.
 *
 * The object created should be freed using u1db__free_sync_target. It holds a
 * pointer to the database that created it, so you must keep the database
 * object alive as long as the synchronization object is alive.
 */
int u1db__get_sync_target(u1database *db, u1db_sync_target **sync_target);


void u1db__free_sync_target(u1db_sync_target **sync_target);

/**
 * Generate count random bytes and put them in buf.
 */
int u1db__random_bytes(void *buf, size_t count);

/**
 * Convert a sequence of binary bytes to hex data.
 *
 * @param bin_in    A string of binary bytes, bin_len long.
 * @param bin_len   Number of bytes of bin_in to convert.
 * @param hex_out   This must be a buffer of length 2*bin_len
 */
void u1db__bin_to_hex(unsigned char *bin_in, int bin_len, char *hex_out);

/**
 * Ask the sync_exchange object what doc_ids we have seen.
 *
 * This is only meant for testing.
 *
 * @param n_ids (OUT) The number of ids present
 * @param doc_ids (OUT) Will return a heap allocated list of doc_ids. The
 *                      strings should not be mutated, and the array needs to
 *                      be freed.
 */
int u1db__sync_exchange_seen_ids(u1db_sync_exchange *se, int *n_ids,
                                 const char ***doc_ids);


/**
 * We have received a doc from source, record it.
 */
int u1db__sync_exchange_insert_doc_from_source(u1db_sync_exchange *se,
        u1db_document *doc, int source_gen, const char *trans_id);

/**
 * We are done receiving docs, find what we want to return.
 *
 * @post se->doc_ids_to_return will be updated with doc_ids to send.
 */
int u1db__sync_exchange_find_doc_ids_to_return(u1db_sync_exchange *se);

/**
 * Invoke the callback for documents identified by find_doc_ids_to_return.
 *
 * @param context   Will be passed as the first parameter to callback
 * @param cb        A callback, will be called for each document. The document
 *                  will be allocated on the heap, and should be freed by
 *                  u1db_free_doc().
 */
int u1db__sync_exchange_return_docs(u1db_sync_exchange *se, void *context,
                                    int (*cb)(void *context,
                                    u1db_document *doc, int gen,
                                    const char *trans_id));

/**
 * Create a sync target pointing at a given URL.
 */
int u1db__create_http_sync_target(const char *url, u1db_sync_target **target);

/**
 * Create a sync target, passing in OAUTH credentials.
 */
int u1db__create_oauth_http_sync_target(const char *url,
    const char *consumer_key, const char *consumer_secret,
    const char *token_key, const char *token_secret,
    u1db_sync_target **target);

/**
 * Sync a database with a sync target.
 */
int u1db__sync_db_to_target(u1database *db, u1db_sync_target *target,
                            int *local_gen_before_sync);

#endif // U1DB_INTERNAL_H
