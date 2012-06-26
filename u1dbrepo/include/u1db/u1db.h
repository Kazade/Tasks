/*
 * Copyright 2011-2012 Canonical Ltd.
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

#ifndef _U1DB_H_
#define _U1DB_H_

// Needed for size_t, etc
#include <stdlib.h>

typedef struct _u1database u1database;
// The document structure. Note that you must use u1db_make_doc to create
// these, as there are private attributes that are required. This is just the
// public interface
typedef struct _u1db_document
{
    char *doc_id;
    size_t doc_id_len;
    char *doc_rev;
    size_t doc_rev_len;
    char *json;
    size_t json_len;
    int has_conflicts;
} u1db_document;


typedef struct _u1query u1query;
typedef int (*u1db_doc_callback)(void *context, u1db_document *doc);
typedef int (*u1db_key_callback)(void *context, int num_fields,
                                 const char **key);
typedef int (*u1db_doc_gen_callback)(void *context, u1db_document *doc,
                                     int gen, const char *trans_id);
typedef int (*u1db_doc_id_gen_callback)(void *context, const char *doc_id, int gen);
typedef int (*u1db_trans_info_callback)(void *context, const char *doc_id,
                                        int gen, const char *trans_id);

#define U1DB_OK 0
#define U1DB_INVALID_PARAMETER -1
// put_doc() was called but the doc_rev stored in the database doesn't match
// the one supplied.
#define U1DB_REVISION_CONFLICT -2
#define U1DB_INVALID_DOC_ID -3
#define U1DB_DOCUMENT_ALREADY_DELETED -4
#define U1DB_DOCUMENT_DOES_NOT_EXIST -5
#define U1DB_NOMEM -6
#define U1DB_NOT_IMPLEMENTED -7
#define U1DB_INVALID_JSON -8
#define U1DB_INVALID_VALUE_FOR_INDEX -9
#define U1DB_INVALID_HTTP_RESPONSE -10
#define U1DB_BROKEN_SYNC_STREAM -11
#define U1DB_INVALID_TRANSFORMATION_FUNCTION -12
#define U1DB_UNKNOWN_OPERATION -13
#define U1DB_UNHANDLED_CHARACTERS -14
#define U1DB_MISSING_FIELD_SPECIFIER -15
#define U1DB_INVALID_FIELD_SPECIFIER -16
#define U1DB_DUPLICATE_INDEX_NAME -17
#define U1DB_INDEX_DOES_NOT_EXIST -18
#define U1DB_INVALID_GLOBBING -19
#define U1DB_INVALID_TRANSACTION_ID -20
#define U1DB_INVALID_GENERATION -21
#define U1DB_INTERNAL_ERROR -999

// Used by put_doc_if_newer
#define U1DB_INSERTED 1
#define U1DB_SUPERSEDED 2
#define U1DB_CONVERGED 3
#define U1DB_CONFLICTED 4

/**
 * The basic constructor for a new connection.
 */
u1database *u1db_open(const char *fname);

/**
 * Close an existing connection, freeing memory, etc.
 * This is generally used as u1db_free(&db);
 * After freeing the memory, we will set the pointer to NULL.
 */
void u1db_free(u1database **db);

/**
 * Set the replica_uid defined for this database.
 */
int u1db_set_replica_uid(u1database *db, const char *replica_uid);

/**
 * Get the replica_uid defined for this database.
 *
 * @param replica_uid (OUT) The unique identifier for this replica. The
 *                     returned pointer is managed by the db object and should
 *                     not be modified.
 */
int u1db_get_replica_uid(u1database *db, const char **replica_uid);

/**
 * Create a new document.
 *
 * @param json The JSON string representing the document. The json will
 *                be copied and managed by the 'doc' parameter.
 * @param doc_id A string identifying the document. If the value supplied is
 *               NULL, then a new doc_id will be generated.
 * @param doc (OUT) a u1db_document that will be allocated and needs to be
 *            freed with u1db_free_doc
 * @return a status code indicating success or failure.
 */
int u1db_create_doc(u1database *db, const char *json, const char *doc_id,
                    u1db_document **doc);

/**
 * Put new document content for the given document identifier.
 *
 * @param doc (IN/OUT) A document whose content we want to update in the
 *            database. The new content should have been set with
 *            u1db_doc_set_content. The document's revision should match what
 *            is currently in the database, and will be updated to point at
 *            the new revision.
 */
int u1db_put_doc(u1database *db, u1db_document *doc);

/**
 * Mark conflicts as having been resolved.
 * @param doc (IN/OUT) The new content. doc->doc_rev will be updated with the
 *            new revision. Also if there are still conflicts remaining, we
 *            will set doc->has_conflicts.
 * @param n_revs The number of revisions being passed
 * @param revs The revisions we are resolving.
 */
int u1db_resolve_doc(u1database *db, u1db_document *doc,
                     int n_revs, const char **revs);

/**
 * Get the document defined by the given document id.
 *
 * @param doc_id The document we are looking for
 * @param include_deleted If true, return the document even if it was deleted.
 * @param doc (OUT) a document (or NULL) matching the request
 * @return status, will be U1DB_OK if there is no error, even if there is no
 *      document matching that doc_id.
 */
int u1db_get_doc(u1database *db, const char *doc_id, int include_deleted,
                 u1db_document **doc);


/**
 * Retrieve multiple documents from the database.
 *
 * @param n_doc_ids The number of document ids being passed.
 * @param doc_ids An array of document ids to retrieve.
 * @param check_for_conflicts If true, check if each document has any
 *          conflicts, if false, the conflict checking will be skipped.
 * @param include_deleted If true, return documents even if they were deleted.
 * @param context A void* that is returned via the callback function.
 * @param cb This will be called with each document requested. The api is
 *           cb(void* context, u1db_document *doc). The returned documents are
 *           allocated on the heap, and must be freed by the caller via
 *           u1db_free_doc.
 */
int u1db_get_docs(u1database *db, int n_doc_ids, const char **doc_ids,
                  int check_for_conflicts, int include_deleted, void *context,
                  u1db_doc_callback cb);

/**
 * Retrieve all documents from the database.
 *
 * @param include_deleted If true, return documents even if they were deleted.
 * @param generation (OUT) the generation the database is at
 * @param context A void* that is returned via the callback function.
 * @param cb This will be called with each document requested. The api is
 *           cb(void* context, u1db_document *doc). The returned documents are
 *           allocated on the heap, and must be freed by the caller via
 *           u1db_free_doc.
 */
int u1db_get_all_docs(u1database *db, int include_deleted, int *generation,
                      void *context, u1db_doc_callback cb);

/**
 * Get all of the contents associated with a conflicted document.
 *
 * If a document is not conflicted, then this will not invoke the callback
 * function.
 *
 * @param context A void* that is returned to the callback function.
 * @param cb This callback function will be invoked for each content that is
 *      conflicted. The first item will be the same document that you get from
 *      get_doc(). The document objects passed into the callback function will
 *      have been allocated on the heap, and the callback is responsible for
 *      freeing the memory (or saving it somewhere).
 */
int u1db_get_doc_conflicts(u1database *db, const char *doc_id, void *context,
                           u1db_doc_callback cb);

/**
 * Mark a document as deleted.
 *
 * @param doc (IN/OUT) The document we want to delete, the document must match
 *                the stored value, or the delete will fail. After being
 *                deleted, the doc_rev parameter will be updated to match the
 *                new value in the database. Also, doc->content will be set to
 *                NULL.
 */
int u1db_delete_doc(u1database *db, u1db_document *doc);

/**
 * Get the document defined by the given document id.
 *
 * @param gen    The global database revision to start at. You can pass '0' to
 *               get all changes in the database. The integer will be updated
 *               to point at the current generation.
 * @param trans_id The transaction identifier associated with the generation.
 *               Callers are responsible for freeing this memory.
 * @param cb     A callback function. This will be called passing in 'context',
 *               and a document identifier for each document that has been
 *               modified. This includes the generation and associated
 *               transaction id for each change. If a document is modified more
 *               than once, only the most recent change will be given.
 *               Note that the strings passed are transient, so must be copied
 *               if callers want to use them after they return.
 * @param context Opaque context, passed back to the caller.
 */
int u1db_whats_changed(u1database *db, int *gen, char **trans_id,
                       void *context, u1db_trans_info_callback cb);


/**
 * Free a u1db_document.
 *
 * @param doc A reference to the doc pointer to be freed. Generally used as:
 *            u1db_free_doc(&doc). If the pointer or its referenced value is
 *            NULL, this is a no-op. We will set the reference to NULL after
 *            freeing the memory.
 */
void u1db_free_doc(u1db_document **doc);

/**
 * Set the content for the document.
 *
 * This will copy the string, since the memory is managed by the doc object
 * itself.
 */
int u1db_doc_set_json(u1db_document *doc, const char *json);


/**
 * Create an index that you can query for matching documents.
 *
 * @param index_name    An identifier for this index.
 * @param n_expressions The number of index expressions.
 * @param exp0... The values to match in the index, all of these should be char*
 */
int u1db_create_index(u1database *db, const char *index_name, int n_expressions,
                      ...);


/**
 * Create an index that you can query for matching documents.
 *
 * @param index_name    An identifier for this index.
 * @param n_expressions The number of index expressions.
 * @param expressions   An array of expressions.
 */
int u1db_create_index_list(u1database *db, const char *index_name,
                           int n_expressions, const char **expressions);

/**
 * Delete a defined index.
 */
int u1db_delete_index(u1database *db, const char *index_name);


/**
 * List indexes which have been defined, along with their definitions.
 *
 * @param context An opaque pointer that will be returned to the callback
 *                function.
 * @param cb A function callback that will be called once for each index that
 *           is defined in the database. The parameters passed are only valid
 *           until the callback returns (memory is managed by the u1db
 *           library). So if users want to keep the information, they must copy
 *           it.
 */
int u1db_list_indexes(u1database *db, void *context,
                      int (*cb)(void *context, const char *index_name,
                                int n_expressions, const char **expressions));


/**
 * Initialize a structure for querying an index.
 *
 * @param index_name The index that you want to query. We will use the database
 *                   definition to determine how many columns need to be
 *                   initialized.
 * @param query (OUT) This will hold the query structure.
 */
int u1db_query_init(u1database *db, const char *index_name, u1query **query);


/**
 * Free the memory pointed to by query and all associated buffers.
 *
 * query will be updated to point to NULL when finished.
 */
void u1db_free_query(u1query **query);


/**
 * Get documents which match a given index.
 *
 * @param query A u1query object, as created by u1db_query_init.
 * @param context Will be returned via the document callback
 * @param n_values The number of parameters being passed, must be >= 1
 * @param values The values to match in the index.
 */
int u1db_get_from_index_list(u1database *db, u1query *query, void *context,
                             u1db_doc_callback cb, int n_values,
                             const char **values);

/**
 * Get documents which match a given index.
 *
 * @param query A u1query object, as created by u1db_query_init.
 * @param context Will be returned via the document callback
 * @param n_values The number of parameters being passed, must be >= 1
 * @param val0... The values to match in the index, all of these should be char*
 */
int u1db_get_from_index(u1database *db, u1query *query, void *context,
                        u1db_doc_callback cb, int n_values, ...);

/**
 * Get documents with key values in the specified range
 *
 * @param query A u1query object, as created by u1db_query_init.
 * @param context Will be returned via the document callback
 * @param n_values The number of values.
 * @param start_values An array of values. If NULL, assume an open ended query.
 * @param end_values An array of values. If NULL, assume an open ended query.
 */
int u1db_get_range_from_index(u1database *db, u1query *query,
                              void *context, u1db_doc_callback cb,
                              int n_values, const char **start_values,
                              const char **end_values);
/**
 * Get keys under which documents are indexed.
 *
 * @param index_name Name of the index for which to get keys.
 * @param context Will be returned via the document callback. cb will be called
 *     once for each column, with a NULL value to separate rows.
 */
int u1db_get_index_keys(u1database *db, char *index_name, void *context,
                        u1db_key_callback cb);


/**
 * Get documents matching a single column index.
 */
int u1db_simple_lookup1(u1database *db, const char *index_name,
                        const char *val1,
                        void *context, u1db_doc_callback cb);

#endif // _U1DB_H_
