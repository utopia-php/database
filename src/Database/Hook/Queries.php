<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Query;
use Utopia\Query\Hook;

/**
 * Hook for rewriting the queries of a read before the read uses them.
 *
 * It is given the queries one read was given and the definition of the collection being read. It runs once per read,
 * after the collection is resolved and each query is checked to be a {@see Query}, and before the read validates,
 * parses or authorizes its queries or resolves their joins. The queries it returns replace the read's queries for the
 * rest of the read, the query-cache key included. getDocument() runs it with {@see Event::DocumentRead}; find(), and
 * so findOne(), cursor() and resolveCursor() with {@see Event::DocumentFind}; count() with {@see Event::DocumentCount};
 * sum() with {@see Event::DocumentSum}; and aggregate() with {@see Event::DocumentAggregate}. Hooks run in the order
 * they were registered, each given the queries the previous one returned.
 *
 * Throwing refuses the read: the exception reaches the caller unchanged, before anything is read. With a hook
 * registered, cursor() resolves the collection and rewrites its queries when it is called, so a missing collection or
 * a refusal throws then, not when the generator is iterated.
 *
 * It runs on every read through those methods, the reads a read makes itself included: relationship population and
 * the reads that resolve queries on relationship attributes, findOne() through find(), each page of cursor(), and
 * resolveCursor()'s read of the joined row it pairs with the document. So it must return the same queries when given
 * its own output. It runs whether or not events are silenced.
 *
 * It never runs for reads of {@see Database::METADATA}, for the query(), mutate() and from() builders, or for writes
 * and the reads they make themselves. The writes are createDocument(), createDocuments(), updateDocument(),
 * updateDocuments(), upsertDocument(), upsertDocuments(), increaseDocumentAttribute(), decreaseDocumentAttribute(),
 * deleteDocument() and deleteDocuments(), and the authorizeCreate(), authorizeUpdate() and authorizeGrants() checks of
 * a later write. Their own reads load the documents they change, read back the ones they wrote, populate the
 * relationships of the documents they return, and check, link and cascade related documents. A read the caller's own
 * code makes while a write runs, as an onNext callback can, runs it.
 *
 * The given queries belong to the caller, so a hook that changes one returns a clone of it. It must not modify the
 * collection, which can be a cached definition. A hook that needs the database implements {@see Attachable}.
 */
interface Queries extends Hook
{
    /**
     * Rewrite the queries of a read before the read uses them.
     *
     * @param  Event  $event  The event of the read
     * @param  Collection  $collection  The definition of the collection being read
     * @param  array<Query>  $queries  The queries the read was given, or the previous hook returned
     * @return array<Query> The queries the read uses
     */
    public function rewrite(Event $event, Collection $collection, array $queries): array;
}
