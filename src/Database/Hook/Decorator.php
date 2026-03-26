<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;
use Utopia\Query\Hook;
use Utopia\Database\Event;

/**
 * Hook for transforming documents after they are read from or written to the database.
 *
 * Decorators run inline in the document pipeline: they receive a document, modify it,
 * and return it. The returned document is what the caller receives. This makes them
 * suitable for adding metadata, transforming attributes, or counting operations.
 *
 * Decorators differ from {@see Lifecycle} hooks in two key ways:
 *
 * 1. **Return value**: Decorators return the (possibly modified) Document back into the
 *    pipeline. Lifecycle hooks return void and cannot influence the result.
 *
 * 2. **Error handling**: Decorator exceptions propagate to the caller and abort the
 *    operation. Lifecycle hook exceptions are silently caught so side effects never
 *    break business logic.
 *
 * Decorators also receive the collection Document as context, which Lifecycle hooks
 * do not. Use Lifecycle hooks for fire-and-forget side effects (auditing, logging,
 * event dispatch). Use Decorators when you need to modify the document before it
 * reaches the caller.
 *
 * Runs after: relationship population, casting, decoding, type mapping.
 * Runs before: lifecycle event triggers.
 */
interface Decorator extends Hook
{
    /**
     * Decorate a document after a database operation.
     *
     * @param  Event  $event  The operation that produced this document
     * @param  Document  $collection  The collection the document belongs to
     * @param  Document  $document  The document to decorate
     * @return Document The decorated document
     */
    public function decorate(Event $event, Document $collection, Document $document): Document;
}
