<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event;
use Utopia\Query\Hook;

/**
 * Lifecycle hook for fire-and-forget side effects on database events.
 *
 * Implementations receive lifecycle events (document CRUD, collection changes, etc.)
 * and can respond with side effects (auditing, logging, analytics, event dispatch).
 *
 * Lifecycle hooks differ from {@see Decorator} hooks in two key ways:
 *
 * 1. **Return value**: Lifecycle hooks return void and cannot modify the document or
 *    influence the operation result. Decorators return a Document back into the pipeline.
 *
 * 2. **Error handling**: Exceptions thrown by lifecycle hooks are silently caught to
 *    prevent side effects from breaking business logic. Decorator exceptions propagate
 *    to the caller.
 *
 * Lifecycle hooks do not receive collection context. Use Decorators when you need to
 * transform documents before they reach the caller.
 */
interface Lifecycle extends Hook
{
    /**
     * Handle a lifecycle event.
     *
     * @param  Event  $event  The event type
     * @param  mixed  $data  The event payload (Document, array, string, int, etc.)
     */
    public function handle(Event $event, mixed $data): void;
}
