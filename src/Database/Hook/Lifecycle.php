<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event;

/**
 * Lifecycle hook for handling database events.
 *
 * Implementations receive lifecycle events (document CRUD, collection changes, etc.)
 * and can respond with side effects (auditing, logging, analytics, etc.).
 *
 * Unlike the `Database::on()` callback system, lifecycle hooks are typed classes
 * that can be tested, composed, and reused. Exceptions thrown by lifecycle hooks
 * are silently caught to prevent side effects from breaking business logic.
 */
interface Lifecycle
{
    /**
     * Handle a lifecycle event.
     *
     * @param  Event  $event  The event type
     * @param  mixed  $data  The event payload (Document, array, string, int, etc.)
     */
    public function handle(Event $event, mixed $data): void;
}
