<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Event;

/**
 * Provides the ability to set query execution timeouts on a database adapter.
 */
interface Timeouts
{
    /**
     * Set a timeout for database operations.
     *
     * @param int $milliseconds The timeout duration in milliseconds.
     * @param Event $event The event scope to apply the timeout to.
     * @return void
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): void;

    /**
     * Clear a timeout for database operations.
     *
     * @param Event $event The event scope to clear the timeout from.
     * @return void
     */
    public function clearTimeout(Event $event = Event::All): void;
}
