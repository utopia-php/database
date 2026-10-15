<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event;
use Utopia\Query\Hook;

/**
 * Hook for transforming SQL queries before execution.
 *
 * Implementations receive the raw SQL string and return a modified version.
 * Registered via Database::addHook() and applied in the adapter's
 * executeResult() path.
 */
interface Transform extends Hook
{
    /**
     * Transform a raw SQL query string before it is executed.
     *
     * @param Event $event The database event type triggering the query
     * @param string $query The raw SQL query string
     * @return string The transformed SQL query string
     */
    public function transform(Event $event, string $query): string;
}
