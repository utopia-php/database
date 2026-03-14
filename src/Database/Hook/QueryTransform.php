<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event;

/**
 * Hook for transforming SQL queries before execution.
 *
 * Implementations receive the raw SQL string and return a modified version.
 * Registered via Adapter::addQueryTransform() and applied in the
 * centralized executeResult() path.
 */
interface QueryTransform
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
