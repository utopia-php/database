<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * Provides the ability to retrieve the underlying database connection identifier.
 */
interface ConnectionId
{
    /**
     * Get the unique identifier for the current database connection.
     *
     * @return string The connection identifier.
     */
    public function getConnectionId(): string;
}
