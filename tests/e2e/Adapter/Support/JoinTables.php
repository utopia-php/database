<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Collection;
use Utopia\Database\Event;
use Utopia\Database\Hook\Queries;

/**
 * Points each join at the table its collection maps to, as Appwrite points a public table ID at its
 * internal collection.
 */
final readonly class JoinTables implements Queries
{
    /**
     * @param  array<string, string>  $tables  The table each joined collection is read from
     */
    public function __construct(private array $tables)
    {
    }

    #[\Override]
    public function rewrite(Event $event, Collection $collection, array $queries): array
    {
        foreach ($queries as $index => $query) {
            $table = $this->tables[$query->getAttribute()] ?? null;
            if ($table !== null && $query->getMethod()->isJoin()) {
                $queries[$index] = (clone $query)->setAttribute($table);
            }
        }

        return $queries;
    }
}
