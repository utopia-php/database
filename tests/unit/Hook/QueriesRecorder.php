<?php

namespace Tests\Unit\Hook;

use Closure;
use Utopia\Database\Collection;
use Utopia\Database\Event;
use Utopia\Database\Hook\Queries;
use Utopia\Database\Query;

/**
 * Records the event and collection of each read it rewrites, and rewrites the read's queries with the given closure.
 */
class QueriesRecorder implements Queries
{
    /** @var list<array{Event, string}> */
    public array $calls = [];

    /** @var Closure(array<Query>): array<mixed> */
    private readonly Closure $rewriter;

    /**
     * @param  (Closure(array<Query>): array<mixed>)|null  $rewriter  Returns the given queries when null
     */
    public function __construct(?Closure $rewriter = null)
    {
        $this->rewriter = $rewriter ?? static fn (array $queries): array => $queries;
    }

    /**
     * A rewriter that adds $added to the queries unless they already hold an equal query, so it returns the same
     * queries when given its own output.
     *
     * @return Closure(array<Query>): array<Query>
     */
    public static function adding(Query $added): Closure
    {
        return static function (array $queries) use ($added): array {
            foreach ($queries as $query) {
                if ($query->toString() === $added->toString()) {
                    return $queries;
                }
            }

            return [...$queries, $added];
        };
    }

    #[\Override]
    public function rewrite(Event $event, Collection $collection, array $queries): array
    {
        $this->calls[] = [$event, $collection->getId()];

        /** @var array<Query> */
        return ($this->rewriter)($queries);
    }
}
