<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Builder\Statement;

/**
 * An SQL adapter that also tells the statement the builder makes for a count() or sum().
 */
interface AggregateReference
{
    /**
     * @param  list<Query>  $queries
     */
    public function builtAggregate(string $operation, Document $collection, array $queries, ?int $max): Statement;

    /**
     * @param  list<mixed>  $bindings
     * @return list<mixed>
     */
    public function boundValues(array $bindings): array;
}
