<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Query\Schema\ColumnType;

/**
 * A collection one join of a query set reads, as the query validators check it: the alias its
 * columns are referenced by and what the collection declares.
 */
final readonly class JoinedCollection
{
    /**
     * @param  string  $alias  The alias the join declares, empty when it declares none
     * @param  array<string, true>  $attributes  The attributes the collection declares, relationships left out
     * @param  array<string, ColumnType>  $numeric  The type of each attribute that holds a single number
     */
    public function __construct(
        public string $alias,
        public array $attributes,
        public array $numeric,
    ) {
    }
}
