<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Builder\Statement;

/**
 * For an SQL adapter: the statement the builder makes for a count() or sum(), as the adapter built
 * it before it wrote such statements out, and the values its bindings are bound as.
 */
trait BuildsAggregates
{
    /**
     * @param  list<Query>  $queries
     */
    public function builtAggregate(string $operation, Document $collection, array $queries, ?int $max): Statement
    {
        $name = $this->filter($collection->getId());
        $builder = $this->newBuilder($name, Query::DEFAULT_ALIAS);
        $builder->filter($queries);

        $perDocument = $collection->getAttribute('documentSecurity', false) || $collection->getId() === Database::METADATA;
        if ($this->authorization->getStatus() && $perDocument) {
            $builder->addHook($this->newPermissionHook($name, $this->authorization->getRoles()));
        }

        if ($max === null) {
            $operation === 'count' ? $builder->count('1', 'sum') : $builder->sum('price', 'sum');

            return $builder->build();
        }

        $operation === 'count' ? $builder->selectRaw('1') : $builder->select(['price']);
        $builder->limit($max);
        $outer = $this->createBuilder();
        $outer->fromSub($builder, 'table_count');
        $operation === 'count' ? $outer->count('1', 'sum') : $outer->sum('price', 'sum');

        return $outer->build();
    }

    /**
     * @param  list<mixed>  $bindings
     * @return list<mixed>
     */
    public function boundValues(array $bindings): array
    {
        return \array_map(fn (mixed $value): mixed => \is_float($value) ? $this->getFloatPrecision($value) : $value, $bindings);
    }
}
