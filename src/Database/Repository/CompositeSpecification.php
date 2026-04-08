<?php

namespace Utopia\Database\Repository;

use Utopia\Database\Query;

class CompositeSpecification implements Specification
{
    /**
     * @param  array<Specification>  $specs
     */
    public function __construct(
        private array $specs,
        private string $operator = 'and',
    ) {
    }

    /**
     * @return array<Query>
     */
    public function toQueries(): array
    {
        $queries = [];

        if ($this->operator === 'or') {
            $groups = [];
            foreach ($this->specs as $spec) {
                $groups[] = $spec->toQueries();
            }

            if ($groups !== []) {
                $orQueries = [];
                foreach ($groups as $group) {
                    $orQueries[] = Query::or($group);
                }

                return $orQueries;
            }

            return [];
        }

        foreach ($this->specs as $spec) {
            $queries = \array_merge($queries, $spec->toQueries());
        }

        return $queries;
    }

    public function and(Specification $other): Specification
    {
        return new self([$this, $other], 'and');
    }

    public function or(Specification $other): Specification
    {
        return new self([$this, $other], 'or');
    }
}
