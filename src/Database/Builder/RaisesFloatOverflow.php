<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * MySQL and MariaDB send a DOUBLE aggregate that overflowed to infinity as `0` over the text protocol, and turn an
 * infinite or undefined variance into DBL_MAX or NULL under STDDEV. Adding zero runs the engine's float range check,
 * which raises error 1690 for a value that is not finite, so each floating aggregate is compiled through one. An
 * unaliased aggregate keeps the name the engine gave it.
 */
trait RaisesFloatOverflow
{
    private const array CHECKED_AGGREGATES = [Method::Sum, Method::Avg, Method::Variance, Method::VarPop, Method::VarSamp];

    #[\Override]
    public function compileAggregate(Query $query): string
    {
        $method = $query->getMethod();
        $variance = match ($method) {
            Method::Stddev => Method::Variance,
            Method::StddevPop => Method::VarPop,
            Method::StddevSamp => Method::VarSamp,
            default => null,
        };

        if ($variance === null && ! \in_array($method, self::CHECKED_AGGREGATES, true)) {
            return parent::compileAggregate($query);
        }

        $expression = parent::compileAggregate(new Query($method, $query->getAttribute()));
        $checked = $variance === null
            ? $expression.' + 0'
            : 'SQRT('.parent::compileAggregate(new Query($variance, $query->getAttribute())).' + 0)';

        $alias = $query->getAlias();

        return $checked.' AS '.($alias === '' ? $this->quoteLiteral($expression) : $this->quote($alias));
    }
}
