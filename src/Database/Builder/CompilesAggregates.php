<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * Compiles the aggregates MySQL and MariaDB answer differently from the other engines. An unaliased aggregate keeps
 * the name the engine gave it.
 *
 * Both send a DOUBLE aggregate that overflowed to infinity as `0` over the text protocol, and turn an infinite or
 * undefined variance into DBL_MAX or NULL under STDDEV. Adding zero runs the engine's float range check, which raises
 * error 1690 for a value that is not finite, so each floating aggregate is compiled through one.
 *
 * Both answer BIT_AND, BIT_OR and BIT_XOR as BIGINT UNSIGNED, so a result with the sign bit set reads as an integer
 * past the range of a signed one. Each is cast to SIGNED, which reads the same 64 bits as the two's complement
 * integer PostgreSQL answers.
 */
trait CompilesAggregates
{
    private const array CHECKED_AGGREGATES = [Method::Sum, Method::Avg, Method::Variance, Method::VarPop, Method::VarSamp];

    private const array BITWISE_AGGREGATES = [Method::BitAnd, Method::BitOr, Method::BitXor];

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

        $bitwise = \in_array($method, self::BITWISE_AGGREGATES, true);
        if ($variance === null && ! $bitwise && ! \in_array($method, self::CHECKED_AGGREGATES, true)) {
            return parent::compileAggregate($query);
        }

        $expression = parent::compileAggregate(new Query($method, $query->getAttribute()));
        $compiled = match (true) {
            $variance !== null => 'SQRT('.parent::compileAggregate(new Query($variance, $query->getAttribute())).' + 0)',
            $bitwise => 'CAST('.$expression.' AS SIGNED)',
            default => $expression.' + 0',
        };

        $alias = $query->getAlias();

        return $compiled.' AS '.($alias === '' ? $this->quoteLiteral($expression) : $this->quote($alias));
    }
}
