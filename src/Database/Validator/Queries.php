<?php

namespace Utopia\Database\Validator;

use Throwable;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Method;
use Utopia\Validator;

/**
 * Validates an array of query objects by dispatching each to the appropriate method-type validator.
 */
class Queries extends Validator
{
    protected string $message = 'Invalid queries';

    /**
     * @var array<Base>
     */
    protected array $validators;

    protected int $length;

    /**
     * Queries constructor
     *
     * @param  array<Base>  $validators
     */
    public function __construct(array $validators = [], int $length = 0)
    {
        $this->validators = $validators;
        $this->length = $length;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Validate an array of queries, checking each against registered method-type validators.
     *
     * @param  mixed  $value  Array of Query objects or query strings
     */
    public function isValid($value): bool
    {
        if (! \is_array($value)) {
            $this->message = 'Queries must be an array';

            return false;
        }
        /** @var array<Query|string> $value */
        if ($this->length && \count($value) > $this->length) {
            return false;
        }

        // One pass over the validators: clear aliases left over from a previous
        // call, and note whether a filter validator is registered. Order
        // validators persist across requests in pooled / long-lived processes,
        // so letting aliases accumulate leaks state and lets an unrelated query
        // order by a stale alias.
        $hasFilterValidator = false;
        foreach ($this->validators as $validator) {
            if ($validator instanceof Order) {
                $validator->resetAggregationAliases();
            }
            if (
                $validator instanceof Select
                || $validator instanceof Filter
                || $validator instanceof Order
            ) {
                $validator->resetJoinAliases();
            }
            if ($validator->getMethodType() === Base::METHOD_TYPE_FILTER) {
                $hasFilterValidator = true;
            }
        }

        // One pass over the input: parse each raw string, and collect the
        // aliases the order, select and filter validators have to know about
        // before dispatch starts. A method can report as both aggregate and
        // join, so both are tested independently rather than as a chain.
        /** @var list<Query> $parsedQueries */
        $parsedQueries = [];
        /** @var array<string> $aggregationAliases */
        $aggregationAliases = [];
        $joinAliases = [];
        foreach ($value as $q) {
            if (! $q instanceof Query) {
                try {
                    $q = Query::parse($q);
                } catch (Throwable $e) {
                    $this->message = 'Invalid query: '.$e->getMessage();

                    return false;
                }
            }

            $parsedQueries[] = $q;

            $method = $q->getMethod();

            if ($method->isAggregate()) {
                $alias = $q->getValue('');
                if (\is_string($alias) && $alias !== '') {
                    $aggregationAliases[] = $alias;
                }
            }

            if ($method->isJoin()) {
                $alias = $q->getJoinAlias();
                if ($alias !== '') {
                    $joinAliases[] = $alias;
                }
            }
        }

        if ($aggregationAliases !== [] || $joinAliases !== []) {
            foreach ($this->validators as $validator) {
                if ($aggregationAliases !== [] && $validator instanceof Order) {
                    $validator->addAggregationAliases($aggregationAliases);
                }
                if (
                    $joinAliases !== []
                    && (
                        $validator instanceof Select
                        || $validator instanceof Filter
                        || $validator instanceof Order
                    )
                ) {
                    $validator->allowJoinAliases($joinAliases);
                }
            }
        }

        // Same pass: nested and/or children must keep the join aliases collected above.
        $pending = $parsedQueries;
        while ($pending !== []) {
            $query = \array_shift($pending);

            if (\in_array($query->getMethod(), Query::LOGICAL_TYPES, true)) {
                foreach ($query->getValues() as $nested) {
                    if (! $nested instanceof Query) {
                        if (! \is_string($nested)) {
                            $this->message = 'Invalid query: nested query must be a string';

                            return false;
                        }
                        try {
                            $nested = Query::parse($nested);
                        } catch (Throwable $e) {
                            $this->message = 'Invalid query: '.$e->getMessage();

                            return false;
                        }
                    }
                    $pending[] = $nested;
                }
            }

            if ($hasFilterValidator && $query->getMethod()->isJoin() && $query->isNestedJoin()) {
                foreach ($query->getJoinOnQueries() as $onQuery) {
                    if ($onQuery->getMethod() === Method::On) {
                        continue;
                    }
                    $pending[] = $onQuery;
                }
            }

            $method = $query->getMethod();

            // Route every aggregate method through the single source of truth
            // on the base enum. Previously this match hand-listed only half
            // of the aggregate methods, silently rejecting stddevPop, varPop,
            // bitAnd, etc. with "Invalid query method".
            if ($method->isAggregate()) {
                $methodType = Base::METHOD_TYPE_AGGREGATE;
            } else {
                $methodType = match ($method) {
                    Method::Select => Base::METHOD_TYPE_SELECT,
                    Method::Limit => Base::METHOD_TYPE_LIMIT,
                    Method::Offset => Base::METHOD_TYPE_OFFSET,
                    Method::CursorAfter,
                    Method::CursorBefore => Base::METHOD_TYPE_CURSOR,
                    Method::OrderAsc,
                    Method::OrderDesc,
                    Method::OrderRandom => Base::METHOD_TYPE_ORDER,
                    Method::Equal,
                    Method::NotEqual,
                    Method::LessThan,
                    Method::LessThanEqual,
                    Method::GreaterThan,
                    Method::GreaterThanEqual,
                    Method::Search,
                    Method::NotSearch,
                    Method::IsNull,
                    Method::IsNotNull,
                    Method::Between,
                    Method::NotBetween,
                    Method::StartsWith,
                    Method::NotStartsWith,
                    Method::EndsWith,
                    Method::NotEndsWith,
                    Method::Contains,
                    Method::ContainsAny,
                    Method::NotContains,
                    Method::And,
                    Method::Or,
                    Method::ContainsAll,
                    Method::ElemMatch,
                    Method::Crosses,
                    Method::NotCrosses,
                    Method::DistanceEqual,
                    Method::DistanceNotEqual,
                    Method::DistanceGreaterThan,
                    Method::DistanceLessThan,
                    Method::Intersects,
                    Method::NotIntersects,
                    Method::Overlaps,
                    Method::NotOverlaps,
                    Method::Touches,
                    Method::NotTouches,
                    Method::Covers,
                    Method::NotCovers,
                    Method::SpatialEquals,
                    Method::NotSpatialEquals,
                    Method::VectorDot,
                    Method::VectorCosine,
                    Method::VectorEuclidean,
                    Method::Regex,
                    Method::Exists,
                    Method::NotExists => Base::METHOD_TYPE_FILTER,
                    Method::Distinct => Base::METHOD_TYPE_DISTINCT,
                    Method::GroupBy => Base::METHOD_TYPE_GROUP_BY,
                    Method::Having => Base::METHOD_TYPE_HAVING,
                    Method::Join,
                    Method::LeftJoin,
                    Method::RightJoin,
                    Method::CrossJoin,
                    Method::FullOuterJoin,
                    Method::NaturalJoin => Base::METHOD_TYPE_JOIN,
                    default => '',
                };
            }

            $methodIsValid = false;
            foreach ($this->validators as $validator) {
                if ($validator->getMethodType() !== $methodType) {
                    continue;
                }
                if (! $validator->isValid($query)) {
                    $this->message = 'Invalid query: '.$validator->getDescription();

                    return false;
                }

                $methodIsValid = true;
            }

            if (! $methodIsValid) {
                $this->message = 'Invalid query method: '.$method->value;

                return false;
            }
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return true;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}
