<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Validator\FloatValidator;

/**
 * Validates having query methods: each condition compares an aggregate alias declared in the same
 * query set or one of its groupBy attributes, under the filter rules of the query set.
 */
class Having extends Base
{
    /**
     * Operators the builder compiles against an aggregate expression.
     */
    private const array ALIAS_METHODS = [
        Method::Equal,
        Method::NotEqual,
        Method::LessThan,
        Method::LessThanEqual,
        Method::GreaterThan,
        Method::GreaterThanEqual,
        Method::Between,
        Method::NotBetween,
        Method::IsNull,
        Method::IsNotNull,
    ];

    /**
     * Aggregates whose result has the type of the aggregated attribute rather than a number.
     */
    private const array EXTREMA = [Method::Min, Method::Max];

    private ?Filter $filter = null;

    /**
     * @var array<string, BaseQuery>
     */
    private array $aggregations = [];

    /**
     * @var array<string, true>
     */
    private array $groupBy = [];

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_HAVING;
    }

    /**
     * The filter rules every condition has to meet. Without one, only the shape of the having
     * query is checked.
     */
    public function setFilter(?Filter $filter): void
    {
        $this->filter = $filter;
    }

    /**
     * @param  array<BaseQuery>  $aggregations  the aggregate queries of the query set
     */
    public function setAggregations(array $aggregations): void
    {
        $this->aggregations = [];

        foreach ($aggregations as $aggregation) {
            $alias = $aggregation->getValue('');
            if (\is_string($alias) && $alias !== '') {
                $this->aggregations[$alias] = $aggregation;
            }
        }
    }

    /**
     * @param  array<mixed>  $attributes  the groupBy attributes of the query set
     */
    public function setGroupBy(array $attributes): void
    {
        $this->groupBy = [];

        foreach ($attributes as $attribute) {
            if (\is_string($attribute) && $attribute !== '') {
                $this->groupBy[$attribute] = true;
            }
        }
    }

    /**
     * Validate a having query has at least one condition, each a Query that meets the filter rules.
     */
    protected function isValidQuery(Query $query): bool
    {
        $conditions = $query->getValues();
        if (empty($conditions)) {
            $this->message = 'Having requires at least one condition';

            return false;
        }

        $queries = [];
        foreach ($conditions as $condition) {
            if (! $condition instanceof Query) {
                $this->message = 'Having conditions must be Query instances';

                return false;
            }

            $queries[] = $condition;
        }

        if ($this->filter === null) {
            return true;
        }

        foreach ($queries as $condition) {
            if (! $this->isValidCondition($condition, $this->filter, nested: false)) {
                return false;
            }
        }

        return true;
    }

    private function isValidCondition(Query $condition, Filter $filter, bool $nested): bool
    {
        $method = $condition->getMethod();

        if (! $this->isFilterMethod($method)) {
            $this->message = 'Having conditions must be filter queries';

            return false;
        }

        if ($method === Method::And || $method === Method::Or) {
            foreach ($condition->getValues() as $child) {
                if ($child instanceof Query && ! $this->isValidCondition($child, $filter, nested: true)) {
                    return false;
                }
            }

            return $this->isValidFilter($condition, $filter);
        }

        $attribute = $condition->getAttribute();
        $aggregation = $this->aggregations[$attribute] ?? null;

        if ($aggregation !== null) {
            if ($nested) {
                $this->message = 'Aggregate alias "'.$attribute.'" can only be compared at the top level of having';

                return false;
            }

            return $this->isValidAliasCondition($condition, $aggregation, $filter);
        }

        if (! isset($this->groupBy[$attribute])) {
            $this->message = 'Having can only compare an aggregate alias or a groupBy attribute: '.$attribute;

            return false;
        }

        return $this->isValidFilter($condition, $filter);
    }

    private function isValidAliasCondition(Query $condition, BaseQuery $aggregation, Filter $filter): bool
    {
        $method = $condition->getMethod();
        $alias = $condition->getAttribute();

        if (! \in_array($method, self::ALIAS_METHODS, true)) {
            $this->message = 'Aggregate alias "'.$alias.'" cannot be compared with '.$method->value;

            return false;
        }

        $values = $condition->getValues();

        if (\in_array($aggregation->getMethod(), self::EXTREMA, true)) {
            return $this->isValidFilter(new Query($method, $aggregation->getAttribute(), $values), $filter);
        }

        if (! $this->isValidValueCount($method, $values)) {
            return false;
        }

        if (\count($values) > $filter->getMaxValuesCount()) {
            $this->message = 'Query on aggregate alias has greater than '.$filter->getMaxValuesCount().' values: '.$alias;

            return false;
        }

        $number = new FloatValidator();
        foreach ($values as $value) {
            if (! $number->isValid($value)) {
                $this->message = 'Query value is invalid for aggregate alias "'.$alias.'"';

                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function isValidValueCount(Method $method, array $values): bool
    {
        $message = match ($method) {
            Method::Equal => $values === [] ? 'require at least one value.' : null,
            Method::Between, Method::NotBetween => \count($values) !== 2 ? 'require exactly two values.' : null,
            Method::IsNull, Method::IsNotNull => null,
            default => \count($values) !== 1 ? 'require exactly one value.' : null,
        };

        if ($message !== null) {
            $this->message = \ucfirst($method->value).' queries '.$message;

            return false;
        }

        return true;
    }

    private function isValidFilter(Query $condition, Filter $filter): bool
    {
        if (! $filter->isValid($condition)) {
            $this->message = $filter->getDescription();

            return false;
        }

        return true;
    }

    private function isFilterMethod(Method $method): bool
    {
        return $method->isFilter()
            || $method->isSpatial()
            || $method->isVector()
            || \in_array($method, [Method::And, Method::Or, Method::ContainsAll, Method::ElemMatch], true);
    }
}
