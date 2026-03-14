<?php

namespace Utopia\Database\Validator;

use Throwable;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Order;
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
     * @param mixed $value Array of Query objects or query strings
     * @return bool
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

        $aggregationAliases = [];
        foreach ($value as $q) {
            if (! $q instanceof Query) {
                try {
                    $q = Query::parse($q);
                } catch (Throwable) {
                    continue;
                }
            }
            if (\in_array($q->getMethod(), [
                Method::Count, Method::CountDistinct, Method::Sum, Method::Avg,
                Method::Min, Method::Max, Method::Stddev, Method::Variance,
            ], true)) {
                $alias = $q->getValue('');
                if ($alias !== '') {
                    $aggregationAliases[] = $alias;
                }
            }
        }
        if (! empty($aggregationAliases)) {
            foreach ($this->validators as $validator) {
                if ($validator instanceof Order) {
                    $validator->addAggregationAliases($aggregationAliases);
                }
            }
        }

        foreach ($value as $query) {
            if (! $query instanceof Query) {
                try {
                    $query = Query::parse($query);
                } catch (Throwable $e) {
                    $this->message = 'Invalid query: '.$e->getMessage();

                    return false;
                }
            }

            if ($query->isNested() && $query->getMethod() !== Method::Having) {
                /** @var array<Query|string> $nestedValues */
                $nestedValues = $query->getValues();
                if (! self::isValid($nestedValues)) {
                    return false;
                }
            }

            $method = $query->getMethod();
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
                Method::Count,
                Method::CountDistinct,
                Method::Sum,
                Method::Avg,
                Method::Min,
                Method::Max,
                Method::Stddev,
                Method::Variance => Base::METHOD_TYPE_AGGREGATE,
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
