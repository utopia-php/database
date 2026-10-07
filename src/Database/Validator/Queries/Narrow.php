<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base as QueryBase;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Method;

/**
 * Validates a list of plain filters, limits, offsets, cursors and orders on the collection's own
 * top-level attributes with the validators the documents validator dispatches those queries to,
 * built from only the attributes the list names. Those validators read nothing but the definition
 * of the attribute a query names, and none of the methods reaches a check of the documents
 * validator beyond them, so the list is valid here exactly when the documents validator of the
 * whole collection accepts it, and is refused with the same message when it does not.
 *
 * Nothing is kept between lists: every list is checked against the collection as it is passed.
 */
final class Narrow extends Base
{
    /**
     * The method type of each method a narrow list may hold.
     */
    private const array METHOD_TYPES = [
        Method::Equal->value => QueryBase::METHOD_TYPE_FILTER,
        Method::NotEqual->value => QueryBase::METHOD_TYPE_FILTER,
        Method::LessThan->value => QueryBase::METHOD_TYPE_FILTER,
        Method::LessThanEqual->value => QueryBase::METHOD_TYPE_FILTER,
        Method::GreaterThan->value => QueryBase::METHOD_TYPE_FILTER,
        Method::GreaterThanEqual->value => QueryBase::METHOD_TYPE_FILTER,
        Method::Between->value => QueryBase::METHOD_TYPE_FILTER,
        Method::NotBetween->value => QueryBase::METHOD_TYPE_FILTER,
        Method::StartsWith->value => QueryBase::METHOD_TYPE_FILTER,
        Method::NotStartsWith->value => QueryBase::METHOD_TYPE_FILTER,
        Method::EndsWith->value => QueryBase::METHOD_TYPE_FILTER,
        Method::NotEndsWith->value => QueryBase::METHOD_TYPE_FILTER,
        Method::Contains->value => QueryBase::METHOD_TYPE_FILTER,
        Method::ContainsAny->value => QueryBase::METHOD_TYPE_FILTER,
        Method::ContainsAll->value => QueryBase::METHOD_TYPE_FILTER,
        Method::NotContains->value => QueryBase::METHOD_TYPE_FILTER,
        Method::IsNull->value => QueryBase::METHOD_TYPE_FILTER,
        Method::IsNotNull->value => QueryBase::METHOD_TYPE_FILTER,
        Method::Regex->value => QueryBase::METHOD_TYPE_FILTER,
        Method::Limit->value => QueryBase::METHOD_TYPE_LIMIT,
        Method::Offset->value => QueryBase::METHOD_TYPE_OFFSET,
        Method::CursorAfter->value => QueryBase::METHOD_TYPE_CURSOR,
        Method::CursorBefore->value => QueryBase::METHOD_TYPE_CURSOR,
        Method::OrderAsc->value => QueryBase::METHOD_TYPE_ORDER,
        Method::OrderDesc->value => QueryBase::METHOD_TYPE_ORDER,
        Method::OrderRandom->value => QueryBase::METHOD_TYPE_ORDER,
    ];

    /**
     * @var array<string, QueryBase>
     */
    private array $byType = [];

    /**
     * @param  array<QueryBase>  $validators
     */
    private function __construct(array $validators)
    {
        parent::__construct($validators);

        foreach ($validators as $validator) {
            $this->byType[$validator->getMethodType()] = $validator;
        }
    }

    /**
     * A narrow list reaches none of the alias, join or aggregation handling of a query list and
     * holds no nested query, so each query only goes to the validator of its method type, in order,
     * once the order validator has dropped what an earlier list registered: of that, only joins
     * and aggregate aliases reach an undotted attribute. Any other value is checked as by any query
     * list validator.
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! \is_array($value) || ! self::accepts($value)) {
            return parent::isValid($value);
        }

        $order = $this->byType[QueryBase::METHOD_TYPE_ORDER] ?? null;
        if ($order instanceof Order) {
            $order->resetAggregationAliases();
            $order->resetJoinAliases();
        }

        /** @var array<Query> $value */
        foreach ($value as $query) {
            $method = $query->getMethod();
            $validator = $this->byType[self::METHOD_TYPES[$method->value] ?? ''] ?? null;

            if ($validator === null) {
                $this->message = 'Invalid query method: '.$method->value;

                return false;
            }

            if (! $validator->isValid($query)) {
                $this->message = 'Invalid query: '.$validator->getDescription();

                return false;
            }
        }

        return true;
    }

    /**
     * Whether the list is narrow: every value a Query of a method listed here, every filter and
     * every attribute order on an attribute without a dot.
     *
     * @param  array<mixed>  $queries
     */
    public static function accepts(array $queries): bool
    {
        foreach ($queries as $query) {
            if (self::methodType($query) === null) {
                return false;
            }
        }

        return true;
    }

    /**
     * The validator of a narrow query list, or null when the list is not narrow.
     *
     * @param  array<mixed>  $queries
     * @param  list<Attribute>  $attributes  The collection's attributes
     */
    public static function of(
        array $queries,
        array $attributes,
        Profile $profile,
        int $maxValuesCount,
    ): ?self {
        $types = [];
        $filtered = [];
        $ordered = [];
        foreach ($queries as $query) {
            $type = self::methodType($query);
            if ($type === null || ! $query instanceof Query) {
                return null;
            }
            $types[$type] = true;

            if ($type === QueryBase::METHOD_TYPE_FILTER) {
                $filtered[$query->getAttribute()] = true;
            } elseif ($type === QueryBase::METHOD_TYPE_ORDER && $query->getMethod() !== Method::OrderRandom) {
                $ordered[$query->getAttribute()] = true;
            }
        }

        $filterAttributes = [];
        $orderAttributes = [];
        if ($filtered !== [] || $ordered !== []) {
            foreach ([...$attributes, ...Documents::internalAttributes()] as $attribute) {
                if (isset($filtered[$attribute->key])) {
                    $filterAttributes[] = $attribute;
                }
                if (isset($ordered[$attribute->key])) {
                    $orderAttributes[] = $attribute;
                }
            }
        }

        $supportForAttributes = $profile->supports(Capability::DefinedAttributes);
        $limits = $profile->limits;
        $validators = [];
        if (isset($types[QueryBase::METHOD_TYPE_LIMIT])) {
            $validators[] = new Limit();
        }
        if (isset($types[QueryBase::METHOD_TYPE_OFFSET])) {
            $validators[] = new Offset();
        }
        if (isset($types[QueryBase::METHOD_TYPE_CURSOR])) {
            $validators[] = new Cursor($limits->uidLength);
        }
        if (isset($types[QueryBase::METHOD_TYPE_FILTER])) {
            $validators[] = new Filter(
                $filterAttributes,
                $limits->idType->value,
                $maxValuesCount,
                $limits->minDateTime,
                $limits->maxDateTime,
                $supportForAttributes,
                $profile->supports(Capability::UnsignedBigInt),
            );
        }
        if (isset($types[QueryBase::METHOD_TYPE_ORDER])) {
            $validators[] = new Order($orderAttributes, $supportForAttributes, $profile->supports(Capability::OrderRandom));
        }

        return new self($validators);
    }

    private static function methodType(mixed $query): ?string
    {
        if (! $query instanceof Query) {
            return null;
        }

        $method = $query->getMethod();
        $type = self::METHOD_TYPES[$method->value] ?? null;
        if ($type === null) {
            return null;
        }

        if (($type === QueryBase::METHOD_TYPE_FILTER || $method === Method::OrderAsc || $method === Method::OrderDesc) && \str_contains($query->getAttribute(), '.')) {
            return null;
        }

        return $type;
    }
}
