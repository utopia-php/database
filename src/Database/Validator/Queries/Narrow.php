<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Base;
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
final class Narrow extends Queries
{
    /**
     * The method type of each method a narrow list may hold.
     */
    private const array METHOD_TYPES = [
        Method::Equal->value => Base::METHOD_TYPE_FILTER,
        Method::NotEqual->value => Base::METHOD_TYPE_FILTER,
        Method::LessThan->value => Base::METHOD_TYPE_FILTER,
        Method::LessThanEqual->value => Base::METHOD_TYPE_FILTER,
        Method::GreaterThan->value => Base::METHOD_TYPE_FILTER,
        Method::GreaterThanEqual->value => Base::METHOD_TYPE_FILTER,
        Method::Between->value => Base::METHOD_TYPE_FILTER,
        Method::NotBetween->value => Base::METHOD_TYPE_FILTER,
        Method::StartsWith->value => Base::METHOD_TYPE_FILTER,
        Method::NotStartsWith->value => Base::METHOD_TYPE_FILTER,
        Method::EndsWith->value => Base::METHOD_TYPE_FILTER,
        Method::NotEndsWith->value => Base::METHOD_TYPE_FILTER,
        Method::Contains->value => Base::METHOD_TYPE_FILTER,
        Method::ContainsAny->value => Base::METHOD_TYPE_FILTER,
        Method::ContainsAll->value => Base::METHOD_TYPE_FILTER,
        Method::NotContains->value => Base::METHOD_TYPE_FILTER,
        Method::IsNull->value => Base::METHOD_TYPE_FILTER,
        Method::IsNotNull->value => Base::METHOD_TYPE_FILTER,
        Method::Regex->value => Base::METHOD_TYPE_FILTER,
        Method::Limit->value => Base::METHOD_TYPE_LIMIT,
        Method::Offset->value => Base::METHOD_TYPE_OFFSET,
        Method::CursorAfter->value => Base::METHOD_TYPE_CURSOR,
        Method::CursorBefore->value => Base::METHOD_TYPE_CURSOR,
        Method::OrderAsc->value => Base::METHOD_TYPE_ORDER,
        Method::OrderDesc->value => Base::METHOD_TYPE_ORDER,
        Method::OrderRandom->value => Base::METHOD_TYPE_ORDER,
    ];

    /**
     * @var array<string, Base>
     */
    private array $byType = [];

    /**
     * @param  array<Base>  $validators
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

        $order = $this->byType[Base::METHOD_TYPE_ORDER] ?? null;
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
     * The validator of a narrow query list, or null when the list is not narrow or a collection
     * attribute is not a Document.
     *
     * @param  array<mixed>  $queries
     * @param  array<mixed>  $attributes  The collection's attributes
     */
    public static function of(
        array $queries,
        array $attributes,
        Bounds $bounds,
        int $maxValuesCount,
        bool $supportForAttributes,
        bool $supportUnsignedBigInt,
        bool $supportForOrderRandom,
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

            if ($type === Base::METHOD_TYPE_FILTER) {
                $filtered[$query->getAttribute()] = true;
            } elseif ($type === Base::METHOD_TYPE_ORDER && $query->getMethod() !== Method::OrderRandom) {
                $ordered[$query->getAttribute()] = true;
            }
        }

        $filterAttributes = [];
        $orderAttributes = [];
        if ($filtered !== [] || $ordered !== []) {
            foreach ($attributes as $attribute) {
                if (! $attribute instanceof Document) {
                    return null;
                }

                $key = $attribute->getAttribute('key');

                if ($filtered !== []) {
                    $filterKey = $key ?? $attribute->getId();
                    if (! \is_string($filterKey) && ! \is_int($filterKey)) {
                        return null;
                    }
                    if (isset($filtered[$filterKey])) {
                        $filterAttributes[] = $attribute;
                    }
                }

                if ($ordered !== []) {
                    $orderKey = $key ?? $attribute->getAttribute(Document::ID) ?? '';
                    if (! \is_string($orderKey) && ! \is_int($orderKey)) {
                        return null;
                    }
                    if (isset($ordered[$orderKey])) {
                        $orderAttributes[] = $attribute;
                    }
                }
            }

            foreach (Documents::INTERNAL_ATTRIBUTES as $key => $definition) {
                if (isset($filtered[$key])) {
                    $filterAttributes[] = new Document($definition);
                }
                if (isset($ordered[$key])) {
                    $orderAttributes[] = new Document($definition);
                }
            }
        }

        $validators = [];
        if (isset($types[Base::METHOD_TYPE_LIMIT])) {
            $validators[] = new Limit();
        }
        if (isset($types[Base::METHOD_TYPE_OFFSET])) {
            $validators[] = new Offset();
        }
        if (isset($types[Base::METHOD_TYPE_CURSOR])) {
            $validators[] = new Cursor($bounds->maxUIDLength);
        }
        if (isset($types[Base::METHOD_TYPE_FILTER])) {
            $validators[] = new Filter(
                $filterAttributes,
                $bounds->idAttributeType,
                $maxValuesCount,
                $bounds->minDateTime,
                $bounds->maxDateTime,
                $supportForAttributes,
                $supportUnsignedBigInt,
            );
        }
        if (isset($types[Base::METHOD_TYPE_ORDER])) {
            $validators[] = new Order($orderAttributes, $supportForAttributes, $supportForOrderRandom);
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

        if (($type === Base::METHOD_TYPE_FILTER || $method === Method::OrderAsc || $method === Method::OrderDesc) && \str_contains($query->getAttribute(), '.')) {
            return null;
        }

        return $type;
    }
}
