<?php

namespace Utopia\Database\Trait;

use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query as BaseQuery;

trait Cursors
{
    private const array MIRRORED_OPERATORS = [
        '<' => '>',
        '<=' => '>=',
        '>' => '<',
        '>=' => '<=',
    ];

    /**
     * The cursor a list pages from when it names a document by its ID: the document's values of the list's orders,
     * which cursorAfter() or cursorBefore() in the same list reads as the row that ID stands for.
     *
     * The document is read with authorization skipped, so a list can page past a document its caller cannot read; the
     * cursor carries only `$id`, `$sequence`, `$collection` and the values the list orders by. An order on a joined
     * attribute takes its value from the joined rows the caller can read that the list pairs with the document: a page
     * after the document starts behind the last of them in the list's order, a page before it ends ahead of the first,
     * and without such a row the value is null, as it is in the list.
     *
     * The order values, joined ones included, come from or are chosen by a document the caller may not be allowed to
     * read, and the caller picks which attributes the list orders by. Pass the cursor only to cursorAfter() or
     * cursorBefore(); never return it, or any of its values, to an end user.
     *
     * @param  array<Query>  $queries  The list's queries; its cursor queries are left out
     * @return Document Empty when the collection holds no document with the ID
     *
     * @throws NotFoundException When the collection does not exist
     * @throws QueryException When the queries are invalid
     * @throws DatabaseException
     */
    public function resolveCursor(string $collection, string $id, CursorDirection $direction, array $queries = []): Document
    {
        $definition = $this->silent(fn (): Collection => $this->getCollection($collection));

        $this->checkQueryTypes($queries);
        $queries = $this->rewriteRelationshipJoins($definition, \array_values(\array_filter(
            $queries,
            static fn (Query $query): bool => $query->getMethod() !== Method::CursorAfter && $query->getMethod() !== Method::CursorBefore,
        )));

        $joinedById = $this->resolveJoinedCollections($queries);
        if ($this->validation()->get()) {
            $this->validateDocumentsQueries($definition, $queries, $joinedById);
        }

        $parsed = Query::groupByType($queries);
        $joins = $parsed->joins;

        if ($joins !== [] && ! $this->adapter->supports(Capability::Joins)) {
            throw new QueryException('Join queries are not supported by this adapter');
        }

        $this->assertJoinCount($joins);

        $document = $this->authorization->skip(fn (): Document => $this->skipRelationships(fn (): Document => $this->readDocument($definition, $id)));
        if ($document->isEmpty()) {
            return new Document();
        }

        $cursor = new Document([
            Document::ID => $document->getId(),
            Document::SEQUENCE => $document->getSequence(),
            Document::COLLECTION => $definition->getId(),
        ]);

        if (\in_array(OrderDirection::Random, $parsed->orderTypes, true)) {
            return $cursor;
        }

        $joinedCollections = $this->joinedCollectionsByAlias($joins, $joinedById);
        $orderAttributes = $parsed->orderAttributes;
        $orderTypes = $parsed->orderTypes;

        if ($joinedCollections !== []) {
            [$orderAttributes] = $this->qualifyJoinedOrders($definition, $orderAttributes, $cursor, $joinedCollections);
        }

        if (! $parsed->distinct) {
            [$orderAttributes, $orderTypes] = $this->addTieBreaks($orderAttributes, $orderTypes, $parsed->filters, $joins, $joinedCollections, true, $parsed->selections);
        }

        $joinedOrders = [];
        foreach ($orderAttributes as $index => $attribute) {
            [$alias, $name] = $this->orderOwner($definition, $attribute, $joinedCollections);
            if ($alias === Query::DEFAULT_ALIAS) {
                $cursor->setAttribute($attribute, $document->getAttribute($attribute));

                continue;
            }

            $descending = ($orderTypes[$index] ?? OrderDirection::Asc) === OrderDirection::Desc;
            $joinedOrders[$alias][] = $descending !== ($direction === CursorDirection::After)
                ? Query::orderDesc($name)
                : Query::orderAsc($name);
        }

        if ($joinedOrders === []) {
            return $cursor;
        }

        $joinsByAlias = [];
        foreach ($joins as $join) {
            $joinsByAlias[$join->getAlias()] = $join;
        }

        $rows = [];
        foreach ($this->pairedJoins($joinsByAlias, $joinedOrders) as $alias => $join) {
            $rows[$alias] = $this->pairedRow($join, $alias, $document, $rows, $parsed->filters, $joinedOrders[$alias] ?? []);
        }

        foreach ($joinedOrders as $alias => $orders) {
            $row = $rows[$alias];
            foreach ($orders as $order) {
                $name = $order->getAttribute();
                $cursor->setAttribute($alias.'.'.$name, $row->isEmpty() ? null : $row->getAttribute($name));
            }
        }

        return $cursor;
    }

    /**
     * @param  array<string, Document>  $joinedCollections
     * @return array{0: string, 1: string}
     */
    private function orderOwner(Collection $collection, string $attribute, array $joinedCollections): array
    {
        $dot = \strpos($attribute, '.');
        if ($dot === false || ! isset($joinedCollections[\substr($attribute, 0, $dot)]) || $this->declaresSumAttribute($collection, $attribute)) {
            return [Query::DEFAULT_ALIAS, $attribute];
        }

        return [\substr($attribute, 0, $dot), \substr($attribute, $dot + 1)];
    }

    /**
     * Keeps the list's join order: a paired row reads the rows paired for the joins before it.
     *
     * @param  array<string, Query>  $joins
     * @param  array<string, list<Query>>  $orders
     * @return array<string, Query>
     */
    private function pairedJoins(array $joins, array $orders): array
    {
        $required = \array_fill_keys(\array_keys($orders), true);

        foreach (\array_reverse($joins, true) as $alias => $join) {
            if (! isset($required[$alias])) {
                continue;
            }

            foreach (self::joinConditions($join) as [$left, , $right]) {
                foreach ([self::columnOwner($left, Query::DEFAULT_ALIAS)[0], self::columnOwner($right, $alias)[0]] as $referenced) {
                    if ($referenced !== $alias && isset($joins[$referenced])) {
                        $required[$referenced] = true;
                    }
                }
            }
        }

        return \array_intersect_key($joins, $required);
    }

    /**
     * Read with the caller's permissions, so the row is the one the caller's list pairs with the document.
     *
     * @param  array<string, Document>  $rows  The rows resolved for earlier joins, by alias
     * @param  array<Query>  $filters
     * @param  list<Query>  $orders
     */
    private function pairedRow(Query $join, string $alias, Document $document, array $rows, array $filters, array $orders): Document
    {
        $conditions = [];
        foreach (self::joinConditions($join) as [$left, $operator, $right]) {
            [$leftAlias, $leftAttribute] = self::columnOwner($left, Query::DEFAULT_ALIAS);
            [$rightAlias, $rightAttribute] = self::columnOwner($right, $alias);

            if ($rightAlias === $alias && $leftAlias !== $alias) {
                $condition = self::comparison($rightAttribute, self::MIRRORED_OPERATORS[$operator] ?? $operator, self::pairedValue($leftAlias, $leftAttribute, $document, $rows));
            } elseif ($leftAlias === $alias && $rightAlias !== $alias) {
                $condition = self::comparison($leftAttribute, $operator, self::pairedValue($rightAlias, $rightAttribute, $document, $rows));
            } else {
                continue;
            }

            if ($condition === null) {
                return new Document();
            }

            $conditions[] = $condition;
        }

        foreach ([...$filters, ...$join->getJoinOnQueries()] as $filter) {
            $condition = self::aliasFilter($filter, $alias);
            if ($condition !== null) {
                $conditions[] = $condition;
            }
        }

        $found = $this->skipRelationships(fn (): array => $this->find($join->getAttribute(), [...$conditions, ...$orders, Query::limit(1)]));

        return $found[0] ?? new Document();
    }

    /**
     * @return list<array{0: string, 1: string, 2: string}>
     */
    private static function joinConditions(Query $join): array
    {
        $conditions = [];
        foreach ($join->getJoinOnQueries() as $on) {
            if ($on->getMethod() !== Method::On) {
                continue;
            }

            [$left, $operator, $right] = \array_pad($on->getValues(), 3, null);
            if (\is_string($left) && \is_string($operator) && \is_string($right)) {
                $conditions[] = [$left, $operator, $right];
            }
        }

        return $conditions;
    }

    /**
     * @return array{0: string, 1: string}
     */
    private static function columnOwner(string $column, string $alias): array
    {
        $dot = \strpos($column, '.');
        if ($dot === false) {
            return [$alias, $column];
        }

        return [\substr($column, 0, $dot), \substr($column, $dot + 1)];
    }

    /**
     * @param  array<string, Document>  $rows
     */
    private static function pairedValue(string $alias, string $attribute, Document $document, array $rows): mixed
    {
        $value = $alias === Query::DEFAULT_ALIAS
            ? $document->getAttribute($attribute)
            : ($rows[$alias] ?? new Document())->getAttribute($attribute);

        return $value instanceof Document ? $value->getId() : $value;
    }

    /**
     * Null when no row can satisfy the condition.
     *
     * @throws QueryException
     */
    private static function comparison(string $attribute, string $operator, mixed $value): ?Query
    {
        if (! \is_scalar($value)) {
            return null;
        }

        return match ($operator) {
            '=' => Query::equal($attribute, [$value]),
            '!=', '<>' => Query::notEqual($attribute, $value),
            '<' => Query::lessThan($attribute, $value),
            '<=' => Query::lessThanEqual($attribute, $value),
            '>' => Query::greaterThan($attribute, $value),
            '>=' => Query::greaterThanEqual($attribute, $value),
            default => throw new QueryException('Invalid join operator: '.$operator),
        };
    }

    /**
     * Null unless the filter constrains only this alias.
     */
    private static function aliasFilter(BaseQuery $filter, string $alias): ?Query
    {
        $method = $filter->getMethod();

        if ($method === Method::And || $method === Method::Or) {
            $children = [];
            foreach ($filter->getValues() as $child) {
                $condition = $child instanceof BaseQuery ? self::aliasFilter($child, $alias) : null;
                if ($condition === null) {
                    return null;
                }
                $children[] = $condition;
            }

            return $children === [] ? null : new Query($method, values: $children);
        }

        if (! $method->isFilter() && ! $method->isSpatial() && ! $method->isJson() && $method !== Method::ElemMatch && $method !== Method::ContainsAll) {
            return null;
        }

        [$owner, $attribute] = self::columnOwner($filter->getAttribute(), Query::DEFAULT_ALIAS);
        if ($owner !== $alias) {
            return null;
        }

        $values = \array_map(
            static fn (mixed $value): mixed => $value instanceof BaseQuery ? clone $value : $value,
            $filter->getValues(),
        );

        return new Query($method, $attribute, $values);
    }
}
