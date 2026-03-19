<?php

namespace Utopia\Database;

use Closure;
use Utopia\Query\Builder;
use Utopia\Query\Builder\BuildResult;

class QueryBuilder
{
    private Database $db;

    private string $collection;

    private ?Builder $builder = null;

    /** @var array<Query> */
    private array $filters = [];

    /** @var array<string> */
    private array $selections = [];

    private ?int $limitValue = null;

    private ?int $offsetValue = null;

    /** @var array<string> */
    private array $orderAttributes = [];

    /** @var array<string> */
    private array $orderDirections = [];

    /** @var array<string> */
    private array $groupByColumns = [];

    /** @var array<Query> */
    private array $havingQueries = [];

    /** @var array<string> */
    private array $eagerLoadRelations = [];

    public function __construct(Database $db, string $collection)
    {
        $this->db = $db;
        $this->collection = $collection;
    }

    public function getBuilder(): Builder
    {
        if ($this->builder === null) {
            $this->builder = $this->db->getAdapter()->newQueryBuilder($this->collection);
        }

        return $this->builder;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function filter(array $queries): static
    {
        $this->filters = \array_merge($this->filters, $queries);

        return $this;
    }

    public function where(string $attribute, mixed $value): static
    {
        $this->filters[] = Query::equal($attribute, \is_array($value) ? $value : [$value]);

        return $this;
    }

    public function whereNot(string $attribute, mixed $value): static
    {
        $this->filters[] = Query::notEqual($attribute, \is_array($value) ? $value : [$value]);

        return $this;
    }

    public function whereGreaterThan(string $attribute, mixed $value): static
    {
        $this->filters[] = Query::greaterThan($attribute, $value);

        return $this;
    }

    public function whereLessThan(string $attribute, mixed $value): static
    {
        $this->filters[] = Query::lessThan($attribute, $value);

        return $this;
    }

    public function whereBetween(string $attribute, mixed $start, mixed $end): static
    {
        $this->filters[] = Query::between($attribute, $start, $end);

        return $this;
    }

    public function whereContains(string $attribute, mixed $value): static
    {
        $this->filters[] = Query::containsAny($attribute, \is_array($value) ? $value : [$value]);

        return $this;
    }

    public function whereIsNull(string $attribute): static
    {
        $this->filters[] = Query::isNull($attribute);

        return $this;
    }

    public function whereIsNotNull(string $attribute): static
    {
        $this->filters[] = Query::isNotNull($attribute);

        return $this;
    }

    public function search(string $attribute, string $value): static
    {
        $this->filters[] = Query::search($attribute, $value);

        return $this;
    }

    /**
     * @param  array<string>  $columns
     */
    public function select(array $columns): static
    {
        $this->selections = $columns;

        return $this;
    }

    public function selectRaw(string $expression, array $bindings = []): static
    {
        $this->getBuilder()->selectRaw($expression, $bindings);

        return $this;
    }

    public function distinct(): static
    {
        $this->getBuilder()->distinct();

        return $this;
    }

    public function limit(int $limit): static
    {
        $this->limitValue = $limit;

        return $this;
    }

    public function offset(int $offset): static
    {
        $this->offsetValue = $offset;

        return $this;
    }

    public function orderAsc(string $attribute): static
    {
        $this->orderAttributes[] = $attribute;
        $this->orderDirections[] = 'asc';

        return $this;
    }

    public function orderDesc(string $attribute): static
    {
        $this->orderAttributes[] = $attribute;
        $this->orderDirections[] = 'desc';

        return $this;
    }

    public function orderRandom(): static
    {
        $this->getBuilder()->sortRandom();

        return $this;
    }

    /**
     * @param  array<string>  $attributes
     */
    public function groupBy(array $attributes): static
    {
        $this->groupByColumns = $attributes;

        return $this;
    }

    /**
     * @param  array<Query>  $conditions
     */
    public function having(array $conditions): static
    {
        $this->havingQueries = $conditions;

        return $this;
    }

    /**
     * @param  array<string>  $relations
     */
    public function eagerLoad(array $relations): static
    {
        $this->eagerLoadRelations = $relations;

        return $this;
    }

    public function join(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->getBuilder()->join($table, $left, $right, $operator);

        return $this;
    }

    public function leftJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->getBuilder()->leftJoin($table, $left, $right, $operator);

        return $this;
    }

    public function rightJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->getBuilder()->rightJoin($table, $left, $right, $operator);

        return $this;
    }

    public function crossJoin(string $table): static
    {
        $this->getBuilder()->crossJoin($table);

        return $this;
    }

    public function naturalJoin(string $table): static
    {
        $this->getBuilder()->naturalJoin($table);

        return $this;
    }

    public function joinWhere(string $table, Closure $callback): static
    {
        $this->getBuilder()->joinWhere($table, $callback);

        return $this;
    }

    public function union(self $other): static
    {
        $this->getBuilder()->union($other->getBuilder());

        return $this;
    }

    public function unionAll(self $other): static
    {
        $this->getBuilder()->unionAll($other->getBuilder());

        return $this;
    }

    public function intersect(self $other): static
    {
        $this->getBuilder()->intersect($other->getBuilder());

        return $this;
    }

    public function except(self $other): static
    {
        $this->getBuilder()->except($other->getBuilder());

        return $this;
    }

    public function with(string $name, self $query): static
    {
        $this->getBuilder()->with($name, $query->getBuilder());

        return $this;
    }

    public function withRecursive(string $name, self $query): static
    {
        $this->getBuilder()->withRecursive($name, $query->getBuilder());

        return $this;
    }

    public function filterWhereIn(string $column, self $subquery): static
    {
        $this->getBuilder()->filterWhereIn($column, $subquery->getBuilder());

        return $this;
    }

    public function filterWhereNotIn(string $column, self $subquery): static
    {
        $this->getBuilder()->filterWhereNotIn($column, $subquery->getBuilder());

        return $this;
    }

    public function selectSub(self $subquery, string $alias): static
    {
        $this->getBuilder()->selectSub($subquery->getBuilder(), $alias);

        return $this;
    }

    /**
     * @param  array<string>|null  $partitionBy
     * @param  array<string>|null  $orderBy
     */
    public function selectWindow(string $function, string $alias, ?array $partitionBy = null, ?array $orderBy = null): static
    {
        $this->getBuilder()->selectWindow($function, $alias, $partitionBy, $orderBy);

        return $this;
    }

    /**
     * @param  array<string>|null  $partitionBy
     * @param  array<string>|null  $orderBy
     */
    public function window(string $name, ?array $partitionBy = null, ?array $orderBy = null): static
    {
        $this->getBuilder()->window($name, $partitionBy, $orderBy);

        return $this;
    }

    public function forUpdate(): static
    {
        $builder = $this->getBuilder();
        if (\method_exists($builder, 'forUpdate')) {
            $builder->forUpdate();
        }

        return $this;
    }

    public function forShare(): static
    {
        $builder = $this->getBuilder();
        if (\method_exists($builder, 'forShare')) {
            $builder->forShare();
        }

        return $this;
    }

    public function when(bool $condition, Closure $callback): static
    {
        if ($condition) {
            $callback($this);
        }

        return $this;
    }

    public function countAggregate(string $attribute = '*', string $alias = ''): static
    {
        $this->getBuilder()->count($attribute, $alias);

        return $this;
    }

    public function sumAggregate(string $attribute, string $alias = ''): static
    {
        $this->getBuilder()->sum($attribute, $alias);

        return $this;
    }

    public function avgAggregate(string $attribute, string $alias = ''): static
    {
        $this->getBuilder()->avg($attribute, $alias);

        return $this;
    }

    public function minAggregate(string $attribute, string $alias = ''): static
    {
        $this->getBuilder()->min($attribute, $alias);

        return $this;
    }

    public function maxAggregate(string $attribute, string $alias = ''): static
    {
        $this->getBuilder()->max($attribute, $alias);

        return $this;
    }

    /**
     * @return array<Query>
     */
    public function buildQueries(): array
    {
        $queries = $this->filters;

        if ($this->selections !== []) {
            $queries[] = Query::select($this->selections);
        }

        if ($this->limitValue !== null) {
            $queries[] = Query::limit($this->limitValue);
        }

        if ($this->offsetValue !== null) {
            $queries[] = Query::offset($this->offsetValue);
        }

        foreach ($this->orderAttributes as $i => $attr) {
            $dir = $this->orderDirections[$i] ?? 'asc';
            $queries[] = $dir === 'desc' ? Query::orderDesc($attr) : Query::orderAsc($attr);
        }

        if ($this->groupByColumns !== []) {
            $queries[] = Query::groupBy($this->groupByColumns);
        }

        foreach ($this->havingQueries as $query) {
            $queries[] = $query;
        }

        return $queries;
    }

    public function build(): BuildResult
    {
        $builder = $this->getBuilder();

        if ($this->filters !== []) {
            $builder->filter($this->filters);
        }

        if ($this->selections !== []) {
            $builder->select($this->selections);
        }

        if ($this->limitValue !== null) {
            $builder->limit($this->limitValue);
        }

        if ($this->offsetValue !== null) {
            $builder->offset($this->offsetValue);
        }

        foreach ($this->orderAttributes as $i => $attr) {
            $dir = $this->orderDirections[$i] ?? 'asc';
            $dir === 'desc' ? $builder->sortDesc($attr) : $builder->sortAsc($attr);
        }

        if ($this->groupByColumns !== []) {
            $builder->groupBy($this->groupByColumns);
        }

        if ($this->havingQueries !== []) {
            $builder->having($this->havingQueries);
        }

        return $builder->build();
    }

    public function toRawSql(): string
    {
        return $this->build()->query;
    }

    public function explain(bool $analyze = false): BuildResult
    {
        $this->build();

        return $this->getBuilder()->explain($analyze);
    }

    /**
     * @return array<Document>
     */
    public function get(): array
    {
        return $this->db->find($this->collection, $this->buildQueries());
    }

    /**
     * @return array<Document>
     */
    public function raw(): array
    {
        $result = $this->build();

        return $this->db->rawQuery($result->query, $result->bindings);
    }

    public function first(): Document
    {
        $this->limitValue = 1;
        $results = $this->get();

        return $results[0] ?? new Document();
    }

    public function count(): int
    {
        return $this->db->count($this->collection, $this->filters);
    }

    public function sum(string $attribute): float|int
    {
        return $this->db->sum($this->collection, $attribute, $this->filters);
    }

    /**
     * @return \Generator<int, Document>
     */
    public function cursor(int $batchSize = 100): \Generator
    {
        $lastDocument = null;

        while (true) {
            $queries = $this->filters;
            $queries[] = Query::limit($batchSize);

            if ($lastDocument !== null) {
                $queries[] = Query::cursorAfter($lastDocument);
            }

            foreach ($this->orderAttributes as $i => $attr) {
                $dir = $this->orderDirections[$i] ?? 'asc';
                $queries[] = $dir === 'desc' ? Query::orderDesc($attr) : Query::orderAsc($attr);
            }

            $documents = $this->db->find($this->collection, $queries);

            if ($documents === []) {
                break;
            }

            foreach ($documents as $document) {
                yield $document;
            }

            $lastDocument = \end($documents);

            if (\count($documents) < $batchSize) {
                break;
            }
        }
    }

    public function getCollection(): string
    {
        return $this->collection;
    }

    public function getDatabase(): Database
    {
        return $this->db;
    }
}
