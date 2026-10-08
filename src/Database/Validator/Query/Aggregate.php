<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Query\Joined\Attributes;
use Utopia\Database\Validator\Query\Joined\Collection;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates aggregate query methods ensuring the aggregated attribute exists in the schema.
 */
class Aggregate extends Base
{
    use Attributes;

    public const int MAX_ALIAS_LENGTH = 63;

    private const string ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    private const array NUMERIC_METHODS = [
        Method::Sum,
        Method::Avg,
        Method::Stddev,
        Method::StddevPop,
        Method::StddevSamp,
        Method::Variance,
        Method::VarPop,
        Method::VarSamp,
    ];

    private const array BITWISE_METHODS = [
        Method::BitAnd,
        Method::BitOr,
        Method::BitXor,
    ];

    private const array EXTREMUM_METHODS = [
        Method::Min,
        Method::Max,
    ];

    /**
     * Types whose values some engine cannot compare for min() and max(): PostgreSQL stores them as
     * BOOLEAN, JSONB, GEOMETRY and VECTOR, for which it has neither.
     */
    private const array UNORDERED_TYPES = [
        ColumnType::Boolean,
        ColumnType::Json,
        ColumnType::Object,
        ColumnType::Point,
        ColumnType::Linestring,
        ColumnType::Polygon,
        ColumnType::Vector,
    ];

    /**
     * @var array<string, true>
     */
    protected array $schema = [];

    /**
     * The type of each attribute that holds a single number.
     *
     * @var array<string, ColumnType>
     */
    protected array $numeric = [];

    /**
     * The attributes whose values min() and max() cannot order: arrays and the unordered types.
     *
     * @var array<string, true>
     */
    protected array $unordered = [];

    /**
     * Every attribute of the collection, and whether it holds a column.
     *
     * @var array<string, bool>
     */
    protected array $columns = [];

    /**
     * How many aggregates of the query set carry each alias.
     *
     * @var array<string, int>
     */
    protected array $aliases = [];

    /**
     * The attribute each group of the query set is returned under, keyed by the name it takes.
     *
     * @var array<string, string>
     */
    protected array $groups = [];

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true, bool $sharedTables = false)
    {
        $attributes = \array_map(
            static fn (Attribute|Document $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
            $attributes,
        );

        foreach ($attributes as $attribute) {
            $this->schema[$attribute->key] = true;

            if (! $attribute->array && $attribute->isNumeric()) {
                $this->numeric[$attribute->key] = $attribute->type;
            }

            if (! self::isOrdered($attribute->type, $attribute->array)) {
                $this->unordered[$attribute->key] = true;
            }
        }

        $this->schema += self::internalColumns($sharedTables);
        $this->columns = Collection::columns($attributes);
    }

    /**
     * The aggregates of the query set: an alias names one column of the result, so no two of them
     * can share it.
     *
     * @param  array<BaseQuery>  $aggregations
     */
    public function setAggregations(array $aggregations): void
    {
        $this->aliases = [];

        foreach ($aggregations as $aggregation) {
            $alias = $aggregation->getAlias();
            if ($alias !== '') {
                $this->aliases[$alias] = ($this->aliases[$alias] ?? 0) + 1;
            }
        }
    }

    /**
     * The attributes the query set groups by. Each group comes back under its column's name, the
     * attribute's own name or, for an internal attribute, its column, so an alias cannot take it.
     *
     * @param  array<mixed>  $attributes
     */
    public function setGroupBy(array $attributes): void
    {
        $this->groups = [];

        foreach ($attributes as $attribute) {
            if (! \is_string($attribute) || $attribute === '') {
                continue;
            }

            $dot = \strpos($attribute, '.');
            $name = $dot === false ? $attribute : \substr($attribute, $dot + 1);
            $this->groups[$name] ??= $attribute;
            $this->groups[Storage::column($name)] ??= $attribute;
        }
    }

    #[\Override]
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_AGGREGATE;
    }

    #[\Override]
    protected function isValidQuery(Query $query): bool
    {
        $attribute = $query->getAttribute();

        if ($attribute === '*' && $query->getMethod() !== Method::Count) {
            $this->message = 'Only count can aggregate "*"';

            return false;
        }

        if (
            $attribute !== '*'
            && $this->supportForAttributes
            && ! isset($this->schema[$attribute])
            && ! $this->isJoinedAttribute($attribute)
        ) {
            return false;
        }

        if (($this->columns[$attribute] ?? true) === false) {
            $this->message = 'Cannot aggregate virtual relationship attribute: '.$attribute;

            return false;
        }

        if (! $this->isValidOperand($query) || ! $this->isValidExtremum($query)) {
            return false;
        }

        $alias = $query->getAlias();
        if ($alias === '') {
            return true;
        }

        if (\preg_match(self::ALIAS_PATTERN, $alias) !== 1) {
            $this->message = 'Invalid aggregate alias';

            return false;
        }

        if (\strlen($alias) > self::MAX_ALIAS_LENGTH) {
            $this->message = 'Aggregate alias is too long: at most '.self::MAX_ALIAS_LENGTH.' characters are allowed';

            return false;
        }

        if (($this->aliases[$alias] ?? 0) > 1) {
            $this->message = 'Aggregate alias "'.$alias.'" is given to more than one aggregate';

            return false;
        }

        if (isset($this->groups[$alias])) {
            $this->message = 'Aggregate alias "'.$alias.'" is the name the groupBy attribute "'.$this->groups[$alias].'" is returned under';

            return false;
        }

        return true;
    }

    /**
     * Arithmetic aggregates need a number and the bitwise ones an integer, as the collection the
     * attribute resolves to declares it: this one, or the join it names. An attribute that
     * collection does not declare, a schemaless one or one of a join whose collection is unknown,
     * has no type to check here.
     */
    private function isValidOperand(Query $query): bool
    {
        $method = $query->getMethod();
        $attribute = $query->getAttribute();
        $bitwise = \in_array($method, self::BITWISE_METHODS, true);

        if (! $bitwise && ! \in_array($method, self::NUMERIC_METHODS, true)) {
            return true;
        }

        if (isset($this->schema[$attribute])) {
            $type = $this->numeric[$attribute] ?? null;
        } else {
            $join = $this->joinOf($attribute);
            $dot = \strpos($attribute, '.');
            $column = $dot === false ? $attribute : \substr($attribute, $dot + 1);

            if ($join !== null && isset($join->attributes[$column])) {
                $type = $join->numeric[$column] ?? null;
            } elseif ($join !== null && $this->isJoinedInternalAttribute($column)) {
                $type = $this->numeric[$column] ?? null;
            } else {
                return true;
            }
        }

        if ($type === null) {
            $this->message = 'Aggregate '.$method->value.' requires a numeric attribute that is not an array: '.$attribute;

            return false;
        }

        if ($bitwise && $type !== ColumnType::Integer && $type !== ColumnType::BigInteger) {
            $this->message = 'Aggregate '.$method->value.' requires an integer attribute that is not an array: '.$attribute;

            return false;
        }

        return true;
    }

    /**
     * min() and max() need values the engine can order, as the collection the attribute resolves to
     * declares them. An attribute with no known definition has no type to check here.
     */
    private function isValidExtremum(Query $query): bool
    {
        $method = $query->getMethod();
        $attribute = $query->getAttribute();

        if (! \in_array($method, self::EXTREMUM_METHODS, true)) {
            return true;
        }

        if (isset($this->schema[$attribute])) {
            $ordered = ! isset($this->unordered[$attribute]);
        } else {
            $join = $this->joinOf($attribute);
            $dot = \strpos($attribute, '.');
            $column = $dot === false ? $attribute : \substr($attribute, $dot + 1);
            $definition = $join?->schema[$column] ?? null;

            if ($definition === null) {
                return true;
            }

            $type = $definition['type'] ?? null;
            $ordered = self::isOrdered($type instanceof ColumnType ? $type : null, (bool) ($definition['array'] ?? false));
        }

        if (! $ordered) {
            $this->message = 'Aggregate '.$method->value.' requires an attribute whose values are ordered, not an array, object, boolean, spatial or vector one: '.$attribute;

            return false;
        }

        return true;
    }

    private static function isOrdered(?ColumnType $type, bool $array): bool
    {
        return ! $array && ! \in_array($type, self::UNORDERED_TYPES, true);
    }

    #[\Override]
    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]);
    }
}
