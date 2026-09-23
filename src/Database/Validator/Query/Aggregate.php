<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates aggregate query methods ensuring the aggregated attribute exists in the schema.
 */
class Aggregate extends Base
{
    use JoinedAttributes;

    public const int MAX_ALIAS_LENGTH = 63;

    private const ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

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
     * @param  array<Document>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true, bool $sharedTables = false)
    {
        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));

            if (\is_string($key)) {
                $this->schema[$key] = true;
            }
        }

        $this->schema += self::internalColumns($sharedTables);

        $this->numeric = self::numericTypes($attributes);
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
            $alias = $aggregation->getValue('');
            if (\is_string($alias) && $alias !== '') {
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

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_AGGREGATE;
    }

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

        if (! $this->isValidOperand($query)) {
            return false;
        }

        $alias = $query->getValues()[0] ?? null;

        if ($alias !== null && (! \is_string($alias) || \preg_match(self::ALIAS_PATTERN, $alias) !== 1)) {
            $this->message = 'Invalid aggregate alias';

            return false;
        }

        if (\is_string($alias) && \strlen($alias) > self::MAX_ALIAS_LENGTH) {
            $this->message = 'Aggregate alias is too long: at most '.self::MAX_ALIAS_LENGTH.' characters are allowed';

            return false;
        }

        if (\is_string($alias) && ($this->aliases[$alias] ?? 0) > 1) {
            $this->message = 'Aggregate alias "'.$alias.'" is given to more than one aggregate';

            return false;
        }

        if (\is_string($alias) && isset($this->groups[$alias])) {
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

        if ($bitwise && ! Attribute::isIntegerType($type)) {
            $this->message = 'Aggregate '.$method->value.' requires an integer attribute that is not an array: '.$attribute;

            return false;
        }

        return true;
    }

    /**
     * The type of each attribute that holds a single number.
     *
     * @param  array<Document>  $attributes
     * @return array<string, ColumnType>
     */
    public static function numericTypes(array $attributes): array
    {
        $types = [];

        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));
            $type = $attribute->getAttribute('type');

            if (! \is_string($key) || ! ($type instanceof ColumnType || \is_string($type)) || (bool) $attribute->getAttribute('array', false)) {
                continue;
            }

            $type = Attribute::tryNormalizeType($type);

            if ($type !== null && Attribute::isNumericType($type)) {
                $types[$key] = $type;
            }
        }

        return $types;
    }

    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]);
    }
}
