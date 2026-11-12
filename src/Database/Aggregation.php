<?php

namespace Utopia\Database;

use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Schema;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * Shapes a row as the engine reads it into the row aggregate() returns: each aggregate typed by its method and the
 * attribute it reads, each group named, typed and decoded as a read of the same attribute returns it.
 */
final readonly class Aggregation
{
    private const array COUNTS = [
        Method::Count,
        Method::CountDistinct,
        Method::BitAnd,
        Method::BitOr,
        Method::BitXor,
    ];

    private const float INTEGER_LIMIT = 9223372036854775808.0;

    private const array STATISTICS = [
        Method::Avg,
        Method::Stddev,
        Method::StddevPop,
        Method::StddevSamp,
        Method::Variance,
        Method::VarPop,
        Method::VarSamp,
    ];

    private const array INTEGERS = [
        ColumnType::TinyInteger,
        ColumnType::SmallInteger,
        ColumnType::Integer,
        ColumnType::BigInteger,
        ColumnType::Serial,
        ColumnType::SmallSerial,
        ColumnType::BigSerial,
    ];

    private const array FLOATS = [
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Decimal,
    ];

    private const array DATETIMES = [
        ColumnType::Datetime,
        ColumnType::Timestamp,
    ];

    private const array OBJECTS = [
        ColumnType::Json,
        ColumnType::Object,
    ];

    /**
     * @param  array<string, Method>  $aggregates  The method of each aggregate, keyed by its alias
     * @param  array<string, Attribute>  $operands  The attribute each aggregate reads, keyed by its alias
     * @param  array<string, string>  $groups  The name a read returns each group under, keyed as the queries name it
     * @param  array<string, Attribute>  $attributes  The attribute of each group but the tenant, which a read returns as the adapter reads it, keyed by the name a read returns it under
     * @param  array<string, string>  $sources  The join alias each group is read from, '' for the collection read, keyed by the name a read returns it under
     * @param  array<string, true>  $internal  The keys of the attributes every document carries
     */
    private function __construct(
        private Database $database,
        private array $aggregates,
        private array $operands,
        private array $groups,
        private array $attributes,
        private array $sources,
        private array $internal,
    ) {
    }

    /**
     * @param  Database  $database  The database whose filters decode the groups
     * @param  Document  $collection  The collection read, with its attribute documents
     * @param  array<string, Document>  $joined  The collection each join reads, keyed by join alias
     * @param  array<Query>  $queries  The queries the rows were aggregated by, each aggregate aliased
     */
    public static function from(Database $database, Document $collection, array $joined, array $queries): self
    {
        $internal = $database->internalAttributes();

        $schema = [];
        $origins = [];
        foreach ([...$internal, ...self::attributes($collection)] as $attribute) {
            $schema[$attribute->key] = $attribute;
            $origins[$attribute->key] = '';
        }
        foreach ($joined as $alias => $definition) {
            foreach ([...$internal, ...self::attributes($definition)] as $attribute) {
                $schema[$alias.'.'.$attribute->key] = $attribute;
                $origins[$alias.'.'.$attribute->key] = $alias;
            }
        }

        $aggregates = [];
        $operands = [];
        $groups = [];
        $attributes = [];
        $sources = [];
        foreach ($queries as $query) {
            $method = $query->getMethod();
            if ($method->isAggregate()) {
                $alias = $query->getAlias();
                $aggregates[$alias] = $method;
                $operand = $schema[self::name($schema, $query->getAttribute())] ?? null;
                if ($operand !== null) {
                    $operands[$alias] = $operand;
                }

                continue;
            }

            if ($method !== Method::GroupBy) {
                continue;
            }

            foreach ($query->getValues() as $group) {
                if (! \is_string($group) || $group === '') {
                    continue;
                }

                $name = self::name($schema, $group);
                $groups[$group] = $name;
                if (isset($schema[$name]) && $schema[$name]->key !== Document::TENANT) {
                    $attributes[$name] = $schema[$name];
                    $sources[$name] = $origins[$name];
                }
            }
        }

        $keys = [];
        foreach ($internal as $attribute) {
            $keys[$attribute->key] = true;
        }

        return new self($database, $aggregates, $operands, $groups, $attributes, $sources, $keys);
    }

    /**
     * @param  array<string, mixed>  $row  A row as the adapter reads it
     * @return array<string, mixed>
     *
     * @throws DatabaseException When a group's filter cannot decode it
     */
    public function decode(array $row): array
    {
        $names = $this->names(\array_keys($row));

        $shaped = [];
        $filtered = [];
        foreach ($row as $column => $value) {
            $method = $this->aggregates[$column] ?? null;
            if ($method !== null) {
                $shaped[$column] = $this->aggregate($method, $this->operands[$column] ?? null, $value);

                continue;
            }

            $name = $names[$column] ?? $column;
            $attribute = $this->attributes[$name] ?? null;
            if ($attribute === null) {
                $shaped[$name] = $value;

                continue;
            }

            $value = self::cast($attribute, $value);
            $shaped[$name] = $value;
            if ($value !== null && $attribute->filters !== []) {
                $filtered[$this->sources[$name]][$name] = $value;
            }
        }

        foreach ($filtered as $alias => $values) {
            foreach ($this->filter($alias, $values) as $name => $value) {
                $shaped[$name] = $value;
            }
        }

        return $shaped;
    }

    /**
     * The name aggregate() gives an aggregate the queries leave unaliased: `<method>_<attribute>`, or `<method>` for
     * count('*'), with each character other than a letter, digit or underscore replaced by an underscore.
     */
    public static function alias(Query $query): string
    {
        $method = $query->getMethod()->value;
        $attribute = $query->getAttribute();
        $alias = $attribute === '*' || $attribute === '' ? $method : $method.'_'.$attribute;

        $sanitized = '';
        foreach (\str_split($alias) as $character) {
            $sanitized .= \ctype_alnum($character) || $character === '_' ? $character : '_';
        }

        return \substr($sanitized, 0, Validator\Query\Aggregate::MAX_ALIAS_LENGTH);
    }

    /**
     * Passes groups read from one collection through the filters their attributes declare, as a read of that
     * collection decodes them.
     *
     * @param  string  $alias  The join alias the groups are read from, '' for the collection read
     * @param  array<string, mixed>  $values  The cast value of each group, keyed by the name a read returns it under
     * @return array<string, mixed>
     *
     * @throws DatabaseException
     */
    private function filter(string $alias, array $values): array
    {
        $prefix = $alias === '' ? '' : $alias.'.';

        $keys = [];
        $definitions = [];
        $stored = new Document();
        foreach ($values as $name => $value) {
            $key = \substr($name, \strlen($prefix));
            $keys[$name] = $key;
            $stored->setAttribute($key, $value);
            if (! isset($this->internal[$key])) {
                $definitions[] = $this->attributes[$name]->toDocument();
            }
        }

        $decoded = $this->database->decode(
            new Document([Document::ID => $alias, 'attributes' => $definitions]),
            $stored,
            \array_values($keys),
        );

        $filtered = [];
        foreach ($keys as $name => $key) {
            $filtered[$name] = $decoded->getAttribute($key);
        }

        return $filtered;
    }

    /**
     * The name a read returns each group column of a row under. A column is named as the queries name its group, or
     * failing that by the group's storage column.
     *
     * @param  list<string>  $columns
     * @return array<string, string>
     */
    private function names(array $columns): array
    {
        $unclaimed = [];
        foreach ($columns as $column) {
            if (! isset($this->aggregates[$column])) {
                $unclaimed[$column] = true;
            }
        }

        $names = [];
        $pending = [];
        foreach ($this->groups as $group => $name) {
            if (isset($unclaimed[$group])) {
                $names[$group] = $name;
                unset($unclaimed[$group]);
            } else {
                $pending[$group] = $name;
            }
        }

        foreach ($pending as $group => $name) {
            $dot = \strpos($group, '.');
            $key = $dot === false ? $group : \substr($group, $dot + 1);
            foreach ([Storage::column($key), $key] as $column) {
                if (isset($unclaimed[$column])) {
                    $names[$column] = $name;
                    unset($unclaimed[$column]);
                    break;
                }
            }
        }

        return $names;
    }

    private function aggregate(Method $method, ?Attribute $operand, mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }

        return match (true) {
            \in_array($method, self::COUNTS, true) => self::integer($value),
            \in_array($method, self::STATISTICS, true) => self::float($value),
            $method === Method::Sum => $operand !== null && \in_array($operand->type, self::INTEGERS, true)
                ? self::integer($value)
                : self::float($value),
            default => $operand === null ? $value : self::cast($operand, $value),
        };
    }

    /**
     * The name a read returns an attribute under: a joined one qualified by its join alias, though the queries may
     * name it bare when no other collection of the read declares it.
     *
     * @param  array<string, Attribute>  $schema
     */
    private static function name(array $schema, string $attribute): string
    {
        if (\str_contains($attribute, '.') || isset($schema[$attribute])) {
            return $attribute;
        }

        $suffix = '.'.$attribute;
        $matches = [];
        foreach (\array_keys($schema) as $name) {
            if (\str_ends_with($name, $suffix)) {
                $matches[] = $name;
            }
        }

        return \count($matches) === 1 ? $matches[0] : $attribute;
    }

    private static function cast(Attribute $attribute, mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }

        $encoded = $attribute->array
            || \in_array($attribute->type, self::OBJECTS, true)
            || \in_array(Filter::Json->value, $attribute->filters, true);

        if ($encoded && \is_string($value)) {
            $value = \json_decode($value, true);
        }

        if (! $attribute->array || ! \is_array($value)) {
            return self::scalar($attribute->type, $value);
        }

        return \array_map(static fn (mixed $node): mixed => self::scalar($attribute->type, $node), $value);
    }

    private static function scalar(ColumnType $type, mixed $value): mixed
    {
        if ($value === null || \is_array($value)) {
            return $value;
        }

        return match (true) {
            $type === ColumnType::Boolean => \is_bool($value) ? $value : \filter_var($value, FILTER_VALIDATE_BOOLEAN),
            $type === ColumnType::Id => \is_scalar($value) ? (string) $value : $value,
            $type === ColumnType::BigInteger && \is_string($value) && BigInt::isIntegerString($value) && ! BigInt::fitsPhpInt($value) => $value,
            \in_array($type, self::INTEGERS, true) => self::integer($value),
            \in_array($type, self::FLOATS, true) => self::float($value),
            \in_array($type, self::DATETIMES, true) => \is_string($value) ? DateTime::formatTz($value) : $value,
            default => $value,
        };
    }

    /**
     * An integer aggregate, or a group or bound of an integer attribute, as the integer it is. MariaDB sums integers
     * into DECIMAL, so a whole value can arrive as "7.0000"; it and MySQL and PostgreSQL sum integers exactly past
     * the range of a 64-bit integer, and a value that does not fit one is refused rather than answered inexactly.
     *
     * @throws LimitException When the value is a whole number past the range of a 64-bit integer
     */
    private static function integer(mixed $value): int|float
    {
        if (\is_int($value)) {
            return $value;
        }

        $whole = \is_string($value) ? BigInt::whole($value) : null;
        if ($whole !== null) {
            if (! BigInt::fitsPhpInt($whole)) {
                throw new LimitException('Value out of range');
            }

            return (int) $whole;
        }

        $float = self::float($value);
        if (\floor($float) !== $float) {
            return $float;
        }

        if ($float < -self::INTEGER_LIMIT || $float >= self::INTEGER_LIMIT) {
            throw new LimitException('Value out of range');
        }

        return (int) $float;
    }

    private static function float(mixed $value): float
    {
        return \is_numeric($value) ? (float) $value : 0.0;
    }

    /**
     * @return list<Attribute>
     */
    private static function attributes(Document $collection): array
    {
        $attributes = [];
        $definitions = $collection->getAttribute('attributes', []);
        foreach (\is_array($definitions) ? $definitions : [] as $attribute) {
            if (! $attribute instanceof Document) {
                continue;
            }

            try {
                $attributes[] = Attribute::fromDocument($attribute);
            } catch (Schema) {
                continue;
            }
        }

        return $attributes;
    }
}
