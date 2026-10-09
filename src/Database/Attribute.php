<?php

namespace Utopia\Database;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Schema\ColumnType;

final readonly class Attribute
{
    /**
     * @var list<ColumnType>
     */
    public const array TYPES = [
        ColumnType::String,
        ColumnType::Varchar,
        ColumnType::Text,
        ColumnType::MediumText,
        ColumnType::LongText,
        ColumnType::Integer,
        ColumnType::BigInteger,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Boolean,
        ColumnType::Datetime,
        ColumnType::Id,
        ColumnType::Relationship,
        ColumnType::Object,
        ColumnType::Point,
        ColumnType::Linestring,
        ColumnType::Polygon,
        ColumnType::Vector,
    ];

    private const string STORED_BIG_INTEGER = 'bigint';

    private const string KEY = 'key';

    private const string TYPE = 'type';

    private const string SIZE = 'size';

    private const string REQUIRED = 'required';

    private const string DEFAULT = 'default';

    private const string SIGNED = 'signed';

    private const string ARRAY = 'array';

    private const string FORMAT = 'format';

    private const string FORMAT_OPTIONS = 'formatOptions';

    private const string FILTERS = 'filters';

    private const string OPTIONS = 'options';

    /**
     * @param  list<string>  $filters
     */
    private function __construct(
        public string $key,
        public ColumnType $type,
        public ?int $size,
        public bool $required,
        public mixed $default,
        public bool $signed,
        public bool $array,
        public ?Format $format,
        public array $filters,
        public ?Relationship $relationship,
        public ?RelationshipSide $side,
    ) {
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public static function string(string $key, int $size = Database::LENGTH_KEY, bool $required = false, mixed $default = null, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::String, $size, $required, $default, true, $array, $format, $filters);
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public static function varchar(string $key, int $size = Database::LENGTH_KEY, bool $required = false, mixed $default = null, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Varchar, $size, $required, $default, true, $array, $format, $filters);
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public static function text(string $key, ?int $size = null, bool $required = false, mixed $default = null, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Text, $size, $required, $default, true, $array, $format, $filters);
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public static function mediumText(string $key, ?int $size = null, bool $required = false, mixed $default = null, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::MediumText, $size, $required, $default, true, $array, $format, $filters);
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public static function longText(string $key, ?int $size = null, bool $required = false, mixed $default = null, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::LongText, $size, $required, $default, true, $array, $format, $filters);
    }

    /**
     * @param  int|list<int>|null  $default
     * @param  list<Filter|string>  $filters
     */
    public static function integer(string $key, bool $required = false, int|array|null $default = null, bool $signed = true, bool $array = false, IntegerWidth $width = IntegerWidth::Bits32, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Integer, $width->size(), $required, $default, $signed, $array, $format, $filters);
    }

    /**
     * @param  int|string|list<int|string>|null  $default
     * @param  list<Filter|string>  $filters
     */
    public static function bigInteger(string $key, bool $required = false, int|string|array|null $default = null, bool $signed = true, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::BigInteger, null, $required, $default, $signed, $array, $format, $filters);
    }

    /**
     * @param  float|int|list<float|int>|null  $default
     * @param  list<Filter|string>  $filters
     */
    public static function float(string $key, bool $required = false, float|int|array|null $default = null, bool $signed = true, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Float, null, $required, $default, $signed, $array, $format, $filters);
    }

    /**
     * @param  float|int|list<float|int>|null  $default
     * @param  list<Filter|string>  $filters
     */
    public static function double(string $key, bool $required = false, float|int|array|null $default = null, bool $signed = true, bool $array = false, ?Format $format = null, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Double, null, $required, $default, $signed, $array, $format, $filters);
    }

    /**
     * @param  bool|list<bool>|null  $default
     * @param  list<Filter|string>  $filters
     */
    public static function boolean(string $key, bool $required = false, bool|array|null $default = null, bool $array = false, array $filters = []): self
    {
        return self::scalar($key, ColumnType::Boolean, null, $required, $default, true, $array, null, $filters);
    }

    /**
     * @param  string|list<string>|null  $default
     */
    public static function datetime(string $key, bool $required = false, string|array|null $default = null, bool $array = false): self
    {
        return self::scalar($key, ColumnType::Datetime, null, $required, $default, false, $array, null, []);
    }

    /**
     * @param  array<mixed>|null  $default
     */
    public static function point(string $key, bool $required = false, ?array $default = null): self
    {
        return self::scalar($key, ColumnType::Point, null, $required, $default, true, false, null, []);
    }

    /**
     * @param  array<mixed>|null  $default
     */
    public static function lineString(string $key, bool $required = false, ?array $default = null): self
    {
        return self::scalar($key, ColumnType::Linestring, null, $required, $default, true, false, null, []);
    }

    /**
     * @param  array<mixed>|null  $default
     */
    public static function polygon(string $key, bool $required = false, ?array $default = null): self
    {
        return self::scalar($key, ColumnType::Polygon, null, $required, $default, true, false, null, []);
    }

    /**
     * @param  list<float|int>|null  $default
     */
    public static function vector(string $key, int $dimensions, bool $required = false, ?array $default = null): self
    {
        return self::scalar($key, ColumnType::Vector, $dimensions, $required, $default, true, false, null, []);
    }

    /**
     * @param  array<mixed>|null  $default
     */
    public static function object(string $key, bool $required = false, ?array $default = null): self
    {
        return self::scalar($key, ColumnType::Object, null, $required, $default, true, false, null, []);
    }

    public static function id(string $key, bool $required = false, int|string|null $default = null, bool $array = false): self
    {
        return self::scalar($key, ColumnType::Id, null, $required, $default, true, $array, null, []);
    }

    /**
     * @throws Structure
     */
    public static function relationship(string $key, Relationship $relationship, RelationshipSide $side): self
    {
        if ($relationship->key === null) {
            $relationship = $relationship->apply(new RelationshipUpdate(key: $key));
        } elseif ($relationship->key !== $key) {
            throw new Structure('Relationship key "'.$relationship->key.'" does not match attribute key "'.$key.'"');
        }

        return self::normalised($key, ColumnType::Relationship, null, false, null, true, false, null, [], $relationship, $side);
    }

    /**
     * @throws Structure
     * @throws RelationshipException
     */
    public static function fromDocument(Document $document): self
    {
        $key = $document->getAttribute(self::KEY);

        return self::hydrate(
            \is_string($key) ? $key : $document->getId(),
            $document->getAttribute(self::TYPE),
            $document->getAttribute(self::SIZE),
            $document->getAttribute(self::REQUIRED),
            $document->getAttribute(self::DEFAULT),
            $document->getAttribute(self::SIGNED),
            $document->getAttribute(self::ARRAY),
            $document->getAttribute(self::FORMAT),
            $document->getAttribute(self::FORMAT_OPTIONS),
            $document->getAttribute(self::FILTERS),
            $document->getAttribute(self::OPTIONS),
        );
    }

    /**
     * @param  array<string, mixed>  $data
     *
     * @throws Structure
     */
    public static function fromArray(array $data): self
    {
        $key = $data[self::KEY] ?? $data[Document::ID] ?? '';

        return self::hydrate(
            \is_string($key) ? $key : '',
            $data[self::TYPE] ?? null,
            $data[self::SIZE] ?? null,
            $data[self::REQUIRED] ?? null,
            $data[self::DEFAULT] ?? null,
            $data[self::SIGNED] ?? null,
            $data[self::ARRAY] ?? null,
            $data[self::FORMAT] ?? null,
            $data[self::FORMAT_OPTIONS] ?? null,
            $data[self::FILTERS] ?? null,
            $data[self::OPTIONS] ?? null,
        );
    }

    public function toDocument(): Document
    {
        $data = [
            Document::ID => $this->key,
            self::KEY => $this->key,
            self::TYPE => self::storedType($this->type),
            self::SIZE => $this->size ?? 0,
            self::REQUIRED => $this->required,
            self::DEFAULT => $this->default,
            self::SIGNED => $this->signed,
            self::ARRAY => $this->array,
            self::FORMAT => $this->format?->name,
            self::FORMAT_OPTIONS => $this->format === null ? [] : $this->format->options,
            self::FILTERS => $this->filters,
        ];

        if ($this->relationship !== null && $this->side !== null) {
            $data[self::OPTIONS] = $this->relationship->toOptions($this->side);
        }

        return new Document($data);
    }

    /**
     * @throws Structure
     */
    public function apply(AttributeUpdate $update): self
    {
        $type = $update->type ?? $this->type;
        if ($type !== $this->type) {
            if ($type === ColumnType::Relationship || $this->type === ColumnType::Relationship) {
                throw new Structure('A relationship attribute cannot change type; use updateRelationship()');
            }
            self::assertType($type);
        }

        $key = $update->key ?? $this->key;
        $relationship = $this->relationship;
        if ($relationship !== null && $relationship->key !== $key) {
            $relationship = $relationship->apply(new RelationshipUpdate(key: $key));
        }

        $filters = match (true) {
            $update->filters !== null => Filter::names($update->filters),
            $type !== $this->type => self::withoutTypeFilter($this->type, $this->filters),
            default => $this->filters,
        };

        return self::normalised(
            $key,
            $type,
            $update->size ?? $this->size,
            $update->required ?? $this->required,
            $update->changesDefault() ? $update->default : $this->default,
            $update->signed ?? $this->signed,
            $update->array ?? $this->array,
            $update->format instanceof Unchanged ? $this->format : $update->format,
            $filters,
            $relationship,
            $this->side,
        );
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    public function withFilters(array $filters): self
    {
        return clone($this, ['filters' => self::withTypeFilter($this->type, Filter::names($filters))]);
    }

    public function width(): ?IntegerWidth
    {
        return $this->type === ColumnType::Integer ? IntegerWidth::fromSize($this->size) : null;
    }

    public function resolvedSize(): int
    {
        if ($this->size !== null && $this->size > 0) {
            return $this->size;
        }

        return match ($this->type) {
            ColumnType::Text => Database::MAX_TEXT_BYTES,
            ColumnType::MediumText => Database::MAX_MEDIUMTEXT_BYTES,
            ColumnType::LongText => Database::MAX_LONGTEXT_BYTES,
            default => 0,
        };
    }

    public function isSpatial(): bool
    {
        return match ($this->type) {
            ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon => true,
            default => false,
        };
    }

    public function isNumeric(): bool
    {
        return match ($this->type) {
            ColumnType::Integer, ColumnType::BigInteger, ColumnType::Float, ColumnType::Double => true,
            default => false,
        };
    }

    public function isInteger(): bool
    {
        return $this->type === ColumnType::Integer || $this->type === ColumnType::BigInteger;
    }

    public function bounds(): ?NumericBounds
    {
        return match ($this->type) {
            ColumnType::Integer => $this->width() === IntegerWidth::Bits64
                ? new NumericBounds($this->signed ? \PHP_INT_MIN : 0, Database::MAX_BIG_INT)
                : new NumericBounds($this->signed ? Database::MIN_INT : 0, Database::MAX_INT),
            ColumnType::BigInteger => new NumericBounds(
                $this->signed ? \PHP_INT_MIN : 0,
                $this->signed ? Database::MAX_BIG_INT : BigInt::UNSIGNED_MAX,
            ),
            ColumnType::Float, ColumnType::Double => new NumericBounds(
                $this->signed ? -Database::MAX_DOUBLE : 0,
                Database::MAX_DOUBLE,
            ),
            default => null,
        };
    }

    public static function isRelationship(Document $attribute): bool
    {
        $type = $attribute->getAttribute(self::TYPE);

        return $type === ColumnType::Relationship->value || $type === ColumnType::Relationship;
    }

    /**
     * @throws Structure
     */
    public static function typeFromStored(string $type): ColumnType
    {
        $columnType = $type === self::STORED_BIG_INTEGER ? ColumnType::BigInteger : ColumnType::tryFrom($type);
        if ($columnType === null) {
            throw new Structure('Unknown attribute type: '.$type);
        }

        self::assertType($columnType);

        return $columnType;
    }

    public static function storedType(ColumnType $type): string
    {
        return $type === ColumnType::BigInteger ? self::STORED_BIG_INTEGER : $type->value;
    }

    /**
     * @return list<ColumnType>
     */
    public static function availableTypes(Profile $profile): array
    {
        $objects = $profile->supports(Capability::Objects);
        $spatial = $profile->hasFeature(Feature\Spatial::class);
        $vectors = $profile->supports(Capability::Vectors);

        $types = [];
        foreach (self::TYPES as $type) {
            $available = match ($type) {
                ColumnType::Object => $objects,
                ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon => $spatial,
                ColumnType::Vector => $vectors,
                default => true,
            };
            if ($available) {
                $types[] = $type;
            }
        }

        return $types;
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    private static function scalar(string $key, ColumnType $type, ?int $size, bool $required, mixed $default, bool $signed, bool $array, ?Format $format, array $filters): self
    {
        return self::normalised($key, $type, $size, $required, $default, $signed, $array, $format, $filters, null, null);
    }

    /**
     * @param  list<Filter|string>  $filters
     */
    private static function normalised(
        string $key,
        ColumnType $type,
        ?int $size,
        bool $required,
        mixed $default,
        bool $signed,
        bool $array,
        ?Format $format,
        array $filters,
        ?Relationship $relationship,
        ?RelationshipSide $side,
    ): self {
        return new self(
            $key,
            $type,
            self::normalisedSize($type, $size),
            $required,
            $default,
            self::normalisedSigned($type, $signed),
            self::normalisedArray($type, $array),
            $format,
            self::withTypeFilter($type, Filter::names($filters)),
            $relationship,
            $side,
        );
    }

    private static function normalisedSize(ColumnType $type, ?int $size): ?int
    {
        return match ($type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText, ColumnType::Vector => $size === 0 ? null : $size,
            ColumnType::Integer => IntegerWidth::fromSize($size)->size(),
            default => null,
        };
    }

    private static function normalisedSigned(ColumnType $type, bool $signed): bool
    {
        return match ($type) {
            ColumnType::Integer, ColumnType::BigInteger, ColumnType::Float, ColumnType::Double => $signed,
            ColumnType::Datetime => false,
            default => true,
        };
    }

    private static function normalisedArray(ColumnType $type, bool $array): bool
    {
        return match ($type) {
            ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon, ColumnType::Vector, ColumnType::Object, ColumnType::Relationship => false,
            default => $array,
        };
    }

    private static function typeFilter(ColumnType $type): ?Filter
    {
        return match ($type) {
            ColumnType::Datetime => Filter::Datetime,
            ColumnType::Point => Filter::Point,
            ColumnType::Linestring => Filter::LineString,
            ColumnType::Polygon => Filter::Polygon,
            ColumnType::Vector => Filter::Vector,
            ColumnType::Object => Filter::Object,
            default => null,
        };
    }

    /**
     * @param  list<string>  $filters
     * @return list<string>
     */
    private static function withTypeFilter(ColumnType $type, array $filters): array
    {
        $filter = self::typeFilter($type)?->value;
        if ($filter === null || \in_array($filter, $filters, true)) {
            return $filters;
        }

        return [$filter, ...$filters];
    }

    /**
     * @param  list<string>  $filters
     * @return list<string>
     */
    private static function withoutTypeFilter(ColumnType $type, array $filters): array
    {
        $filter = self::typeFilter($type)?->value;
        if ($filter === null) {
            return $filters;
        }

        return \array_values(\array_filter($filters, static fn (string $name): bool => $name !== $filter));
    }

    /**
     * @throws Structure
     */
    private static function assertType(ColumnType $type): void
    {
        if (! \in_array($type, self::TYPES, true)) {
            throw new Structure('Unknown attribute type: '.$type->value);
        }
    }

    /**
     * @throws Structure
     */
    private static function resolveType(mixed $type): ColumnType
    {
        if (\is_string($type)) {
            return self::typeFromStored($type);
        }

        if ($type instanceof ColumnType) {
            self::assertType($type);

            return $type;
        }

        throw new Structure('Attribute type must be a string, '.\get_debug_type($type).' given');
    }

    /**
     * @throws Structure
     * @throws RelationshipException
     */
    private static function hydrate(
        string $key,
        mixed $type,
        mixed $size,
        mixed $required,
        mixed $default,
        mixed $signed,
        mixed $array,
        mixed $format,
        mixed $formatOptions,
        mixed $filters,
        mixed $options,
    ): self {
        $columnType = self::resolveType($type);

        $relationship = null;
        $side = null;
        if ($columnType === ColumnType::Relationship) {
            [$relationship, $side] = self::hydrateRelationship($key, $options);
        }

        return self::normalised(
            $key,
            $columnType,
            self::storedSize($size),
            (bool) ($required ?? false),
            $default,
            (bool) ($signed ?? true),
            (bool) ($array ?? false),
            self::hydrateFormat($format, $formatOptions),
            self::hydrateFilters($filters),
            $relationship,
            $side,
        );
    }

    private static function storedSize(mixed $size): ?int
    {
        $size = \is_numeric($size) ? (int) $size : null;

        return $size === 0 ? null : $size;
    }

    private static function hydrateFormat(mixed $format, mixed $options): ?Format
    {
        if (! \is_string($format) || $format === '') {
            return null;
        }

        if ($options instanceof Document) {
            $options = $options->getArrayCopy();
        }

        /** @var array<string, mixed> $options */
        $options = \is_array($options) ? $options : [];

        return new Format($format, $options);
    }

    /**
     * @return list<string>
     */
    private static function hydrateFilters(mixed $filters): array
    {
        /** @var list<string> */
        return \is_array($filters) ? \array_values($filters) : [];
    }

    /**
     * @return array{Relationship, RelationshipSide}
     *
     * @throws Structure
     * @throws RelationshipException
     */
    private static function hydrateRelationship(string $key, mixed $options): array
    {
        if ($options instanceof Document) {
            $options = $options->getArrayCopy();
        }

        if (! \is_array($options)) {
            throw new Structure('Relationship attribute "'.$key.'" has no relationship options');
        }

        $side = $options[Relationship::SIDE] ?? RelationshipSide::Parent->value;
        $side = $side instanceof RelationshipSide ? $side : RelationshipSide::tryFrom(\is_string($side) ? $side : '');
        if ($side === null) {
            throw new Structure('Relationship attribute "'.$key.'" has an unknown side');
        }

        unset($options[Relationship::SIDE]);
        $options[self::KEY] = $key;

        /** @var array<string, mixed> $options */
        return [Relationship::fromArray($options), $side];
    }
}
