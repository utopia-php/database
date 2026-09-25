<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Schema\ColumnType;

/**
 * Represents a database collection attribute with its type, constraints, and formatting options.
 *
 * @property string $key
 * @property ColumnType $type
 * @property int $size
 * @property bool $required
 * @property mixed $default
 * @property bool $signed
 * @property bool $array
 * @property string|null $format
 * @property array<string, mixed> $formatOptions
 * @property array<string> $filters
 * @property string|null $status
 * @property array<string, mixed>|null $options
 */
class Attribute extends Document
{
    private const string PERSISTED_BIG_INTEGER = 'bigint';

    /**
     * The column types an attribute can be stored as. Object, spatial and vector attributes also need
     * the adapter to support them.
     *
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

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public function __construct(
        string $key = '',
        ColumnType $type = ColumnType::String,
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ) {
        if ($type === ColumnType::BigInteger) {
            $size = 0;
        }

        $data = [
            self::ID => $key,
            'key' => $key,
            'type' => self::persistedType($type),
            'size' => $size,
            'required' => $required,
            'default' => $default,
            'signed' => $signed,
            'array' => $array,
            'format' => $format === '' ? null : $format,
            'formatOptions' => $formatOptions,
            'filters' => $filters,
        ];
        if ($status !== null) {
            $data['status'] = $status;
        }
        if ($options !== null) {
            $data['options'] = $options;
        }
        parent::__construct($data);
    }

    /**
     * @return (
     *     $name is 'key' ? string :
     *     $name is 'type' ? ColumnType :
     *     $name is 'size' ? int :
     *     $name is 'required' ? bool :
     *     $name is 'default' ? mixed :
     *     $name is 'signed' ? bool :
     *     $name is 'array' ? bool :
     *     $name is 'format' ? string|null :
     *     $name is 'formatOptions' ? array<string, mixed> :
     *     $name is 'filters' ? array<string> :
     *     $name is 'status' ? string|null :
     *     $name is 'options' ? array<string, mixed>|null :
     *     mixed
     * )
     */
    public function __get(string $name): mixed
    {
        switch ($name) {
            case 'key':
                /** @var string $key */
                $key = $this->getAttribute('key', $this->getId());

                return $key;
            case 'type':
                /** @var ColumnType|string $type */
                $type = $this->getAttribute('type', ColumnType::String->value);

                return self::normalizeType($type);
            case 'size':
                /** @var int $size */
                $size = $this->getAttribute('size', 0);

                return $size;
            case 'required':
                return (bool) $this->getAttribute('required', false);
            case 'default':
                return $this->getAttribute('default');
            case 'signed':
                return (bool) $this->getAttribute('signed', true);
            case 'array':
                return (bool) $this->getAttribute('array', false);
            case 'format':
                $format = $this->getAttribute('format');

                return \is_string($format) && $format !== '' ? $format : null;
            case 'formatOptions':
                $formatOptions = $this->getAttribute('formatOptions', []);
                if (! \is_array($formatOptions)) {
                    return [];
                }
                /** @var array<string, mixed> $formatOptions */

                return $formatOptions;
            case 'filters':
                $filters = $this->getAttribute('filters', []);
                if (! \is_array($filters)) {
                    return [];
                }
                /** @var array<string> $filters */

                return $filters;
            case 'status':
                $status = $this->getAttribute('status');

                return \is_string($status) ? $status : null;
            case 'options':
                $options = $this->getAttribute('options');
                if (! \is_array($options)) {
                    return null;
                }
                /** @var array<string, mixed> $options */

                return $options;
            default:
                return $this->getAttribute($name);
        }
    }

    public function __set(string $name, mixed $value): void
    {
        match ($name) {
            'key' => $this->setAttribute('key', $value)->setAttribute(self::ID, $value),
            'type' => $this->setAttribute('type', $value),
            'size' => $this->setAttribute('size', $value),
            'required' => $this->setAttribute('required', $value),
            'default' => $this->setAttribute('default', $value),
            'signed' => $this->setAttribute('signed', $value),
            'array' => $this->setAttribute('array', $value),
            'format' => $this->setAttribute('format', $value === '' ? null : $value),
            'formatOptions' => $this->setAttribute('formatOptions', $value),
            'filters' => $this->setAttribute('filters', $value),
            'status' => $this->setAttribute('status', $value),
            'options' => $this->setAttribute('options', $value),
            default => $this->setAttribute($name, $value),
        };
    }

    public function __isset(string $name): bool
    {
        return match ($name) {
            'key', 'type', 'size', 'required', 'default', 'signed', 'array', 'format', 'formatOptions', 'filters', 'status', 'options' => true,
            default => $this->offsetExists($name),
        };
    }

    /**
     * @param  string|null  $key
     */
    #[\Override]
    public function offsetSet(mixed $key, mixed $value): void
    {
        $type = $key === 'type' && ($value instanceof ColumnType || \is_string($value))
            ? self::tryNormalizeType($value)
            : null;

        parent::offsetSet($key, $type === null ? $value : self::persistedType($type));
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function string(
        string $key = '',
        int $size = Database::LENGTH_KEY,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\StringType {
        return new Attribute\StringType(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function varchar(
        string $key = '',
        int $size = Database::LENGTH_KEY,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Varchar {
        return new Attribute\Varchar(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function text(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Text {
        return new Attribute\Text(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function mediumText(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\MediumText {
        return new Attribute\MediumText(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function longText(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\LongText {
        return new Attribute\LongText(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function integer(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Integer {
        return new Attribute\Integer(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function bigInteger(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\BigInteger {
        return new Attribute\BigInteger(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function float(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\FloatType {
        return new Attribute\FloatType(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function double(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Double {
        return new Attribute\Double(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function boolean(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Boolean {
        return new Attribute\Boolean(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function datetime(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Datetime {
        return new Attribute\Datetime(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function point(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Point {
        return new Attribute\Point(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function linestring(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Linestring {
        return new Attribute\Linestring(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function polygon(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Polygon {
        return new Attribute\Polygon(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function vector(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Vector {
        return new Attribute\Vector(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function id(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Id {
        return new Attribute\Id(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function object(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\ObjectType {
        return new Attribute\ObjectType(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function relationship(
        string $key = '',
        int $size = 0,
        bool $required = false,
        mixed $default = null,
        bool $signed = true,
        bool $array = false,
        ?string $format = null,
        array $formatOptions = [],
        array $filters = [],
        ?string $status = null,
        ?array $options = null,
    ): Attribute\Relationship {
        return new Attribute\Relationship(
            key: $key,
            size: $size,
            required: $required,
            default: $default,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    public static function persistedType(ColumnType $type): string
    {
        return $type === ColumnType::BigInteger ? self::PERSISTED_BIG_INTEGER : $type->value;
    }

    public static function normalizeType(ColumnType|string $type): ColumnType
    {
        if ($type instanceof ColumnType) {
            return $type;
        }

        return $type === self::PERSISTED_BIG_INTEGER ? ColumnType::BigInteger : ColumnType::from($type);
    }

    public static function tryNormalizeType(ColumnType|string $type): ?ColumnType
    {
        if ($type instanceof ColumnType) {
            return $type;
        }

        return $type === self::PERSISTED_BIG_INTEGER ? ColumnType::BigInteger : ColumnType::tryFrom($type);
    }

    /**
     * The types in {@see self::TYPES} that the given capabilities make available, in table order.
     *
     * @return list<ColumnType>
     */
    public static function availableTypes(bool $objects, bool $spatial, bool $vectors): array
    {
        return \array_values(\array_filter(
            self::TYPES,
            fn (ColumnType $type): bool => match (true) {
                $type === ColumnType::Object => $objects,
                self::isSpatialType($type) => $spatial,
                $type === ColumnType::Vector => $vectors,
                default => true,
            },
        ));
    }

    public static function isSpatialType(ColumnType|string $type): bool
    {
        $type = self::tryNormalizeType($type);

        return \in_array($type, [
            ColumnType::Point,
            ColumnType::Linestring,
            ColumnType::Polygon,
        ], true);
    }

    public static function isNumericType(ColumnType|string $type): bool
    {
        $type = self::tryNormalizeType($type);

        return \in_array($type, [
            ColumnType::Integer,
            ColumnType::BigInteger,
            ColumnType::Float,
            ColumnType::Double,
        ], true);
    }

    public static function isIntegerType(ColumnType|string $type): bool
    {
        $type = self::tryNormalizeType($type);

        return \in_array($type, [
            ColumnType::Integer,
            ColumnType::BigInteger,
        ], true);
    }

    /**
     * @return array{min: int|float|string, max: int|float|string}|null
     */
    public static function getNumericBounds(ColumnType|string $type, bool $signed = true): ?array
    {
        $type = self::tryNormalizeType($type);

        return match ($type) {
            ColumnType::Integer => [
                'min' => $signed ? Database::MIN_INT : 0,
                'max' => Database::MAX_INT,
            ],
            ColumnType::BigInteger => [
                'min' => $signed ? \PHP_INT_MIN : 0,
                'max' => $signed ? Database::MAX_BIG_INT : BigInt::UNSIGNED_MAX,
            ],
            ColumnType::Float,
            ColumnType::Double => [
                'min' => $signed ? -Database::MAX_DOUBLE : 0,
                'max' => Database::MAX_DOUBLE,
            ],
            default => null,
        };
    }

    /**
     * Convert this attribute to a Document representation.
     *
     * @return Document
     */
    public function toDocument(): Document
    {
        $data = [
            Document::ID => ID::custom($this->key),
            'key' => $this->key,
            'type' => self::persistedType($this->type),
            'size' => $this->size,
            'required' => $this->required,
            'default' => $this->default,
            'signed' => $this->signed,
            'array' => $this->array,
            'format' => $this->format,
            'formatOptions' => $this->formatOptions,
            'filters' => $this->filters,
        ];

        if ($this->status !== null) {
            $data['status'] = $this->status;
        }

        if ($this->options !== null) {
            $data['options'] = $this->options;
        }

        return new Document($data);
    }

    /**
     * Create an Attribute instance from a Document.
     *
     * @param Document $document The document to convert
     * @return self
     */
    public static function fromDocument(Document $document): self
    {
        /** @var string $key */
        $key = $document->getAttribute('key', $document->getId());
        /** @var ColumnType|string $type */
        $type = $document->getAttribute('type', 'string');
        /** @var int $size */
        $size = $document->getAttribute('size', 0);
        /** @var bool $required */
        $required = $document->getAttribute('required', false);
        /** @var bool $signed */
        $signed = $document->getAttribute('signed', true);
        /** @var bool $array */
        $array = $document->getAttribute('array', false);
        /** @var string|null $format */
        $format = $document->getAttribute('format');
        /** @var array<string, mixed> $formatOptions */
        $formatOptions = $document->getAttribute('formatOptions', []);
        /** @var array<string> $filters */
        $filters = $document->getAttribute('filters', []);
        /** @var string|null $status */
        $status = $document->getAttribute('status');
        /** @var array<string, mixed>|null $options */
        $options = $document->getAttribute('options');

        return self::make(
            key: $key,
            type: self::normalizeType($type),
            size: $size,
            required: $required,
            default: $document->getAttribute('default'),
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }

    /**
     * Cheap relationship-type check that avoids materializing a typed Attribute.
     * Use in hot read paths where only the type matters.
     *
     * Mirrors the normalization in {@see self::fromDocument()} — accepts both
     * the (always-stored) string form and the defensive ColumnType-enum form.
     */
    public static function isRelationship(self|Document $attribute): bool
    {
        if ($attribute instanceof self) {
            return $attribute->type === ColumnType::Relationship;
        }

        $type = $attribute->getAttribute('type');

        if ($type instanceof ColumnType) {
            return $type === ColumnType::Relationship;
        }

        return $type === ColumnType::Relationship->value;
    }

    /**
     * Create from an associative array (used by batch operations).
     *
     * @param  array<string, mixed>  $data
     * @return self
     */
    public static function fromArray(array $data): self
    {
        /** @var ColumnType|string $type */
        $type = $data['type'] ?? 'string';

        /** @var string $key */
        $key = $data[Document::ID] ?? $data['key'] ?? '';
        /** @var int $size */
        $size = $data['size'] ?? 0;
        /** @var bool $required */
        $required = $data['required'] ?? false;
        /** @var bool $signed */
        $signed = $data['signed'] ?? true;
        /** @var bool $array */
        $array = $data['array'] ?? false;
        /** @var string|null $format */
        $format = $data['format'] ?? null;
        /** @var array<string, mixed> $formatOptions */
        $formatOptions = $data['formatOptions'] ?? [];
        /** @var array<string> $filters */
        $filters = $data['filters'] ?? [];
        /** @var string|null $status */
        $status = $data['status'] ?? null;
        /** @var array<string, mixed>|null $options */
        $options = $data['options'] ?? null;

        return self::make(
            key: $key,
            type: self::normalizeType($type),
            size: $size,
            required: $required,
            default: $data['default'] ?? null,
            signed: $signed,
            array: $array,
            format: $format,
            formatOptions: $formatOptions,
            filters: $filters,
            status: $status,
            options: $options,
        );
    }


    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    private static function make(
        string $key,
        ColumnType $type,
        int $size,
        bool $required,
        mixed $default,
        bool $signed,
        bool $array,
        ?string $format,
        array $formatOptions,
        array $filters,
        ?string $status = null,
        ?array $options = null,
    ): self {
        return match ($type) {
            ColumnType::String => new Attribute\StringType(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Varchar => new Attribute\Varchar(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Text => new Attribute\Text(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::MediumText => new Attribute\MediumText(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::LongText => new Attribute\LongText(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Integer => new Attribute\Integer(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::BigInteger => new Attribute\BigInteger(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Float => new Attribute\FloatType(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Double => new Attribute\Double(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Boolean => new Attribute\Boolean(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Datetime => new Attribute\Datetime(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Point => new Attribute\Point(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Linestring => new Attribute\Linestring(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Polygon => new Attribute\Polygon(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Vector => new Attribute\Vector(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Id => new Attribute\Id(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Object => new Attribute\ObjectType(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            ColumnType::Relationship => new Attribute\Relationship(
                key: $key,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
            default => new self(
                key: $key,
                type: $type,
                size: $size,
                required: $required,
                default: $default,
                signed: $signed,
                array: $array,
                format: $format,
                formatOptions: $formatOptions,
                filters: $filters,
                status: $status,
                options: $options,
            ),
        };
    }
}
