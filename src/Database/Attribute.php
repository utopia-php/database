<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Schema\ColumnType;

/**
 * Represents a database collection attribute with its type, constraints, and formatting options.
 */
class Attribute
{
    public const string LEGACY_BIG_INTEGER = 'bigint';

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public function __construct(
        public string $key = '',
        public ColumnType $type = ColumnType::String,
        public int $size = 0,
        public bool $required = false,
        public mixed $default = null,
        public bool $signed = true,
        public bool $array = false,
        public ?string $format = null,
        public array $formatOptions = [],
        public array $filters = [],
        public ?string $status = null,
        public ?array $options = null,
    ) {
        if (\in_array($this->type, [ColumnType::BigInteger, ColumnType::BigSerial], true)) {
            $this->size = 0;
        }
    }

    public static function normalizeType(ColumnType|string $type): ColumnType
    {
        if ($type instanceof ColumnType) {
            return $type;
        }

        return ColumnType::from($type === self::LEGACY_BIG_INTEGER ? ColumnType::BigInteger->value : $type);
    }

    public static function tryNormalizeType(ColumnType|string $type): ?ColumnType
    {
        if ($type instanceof ColumnType) {
            return $type;
        }

        return ColumnType::tryFrom($type === self::LEGACY_BIG_INTEGER ? ColumnType::BigInteger->value : $type);
    }

    public static function isNumericType(ColumnType|string $type): bool
    {
        $type = self::tryNormalizeType($type);

        return \in_array($type, [
            ColumnType::Integer,
            ColumnType::BigInteger,
            ColumnType::Float,
            ColumnType::Double,
            ColumnType::BigSerial,
        ], true);
    }

    public static function isIntegerType(ColumnType|string $type): bool
    {
        $type = self::tryNormalizeType($type);

        return \in_array($type, [
            ColumnType::Integer,
            ColumnType::BigInteger,
            ColumnType::BigSerial,
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
            ColumnType::BigInteger,
            ColumnType::BigSerial => [
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
            'type' => $this->type->value,
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

        return new self(
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
    public static function isRelationship(Document $attribute): bool
    {
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

        return new self(
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
        );
    }
}
