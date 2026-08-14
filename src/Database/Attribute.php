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
    public static function tinyInteger(
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
    ): Attribute\TinyInteger {
        return new Attribute\TinyInteger(
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
    public static function smallInteger(
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
    ): Attribute\SmallInteger {
        return new Attribute\SmallInteger(
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
    public static function decimal(
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
    ): Attribute\Decimal {
        return new Attribute\Decimal(
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
    public static function timestamp(
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
    ): Attribute\Timestamp {
        return new Attribute\Timestamp(
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
    public static function json(
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
    ): Attribute\Json {
        return new Attribute\Json(
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
    public static function binary(
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
    ): Attribute\Binary {
        return new Attribute\Binary(
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
    public static function enum(
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
    ): Attribute\EnumType {
        return new Attribute\EnumType(
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
    public static function uuid(
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
    ): Attribute\Uuid {
        return new Attribute\Uuid(
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
    public static function uuid7(
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
    ): Attribute\Uuid7 {
        return new Attribute\Uuid7(
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

    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public static function serial(
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
    ): Attribute\Serial {
        return new Attribute\Serial(
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
    public static function bigSerial(
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
    ): Attribute\BigSerial {
        return new Attribute\BigSerial(
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
    public static function smallSerial(
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
    ): Attribute\SmallSerial {
        return new Attribute\SmallSerial(
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
    public static function array(
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
    ): Attribute\ArrayType {
        return new Attribute\ArrayType(
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
    public static function tuple(
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
    ): Attribute\Tuple {
        return new Attribute\Tuple(
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
            ColumnType::TinyInteger => new Attribute\TinyInteger(
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
            ColumnType::SmallInteger => new Attribute\SmallInteger(
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
            ColumnType::Decimal => new Attribute\Decimal(
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
            ColumnType::Timestamp => new Attribute\Timestamp(
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
            ColumnType::Json => new Attribute\Json(
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
            ColumnType::Binary => new Attribute\Binary(
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
            ColumnType::Enum => new Attribute\EnumType(
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
            ColumnType::Uuid => new Attribute\Uuid(
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
            ColumnType::Uuid7 => new Attribute\Uuid7(
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
            ColumnType::Serial => new Attribute\Serial(
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
            ColumnType::BigSerial => new Attribute\BigSerial(
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
            ColumnType::SmallSerial => new Attribute\SmallSerial(
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
            ColumnType::Array => new Attribute\ArrayType(
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
            ColumnType::Tuple => new Attribute\Tuple(
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
        };
    }
}
