<?php

namespace Utopia\Database\Attribute;

use Utopia\Database\Attribute as Base;
use Utopia\Database\Database;
use Utopia\Query\Schema\ColumnType;

final class StringType extends Base
{
    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     * @param  array<string, mixed>|null  $options
     */
    public function __construct(
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
    ) {
        parent::__construct(
            key: $key,
            type: ColumnType::String,
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
}
