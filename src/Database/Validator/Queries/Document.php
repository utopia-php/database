<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Document as BaseDocument;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates queries for single document retrieval, supporting select operations on document attributes.
 */
class Document extends Queries
{
    /**
     * @param  array<BaseDocument>  $attributes
     *
     * @throws Exception
     */
    public function __construct(array $attributes, bool $supportForAttributes = true)
    {
        $attributes[] = new BaseDocument([
            '$id' => '$id',
            'key' => '$id',
            'type' => ColumnType::String->value,
            'array' => false,
        ]);
        $attributes[] = new BaseDocument([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => ColumnType::Datetime->value,
            'array' => false,
        ]);
        $attributes[] = new BaseDocument([
            '$id' => '$updatedAt',
            'key' => '$updatedAt',
            'type' => ColumnType::Datetime->value,
            'array' => false,
        ]);

        $validators = [
            new Select($attributes, $supportForAttributes),
        ];

        parent::__construct($validators);
    }
}
