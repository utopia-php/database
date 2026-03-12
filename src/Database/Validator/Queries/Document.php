<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

class Document extends Queries
{
    /**
     * @param array<mixed> $attributes
     * @param bool $supportForAttributes
     * @throws Exception
     */
    public function __construct(array $attributes, bool $supportForAttributes = true)
    {
        $attributes[] = new \Utopia\Database\Document([
            '$id' => '$id',
            'key' => '$id',
            'type' => ColumnType::String->value,
            'array' => false,
        ]);
        $attributes[] = new \Utopia\Database\Document([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => ColumnType::Datetime->value,
            'array' => false,
        ]);
        $attributes[] = new \Utopia\Database\Document([
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
