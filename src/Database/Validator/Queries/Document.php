<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Select;

class Document extends Queries
{
    /**
     * @param array<mixed> $attributes
     * @throws Exception
     */
    public function __construct(array $attributes)
    {
        $attributes[] = new \Utopia\Database\Document([
            '$id' => '$id',
            'key' => '$id',
            'type' => Database::VAR_STRING,
            'array' => false,
        ]);

        $attributes[] = new \Utopia\Database\Document([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $attributes[] = new \Utopia\Database\Document([
            '$id' => '$updatedAt',
            'key' => '$updatedAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $validators = [
            new Select($attributes),
        ];

        parent::__construct($validators);
    }
}
