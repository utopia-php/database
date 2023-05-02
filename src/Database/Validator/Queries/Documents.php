<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;

class Documents extends IndexedQueries
{
    /**
     * Expression constructor
     *
     * @param array<mixed> $attributes
     * @param array<mixed> $indexes
     * @throws Exception
     */
    public function __construct(array $attributes, array $indexes)
    {
        $attributes[] = new Document([
            '$id' => '$id',
            'key' => '$id',
            'type' => Database::VAR_STRING,
            'array' => false,
        ]);

        $attributes[] = new Document([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $attributes[] = new Document([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $validators = [
            new Limit(),
            new Offset(),
            new Cursor(), // I think this should be checks against $attributes?
            new Filter($attributes),
            new Order($attributes),
            new Select($attributes),
        ];

        parent::__construct($attributes, $indexes, ...$validators);
    }
}
