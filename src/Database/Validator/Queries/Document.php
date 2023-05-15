<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Select;
use Utopia\Database\Document as utopiaDocument;

class Document extends Queries
{
    /**
     * @param array<mixed> $attributes
     * @throws Exception
     */
    public function __construct(array $attributes)
    {
        $attributes[] = new utopiaDocument([
            '$id' => '$id',
            'key' => '$id',
            'type' => Database::VAR_STRING,
            'array' => false,
        ]);

        $attributes[] = new utopiaDocument([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $attributes[] = new utopiaDocument([
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
