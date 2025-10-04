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
     * @param string $idAttributeType
     * @throws Exception
     */
    public function __construct(
        array $attributes,
        array $indexes,
        string $idAttributeType,
        int $maxValuesCount = 5000,
        \DateTime $minAllowedDate = new \DateTime('0000-01-01'),
        \DateTime $maxAllowedDate = new \DateTime('9999-12-31'),
        bool $supportForAttributes = true
    ) {
        $attributes[] = new Document([
            '$id' => '$id',
            'key' => '$id',
            'type' => Database::VAR_STRING,
            'array' => false,
        ]);
        $attributes[] = new Document([
            '$id' => '$sequence',
            'key' => '$sequence',
            'type' => Database::VAR_ID,
            'array' => false,
        ]);
        $attributes[] = new Document([
            '$id' => '$createdAt',
            'key' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);
        $attributes[] = new Document([
            '$id' => '$updatedAt',
            'key' => '$updatedAt',
            'type' => Database::VAR_DATETIME,
            'array' => false,
        ]);

        $validators = [
            new Limit(),
            new Offset(),
            new Cursor(),
            new Filter(
                $attributes,
                $idAttributeType,
                $maxValuesCount,
                $minAllowedDate,
                $maxAllowedDate,
                $supportForAttributes
            ),
            new Order($attributes, $supportForAttributes),
            new Select($attributes, $supportForAttributes),
        ];

        parent::__construct($attributes, $indexes, $validators);
    }
}
