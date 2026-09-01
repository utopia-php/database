<?php

namespace Utopia\Database\Validator\Queries;

use DateTime;
use Utopia\Database\Document;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Distinct;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates queries for document listing, supporting filters, ordering, pagination, aggregation, and joins.
 */
class Documents extends IndexedQueries
{
    /**
     * @param  array<Document>  $attributes
     * @param  array<Document>  $indexes
     *
     * @throws \Utopia\Database\Exception
     */
    public function __construct(
        array $attributes,
        array $indexes,
        string $idAttributeType,
        int $maxValuesCount = 5000,
        int $maxUIDLength = 36,
        DateTime $minAllowedDate = new DateTime('0000-01-01'),
        DateTime $maxAllowedDate = new DateTime('9999-12-31'),
        bool $supportForAttributes = true,
        bool $supportUnsignedBigInt = false
    ) {
        $attributes[] = new Document([
            Document::ID => Document::ID,
            'key' => Document::ID,
            'type' => ColumnType::String->value,
            'array' => false,
        ]);
        $attributes[] = new Document([
            Document::ID => Document::SEQUENCE,
            'key' => Document::SEQUENCE,
            'type' => ColumnType::Id->value,
            'array' => false,
        ]);
        $attributes[] = new Document([
            Document::ID => Document::CREATED_AT,
            'key' => Document::CREATED_AT,
            'type' => ColumnType::Datetime->value,
            'array' => false,
        ]);
        $attributes[] = new Document([
            Document::ID => Document::UPDATED_AT,
            'key' => Document::UPDATED_AT,
            'type' => ColumnType::Datetime->value,
            'array' => false,
        ]);

        $validators = [
            new Limit(),
            new Offset(),
            new Cursor($maxUIDLength),
            new Filter(
                $attributes,
                $idAttributeType,
                $maxValuesCount,
                $minAllowedDate,
                $maxAllowedDate,
                $supportForAttributes,
                $supportUnsignedBigInt
            ),
            new Order($attributes, $supportForAttributes),
            new Select($attributes, $supportForAttributes),
            new Join(),
            new Aggregate(),
            new GroupBy(),
            new Having(),
            new Distinct(),
        ];

        parent::__construct($attributes, $indexes, $validators);
    }
}
