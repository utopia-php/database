<?php

namespace Tests\Unit\Validator;

use Utopia\Database\Document;
use Utopia\Database\Validator\IndexedQueries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

trait QueryShapeAttributes
{
    private const int MAX_VALUES = 100;

    /**
     * @return array<Document>
     */
    private function attributes(): array
    {
        $attributes = [
            'name' => [ColumnType::String, false],
            'body' => [ColumnType::String, false],
            'price' => [ColumnType::Integer, false],
            'stock' => [ColumnType::BigInteger, false],
            'rating' => [ColumnType::Double, false],
            'active' => [ColumnType::Boolean, false],
            'created' => [ColumnType::Datetime, false],
            'tags' => [ColumnType::String, true],
            'scores' => [ColumnType::Integer, true],
        ];

        $documents = [];
        foreach ($attributes as $key => [$type, $array]) {
            $documents[] = new Document([
                '$id' => $key,
                'key' => $key,
                'type' => $type->value,
                'size' => $type === ColumnType::String ? 1000 : 0,
                'signed' => true,
                'array' => $array,
            ]);
        }

        return $documents;
    }

    /**
     * @param  array<Document>  $indexes
     */
    private function validator(array $indexes = []): IndexedQueries
    {
        $attributes = $this->attributes();

        return new IndexedQueries($attributes, $indexes, [
            new Limit(),
            new Filter($attributes, ColumnType::Integer->value, self::MAX_VALUES),
            new Order($attributes),
            new Select($attributes),
            new Join(),
            new Aggregate($attributes),
            new GroupBy($attributes),
            new Having(),
        ]);
    }
}
