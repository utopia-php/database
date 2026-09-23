<?php

namespace Tests\Unit\Validator;

use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

trait QueryShapeAttributes
{
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
}
