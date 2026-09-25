<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Schema\ColumnType;

class DocumentsQueriesUnsignedBigIntTest extends TestCase
{
    private const string ABOVE_SIGNED_MAX = '10000000000000000000';

    /**
     * @var array<Document>
     */
    private array $attributes;

    protected function setUp(): void
    {
        $this->attributes = [
            new Document([
                '$id' => 'counter',
                'key' => 'counter',
                'type' => ColumnType::BigInteger->value,
                'size' => 0,
                'required' => false,
                'signed' => false,
                'array' => false,
                'filters' => [],
            ]),
        ];
    }

    public function test_documents_validator_accepts_unsigned_values_above_signed_max_by_default(): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
        );

        $this->assertTrue($validator->isValid([Query::equal('counter', [self::ABOVE_SIGNED_MAX])]), $validator->getDescription());
    }

    public function test_filter_validator_accepts_unsigned_values_above_signed_max_by_default(): void
    {
        $validator = new Filter(
            attributes: $this->attributes,
            idAttributeType: ColumnType::Integer->value,
        );

        $this->assertTrue($validator->isValid(Query::equal('counter', [self::ABOVE_SIGNED_MAX])), $validator->getDescription());
    }

    public function test_adapters_without_unsigned_bigint_still_reject_values_above_signed_max(): void
    {
        $validator = new Documents(
            attributes: $this->attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportUnsignedBigInt: false,
        );

        $this->assertFalse($validator->isValid([Query::equal('counter', [self::ABOVE_SIGNED_MAX])]));
        $this->assertSame('Invalid query: Query value is invalid for attribute "counter"', $validator->getDescription());
    }
}
