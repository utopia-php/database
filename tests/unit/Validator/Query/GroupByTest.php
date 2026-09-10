<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

class GroupByTest extends TestCase
{
    protected GroupBy $validator;

    protected function setUp(): void
    {
        $this->validator = new GroupBy(
            attributes: [
                new Document([
                    '$id' => 'name',
                    'key' => 'name',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'status',
                    'key' => 'status',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::groupBy(['name'])));
        $this->assertTrue($this->validator->isValid(Query::groupBy(['name', 'status'])));
        $this->assertTrue($this->validator->isValid(Query::groupBy(['$id'])));
    }

    public function testValueFailure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::groupBy([])));
        $this->assertSame('GroupBy requires at least one attribute', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(new Query(Method::GroupBy, '', [123])));
        $this->assertSame('GroupBy attributes must be non-empty strings', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(new Query(Method::GroupBy, '', [''])));
        $this->assertSame('GroupBy attributes must be non-empty strings', $this->validator->getDescription());
    }

    public function testUnknownAttributeRejected(): void
    {
        // Reaches the adapter as a bare identifier, so an unknown name is a 500 from the
        // engine rather than a 400 from validation, and doubles as a probe for column names.
        $this->assertFalse($this->validator->isValid(Query::groupBy(['nonexistent'])));
        $this->assertSame('Attribute not found in schema: nonexistent', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::groupBy(['name', 'nonexistent'])));
        $this->assertSame('Attribute not found in schema: nonexistent', $this->validator->getDescription());
    }

    public function testUnknownAttributeAllowedWithoutAttributeSupport(): void
    {
        $validator = new GroupBy(attributes: [], supportForAttributes: false);

        $this->assertTrue($validator->isValid(Query::groupBy(['anything'])));
    }
}
