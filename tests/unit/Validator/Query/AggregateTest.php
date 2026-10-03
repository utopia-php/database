<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Query\Schema\ColumnType;

class AggregateTest extends TestCase
{
    protected Aggregate $validator;

    protected function setUp(): void
    {
        $this->validator = new Aggregate(
            attributes: [
                new Document([
                    '$id' => 'price',
                    'key' => 'price',
                    'type' => ColumnType::Double->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::sum('price')));
        $this->assertTrue($this->validator->isValid(Query::avg('price', 'avg_price')));
        $this->assertTrue($this->validator->isValid(Query::count('*', 'cnt')));
        $this->assertTrue($this->validator->isValid(Query::count()));
        $this->assertTrue($this->validator->isValid(Query::countDistinct('$id')));
    }

    public function testUnknownAttributeRejected(): void
    {
        // Reaches the adapter as a bare identifier, so an unknown name is a 500 from the
        // engine rather than a 400 from validation, and doubles as a probe for column names.
        $this->assertFalse($this->validator->isValid(Query::sum('nonexistent')));
        $this->assertSame('Attribute not found in schema: nonexistent', $this->validator->getDescription());
    }

    public function testInvalidAliasRejected(): void
    {
        $this->assertFalse($this->validator->isValid(Query::sum('price', 'a b')));
        $this->assertSame('Invalid aggregate alias', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::sum('price', '1alias')));
        $this->assertSame('Invalid aggregate alias', $this->validator->getDescription());
    }

    public function testUnknownAttributeAllowedWithoutAttributeSupport(): void
    {
        $validator = new Aggregate(attributes: [], supportForAttributes: false);

        $this->assertTrue($validator->isValid(Query::sum('anything')));
    }
}
