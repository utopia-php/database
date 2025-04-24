<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class FilterTest extends TestCase
{
    protected DocumentsValidator $validator;

    /**
     * @throws \Utopia\Database\Exception
     */
    public function setUp(): void
    {
        $collection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            'name' => 'movies',
            'attributes' => [],
            'indexes' => [],
        ]);

        $collection->setAttribute('attributes', [
            new Document([
                '$id' => 'string',
                'key' => 'string',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'string_array',
                'key' => 'string_array',
                'type' => Database::VAR_STRING,
                'array' => true,
            ]),
            new Document([
                '$id' => 'integer_array',
                'key' => 'integer_array',
                'type' => Database::VAR_INTEGER,
                'array' => true,
            ]),
            new Document([
                '$id' => 'integer',
                'key' => 'integer',
                'type' => Database::VAR_INTEGER,
                'array' => false,
            ]),
        ]);

        $context = new QueryContext();

        $context->add($collection);

        $this->validator = new DocumentsValidator($context);
    }

    public function testSuccess(): void
    {
        $this->assertTrue($this->validator->isValid([Query::between('string', '1975-12-06', '2050-12-06')]));
        $this->assertTrue($this->validator->isValid([Query::isNotNull('string')]));
        $this->assertTrue($this->validator->isValid([Query::isNull('string')]));
        $this->assertTrue($this->validator->isValid([Query::startsWith('string', 'super')]));
        $this->assertTrue($this->validator->isValid([Query::endsWith('string', 'man')]));
        $this->assertTrue($this->validator->isValid([Query::contains('string_array', ['super'])]));
        $this->assertTrue($this->validator->isValid([Query::contains('integer_array', [100,10,-1])]));
        $this->assertTrue($this->validator->isValid([Query::contains('string_array', ["1","10","-1"])]));
        $this->assertTrue($this->validator->isValid([Query::contains('string', ['super'])]));

        /**
         * Non filters, Now we allow all types
         */

        $this->assertTrue($this->validator->isValid([Query::limit(1)]));
        $this->assertTrue($this->validator->isValid([Query::limit(5000)]));
        $this->assertTrue($this->validator->isValid([Query::offset(1)]));
        $this->assertTrue($this->validator->isValid([Query::offset(5000)]));
        $this->assertTrue($this->validator->isValid([Query::offset(0)]));
        $this->assertTrue($this->validator->isValid([Query::orderAsc('string')]));
        $this->assertTrue($this->validator->isValid([Query::orderDesc('string')]));

    }

    public function testFailure(): void
    {
        $this->assertFalse($this->validator->isValid([Query::select(['attr'])]));
        $this->assertEquals('Invalid query: Attribute not found in schema: attr', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid([Query::limit(0)]));
        $this->assertFalse($this->validator->isValid([Query::limit(-1)]));
        $this->assertFalse($this->validator->isValid([Query::offset(-1)]));
        $this->assertFalse($this->validator->isValid([Query::equal('dne', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::equal('', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::cursorAfter(new Document(['asdf']))]));
        $this->assertFalse($this->validator->isValid([Query::cursorBefore(new Document(['asdf']))]));
        $this->assertFalse($this->validator->isValid([Query::contains('integer', ['super'])]));
        $this->assertFalse($this->validator->isValid([Query::equal('integer_array', [100,-1])]));
        $this->assertFalse($this->validator->isValid([Query::contains('integer_array', [10.6])]));
    }

    public function testTypeMismatch(): void
    {
        $this->assertFalse($this->validator->isValid([Query::equal('string', [false])]));
        $this->assertEquals('Invalid query: Query value is invalid for attribute "string"', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid([Query::equal('string', [1])]));
        $this->assertEquals('Invalid query: Query value is invalid for attribute "string"', $this->validator->getDescription());
    }

    public function testEmptyValues(): void
    {
        $this->assertFalse($this->validator->isValid([Query::contains('string', [])]));
        $this->assertEquals('Invalid query: Contains queries require at least one value.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid([Query::equal('string', [])]));
        $this->assertEquals('Invalid query: Equal queries require at least one value.', $this->validator->getDescription());
    }

    public function testMaxValuesCount(): void
    {
        $values = [];
        for ($i = 1; $i <= 200; $i++) {
            $values[] = $i;
        }

        $this->assertFalse($this->validator->isValid([Query::equal('integer', $values)]));
        $this->assertEquals('Invalid query: Query on attribute has greater than 100 values: integer', $this->validator->getDescription());
    }
}
