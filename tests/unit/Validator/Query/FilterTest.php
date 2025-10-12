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
        $this->assertFalse($this->validator->isValid([Query::select('attr')]));
        $this->assertEquals('Invalid query: Attribute not found in schema: attr', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid([Query::limit(0)]));
        $this->assertFalse($this->validator->isValid([Query::limit(-1)]));
        $this->assertFalse($this->validator->isValid([Query::offset(-1)]));
        $this->assertFalse($this->validator->isValid([Query::equal('dne', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::equal('', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::cursorAfter(new Document(['$uid'=>'asdf']))]));
        $this->assertFalse($this->validator->isValid([Query::cursorBefore(new Document(['$uid'=>'asdf']))]));
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
        $max = $this->validator->getMaxValuesCount();
        $values = [];
        for ($i = 1; $i <= $max + 1; $i++) {
            $values[] = $i;
        }

        $this->assertFalse($this->validator->isValid(Query::equal('integer', $values)));
        $this->assertEquals('Query on attribute has greater than '.$max.' values: integer', $this->validator->getDescription());
    }

    public function testNotContains(): void
    {
        // Test valid notContains queries
        $this->assertTrue($this->validator->isValid(Query::notContains('string', ['unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('string_array', ['spam', 'unwanted'])));
        $this->assertTrue($this->validator->isValid(Query::notContains('integer_array', [100, 200])));

        // Test invalid notContains queries (empty values)
        $this->assertFalse($this->validator->isValid(Query::notContains('string', [])));
        $this->assertEquals('NotContains queries require at least one value.', $this->validator->getDescription());
    }

    public function testNotSearch(): void
    {
        // Test valid notSearch queries
        $this->assertTrue($this->validator->isValid(Query::notSearch('string', 'unwanted')));

        // Test that arrays cannot use notSearch
        $this->assertFalse($this->validator->isValid(Query::notSearch('string_array', 'unwanted')));
        $this->assertEquals('Cannot query notSearch on attribute "string_array" because it is an array.', $this->validator->getDescription());

        // Test multiple values not allowed
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_NOT_SEARCH, 'string', ['word1', 'word2'])));
        $this->assertEquals('NotSearch queries require exactly one value.', $this->validator->getDescription());
    }

    public function testNotStartsWith(): void
    {
        // Test valid notStartsWith queries
        $this->assertTrue($this->validator->isValid(Query::notStartsWith('string', 'temp')));

        // Test that arrays cannot use notStartsWith
        $this->assertFalse($this->validator->isValid(Query::notStartsWith('string_array', 'temp')));
        $this->assertEquals('Cannot query notStartsWith on attribute "string_array" because it is an array.', $this->validator->getDescription());

        // Test multiple values not allowed
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_NOT_STARTS_WITH, 'string', ['prefix1', 'prefix2'])));
        $this->assertEquals('NotStartsWith queries require exactly one value.', $this->validator->getDescription());
    }

    public function testNotEndsWith(): void
    {
        // Test valid notEndsWith queries
        $this->assertTrue($this->validator->isValid(Query::notEndsWith('string', '.tmp')));

        // Test that arrays cannot use notEndsWith
        $this->assertFalse($this->validator->isValid(Query::notEndsWith('string_array', '.tmp')));
        $this->assertEquals('Cannot query notEndsWith on attribute "string_array" because it is an array.', $this->validator->getDescription());

        // Test multiple values not allowed
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_NOT_ENDS_WITH, 'string', ['suffix1', 'suffix2'])));
        $this->assertEquals('NotEndsWith queries require exactly one value.', $this->validator->getDescription());
    }

    public function testNotBetween(): void
    {
        // Test valid notBetween queries
        $this->assertTrue($this->validator->isValid(Query::notBetween('integer', 0, 50)));

        // Test that arrays cannot use notBetween
        $this->assertFalse($this->validator->isValid(Query::notBetween('integer_array', 1, 10)));
        $this->assertEquals('Cannot query notBetween on attribute "integer_array" because it is an array.', $this->validator->getDescription());

        // Test wrong number of values
        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_NOT_BETWEEN, 'integer', [10])));
        $this->assertEquals('NotBetween queries require exactly two values.', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(new Query(Query::TYPE_NOT_BETWEEN, 'integer', [10, 20, 30])));
        $this->assertEquals('NotBetween queries require exactly two values.', $this->validator->getDescription());
    }
}
