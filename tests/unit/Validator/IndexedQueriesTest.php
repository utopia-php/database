<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Swoole\FastCGI\Record\Data;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class IndexedQueriesTest extends TestCase
{
    protected Document $collection;

    /**
     * @throws Exception
     * @throws Exception\Query
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

        $this->collection = $collection;
    }

    public function tearDown(): void
    {
    }

    public function testEmptyQueries(): void
    {
        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function testInvalidQuery(): void
    {
        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $this->assertEquals(false, $validator->isValid(["this.is.invalid"]));
    }

    public function testInvalidMethod(): void
    {
        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $this->assertEquals(false, $validator->isValid(['equal("attr", "value")']));
    }

    public function testInvalidValue(): void
    {
        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $this->assertEquals(false, $validator->isValid(['limit(-1)']));
    }

    public function testValid(): void
    {
        $collection = $this->collection;

        $collection->setAttribute('attributes', [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ]);

        $collection->setAttribute('indexes', [
            new Document([
                'type' => Database::INDEX_KEY,
                'attributes' => ['name'],
            ]),
            new Document([
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['name'],
            ]),
        ]);

        $context = new QueryContext();

        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $query = Query::cursorAfter(new Document(['$id' => 'abc']));
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"cursorAfter","attribute":"","values":["abc"]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::parse('{"method":"cursorAfter","values":["abc"]}'); // No attribute required
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::equal('name', ['value']);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"equal","attribute":"name","values":["value"]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::limit(10);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"limit","values":[10]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::offset(10);
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"offset","values":[10]}');
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::orderAsc('name');
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"orderAsc","attribute":"name"}'); // No values required
        $this->assertEquals(true, $validator->isValid([$query]));

        $query = Query::search('name', 'value');
        $this->assertEquals(true, $validator->isValid([$query]));
        $query = Query::parse('{"method":"search","attribute":"name","values":["value"]}');
        $this->assertEquals(true, $validator->isValid([$query]));
    }

    public function testMissingIndex(): void
    {
        $collection = $this->collection;

        $collection->setAttribute('attributes', [
            new Document([
                'key' => 'name',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ]);

        $collection->setAttribute('indexes', [
            new Document([
                'type' => Database::INDEX_KEY,
                'attributes' => ['name'],
            ]),
        ]);

        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $query = Query::equal('dne', ['value']);
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::orderAsc('dne');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('dne', 'phrase');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Invalid query: Attribute not found in schema: dne', $validator->getDescription());

        $query = Query::search('name', 'phrase');
        $this->assertEquals(false, $validator->isValid([$query]));
        $this->assertEquals('Searching by attribute "name" requires a fulltext index.', $validator->getDescription());
    }

    public function testTwoAttributesFulltext(): void
    {
        $collection = $this->collection;

        $collection->setAttribute('attributes', [
            new Document([
                '$id' => 'ft1',
                'key' => 'ft1',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'ft2',
                'key' => 'ft2',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ]);

        $collection->setAttribute('indexes', [
            new Document([
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['ft1','ft2'],
            ]),
        ]);

        $context = new QueryContext();
        $context->add($this->collection);

        $validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER
        );

        $this->assertEquals(false, $validator->isValid([Query::search('ft1', 'value')]));
    }


    public function testJsonParse(): void
    {
        try {
            Query::parse('{"method":"equal","attribute":"name","values":["value"]'); // broken Json;
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: Syntax error', $e->getMessage());
        }
    }
}
