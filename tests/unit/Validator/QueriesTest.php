<?php

namespace Tests\Unit\Validator;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;

class QueriesTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testEmptyQueries(): void
    {
        $validator = new Queries();

        $this->assertEquals(true, $validator->isValid([]));
    }

    public function testInvalidMethod(): void
    {
        $validator = new Queries();
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ["value"])]));

        $validator = new Queries([new Limit()]);
        $this->assertEquals(false, $validator->isValid([Query::equal('attr', ["value"])]));
    }

    public function testInvalidValue(): void
    {
        $validator = new Queries([new Limit()]);
        $this->assertEquals(false, $validator->isValid([Query::limit(-1)]));
    }

    /**
     * @throws Exception
     */
    public function testValid(): void
    {
        $attributes = [
            new Document([
                '$id' => 'name',
                'key' => 'name',
                'type' => Database::VAR_STRING,
                'array' => false,
            ])
        ];

        $validator = new Queries(
            [
                new Cursor(),
                new Filter($attributes, 'int'),
                new Limit(),
                new Offset(),
                new Order($attributes)
            ]
        );

        $this->assertEquals(true, $validator->isValid([Query::cursorAfter(new Document(['$id' => 'asdf']))]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::equal('name', ['value'])]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::limit(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::offset(10)]), $validator->getDescription());
        $this->assertEquals(true, $validator->isValid([Query::orderAsc('name')]), $validator->getDescription());
    }
}
