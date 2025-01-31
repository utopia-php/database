<?php

namespace Utopia\Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Count;

class CountTest extends TestCase
{
    protected Base|null $validator = null;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $this->validator = new Count(
            attributes: [
                new Document([
                    '$id' => 'value',
                    'key' => 'value',
                    'type' => Database::VAR_INTEGER,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'valueFloat',
                    'key' => 'valueFloat',
                    'type' => Database::VAR_FLOAT,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'valueStr',
                    'key' => 'valueStr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::count('value')));
        $this->assertTrue($this->validator->isValid(Query::count('valueFloat')));
    }

    public function testValueFailure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(1)));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::count('valueStr')));
    }
}
