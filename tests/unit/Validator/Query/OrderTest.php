<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Order;
use Utopia\Query\Schema\ColumnType;

class OrderTest extends TestCase
{
    protected Base|null $validator = null;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $this->validator = new Order(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => '$sequence',
                    'key' => '$sequence',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::orderAsc('attr')));
        $this->assertTrue($this->validator->isValid(Query::orderAsc()));
        $this->assertTrue($this->validator->isValid(Query::orderDesc('attr')));
        $this->assertTrue($this->validator->isValid(Query::orderDesc()));
    }

    public function testValueFailure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(-1)));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::limit(101)));
        $this->assertFalse($this->validator->isValid(Query::offset(-1)));
        $this->assertFalse($this->validator->isValid(Query::offset(5001)));
        $this->assertFalse($this->validator->isValid(Query::equal('attr', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::equal('dne', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::equal('', ['v'])));
        $this->assertFalse($this->validator->isValid(Query::orderDesc('dne')));
        $this->assertFalse($this->validator->isValid(Query::orderAsc('dne')));
    }
}
