<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Order;

class OrderTest extends TestCase
{
    public function testValue(): void
    {
        $validator = new Order(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );

        // Test for Success
        $this->assertEquals($validator->isValid(Query::orderAsc('attr')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderAsc('')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('attr')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('')), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::limit(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(101)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5001)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('attr', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('dne', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('dne')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderAsc('dne')), false, $validator->getDescription());
    }
}
