<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Limit;

class LimitTest extends TestCase
{
    public function testValue(): void
    {
        $validator = new Limit(100);

        // Test for Success
        $this->assertEquals($validator->isValid(Query::limit(1)), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(100)), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::limit(0)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(101)), false, $validator->getDescription());
    }
}
