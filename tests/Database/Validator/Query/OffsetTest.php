<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Offset;

class OffsetTest extends TestCase
{
    public function testValue(): void
    {
        $validator = new Offset(5000);

        // Test for Success
        $this->assertEquals($validator->isValid(Query::offset(1)), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(0)), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5000)), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::offset(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5001)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('attr', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderAsc('attr')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('attr')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(100)), false, $validator->getDescription());
    }
}
