<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;

class CursorTest extends TestCase
{
    public function testValue(): void
    {
        $validator = new Cursor();

        // Test for Success
        $this->assertEquals($validator->isValid(new Query(Query::TYPE_CURSORAFTER, values: ['asdf'])), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: ['asdf'])), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::limit(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(101)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5001)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('attr', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderAsc('attr')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('attr')), false, $validator->getDescription());
    }
}
