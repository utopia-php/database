<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;

class CursorTest extends TestCase
{
    public function test_value_success(): void
    {
        $validator = new Cursor;

        $this->assertTrue($validator->isValid(new Query(Query::TYPE_CURSOR_AFTER, values: ['asdf'])));
        $this->assertTrue($validator->isValid(new Query(Query::TYPE_CURSOR_BEFORE, values: ['asdf'])));
    }

    public function test_value_failure(): void
    {
        $validator = new Cursor;

        $this->assertFalse($validator->isValid(Query::limit(-1)));
        $this->assertEquals('Invalid query', $validator->getDescription());
        $this->assertFalse($validator->isValid(Query::limit(101)));
        $this->assertFalse($validator->isValid(Query::offset(-1)));
        $this->assertFalse($validator->isValid(Query::offset(5001)));
        $this->assertFalse($validator->isValid(Query::equal('attr', ['v'])));
        $this->assertFalse($validator->isValid(Query::orderAsc('attr')));
        $this->assertFalse($validator->isValid(Query::orderDesc('attr')));
    }
}
