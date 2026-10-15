<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Query\Method;

class CursorTest extends TestCase
{
    public function test_value_success(): void
    {
        $validator = new Cursor();

        $this->assertTrue($validator->isValid(new Query(Method::CursorAfter, values: ['asdf'])));
        $this->assertTrue($validator->isValid(new Query(Method::CursorBefore, values: ['asdf'])));
    }

    public function test_value_failure(): void
    {
        $validator = new Cursor();

        $this->assertFalse($validator->isValid(Query::limit(-1)));
        $this->assertEquals('Invalid query', $validator->getDescription());
        $this->assertFalse($validator->isValid(Query::limit(101)));
        $this->assertFalse($validator->isValid(Query::offset(-1)));
        $this->assertFalse($validator->isValid(Query::offset(5001)));
        $this->assertFalse($validator->isValid(Query::equal('attr', ['v'])));
        $this->assertFalse($validator->isValid(Query::orderAsc('attr')));
        $this->assertFalse($validator->isValid(Query::orderDesc('attr')));
    }

    public function test_non_query_value_returns_false(): void
    {
        $validator = new Cursor();

        $this->assertFalse($validator->isValid('some_string'));
        $this->assertFalse($validator->isValid(42));
        $this->assertFalse($validator->isValid(null));
        $this->assertFalse($validator->isValid(['array']));
    }

    public function test_invalid_cursor_value_fails_uid_validation(): void
    {
        $validator = new Cursor();

        $tooLong = str_repeat('x', 300);
        $query = new Query(Method::CursorAfter, values: [$tooLong]);
        $this->assertFalse($validator->isValid($query));
        $this->assertStringContainsString('Invalid cursor', $validator->getDescription());

        $emptyQuery = new Query(Method::CursorBefore, values: ['']);
        $this->assertFalse($validator->isValid($emptyQuery));
    }
}
