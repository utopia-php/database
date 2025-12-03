<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;

class CursorTest extends TestCase
{
    /**
     * @throws Exception
     */
    public function test_value_success(): void
    {
        $validator = new Cursor();

        $this->assertTrue($validator->isValid(Query::cursorAfter(new Document(['$id' => 'asb']))));
        $this->assertTrue($validator->isValid(Query::cursorBefore(new Document(['$id' => 'asb']))));
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
}
