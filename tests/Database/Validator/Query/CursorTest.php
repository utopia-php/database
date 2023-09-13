<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;

class CursorTest extends TestCase
{
    public function testValueSuccess(): void
    {
        $validator = new Cursor();

        $this->assertTrue($validator->isValid(new Query(Query::TYPE_CURSORAFTER, values: ['asdf'])));
        $this->assertTrue($validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: ['asdf'])));
        $this->assertTrue($validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: [
            new Document(['$id' => 'abc102030'])
        ])));
    }

    public function testValueFailure(): void
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

        $uid = 'uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_uid0123456_';
        $this->assertFalse($validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: [
            new Document(['$id' => $uid])
        ])));

        $this->assertEquals('Invalid cursor: Cursor must contain at most 100 chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore', $validator->getDescription());
    }
}
