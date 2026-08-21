<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Having;

class HavingTest extends TestCase
{
    public function testValueSuccess(): void
    {
        $validator = new Having();

        $this->assertTrue($validator->isValid(Query::having([Query::equal('count', [1])])));
    }

    public function testValueFailure(): void
    {
        $validator = new Having();

        $this->assertFalse($validator->isValid(Query::having([])));
        $this->assertSame('Having requires at least one condition', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::having(['count > 1'])));
        $this->assertSame('Having conditions must be Query instances', $validator->getDescription());
    }
}
