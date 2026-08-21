<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Query\Method;

class GroupByTest extends TestCase
{
    public function testValueSuccess(): void
    {
        $validator = new GroupBy();

        $this->assertTrue($validator->isValid(Query::groupBy(['name'])));
        $this->assertTrue($validator->isValid(Query::groupBy(['name', 'status'])));
    }

    public function testValueFailure(): void
    {
        $validator = new GroupBy();

        $this->assertFalse($validator->isValid(Query::groupBy([])));
        $this->assertSame('GroupBy requires at least one attribute', $validator->getDescription());

        $this->assertFalse($validator->isValid(new Query(Method::GroupBy, '', [123])));
        $this->assertSame('GroupBy attributes must be non-empty strings', $validator->getDescription());

        $this->assertFalse($validator->isValid(new Query(Method::GroupBy, '', [''])));
        $this->assertSame('GroupBy attributes must be non-empty strings', $validator->getDescription());
    }
}
