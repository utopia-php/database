<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Id;

class IdTest extends TestCase
{
    public function test_custom_id(): void
    {
        $id = Id::custom('test');
        $this->assertEquals('test', $id);
    }

    public function test_unique_id(): void
    {
        $id = Id::unique();
        $this->assertNotEmpty($id);
        $this->assertIsString($id); // @phpstan-ignore method.alreadyNarrowedType
    }
}
