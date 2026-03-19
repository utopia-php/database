<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\IdentityMap;

class IdentityMapTest extends TestCase
{
    private IdentityMap $map;

    protected function setUp(): void
    {
        $this->map = new IdentityMap();
    }

    public function testPutAndGet(): void
    {
        $entity = new \stdClass();
        $entity->name = 'test';

        $this->map->put('users', 'abc123', $entity);

        $this->assertSame($entity, $this->map->get('users', 'abc123'));
    }

    public function testGetReturnsNullForMissing(): void
    {
        $this->assertNull($this->map->get('users', 'nonexistent'));
        $this->assertNull($this->map->get('nonexistent', 'abc'));
    }

    public function testHas(): void
    {
        $entity = new \stdClass();
        $this->map->put('users', 'abc', $entity);

        $this->assertTrue($this->map->has('users', 'abc'));
        $this->assertFalse($this->map->has('users', 'xyz'));
        $this->assertFalse($this->map->has('other', 'abc'));
    }

    public function testRemove(): void
    {
        $entity = new \stdClass();
        $this->map->put('users', 'abc', $entity);
        $this->map->remove('users', 'abc');

        $this->assertFalse($this->map->has('users', 'abc'));
        $this->assertNull($this->map->get('users', 'abc'));
    }

    public function testClear(): void
    {
        $this->map->put('users', 'a', new \stdClass());
        $this->map->put('users', 'b', new \stdClass());
        $this->map->put('posts', 'c', new \stdClass());

        $this->map->clear();

        $this->assertEmpty($this->map->all());
        $this->assertFalse($this->map->has('users', 'a'));
    }

    public function testAll(): void
    {
        $e1 = new \stdClass();
        $e2 = new \stdClass();
        $e3 = new \stdClass();

        $this->map->put('users', 'a', $e1);
        $this->map->put('users', 'b', $e2);
        $this->map->put('posts', 'c', $e3);

        $all = $this->map->all();
        $this->assertCount(3, $all);
        $this->assertContains($e1, $all);
        $this->assertContains($e2, $all);
        $this->assertContains($e3, $all);
    }

    public function testOverwrite(): void
    {
        $e1 = new \stdClass();
        $e1->v = 1;
        $e2 = new \stdClass();
        $e2->v = 2;

        $this->map->put('users', 'a', $e1);
        $this->map->put('users', 'a', $e2);

        $this->assertSame($e2, $this->map->get('users', 'a'));
        $this->assertCount(1, $this->map->all());
    }
}
