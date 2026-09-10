<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Change;
use Utopia\Database\Document;

class ChangeTest extends TestCase
{
    public function testConstructorWithOldAndNew(): void
    {
        $old = new Document(['$id' => 'doc1', 'name' => 'Old Name']);
        $new = new Document(['$id' => 'doc1', 'name' => 'New Name']);

        $change = new Change($old, $new);

        $this->assertSame($old, $change->getOld());
        $this->assertSame($new, $change->getNew());
    }

    public function testGetOldAndGetNew(): void
    {
        $old = new Document(['$id' => 'test', 'status' => 'draft']);
        $new = new Document(['$id' => 'test', 'status' => 'published']);

        $change = new Change($old, $new);

        $this->assertSame('draft', $change->getOld()->getAttribute('status'));
        $this->assertSame('published', $change->getNew()->getAttribute('status'));
        $this->assertSame('test', $change->getOld()->getId());
        $this->assertSame('test', $change->getNew()->getId());
    }

    public function testSetOld(): void
    {
        $old = new Document(['$id' => 'doc', 'val' => 1]);
        $new = new Document(['$id' => 'doc', 'val' => 2]);
        $change = new Change($old, $new);

        $replacement = new Document(['$id' => 'doc', 'val' => 0]);
        $change->setOld($replacement);

        $this->assertSame($replacement, $change->getOld());
        $this->assertSame(0, $change->getOld()->getAttribute('val'));
        $this->assertSame($new, $change->getNew());
    }

    public function testSetNew(): void
    {
        $old = new Document(['$id' => 'doc', 'val' => 1]);
        $new = new Document(['$id' => 'doc', 'val' => 2]);
        $change = new Change($old, $new);

        $replacement = new Document(['$id' => 'doc', 'val' => 99]);
        $change->setNew($replacement);

        $this->assertSame($old, $change->getOld());
        $this->assertSame($replacement, $change->getNew());
        $this->assertSame(99, $change->getNew()->getAttribute('val'));
    }

    public function testWithEmptyDocuments(): void
    {
        $old = new Document();
        $new = new Document();

        $change = new Change($old, $new);

        $this->assertTrue($change->getOld()->isEmpty());
        $this->assertTrue($change->getNew()->isEmpty());
    }
}
