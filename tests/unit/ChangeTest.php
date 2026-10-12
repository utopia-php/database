<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Change;
use Utopia\Database\Document;

final class ChangeTest extends TestCase
{
    public function testConstructorWithOldAndNew(): void
    {
        $old = new Document(['$id' => 'doc1', 'name' => 'Old Name']);
        $new = new Document(['$id' => 'doc1', 'name' => 'New Name']);

        $change = new Change($old, $new);

        $this->assertSame($old, $change->old);
        $this->assertSame($new, $change->new);
    }

    public function testOldAndNewCarryTheirDocuments(): void
    {
        $old = new Document(['$id' => 'test', 'status' => 'draft']);
        $new = new Document(['$id' => 'test', 'status' => 'published']);

        $change = new Change($old, $new);

        $this->assertSame('draft', $change->old->getAttribute('status'));
        $this->assertSame('published', $change->new->getAttribute('status'));
        $this->assertSame('test', $change->old->getId());
        $this->assertSame('test', $change->new->getId());
    }

    public function testDocumentsCannotBeReplacedOnceSet(): void
    {
        $change = new Change(new Document(['$id' => 'doc', 'val' => 1]), new Document(['$id' => 'doc', 'val' => 2]));

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Cannot modify readonly property Utopia\Database\Change::$old');

        (static function (Change $change): void {
            /** @phpstan-ignore-next-line property.readOnlyAssignOutOfClass */
            $change->old = new Document(['$id' => 'doc', 'val' => 0]);
        })($change);
    }

    public function testWithEmptyDocuments(): void
    {
        $old = new Document();
        $new = new Document();

        $change = new Change($old, $new);

        $this->assertTrue($change->old->isEmpty());
        $this->assertTrue($change->new->isEmpty());
    }
}
