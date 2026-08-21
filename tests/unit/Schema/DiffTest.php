<?php

namespace Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Index;
use Utopia\Database\Schema\ChangeType;
use Utopia\Database\Schema\Diff;

class DiffTest extends TestCase
{
    private Diff $differ;

    protected function setUp(): void
    {
        $this->differ = new Diff();
    }

    public function testNoChanges(): void
    {
        $collection = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
            ],
        );

        $result = $this->differ->diff($collection, $collection);

        $this->assertFalse($result->hasChanges());
        $this->assertEmpty($result->changes);
    }

    public function testDetectAddedAttribute(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
                Attribute::string(key: 'email'),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $this->assertTrue($result->hasChanges());
        $additions = $result->getAdditions();
        $this->assertCount(1, $additions);
        $change = \array_values($additions)[0];
        $this->assertEquals(ChangeType::AddAttribute, $change->type);
        $this->assertEquals('email', $change->attribute->key);
    }

    public function testDetectRemovedAttribute(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
                Attribute::string(key: 'email'),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $removals = $result->getRemovals();
        $this->assertCount(1, $removals);
        $change = \array_values($removals)[0];
        $this->assertEquals(ChangeType::DropAttribute, $change->type);
        $this->assertEquals('email', $change->attribute->key);
    }

    public function testDetectModifiedAttribute(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name', size: 100),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $modifications = $result->getModifications();
        $this->assertCount(1, $modifications);
        $change = \array_values($modifications)[0];
        $this->assertEquals(ChangeType::ModifyAttribute, $change->type);
        $this->assertEquals(255, $change->attribute->size);
        $this->assertEquals(100, $change->previousAttribute->size);
    }

    public function testDetectAddedIndex(): void
    {
        $source = new Collection(id: 'test');
        $target = new Collection(
            id: 'test',
            indexes: [
                Index::index(key: 'idx_name', attributes: ['name']),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $additions = $result->getAdditions();
        $this->assertCount(1, $additions);
        $change = \array_values($additions)[0];
        $this->assertEquals(ChangeType::AddIndex, $change->type);
        $this->assertEquals('idx_name', $change->index->key);
    }

    public function testDetectRemovedIndex(): void
    {
        $source = new Collection(
            id: 'test',
            indexes: [
                Index::index(key: 'idx_name', attributes: ['name']),
            ],
        );
        $target = new Collection(id: 'test');

        $result = $this->differ->diff($source, $target);

        $removals = $result->getRemovals();
        $this->assertCount(1, $removals);
        $change = \array_values($removals)[0];
        $this->assertEquals(ChangeType::DropIndex, $change->type);
    }

    public function testComplexDiff(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name', size: 100),
                Attribute::string(key: 'old_field', size: 50),
            ],
            indexes: [
                Index::index(key: 'idx_old', attributes: ['old_field']),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                Attribute::string(key: 'name'),
                Attribute::integer(key: 'new_field'),
            ],
            indexes: [
                Index::index(key: 'idx_new', attributes: ['new_field']),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $this->assertTrue($result->hasChanges());
        $this->assertNotEmpty($result->getAdditions());
        $this->assertNotEmpty($result->getRemovals());
        $this->assertNotEmpty($result->getModifications());
    }
}
