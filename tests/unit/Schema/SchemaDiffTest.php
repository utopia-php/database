<?php

namespace Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Index;
use Utopia\Database\Schema\SchemaChangeType;
use Utopia\Database\Schema\SchemaDiff;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class SchemaDiffTest extends TestCase
{
    private SchemaDiff $differ;

    protected function setUp(): void
    {
        $this->differ = new SchemaDiff();
    }

    public function testNoChanges(): void
    {
        $collection = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
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
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
                new Attribute(key: 'email', type: ColumnType::String, size: 255),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $this->assertTrue($result->hasChanges());
        $additions = $result->getAdditions();
        $this->assertCount(1, $additions);
        $change = \array_values($additions)[0];
        $this->assertEquals(SchemaChangeType::AddAttribute, $change->type);
        $this->assertEquals('email', $change->attribute->key);
    }

    public function testDetectRemovedAttribute(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
                new Attribute(key: 'email', type: ColumnType::String, size: 255),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $removals = $result->getRemovals();
        $this->assertCount(1, $removals);
        $change = \array_values($removals)[0];
        $this->assertEquals(SchemaChangeType::DropAttribute, $change->type);
        $this->assertEquals('email', $change->attribute->key);
    }

    public function testDetectModifiedAttribute(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 100),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $modifications = $result->getModifications();
        $this->assertCount(1, $modifications);
        $change = \array_values($modifications)[0];
        $this->assertEquals(SchemaChangeType::ModifyAttribute, $change->type);
        $this->assertEquals(255, $change->attribute->size);
        $this->assertEquals(100, $change->previousAttribute->size);
    }

    public function testDetectAddedIndex(): void
    {
        $source = new Collection(id: 'test');
        $target = new Collection(
            id: 'test',
            indexes: [
                new Index(key: 'idx_name', type: IndexType::Index, attributes: ['name']),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $additions = $result->getAdditions();
        $this->assertCount(1, $additions);
        $change = \array_values($additions)[0];
        $this->assertEquals(SchemaChangeType::AddIndex, $change->type);
        $this->assertEquals('idx_name', $change->index->key);
    }

    public function testDetectRemovedIndex(): void
    {
        $source = new Collection(
            id: 'test',
            indexes: [
                new Index(key: 'idx_name', type: IndexType::Index, attributes: ['name']),
            ],
        );
        $target = new Collection(id: 'test');

        $result = $this->differ->diff($source, $target);

        $removals = $result->getRemovals();
        $this->assertCount(1, $removals);
        $change = \array_values($removals)[0];
        $this->assertEquals(SchemaChangeType::DropIndex, $change->type);
    }

    public function testComplexDiff(): void
    {
        $source = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 100),
                new Attribute(key: 'old_field', type: ColumnType::String, size: 50),
            ],
            indexes: [
                new Index(key: 'idx_old', type: IndexType::Index, attributes: ['old_field']),
            ],
        );

        $target = new Collection(
            id: 'test',
            attributes: [
                new Attribute(key: 'name', type: ColumnType::String, size: 255),
                new Attribute(key: 'new_field', type: ColumnType::Integer, size: 0),
            ],
            indexes: [
                new Index(key: 'idx_new', type: IndexType::Index, attributes: ['new_field']),
            ],
        );

        $result = $this->differ->diff($source, $target);

        $this->assertTrue($result->hasChanges());
        $this->assertNotEmpty($result->getAdditions());
        $this->assertNotEmpty($result->getRemovals());
        $this->assertNotEmpty($result->getModifications());
    }
}
