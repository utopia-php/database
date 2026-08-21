<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class MetadataFactoryTest extends TestCase
{
    private MetadataFactory $factory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->factory = new MetadataFactory();
    }

    public function testParseEntityAttribute(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertEquals('users', $metadata->collection);
        $this->assertTrue($metadata->documentSecurity);
        $this->assertEquals(TestEntity::class, $metadata->className);
    }

    public function testParseIdProperty(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertEquals('id', $metadata->idProperty);
    }

    public function testParseVersionProperty(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertEquals('version', $metadata->versionProperty);
    }

    public function testParseTimestampProperties(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertEquals('createdAt', $metadata->createdAtProperty);
        $this->assertEquals('updatedAt', $metadata->updatedAtProperty);
    }

    public function testParsePermissionsProperty(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertEquals('permissions', $metadata->permissionsProperty);
    }

    public function testParseColumns(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertCount(4, $metadata->columns);
        $this->assertArrayHasKey('name', $metadata->columns);
        $this->assertArrayHasKey('email', $metadata->columns);
        $this->assertArrayHasKey('age', $metadata->columns);
        $this->assertArrayHasKey('active', $metadata->columns);

        $nameMapping = $metadata->columns['name'];
        $this->assertEquals('name', $nameMapping->propertyName);
        $this->assertEquals('name', $nameMapping->documentKey);
        $this->assertEquals(ColumnType::String, $nameMapping->column->type);
        $this->assertEquals(255, $nameMapping->column->size);
        $this->assertTrue($nameMapping->column->required);

        $ageMapping = $metadata->columns['age'];
        $this->assertEquals(ColumnType::Integer, $ageMapping->column->type);
        $this->assertFalse($ageMapping->column->required);
    }

    public function testParseRelationships(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertCount(1, $metadata->relationships);
        $this->assertArrayHasKey('posts', $metadata->relationships);

        $rel = $metadata->relationships['posts'];
        $this->assertEquals('posts', $rel->propertyName);
        $this->assertEquals('posts', $rel->documentKey);
        $this->assertEquals(RelationType::OneToMany, $rel->type);
        $this->assertEquals(TestPost::class, $rel->targetClass);
        $this->assertEquals('author', $rel->twoWayKey);
        $this->assertTrue($rel->twoWay);
    }

    public function testParseIndexes(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertCount(2, $metadata->indexes);
        $this->assertEquals('idx_email', $metadata->indexes[0]->key);
        $this->assertEquals(IndexType::Unique, $metadata->indexes[0]->type);
        $this->assertEquals(['email'], $metadata->indexes[0]->attributes);

        $this->assertEquals('idx_name', $metadata->indexes[1]->key);
        $this->assertEquals(IndexType::Index, $metadata->indexes[1]->type);
    }

    public function testCaching(): void
    {
        $metadata1 = $this->factory->getMetadata(TestEntity::class);
        $metadata2 = $this->factory->getMetadata(TestEntity::class);

        $this->assertSame($metadata1, $metadata2);
    }

    public function testGetCollection(): void
    {
        $this->assertEquals('users', $this->factory->getCollection(TestEntity::class));
        $this->assertEquals('posts', $this->factory->getCollection(TestPost::class));
    }

    public function testNonEntityThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->factory->getMetadata(\stdClass::class);
    }

    public function testBelongsToRelationship(): void
    {
        $metadata = $this->factory->getMetadata(TestPost::class);

        $this->assertCount(1, $metadata->relationships);
        $this->assertArrayHasKey('author', $metadata->relationships);

        $rel = $metadata->relationships['author'];
        $this->assertEquals(RelationType::ManyToOne, $rel->type);
        $this->assertEquals(TestEntity::class, $rel->targetClass);
    }
}
