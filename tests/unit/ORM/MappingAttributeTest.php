<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\Mapping\BelongsTo;
use Utopia\Database\ORM\Mapping\BelongsToMany;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\CreatedAt;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\HasMany;
use Utopia\Database\ORM\Mapping\HasOne;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\Permissions;
use Utopia\Database\ORM\Mapping\TableIndex;
use Utopia\Database\ORM\Mapping\Tenant;
use Utopia\Database\ORM\Mapping\UpdatedAt;
use Utopia\Database\ORM\Mapping\Version;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;

class MappingAttributeTest extends TestCase
{
    private MetadataFactory $factory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->factory = new MetadataFactory();
    }

    public function testEntityAttributeWithAllParameters(): void
    {
        $entity = new Entity(
            collection: 'custom_collection',
            documentSecurity: false,
            permissions: ['read("any")', 'write("users")'],
        );

        $this->assertEquals('custom_collection', $entity->collection);
        $this->assertFalse($entity->documentSecurity);
        $this->assertEquals(['read("any")', 'write("users")'], $entity->permissions);
    }

    public function testEntityAttributeWithDefaults(): void
    {
        $entity = new Entity(collection: 'test');

        $this->assertEquals('test', $entity->collection);
        $this->assertTrue($entity->documentSecurity);
        $this->assertEquals([], $entity->permissions);
    }

    public function testColumnAttributeWithAllParameters(): void
    {
        $column = new Column(
            type: ColumnType::String,
            size: 500,
            required: true,
            default: 'hello',
            signed: false,
            array: true,
            format: 'email',
            formatOptions: ['domain' => 'example.com'],
            filters: ['trim', 'lowercase'],
            key: 'custom_key',
        );

        $this->assertEquals(ColumnType::String, $column->type);
        $this->assertEquals(500, $column->size);
        $this->assertTrue($column->required);
        $this->assertEquals('hello', $column->default);
        $this->assertFalse($column->signed);
        $this->assertTrue($column->array);
        $this->assertEquals('email', $column->format);
        $this->assertEquals(['domain' => 'example.com'], $column->formatOptions);
        $this->assertEquals(['trim', 'lowercase'], $column->filters);
        $this->assertEquals('custom_key', $column->key);
    }

    public function testColumnAttributeWithDefaults(): void
    {
        $column = new Column();

        $this->assertEquals(ColumnType::String, $column->type);
        $this->assertEquals(0, $column->size);
        $this->assertFalse($column->required);
        $this->assertNull($column->default);
        $this->assertTrue($column->signed);
        $this->assertFalse($column->array);
        $this->assertNull($column->format);
        $this->assertEquals([], $column->formatOptions);
        $this->assertEquals([], $column->filters);
        $this->assertNull($column->key);
    }

    public function testColumnWithCustomKeyOverride(): void
    {
        $column = new Column(type: ColumnType::Integer, key: 'db_age');

        $this->assertEquals('db_age', $column->key);
        $this->assertEquals(ColumnType::Integer, $column->type);
    }

    public function testIdAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(Id::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
        $attr = $attrs[0]->newInstance();
        $this->assertEquals(\Attribute::TARGET_PROPERTY, $attr->flags);
    }

    public function testVersionAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(Version::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testCreatedAtAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(CreatedAt::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testUpdatedAtAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(UpdatedAt::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testTenantAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(Tenant::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testPermissionsAttributeIsMarker(): void
    {
        $ref = new \ReflectionClass(Permissions::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testHasOneWithAllParameters(): void
    {
        $hasOne = new HasOne(
            target: TestEntity::class,
            key: 'profile',
            twoWayKey: 'user',
            twoWay: false,
            onDelete: ForeignKeyAction::Cascade,
        );

        $this->assertEquals(TestEntity::class, $hasOne->target);
        $this->assertEquals('profile', $hasOne->key);
        $this->assertEquals('user', $hasOne->twoWayKey);
        $this->assertFalse($hasOne->twoWay);
        $this->assertEquals(ForeignKeyAction::Cascade, $hasOne->onDelete);
    }

    public function testHasOneWithDefaults(): void
    {
        $hasOne = new HasOne(target: TestEntity::class);

        $this->assertEquals(TestEntity::class, $hasOne->target);
        $this->assertEquals('', $hasOne->key);
        $this->assertEquals('', $hasOne->twoWayKey);
        $this->assertTrue($hasOne->twoWay);
        $this->assertEquals(ForeignKeyAction::Restrict, $hasOne->onDelete);
    }

    public function testBelongsToWithAllParameters(): void
    {
        $belongsTo = new BelongsTo(
            target: TestEntity::class,
            key: 'author',
            twoWayKey: 'posts',
            twoWay: false,
            onDelete: ForeignKeyAction::Cascade,
        );

        $this->assertEquals(TestEntity::class, $belongsTo->target);
        $this->assertEquals('author', $belongsTo->key);
        $this->assertEquals('posts', $belongsTo->twoWayKey);
        $this->assertFalse($belongsTo->twoWay);
        $this->assertEquals(ForeignKeyAction::Cascade, $belongsTo->onDelete);
    }

    public function testBelongsToWithDefaults(): void
    {
        $belongsTo = new BelongsTo(target: TestEntity::class);

        $this->assertEquals(ForeignKeyAction::Restrict, $belongsTo->onDelete);
        $this->assertTrue($belongsTo->twoWay);
    }

    public function testHasManyDefaultOnDeleteIsSetNull(): void
    {
        $hasMany = new HasMany(target: TestPost::class);

        $this->assertEquals(ForeignKeyAction::SetNull, $hasMany->onDelete);
    }

    public function testHasManyWithAllParameters(): void
    {
        $hasMany = new HasMany(
            target: TestPost::class,
            key: 'posts',
            twoWayKey: 'author',
            twoWay: false,
            onDelete: ForeignKeyAction::Cascade,
        );

        $this->assertEquals(TestPost::class, $hasMany->target);
        $this->assertEquals('posts', $hasMany->key);
        $this->assertEquals('author', $hasMany->twoWayKey);
        $this->assertFalse($hasMany->twoWay);
        $this->assertEquals(ForeignKeyAction::Cascade, $hasMany->onDelete);
    }

    public function testBelongsToManyDefaultOnDeleteIsCascade(): void
    {
        $belongsToMany = new BelongsToMany(target: TestEntity::class);

        $this->assertEquals(ForeignKeyAction::Cascade, $belongsToMany->onDelete);
    }

    public function testBelongsToManyWithAllParameters(): void
    {
        $belongsToMany = new BelongsToMany(
            target: TestEntity::class,
            key: 'tags',
            twoWayKey: 'posts',
            twoWay: false,
            onDelete: ForeignKeyAction::SetNull,
        );

        $this->assertEquals(TestEntity::class, $belongsToMany->target);
        $this->assertEquals('tags', $belongsToMany->key);
        $this->assertEquals('posts', $belongsToMany->twoWayKey);
        $this->assertFalse($belongsToMany->twoWay);
        $this->assertEquals(ForeignKeyAction::SetNull, $belongsToMany->onDelete);
    }

    public function testTableIndexWithAllParameters(): void
    {
        $index = new TableIndex(
            key: 'idx_test',
            type: IndexType::Fulltext,
            attributes: ['title', 'content'],
            lengths: [100, 200],
            orders: ['asc', 'desc'],
        );

        $this->assertEquals('idx_test', $index->key);
        $this->assertEquals(IndexType::Fulltext, $index->type);
        $this->assertEquals(['title', 'content'], $index->attributes);
        $this->assertEquals([100, 200], $index->lengths);
        $this->assertEquals(['asc', 'desc'], $index->orders);
    }

    public function testTableIndexWithDefaults(): void
    {
        $index = new TableIndex(key: 'idx_basic');

        $this->assertEquals('idx_basic', $index->key);
        $this->assertEquals(IndexType::Index, $index->type);
        $this->assertEquals([], $index->attributes);
        $this->assertEquals([], $index->lengths);
        $this->assertEquals([], $index->orders);
    }

    public function testTableIndexIsRepeatable(): void
    {
        $ref = new \ReflectionClass(TableIndex::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
        $attr = $attrs[0]->newInstance();
        $this->assertTrue(($attr->flags & \Attribute::IS_REPEATABLE) !== 0);
    }

    public function testTestEntityHasTwoIndexes(): void
    {
        $metadata = $this->factory->getMetadata(TestEntity::class);

        $this->assertCount(2, $metadata->indexes);
    }

    public function testEntityWithNoRelationships(): void
    {
        $metadata = $this->factory->getMetadata(TestNoRelationsEntity::class);

        $this->assertEmpty($metadata->relationships);
        $this->assertEquals('no_relations', $metadata->collection);
    }

    public function testEntityWithCustomKeyOnColumn(): void
    {
        $metadata = $this->factory->getMetadata(TestCustomKeyEntity::class);

        $this->assertArrayHasKey('displayName', $metadata->columns);
        $this->assertEquals('display_name', $metadata->columns['displayName']->documentKey);
        $this->assertEquals('displayName', $metadata->columns['displayName']->propertyName);
    }

    public function testEntityWithTenantAttribute(): void
    {
        $metadata = $this->factory->getMetadata(TestTenantEntity::class);

        $this->assertEquals('tenantId', $metadata->tenantProperty);
        $this->assertEquals('tenant_items', $metadata->collection);
    }

    public function testEntityWithAllRelationshipTypes(): void
    {
        $metadata = $this->factory->getMetadata(TestAllRelationsEntity::class);

        $this->assertCount(4, $metadata->relationships);
        $this->assertArrayHasKey('profile', $metadata->relationships);
        $this->assertArrayHasKey('team', $metadata->relationships);
        $this->assertArrayHasKey('posts', $metadata->relationships);
        $this->assertArrayHasKey('tags', $metadata->relationships);

        $this->assertEquals(RelationType::OneToOne, $metadata->relationships['profile']->type);
        $this->assertEquals(RelationType::ManyToOne, $metadata->relationships['team']->type);
        $this->assertEquals(RelationType::OneToMany, $metadata->relationships['posts']->type);
        $this->assertEquals(RelationType::ManyToMany, $metadata->relationships['tags']->type);
    }

    public function testEntityWithNoIndexes(): void
    {
        $metadata = $this->factory->getMetadata(TestNoRelationsEntity::class);

        $this->assertEmpty($metadata->indexes);
    }

    public function testEntityAttributeTargetsClass(): void
    {
        $ref = new \ReflectionClass(Entity::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
        $attr = $attrs[0]->newInstance();
        $this->assertEquals(\Attribute::TARGET_CLASS, $attr->flags);
    }

    public function testColumnAttributeTargetsProperty(): void
    {
        $ref = new \ReflectionClass(Column::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
        $attr = $attrs[0]->newInstance();
        $this->assertEquals(\Attribute::TARGET_PROPERTY, $attr->flags);
    }

    public function testTableIndexTargetsClassAndIsRepeatable(): void
    {
        $ref = new \ReflectionClass(TableIndex::class);
        $attrs = $ref->getAttributes(\Attribute::class);
        $attr = $attrs[0]->newInstance();

        $this->assertTrue(($attr->flags & \Attribute::TARGET_CLASS) !== 0);
        $this->assertTrue(($attr->flags & \Attribute::IS_REPEATABLE) !== 0);
    }

    public function testColumnWithEveryColumnType(): void
    {
        $types = [
            ColumnType::String,
            ColumnType::Integer,
            ColumnType::Boolean,
            ColumnType::Float,
            ColumnType::Datetime,
            ColumnType::Json,
        ];

        foreach ($types as $type) {
            $column = new Column(type: $type);
            $this->assertEquals($type, $column->type);
        }
    }

    public function testHasOneAttributeTargetsProperty(): void
    {
        $ref = new \ReflectionClass(HasOne::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
        $attr = $attrs[0]->newInstance();
        $this->assertEquals(\Attribute::TARGET_PROPERTY, $attr->flags);
    }

    public function testHasManyAttributeTargetsProperty(): void
    {
        $ref = new \ReflectionClass(HasMany::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testBelongsToAttributeTargetsProperty(): void
    {
        $ref = new \ReflectionClass(BelongsTo::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testBelongsToManyAttributeTargetsProperty(): void
    {
        $ref = new \ReflectionClass(BelongsToMany::class);
        $attrs = $ref->getAttributes(\Attribute::class);

        $this->assertNotEmpty($attrs);
    }

    public function testEntityWithPermissionsInAttribute(): void
    {
        $metadata = $this->factory->getMetadata(TestPermissionEntity::class);

        $this->assertEquals(['read("any")', 'write("users")'], $metadata->permissions);
    }

    public function testEntityWithDocumentSecurityFalse(): void
    {
        $metadata = $this->factory->getMetadata(TestPermissionEntity::class);

        $this->assertFalse($metadata->documentSecurity);
    }
}
