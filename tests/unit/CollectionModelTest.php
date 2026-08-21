<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Attribute\Boolean;
use Utopia\Database\Attribute\Integer;
use Utopia\Database\Attribute\StringType;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\Order;

class CollectionModelTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $collection = new Collection();

        $this->assertSame('', $collection->id);
        $this->assertSame('', $collection->name);
        $this->assertSame([], $collection->attributes);
        $this->assertSame([], $collection->indexes);
        $this->assertNull($collection->permissions);
        $this->assertTrue($collection->documentSecurity);
        $this->assertSame([], $collection->metadata);
    }

    public function testConstructorRequiresAttributeAndIndexModels(): void
    {
        $collection = new Collection(
            id: 'nested',
            attributes: [
                Attribute::string(key: 'name', size: 64, required: true),
            ],
            indexes: [
                Index::key(key: 'idx_name', attributes: ['name'], lengths: [64], orders: [Order::Asc]),
            ],
        );

        $this->assertInstanceOf(Attribute::class, $collection->attributes[0]);
        $this->assertSame('name', $collection->attributes[0]->key);
        $this->assertInstanceOf(Index::class, $collection->indexes[0]);
        $this->assertSame('idx_name', $collection->indexes[0]->key);
    }

    public function testConstructorWithValues(): void
    {
        $attr = Attribute::string(key: 'title', size: 128);
        $idx = Index::key(key: 'idx_title', attributes: ['title']);

        $collection = new Collection(
            id: 'users',
            name: 'Users',
            attributes: [$attr],
            indexes: [$idx],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );

        $this->assertSame('users', $collection->id);
        $this->assertSame('Users', $collection->name);
        $this->assertCount(1, $collection->attributes);
        $this->assertCount(1, $collection->indexes);
        $this->assertNotNull($collection->permissions);
        $this->assertCount(1, $collection->permissions);
        $this->assertFalse($collection->documentSecurity);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $attr = Attribute::string(key: 'email', size: 256, required: true);
        $idx = Index::unique(key: 'idx_email', attributes: ['email']);

        $collection = new Collection(
            id: 'accounts',
            name: 'Accounts',
            attributes: [$attr],
            indexes: [$idx],
            permissions: [Permission::read(Role::any()), Permission::create(Role::user('admin'))],
            documentSecurity: true,
        );

        $doc = $collection;

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertSame('accounts', $doc->getId());
        $this->assertSame('Accounts', $doc->getAttribute('name'));
        $this->assertTrue($doc->getAttribute('documentSecurity'));
        $this->assertCount(1, $doc->attributes);
        $this->assertInstanceOf(Attribute::class, $doc->attributes[0]);
        $this->assertCount(1, $doc->indexes);
        $this->assertInstanceOf(Index::class, $doc->indexes[0]);
        $this->assertCount(2, $doc->getPermissions());
    }

    public function testToDocumentUsesIdWhenNameEmpty(): void
    {
        $collection = new Collection(id: 'myCol', name: '');

        $this->assertSame('myCol', $collection->getAttribute('name'));
    }

    public function testToDocumentPreservesNameWhenSet(): void
    {
        $collection = new Collection(id: 'myCol', name: 'My Collection');

        $this->assertSame('My Collection', $collection->getAttribute('name'));
    }

    public function testFromDocumentRoundtrip(): void
    {
        $attr = Attribute::string(key: 'status', size: 32, required: false, default: 'active');
        $idx = Index::key(key: 'idx_status', attributes: ['status']);

        $original = new Collection(
            id: 'projects',
            name: 'Projects',
            attributes: [$attr],
            indexes: [$idx],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );

        $restored = Collection::fromArray($original->getArrayCopy());

        $this->assertSame($original->id, $restored->id);
        $this->assertSame($original->name, $restored->name);
        $this->assertSame($original->documentSecurity, $restored->documentSecurity);
        $this->assertCount(count($original->attributes), $restored->attributes);
        $this->assertCount(count($original->indexes), $restored->indexes);
        $this->assertSame($original->attributes[0]->key, $restored->attributes[0]->key);
        $this->assertSame($original->indexes[0]->key, $restored->indexes[0]->key);
        $this->assertInstanceOf(StringType::class, $restored->attributes[0]);
        $this->assertInstanceOf(Attribute::class, $original->attributes[0]);
        $this->assertInstanceOf(Index::class, $original->indexes[0]);
    }

    public function testFromArrayWithEmptyPayload(): void
    {
        $collection = Collection::fromArray([]);

        $this->assertSame('', $collection->id);
        $this->assertSame('', $collection->name);
        $this->assertSame([], $collection->attributes);
        $this->assertSame([], $collection->indexes);
        $this->assertNull($collection->permissions);
        $this->assertTrue($collection->documentSecurity);
    }

    public function testWithMultipleAttributes(): void
    {
        $attrs = [
            Attribute::string(key: 'name', size: 128, required: true),
            Attribute::string(key: 'email', size: 256, required: true),
            new Integer(key: 'age', required: false, default: 0),
            new Boolean(key: 'active'),
        ];

        $collection = new Collection(id: 'users', attributes: $attrs);

        $this->assertCount(4, $collection->attributes);

        $restored = Collection::fromArray($collection->getArrayCopy());
        $this->assertCount(4, $restored->attributes);
        $this->assertSame('name', $restored->attributes[0]->key);
        $this->assertSame('active', $restored->attributes[3]->key);
        $this->assertInstanceOf(StringType::class, $restored->attributes[0]);
        $this->assertInstanceOf(StringType::class, $restored->attributes[1]);
        $this->assertInstanceOf(Integer::class, $restored->attributes[2]);
        $this->assertInstanceOf(Boolean::class, $restored->attributes[3]);
    }

    public function testWithMultipleIndexes(): void
    {
        $indexes = [
            Index::key(key: 'idx_name', attributes: ['name']),
            Index::unique(key: 'idx_email', attributes: ['email']),
            Index::key(key: 'idx_compound', attributes: ['name', 'email']),
        ];

        $collection = new Collection(id: 'users', indexes: $indexes);

        $this->assertCount(3, $collection->indexes);

        $restored = Collection::fromArray($collection->getArrayCopy());
        $this->assertCount(3, $restored->indexes);
        $this->assertSame('idx_compound', $restored->indexes[2]->key);
    }

    public function testWithPermissions(): void
    {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::user('admin')),
            Permission::update(Role::team('editors')),
            Permission::delete(Role::user('owner')),
        ];

        $collection = new Collection(id: 'posts', permissions: $permissions);

        $this->assertCount(4, $collection->getPermissions());
        $this->assertContains(Permission::read(Role::any()), $collection->getPermissions());
    }

    public function testDocumentSecurityTrue(): void
    {
        $collection = new Collection(id: 'secure', documentSecurity: true);

        $this->assertTrue($collection->getAttribute('documentSecurity'));
    }

    public function testDocumentSecurityFalse(): void
    {
        $collection = new Collection(id: 'insecure', documentSecurity: false);

        $this->assertFalse($collection->getAttribute('documentSecurity'));
    }

    public function testFromDocumentPreservesPermissions(): void
    {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ];

        $collection = Collection::fromArray([
            '$id' => 'test',
            '$permissions' => $permissions,
            'name' => 'test',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
        $this->assertNotNull($collection->permissions);
        $this->assertCount(2, $collection->permissions);
    }

    public function testAttributeModelsStayModels(): void
    {
        $attr = Attribute::string(key: 'title', size: 64);
        $collection = new Collection(id: 'articles', attributes: [$attr]);

        $attributes = $collection->attributes;

        $this->assertInstanceOf(Attribute::class, $attributes[0]);
        $this->assertSame('title', $attributes[0]->key);
        $this->assertSame('string', $attributes[0]->getAttribute('type'));
    }

    public function testIndexModelsStayModels(): void
    {
        $idx = Index::fullText(key: 'idx_test', attributes: ['body']);
        $collection = new Collection(id: 'articles', indexes: [$idx]);

        $indexes = $collection->indexes;

        $this->assertInstanceOf(Index::class, $indexes[0]);
        $this->assertSame('idx_test', $indexes[0]->key);
        $this->assertSame('fulltext', $indexes[0]->getAttribute('type'));
    }
}
