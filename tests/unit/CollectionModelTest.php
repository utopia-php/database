<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class CollectionModelTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $collection = new Collection();

        $this->assertSame('', $collection->id);
        $this->assertSame('', $collection->name);
        $this->assertSame([], $collection->attributes);
        $this->assertSame([], $collection->indexes);
        $this->assertSame([], $collection->permissions);
        $this->assertTrue($collection->documentSecurity);
    }

    public function testConstructorWithValues(): void
    {
        $attr = new Attribute(key: 'title', type: ColumnType::String, size: 128);
        $idx = new Index(key: 'idx_title', type: IndexType::Key, attributes: ['title']);

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
        $this->assertCount(1, $collection->permissions);
        $this->assertFalse($collection->documentSecurity);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $attr = new Attribute(key: 'email', type: ColumnType::String, size: 256, required: true);
        $idx = new Index(key: 'idx_email', type: IndexType::Unique, attributes: ['email']);

        $collection = new Collection(
            id: 'accounts',
            name: 'Accounts',
            attributes: [$attr],
            indexes: [$idx],
            permissions: [Permission::read(Role::any()), Permission::create(Role::user('admin'))],
            documentSecurity: true,
        );

        $doc = $collection->toDocument();

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertSame('accounts', $doc->getId());
        $this->assertSame('Accounts', $doc->getAttribute('name'));
        $this->assertTrue($doc->getAttribute('documentSecurity'));
        $this->assertCount(1, $doc->getAttribute('attributes'));
        $this->assertCount(1, $doc->getAttribute('indexes'));
        $this->assertCount(2, $doc->getPermissions());
    }

    public function testToDocumentUsesIdWhenNameEmpty(): void
    {
        $collection = new Collection(id: 'myCol', name: '');
        $doc = $collection->toDocument();

        $this->assertSame('myCol', $doc->getAttribute('name'));
    }

    public function testToDocumentPreservesNameWhenSet(): void
    {
        $collection = new Collection(id: 'myCol', name: 'My Collection');
        $doc = $collection->toDocument();

        $this->assertSame('My Collection', $doc->getAttribute('name'));
    }

    public function testFromDocumentRoundtrip(): void
    {
        $attr = new Attribute(key: 'status', type: ColumnType::String, size: 32, required: false, default: 'active');
        $idx = new Index(key: 'idx_status', type: IndexType::Key, attributes: ['status']);

        $original = new Collection(
            id: 'projects',
            name: 'Projects',
            attributes: [$attr],
            indexes: [$idx],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );

        $doc = $original->toDocument();
        $restored = Collection::fromDocument($doc);

        $this->assertSame($original->id, $restored->id);
        $this->assertSame($original->name, $restored->name);
        $this->assertSame($original->documentSecurity, $restored->documentSecurity);
        $this->assertCount(count($original->attributes), $restored->attributes);
        $this->assertCount(count($original->indexes), $restored->indexes);
        $this->assertSame($original->attributes[0]->key, $restored->attributes[0]->key);
        $this->assertSame($original->indexes[0]->key, $restored->indexes[0]->key);
    }

    public function testFromDocumentWithEmptyDocument(): void
    {
        $doc = new Document();
        $collection = Collection::fromDocument($doc);

        $this->assertSame('', $collection->id);
        $this->assertSame('', $collection->name);
        $this->assertSame([], $collection->attributes);
        $this->assertSame([], $collection->indexes);
        $this->assertSame([], $collection->permissions);
        $this->assertTrue($collection->documentSecurity);
    }

    public function testWithMultipleAttributes(): void
    {
        $attrs = [
            new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true),
            new Attribute(key: 'email', type: ColumnType::String, size: 256, required: true),
            new Attribute(key: 'age', type: ColumnType::Integer, size: 0, required: false, default: 0),
            new Attribute(key: 'active', type: ColumnType::Boolean),
        ];

        $collection = new Collection(id: 'users', attributes: $attrs);

        $doc = $collection->toDocument();
        $restoredAttrs = $doc->getAttribute('attributes');
        $this->assertCount(4, $restoredAttrs);

        $restored = Collection::fromDocument($doc);
        $this->assertCount(4, $restored->attributes);
        $this->assertSame('name', $restored->attributes[0]->key);
        $this->assertSame('active', $restored->attributes[3]->key);
    }

    public function testWithMultipleIndexes(): void
    {
        $indexes = [
            new Index(key: 'idx_name', type: IndexType::Key, attributes: ['name']),
            new Index(key: 'idx_email', type: IndexType::Unique, attributes: ['email']),
            new Index(key: 'idx_compound', type: IndexType::Key, attributes: ['name', 'email']),
        ];

        $collection = new Collection(id: 'users', indexes: $indexes);

        $doc = $collection->toDocument();
        $this->assertCount(3, $doc->getAttribute('indexes'));

        $restored = Collection::fromDocument($doc);
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
        $doc = $collection->toDocument();

        $this->assertCount(4, $doc->getPermissions());
        $this->assertContains(Permission::read(Role::any()), $doc->getPermissions());
    }

    public function testDocumentSecurityTrue(): void
    {
        $collection = new Collection(id: 'secure', documentSecurity: true);
        $doc = $collection->toDocument();

        $this->assertTrue($doc->getAttribute('documentSecurity'));
    }

    public function testDocumentSecurityFalse(): void
    {
        $collection = new Collection(id: 'insecure', documentSecurity: false);
        $doc = $collection->toDocument();

        $this->assertFalse($doc->getAttribute('documentSecurity'));
    }

    public function testFromDocumentPreservesPermissions(): void
    {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ];

        $doc = new Document([
            '$id' => 'test',
            '$permissions' => $permissions,
            'name' => 'test',
            'attributes' => [],
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $collection = Collection::fromDocument($doc);
        $this->assertCount(2, $collection->permissions);
    }

    public function testAttributeDocumentsAreProperDocuments(): void
    {
        $attr = new Attribute(key: 'title', type: ColumnType::String, size: 64);
        $collection = new Collection(id: 'articles', attributes: [$attr]);

        $doc = $collection->toDocument();
        $attrDocs = $doc->getAttribute('attributes');

        $this->assertInstanceOf(Document::class, $attrDocs[0]);
        $this->assertSame('title', $attrDocs[0]->getAttribute('key'));
        $this->assertSame('string', $attrDocs[0]->getAttribute('type'));
    }

    public function testIndexDocumentsAreProperDocuments(): void
    {
        $idx = new Index(key: 'idx_test', type: IndexType::Fulltext, attributes: ['body']);
        $collection = new Collection(id: 'articles', indexes: [$idx]);

        $doc = $collection->toDocument();
        $idxDocs = $doc->getAttribute('indexes');

        $this->assertInstanceOf(Document::class, $idxDocs[0]);
        $this->assertSame('idx_test', $idxDocs[0]->getAttribute('key'));
        $this->assertSame('fulltext', $idxDocs[0]->getAttribute('type'));
    }
}
