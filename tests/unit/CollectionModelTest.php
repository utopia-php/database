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
                Index::key(key: 'idx_name', attributes: ['name'], lengths: [64], orders: ['ASC']),
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

        $doc = $original->toDocument();
        $restored = Collection::fromDocument($doc);

        $this->assertSame($original->id, $restored->id);
        $this->assertSame($original->name, $restored->name);
        $this->assertSame($original->documentSecurity, $restored->documentSecurity);
        $this->assertCount(count($original->attributes), $restored->attributes);
        $this->assertCount(count($original->indexes), $restored->indexes);
        $this->assertSame($original->attributes[0]->key, $restored->attributes[0]->key);
        $this->assertSame($original->indexes[0]->key, $restored->indexes[0]->key);
        $this->assertInstanceOf(StringType::class, $restored->attributes[0]);
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
            Attribute::string(key: 'name', size: 128, required: true),
            Attribute::string(key: 'email', size: 256, required: true),
            new Integer(key: 'age', required: false, default: 0),
            new Boolean(key: 'active'),
        ];

        $collection = new Collection(id: 'users', attributes: $attrs);

        $doc = $collection->toDocument();
        $restoredAttrs = $doc->getAttribute('attributes');
        $this->assertCount(4, $restoredAttrs);

        $restored = Collection::fromDocument($doc);
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
        $this->assertNotNull($collection->permissions);
        $this->assertCount(2, $collection->permissions);
    }

    public function testAttributeDocumentsAreProperDocuments(): void
    {
        $attr = Attribute::string(key: 'title', size: 64);
        $collection = new Collection(id: 'articles', attributes: [$attr]);

        $doc = $collection->toDocument();
        $attrDocs = $doc->getAttribute('attributes');

        $this->assertInstanceOf(Document::class, $attrDocs[0]);
        $this->assertSame('title', $attrDocs[0]->getAttribute('key'));
        $this->assertSame('string', $attrDocs[0]->getAttribute('type'));
    }

    public function testIndexDocumentsAreProperDocuments(): void
    {
        $idx = Index::fullText(key: 'idx_test', attributes: ['body']);
        $collection = new Collection(id: 'articles', indexes: [$idx]);

        $doc = $collection->toDocument();
        $idxDocs = $doc->getAttribute('indexes');

        $this->assertInstanceOf(Document::class, $idxDocs[0]);
        $this->assertSame('idx_test', $idxDocs[0]->getAttribute('key'));
        $this->assertSame('fulltext', $idxDocs[0]->getAttribute('type'));
    }
}
