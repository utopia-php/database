<?php

namespace Tests\E2E\Adapter\Scopes;

use RuntimeException;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;

/**
 * Collection definitions are served from the document cache. Every schema
 * mutation must therefore make the next read observe the new schema; a
 * definition that outlives its mutation serves a schema the storage no
 * longer has.
 */
trait MetadataCacheTests
{
    /**
     * Create a collection with one string attribute, one document, and read
     * that document back so the collection definition is cached.
     */
    private function warmedCollection(Database $database, string $suffix): string
    {
        $collection = 'meta'.$suffix.\substr(\uniqid(), -8);

        $database->createCollection(new Collection(
            id: $collection,
            attributes: [Attribute::string(key: 'name', size: 128)],
            permissions: [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument($collection, new Document([
            '$id' => 'warm',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'warm',
        ]));

        $this->assertSame('warm', $database->getDocument($collection, 'warm')->getAttribute('name'));

        return $collection;
    }

    /**
     * @return array<Attribute>
     */
    private function definedAttributes(Database $database, string $collection): array
    {
        $attributes = $database->getCollection($collection)->getAttribute('attributes', []);
        $this->assertIsArray($attributes);

        $defined = [];
        foreach ($attributes as $attribute) {
            $this->assertInstanceOf(Attribute::class, $attribute);
            $defined[] = $attribute;
        }

        return $defined;
    }

    /**
     * @return array<string>
     */
    private function definedAttributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            fn (Attribute $attribute) => $attribute->key,
            $this->definedAttributes($database, $collection)
        );
    }

    /**
     * @return array<string>
     */
    private function definedIndexKeys(Database $database, string $collection): array
    {
        $indexes = $database->getCollection($collection)->getAttribute('indexes', []);
        $this->assertIsArray($indexes);

        $keys = [];
        foreach ($indexes as $index) {
            $this->assertInstanceOf(Index::class, $index);
            $keys[] = $index->key;
        }

        return $keys;
    }

    public function testCreateAttributeIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->warmedCollection($database, 'createattr');

        $database->createAttribute($collection, Attribute::string(key: 'nickname', size: 128));

        $this->assertContains('nickname', $this->definedAttributeKeys($database, $collection));

        $database->createDocument($collection, new Document([
            '$id' => 'after',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'after',
            'nickname' => 'nick',
        ]));

        $this->assertSame('nick', $database->getDocument($collection, 'after')->getAttribute('nickname'));
    }

    public function testDeleteAttributeIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->warmedCollection($database, 'delattr');
        $database->createAttribute($collection, Attribute::string(key: 'doomed', size: 128));
        $this->assertContains('doomed', $this->definedAttributeKeys($database, $collection));

        $this->assertSame('warm', $database->getDocument($collection, 'warm')->getAttribute('name'));

        $database->deleteAttribute($collection, 'doomed');

        $this->assertNotContains('doomed', $this->definedAttributeKeys($database, $collection));

        $failed = false;
        try {
            $database->createDocument($collection, new Document([
                '$id' => 'orphan',
                '$permissions' => [Permission::read(Role::any())],
                'name' => 'orphan',
                'doomed' => 'value',
            ]));
        } catch (Throwable) {
            $failed = true;
        }

        $this->assertTrue($failed, 'a write to the deleted attribute was accepted against the stale schema');
    }

    public function testUpdateAttributeIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (
            ! $database->getAdapter()->supports(Capability::DefinedAttributes)
            || ! $database->getAdapter()->supports(Capability::AttributeResizing)
        ) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->warmedCollection($database, 'updattr');

        $database->updateAttribute($collection, 'name', size: 2048);

        $sizes = [];
        foreach ($this->definedAttributes($database, $collection) as $attribute) {
            $sizes[$attribute->key] = $attribute->size;
        }

        $this->assertSame(2048, $sizes['name']);

        $long = \str_repeat('a', 1024);
        $database->createDocument($collection, new Document([
            '$id' => 'long',
            '$permissions' => [Permission::read(Role::any())],
            'name' => $long,
        ]));

        $this->assertSame($long, $database->getDocument($collection, 'long')->getAttribute('name'));
    }

    public function testRenameAttributeIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->warmedCollection($database, 'renattr');

        $database->renameAttribute($collection, 'name', 'label');

        $keys = $this->definedAttributeKeys($database, $collection);
        $this->assertContains('label', $keys);
        $this->assertNotContains('name', $keys);

        $this->assertSame('warm', $database->getDocument($collection, 'warm')->getAttribute('label'));
    }

    public function testCreateIndexIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        $collection = $this->warmedCollection($database, 'createidx');

        $database->createIndex($collection, Index::key(key: 'byName', attributes: ['name'], lengths: [128]));

        $this->assertContains('byName', $this->definedIndexKeys($database, $collection));
    }

    public function testDeleteIndexIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        $collection = $this->warmedCollection($database, 'delidx');
        $database->createIndex($collection, Index::key(key: 'byName', attributes: ['name'], lengths: [128]));
        $this->assertContains('byName', $this->definedIndexKeys($database, $collection));

        $this->assertSame('warm', $database->getDocument($collection, 'warm')->getAttribute('name'));

        $database->deleteIndex($collection, 'byName');

        $this->assertNotContains('byName', $this->definedIndexKeys($database, $collection));
    }

    public function testUpdateCollectionIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        $collection = $this->warmedCollection($database, 'updcoll');

        $this->assertFalse($database->getCollection($collection)->getAttribute('documentSecurity'));

        $database->updateCollection($collection, [Permission::read(Role::any())], true);

        $updated = $database->getCollection($collection);
        $this->assertTrue($updated->getAttribute('documentSecurity'));
        $this->assertSame(['any'], $updated->getRead());
    }

    public function testDeleteCollectionIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        $collection = $this->warmedCollection($database, 'delcoll');

        $database->deleteCollection($collection);

        $this->assertTrue($database->getCollection($collection)->isEmpty());

        $this->expectException(NotFoundException::class);
        $database->getDocument($collection, 'warm');
    }

    public function testCreateRelationshipIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parent = $this->warmedCollection($database, 'relparent');
        $child = $this->warmedCollection($database, 'relchild');

        $database->createRelationship(Relationship::oneToOne(
            collection: $parent,
            relatedCollection: $child,
            twoWay: false,
            key: 'child',
        ));

        $this->assertContains('child', $this->definedAttributeKeys($database, $parent));

        $database->createDocument($parent, new Document([
            '$id' => 'linked',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'linked',
            'child' => [
                '$id' => 'linkedchild',
                '$permissions' => [Permission::read(Role::any())],
                'name' => 'linkedchild',
            ],
        ]));

        $related = $database->getDocument($parent, 'linked')->getAttribute('child');
        $this->assertInstanceOf(Document::class, $related);
        $this->assertSame('linkedchild', $related->getAttribute('name'));
    }

    public function testDeleteRelationshipIsVisibleToTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parent = $this->warmedCollection($database, 'unrelparent');
        $child = $this->warmedCollection($database, 'unrelchild');

        $database->createRelationship(Relationship::oneToOne(
            collection: $parent,
            relatedCollection: $child,
            twoWay: true,
            key: 'child',
            twoWayKey: 'parent',
        ));

        $this->assertSame('warm', $database->getDocument($parent, 'warm')->getAttribute('name'));
        $this->assertSame('warm', $database->getDocument($child, 'warm')->getAttribute('name'));

        $database->deleteRelationship($parent, 'child');

        $this->assertNotContains('child', $this->definedAttributeKeys($database, $parent));
        $this->assertNotContains('parent', $this->definedAttributeKeys($database, $child));
    }

    public function testRolledBackCollectionUpdateNeverReachesTheNextRead(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::AtomicTransactions)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->warmedCollection($database, 'rollback');

        $rolledBack = false;
        try {
            $database->withTransaction(function () use ($database, $collection): void {
                $database->updateCollection($collection, [Permission::read(Role::any())], true);
                $database->getCollection($collection);

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException $error) {
            $rolledBack = $error->getMessage() === 'rollback';
        }

        $this->assertTrue($rolledBack, 'the transaction was expected to roll back');

        $this->assertFalse(
            $database->getCollection($collection)->getAttribute('documentSecurity'),
            'a rolled back schema change was served from the cache'
        );

        $database->updateCollection($collection, [Permission::read(Role::any())], true);

        $this->assertTrue(
            $database->getCollection($collection)->getAttribute('documentSecurity'),
            'the rollback left the cached definition unable to observe a later commit'
        );
    }
}
