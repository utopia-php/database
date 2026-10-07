<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;

final class DefinitionModelCacheTest extends TestCase
{
    private const string COLLECTION = 'books';

    public function testEverySchemaChangeIsServedFromTheCachedDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new Cache(new MemoryCache()));

        $this->assertSame(['title'], $this->cachedRead($database, $adapter)['attributes']);

        $database->createAttribute(self::COLLECTION, Attribute::integer(key: 'pages'));
        $this->assertSame(['title', 'pages'], $this->cachedRead($database, $adapter)['attributes']);

        $database->updateAttribute(self::COLLECTION, 'title', new AttributeUpdate(size: 128));
        $read = $this->cachedRead($database, $adapter);
        $this->assertSame(128, $read['sizes']['title']);

        $database->renameAttribute(self::COLLECTION, 'pages', 'length');
        $this->assertSame(['title', 'length'], $this->cachedRead($database, $adapter)['attributes']);

        $database->createIndex(self::COLLECTION, Index::key(key: 'by_length', attributes: ['length']));
        $read = $this->cachedRead($database, $adapter);
        $this->assertSame(['by_length'], $read['indexes']);

        $database->deleteIndex(self::COLLECTION, 'by_length');
        $database->deleteAttribute(self::COLLECTION, 'length');
        $read = $this->cachedRead($database, $adapter);
        $this->assertSame(['title'], $read['attributes']);
        $this->assertSame([], $read['indexes']);
    }

    public function testASchemaChangeByAnotherInstanceIsServed(): void
    {
        $adapter = new CountingMemory();
        $cache = new Cache(new MemoryCache());
        $reader = $this->database($adapter, $cache);
        $writer = new Database($adapter, $cache);
        $writer->setDatabase($reader->getDatabase())->setNamespace($reader->getNamespace());

        $this->assertSame(['title'], $this->cachedRead($reader, $adapter)['attributes']);

        $writer->createAttribute(self::COLLECTION, Attribute::boolean(key: 'lent'));

        $this->assertSame(['title', 'lent'], $this->cachedRead($reader, $adapter)['attributes']);
    }

    public function testChangingAReadModelDoesNotChangeTheNextRead(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new Cache(new MemoryCache()));
        $expected = $this->cachedRead($database, $adapter);

        $collection = $database->getCollection(self::COLLECTION);
        /** @var list<Document> $attributes */
        $attributes = $collection->getAttribute('attributes');
        $attributes[0]->setAttribute('key', 'renamed');
        $attributes[0]->setAttribute('size', 1);
        $collection->setAttribute('name', 'changed');
        $collection->setAttribute('attributes', []);

        $this->assertSame($expected, $this->cachedRead($database, $adapter));
    }

    public function testTheCachedDefinitionServesItsCurrentPermissions(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter, new Cache(new MemoryCache()));
        $this->cachedRead($database, $adapter);

        $this->assertSame(['any'], $database->getCollection(self::COLLECTION)->getRead());

        $database->updateCollection(self::COLLECTION, new CollectionUpdate(permissions: [Permission::read(Role::users()), Permission::update(Role::any())], documentSecurity: false));
        $this->cachedRead($database, $adapter);
        $collection = $database->getCollection(self::COLLECTION);
        $this->assertSame(['users'], $collection->getRead());
        $this->assertSame(['any'], $collection->getUpdate());

        $collection->setAttribute('$permissions', [Permission::read(Role::guests())]);
        $this->assertSame(['guests'], $collection->getRead());
        $this->assertSame([], $collection->getUpdate());
        $this->assertSame(['users'], $database->getCollection(self::COLLECTION)->getRead());
    }

    /**
     * Reads the definition twice and checks the second read was served by the cache.
     *
     * @return array{attributes: list<string>, sizes: array<string, int|null>, indexes: list<string>, name: string}
     */
    private function cachedRead(Database $database, CountingMemory $adapter): array
    {
        $database->getCollection(self::COLLECTION);
        $adapter->reset();
        $collection = $database->getCollection(self::COLLECTION);
        $this->assertSame(0, $adapter->metadataReads, 'the definition was not served by the cache');

        $sizes = [];
        foreach ($collection->attributes() as $attribute) {
            $sizes[$attribute->key] = $attribute->size;
        }

        return [
            'attributes' => \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes()),
            'sizes' => $sizes,
            'indexes' => \array_map(static fn (Index $index): string => $index->key, $collection->indexes()),
            'name' => $collection->name(),
        ];
    }

    private function database(CountingMemory $adapter, Cache $cache): Database
    {
        $database = new Database($adapter, $cache);
        $database->setDatabase('definitions')->setNamespace('definitions_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
