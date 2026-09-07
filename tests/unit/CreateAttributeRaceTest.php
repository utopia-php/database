<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Adapter\None as NoneCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

/**
 * A schema change is recorded by appending to the collection's metadata row. The
 * copy of that row the caller appends to is read before the change and can be
 * served from cache, so writing it back whole drops whatever another writer
 * added in between: the column exists, its attribute row says available, and the
 * metadata list no longer mentions it.
 *
 * Each test here stands a peer process up on the same database with its own
 * cache, so the peer's purge cannot reach the copy the first process holds — the
 * same staleness a lost purge or an in-request writer produces in a fleet.
 */
class CreateAttributeRaceTest extends TestCase
{
    private DatabaseMemory $adapter;

    private Database $database;

    private Database $peer;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->database = new Database($this->adapter, new Cache(new CacheMemory()));
        $this->database
            ->setDatabase('utopiaTests')
            ->setNamespace('attribute_race_' . uniqid());
        $this->database->getAuthorization()->addRole(Role::any()->toString());
        $this->database->create();

        // Same database, its own cache: its writes do not purge the copy the
        // first process is about to read.
        $this->peer = (new Database($this->adapter, new Cache(new NoneCache())))
            ->setAuthorization($this->database->getAuthorization());
    }

    public function testCreateAttributeKeepsPeerAttribute(): void
    {
        $collection = $this->seed('concurrentAttribute');

        $before = $this->attributeKeys($this->database, $collection);
        $this->assertContains('first', $before);

        $this->assertTrue($this->peer->createAttribute($collection, 'peer', Database::VAR_STRING, 32, false));
        $this->assertTrue($this->database->createAttribute($collection, 'mine', Database::VAR_STRING, 32, false));

        $this->assertEqualsCanonicalizing(
            [...$before, 'peer', 'mine'],
            $this->attributeKeys($this->peer, $collection),
            'Peer attribute is missing from the collection metadata'
        );

        // The list has to match the schema: an attribute the metadata does not
        // mention is dropped from a write that names it.
        $document = $this->database->createDocument($collection, new Document([
            '$id' => ID::custom('row'),
            '$permissions' => [Permission::read(Role::any())],
            'first' => 'a',
            'peer' => 'b',
            'mine' => 'c',
        ]));

        $this->assertSame('b', $document->getAttribute('peer'));
        $this->assertSame('c', $document->getAttribute('mine'));
    }

    public function testCreateAttributesKeepsPeerAttribute(): void
    {
        $collection = $this->seed('concurrentAttributes');

        $before = $this->attributeKeys($this->database, $collection);

        $this->assertTrue($this->peer->createAttribute($collection, 'peer', Database::VAR_STRING, 32, false));

        $this->assertTrue($this->database->createAttributes($collection, [
            ['$id' => ID::custom('batchA'), 'type' => Database::VAR_STRING, 'size' => 32, 'required' => false],
            ['$id' => ID::custom('batchB'), 'type' => Database::VAR_STRING, 'size' => 32, 'required' => false],
        ]));

        $this->assertEqualsCanonicalizing(
            [...$before, 'peer', 'batchA', 'batchB'],
            $this->attributeKeys($this->peer, $collection),
            'Peer attribute is missing from the collection metadata'
        );
    }

    public function testCreateIndexKeepsPeerIndex(): void
    {
        $collection = $this->seed('concurrentIndex', ['first', 'second']);

        $before = \array_map(
            fn (Document $index) => $index->getId(),
            $this->database->getCollection($collection)->getAttribute('indexes', [])
        );

        $this->assertTrue($this->peer->createIndex($collection, 'peerIdx', Database::INDEX_KEY, ['first']));
        $this->assertTrue($this->database->createIndex($collection, 'mineIdx', Database::INDEX_KEY, ['second']));

        $ids = \array_map(
            fn (Document $index) => $index->getId(),
            $this->peer->getCollection($collection)->getAttribute('indexes', [])
        );

        $this->assertEqualsCanonicalizing(
            [...$before, 'peerIdx', 'mineIdx'],
            $ids,
            'Peer index is missing from the collection metadata'
        );
    }

    /**
     * Both processes create the same key. The append converges on the entry
     * describing the column that exists, rather than listing the key twice or
     * replacing the winner's spec with the loser's.
     */
    public function testCreateAttributeConvergesOnAKeyThePeerAlreadyAdded(): void
    {
        $collection = $this->seed('convergingAttribute');

        $this->database->getCollection($collection);

        $this->assertTrue($this->peer->createAttribute($collection, 'shared', Database::VAR_STRING, 32, false));
        $this->assertTrue($this->database->createAttribute($collection, 'shared', Database::VAR_STRING, 64, false));

        $attributes = \array_values(\array_filter(
            $this->peer->getCollection($collection)->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('key', $attribute->getId()) === 'shared'
        ));

        $this->assertCount(1, $attributes, 'Attribute key was listed twice in the collection metadata');
        $this->assertSame(32, $attributes[0]->getAttribute('size'), 'Metadata describes a column width the schema does not have');
    }

    /**
     * @param array<string> $attributes
     */
    private function seed(string $collection, array $attributes = ['first']): string
    {
        $this->database->createCollection($collection, \array_map(fn (string $key) => new Document([
            '$id' => ID::custom($key),
            'type' => Database::VAR_STRING,
            'size' => 32,
            'required' => false,
        ]), $attributes), permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
        ]);

        return $collection;
    }

    /**
     * @return array<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            fn (Document $attribute) => $attribute->getAttribute('key', $attribute->getId()),
            $database->getCollection($collection)->getAttribute('attributes', [])
        );
    }
}
