<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Query;
use Utopia\Database\Relationship;

class MetadataCacheTest extends TestCase
{
    private CountingMemory $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new CountingMemory();
        $this->database = new Database($this->adapter, new Cache(new CacheMemory()));
        $this->database
            ->setDatabase('utopiaTests')
            ->setNamespace('metadata_cache_'.\uniqid());
        $this->database->addHook(new Relationships($this->database));

        $this->database->create();

        $this->database->createCollection(new Collection(id: 'authors'));
        $this->database->createAttribute('authors', Attribute::string(key: 'name'));

        $this->database->createCollection(new Collection(id: 'books'));
        $this->database->createAttribute('books', Attribute::string(key: 'title'));
        $this->database->createRelationship(Relationship::oneToOne(
            collection: 'books',
            relatedCollection: 'authors',
            twoWay: false,
            key: 'author',
        ));

        for ($i = 0; $i < 10; $i++) {
            $this->database->createDocument('books', new Document([
                '$id' => 'book'.$i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'title' => 'title'.$i,
                'author' => [
                    '$id' => 'author'.$i,
                    '$permissions' => [Permission::read(Role::any())],
                    'name' => 'author'.$i,
                ],
            ]));
        }
    }

    public function testPointReadsDoNotReplayTheMetadataRead(): void
    {
        $this->database->getDocument('books', 'book0');

        $this->adapter->reset();

        for ($i = 1; $i < 10; $i++) {
            $this->assertSame('title'.$i, $this->database->getDocument('books', 'book'.$i)->getAttribute('title'));
        }

        $this->assertSame(0, $this->adapter->metadataReads, 'getDocument re-read the collection definition from the adapter');
        $this->assertGreaterThanOrEqual(9, $this->adapter->documentReads, 'the reads under test did not reach the adapter at all');
    }

    public function testRepeatedRelationshipReadsDoNotReplayTheMetadataRead(): void
    {
        $this->database->getDocument('books', 'book0');

        $this->adapter->reset();

        for ($i = 0; $i < 10; $i++) {
            $author = $this->database->getDocument('books', 'book'.$i)->getAttribute('author');
            $this->assertInstanceOf(Document::class, $author);
            $this->assertSame('author'.$i, $author->getAttribute('name'));
        }

        $this->assertSame(0, $this->adapter->metadataReads, 'the relationship hook re-read collection definitions from the adapter');
    }

    public function testFindDoesNotReplayTheMetadataRead(): void
    {
        $this->database->find('books', [Query::limit(10)]);

        $this->adapter->reset();

        $found = $this->database->find('books', [Query::limit(10), Query::notEqual('title', 'title0')]);

        $this->assertCount(9, $found);
        $this->assertSame(0, $this->adapter->metadataReads, 'find re-read the collection definition from the adapter');
        $this->assertGreaterThanOrEqual(1, $this->adapter->finds, 'the find under test did not reach the adapter at all');
    }

    public function testRowWritesDoNotDiscardTheCachedCollectionDefinition(): void
    {
        $this->database->getDocument('books', 'book0');
        $this->database->updateDocument('books', 'book0', new Document(['title' => 'updated']));

        $this->adapter->reset();

        $document = $this->database->getDocument('books', 'book0');

        $this->assertSame('updated', $document->getAttribute('title'));
        $this->assertSame(0, $this->adapter->metadataReads, 'a row write invalidated the cached collection definition');
    }
}
