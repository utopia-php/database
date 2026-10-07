<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;

final class ExtrasTest extends TestCase
{
    public function testFromArrayExtrasAreKeptInStorage(): void
    {
        $collection = Collection::fromArray([
            '$id' => 'books',
            'attributes' => [Attribute::string('title', 64)],
            'category' => 'fiction',
        ]);

        $this->assertSame('fiction', $collection->getAttribute('category'));
        $this->assertSame('fiction', $collection->toDocument()->getAttribute('category'));
    }

    public function testDocumentSecurityFalseIsStored(): void
    {
        $collection = Collection::create('books', documentSecurity: false);

        $this->assertFalse($collection->documentSecurity());
        $this->assertFalse($collection->getAttribute('documentSecurity'));
    }

    public function testDocumentSecurityFalseIsStoredWithoutAnId(): void
    {
        $created = Collection::create('', documentSecurity: false);
        $hydrated = Collection::fromArray(['documentSecurity' => false]);

        $this->assertFalse($created->documentSecurity());
        $this->assertFalse($created->getAttribute('documentSecurity'));
        $this->assertFalse($hydrated->documentSecurity());
        $this->assertFalse($hydrated->getAttribute('documentSecurity'));
    }

    public function testFromArrayExtrasSurviveCreateCollection(): void
    {
        $database = $this->database();

        $database->createCollection(Collection::fromArray([
            '$id' => 'books',
            'name' => 'Books',
            'attributes' => [Attribute::string('title', 64)],
            'category' => 'fiction',
        ]));

        $this->assertSame('fiction', $database->getCollection('books')->getAttribute('category'));
    }

    public function testCreateMetadataSurvivesCreateCollection(): void
    {
        $database = $this->database();

        $database->createCollection(Collection::create('books', documentSecurity: false, metadata: ['category' => 'fiction']));

        $stored = $database->getCollection('books');

        $this->assertSame('fiction', $stored->getAttribute('category'));
        $this->assertFalse($stored->documentSecurity());
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('extras')->setNamespace('extras_'.\uniqid());
        $database->create();

        return $database;
    }
}
