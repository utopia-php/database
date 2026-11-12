<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Structure as StructureException;

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

    public function testKnownKeysSurviveCreateCollection(): void
    {
        $database = $this->database();

        $database->createCollection(Collection::fromArray([
            '$id' => 'books',
            'name' => 'Books',
            'documentSecurity' => false,
            'attributes' => [Attribute::string('title', 64)],
        ]));

        $stored = $database->getCollection('books');

        $this->assertSame('Books', $stored->name());
        $this->assertFalse($stored->documentSecurity());
        $this->assertSame(['title'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $stored->attributes()));
    }

    public function testUnknownExtraThrowsOnCreateCollection(): void
    {
        $database = $this->database();

        try {
            $database->createCollection(Collection::create('books', documentSecurity: false, metadata: ['category' => 'fiction']));
            $this->fail('An unknown collection key must be rejected');
        } catch (StructureException $error) {
            $this->assertStringContainsString('category', $error->getMessage());
        }

        $this->assertNull($database->findCollection('books'));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('extras')->setNamespace('extras_'.\uniqid());
        $database->create();

        return $database;
    }
}
