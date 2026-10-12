<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Validator\Authorization;

final class CreateReturnsStoredTest extends TestCase
{
    public function testCreateAttributeReturnsTheStoredModel(): void
    {
        $database = $this->database();

        $created = $database->createAttribute('items', Attribute::fromArray([
            'key' => 'at',
            'type' => 'datetime',
            'signed' => true,
        ]));

        $this->assertSame(['datetime'], $created->filters);
        $this->assertFalse($created->signed);
        $this->assertSame($created->toDocument()->getArrayCopy(), $this->stored($database, 'at')->toDocument()->getArrayCopy());
    }

    public function testCreateAttributesReturnsTheStoredModelsInOrder(): void
    {
        $database = $this->database();

        $created = $database->createAttributes('items', [
            Attribute::string('title', 32),
            Attribute::fromArray(['key' => 'seen', 'type' => 'datetime']),
            Attribute::integer('count'),
        ]);

        $this->assertSame(['title', 'seen', 'count'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $created));
        foreach ($created as $attribute) {
            $this->assertSame($attribute->toDocument()->getArrayCopy(), $this->stored($database, $attribute->key)->toDocument()->getArrayCopy());
        }
        $this->assertSame(['datetime'], $created[1]->filters);
    }

    public function testCreateAttributeRefusesADuplicateKey(): void
    {
        $database = $this->database();
        $database->createAttribute('items', Attribute::string('title', 32));

        $this->expectException(DuplicateException::class);

        $database->createAttribute('items', Attribute::string('title', 32));
    }

    public function testCreateAttributeOnAMissingCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->createAttribute('missing', Attribute::string('title', 32));
    }

    public function testCreateCollectionStoresTheTypeFilters(): void
    {
        $database = $this->database();

        $database->createCollection(Collection::fromArray([
            '$id' => 'events',
            'attributes' => [['key' => 'at', 'type' => 'datetime']],
        ]));

        $this->assertSame(['datetime'], $database->getCollection('events')->attributes()[0]->filters);
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('create_returns_stored')
            ->setNamespace('create_returns_stored_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create('items'));

        return $database;
    }

    private function stored(Database $database, string $key): Attribute
    {
        foreach ($database->getCollection('items')->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Attribute '.$key.' is missing from the collection metadata');
    }
}
