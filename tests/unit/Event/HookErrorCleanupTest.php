<?php

namespace Tests\Unit\Event;

use PDO;
use PHPUnit\Framework\TestCase;
use Throwable;
use TypeError;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;

final class HookErrorCleanupTest extends TestCase
{
    private const string COLLECTION = 'notes';

    public function testCollectionDeleteClearsTheCachedDocumentsWhenAHookFails(): void
    {
        $database = $this->database();
        $this->createNotes($database);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'one', 'name' => 'old']));
        $database->getDocument(self::COLLECTION, 'one');
        $database->addHook(new FailingLifecycle(Event::CollectionDelete, new TypeError('broken hook')));

        $this->assertInstanceOf(TypeError::class, $this->failureOf(fn () => $database->deleteCollection(self::COLLECTION)));

        $this->createNotes($database);

        $this->assertTrue(
            $database->getDocument(self::COLLECTION, 'one')->isEmpty(),
            'The re-created collection served a document cached before the delete',
        );
    }

    public function testDatabaseDeleteFlushesTheCacheWhenAHookFails(): void
    {
        $database = $this->database(new Memory());
        $this->createNotes($database);
        $database->getCollection(self::COLLECTION);
        $database->addHook(new FailingLifecycle(Event::DatabaseDelete, new TypeError('broken hook')));

        $this->assertInstanceOf(TypeError::class, $this->failureOf(fn () => $database->delete()));

        $database->create();

        $this->assertTrue(
            $database->getCollection(self::COLLECTION)->isEmpty(),
            'The re-created database served collection metadata cached before the delete',
        );
    }

    public function testRelationshipRenameKeepsMetadataAndColumnsTogetherWhenAHookFails(): void
    {
        $database = $this->database();
        foreach (['parent', 'child'] as $id) {
            $database->createCollection(new Collection(id: $id, permissions: $this->permissions(), documentSecurity: false));
        }
        $database->createRelationship(Relationship::oneToMany(
            collection: 'parent',
            relatedCollection: 'child',
            twoWay: true,
            key: 'children',
            twoWayKey: 'parent',
            onDelete: ForeignKeyAction::SetNull,
        ));
        $database->createDocument('parent', new Document([Document::ID => 'owner']));
        $database->createDocument('child', new Document([Document::ID => 'member', 'parent' => 'owner']));
        $database->addHook(new FailingLifecycle(Event::AttributeUpdate, new TypeError('broken hook')));

        $this->assertInstanceOf(
            TypeError::class,
            $this->failureOf(fn () => $database->updateRelationship('parent', 'children', newTwoWayKey: 'owner')),
        );

        $owner = $database->getDocument('child', 'member')->getAttribute('owner');

        $this->assertSame(
            'owner',
            $owner instanceof Document ? $owner->getId() : $owner,
            'The metadata names the renamed key while the column kept its old name',
        );
    }

    private function database(?Adapter $adapter = null): Database
    {
        $database = new Database($adapter ?? new SQLite(new PDO('sqlite::memory:')), new Cache(new MemoryCache()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('hooks')
            ->setNamespace('hooks_'.\uniqid());
        $database->create();

        return $database;
    }

    private function createNotes(Database $database): void
    {
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: $this->permissions(),
            documentSecurity: false,
        ));
    }

    /**
     * @return array<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function failureOf(callable $operation): ?Throwable
    {
        try {
            $operation();
        } catch (Throwable $failure) {
            return $failure;
        }

        return null;
    }
}
