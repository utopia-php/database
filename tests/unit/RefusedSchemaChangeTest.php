<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use Tests\Unit\Support\VerdictMemory;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Refused as RefusedException;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Covers schema calls whose adapter returns false: the call throws Exception\Refused with one message of its own,
 * while an error the adapter raises never becomes a refusal.
 */
final class RefusedSchemaChangeTest extends TestCase
{
    /**
     * @return array<string, array{string, Closure(Database): mixed, string}>
     */
    public static function operations(): array
    {
        return [
            'create' => [
                'create',
                static fn (Database $database): bool => $database->create('archive'),
                'Failed to create database',
            ],
            'update' => [
                'update',
                static fn (Database $database): bool => $database->update('refused', 'renamed'),
                "Failed to rename database 'refused' to 'renamed'",
            ],
            'delete' => [
                'delete',
                static fn (Database $database): bool => $database->delete('refused'),
                'Failed to delete database',
            ],
            'createCollection' => [
                'createCollection',
                static fn (Database $database): Collection => $database->createCollection(Collection::create(id: 'drafts')),
                'Failed to create collection',
            ],
            'deleteCollection' => [
                'deleteCollection',
                static fn (Database $database) => $database->deleteCollection('notes'),
                'Failed to delete collection',
            ],
            'createAttribute' => [
                'createAttribute',
                static fn (Database $database): Attribute => $database->createAttribute('books', Attribute::string(key: 'summary', size: 64)),
                'Failed to create attribute',
            ],
            'createAttributes' => [
                'createAttributes',
                static fn (Database $database): array => $database->createAttributes('books', [Attribute::string(key: 'summary', size: 64), Attribute::integer(key: 'pages')]),
                'Failed to create attributes',
            ],
            'updateAttribute' => [
                'updateAttribute',
                static fn (Database $database): Attribute => $database->updateAttribute('books', 'label', new AttributeUpdate(size: 128)),
                'Failed to update attribute',
            ],
            'updateAttribute relaxing required' => [
                'relaxAttributeRequired',
                static fn (Database $database): Attribute => $database->updateAttribute('books', 'title', new AttributeUpdate(required: false)),
                'Failed to update attribute',
            ],
            'deleteAttribute' => [
                'deleteAttribute',
                static fn (Database $database) => $database->deleteAttribute('books', 'label'),
                'Failed to delete attribute',
            ],
            'renameAttribute' => [
                'renameAttribute',
                static fn (Database $database) => $database->renameAttribute('books', 'label', 'caption'),
                "Failed to rename attribute 'label' to 'caption'",
            ],
            'createIndex' => [
                'createIndex',
                static fn (Database $database): Index => $database->createIndex('books', Index::key(key: 'byLabel', attributes: ['label'])),
                'Failed to create index',
            ],
            'deleteIndex' => [
                'deleteIndex',
                static fn (Database $database) => $database->deleteIndex('books', 'byTitle'),
                'Failed to delete index',
            ],
            'renameIndex' => [
                'renameIndex',
                static fn (Database $database) => $database->renameIndex('books', 'byTitle', 'byHeading'),
                "Failed to rename index 'byTitle' to 'byHeading'",
            ],
            'createRelationship' => [
                'createRelationship',
                static fn (Database $database): Relationship => $database->createRelationship('books', Relationship::oneToMany(relatedCollection: 'authors', twoWay: true, key: 'editors', twoWayKey: 'edited')),
                'Failed to create relationship',
            ],
            'updateRelationship' => [
                'updateRelationship',
                static fn (Database $database): Relationship => $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer')),
                "Failed to update relationship 'author'",
            ],
            'deleteRelationship' => [
                'deleteRelationship',
                static fn (Database $database) => $database->deleteRelationship('books', 'author'),
                'Failed to delete relationship',
            ],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    #[DataProvider('operations')]
    public function testAFalseReturnIsRefusedWithASingleMessage(string $method, Closure $operation, string $message): void
    {
        [$database, $adapter] = $this->database();
        $adapter->verdicts[$method] = static fn (): bool => false;

        $error = $this->attempt($database, $operation);

        $this->assertInstanceOf(RefusedException::class, $error);
        $this->assertSame($message, $error->getMessage());
        $this->assertNull($error->getPrevious(), 'A refusal has no cause to wrap');
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    #[DataProvider('operations')]
    public function testAnAdapterErrorIsNotARefusal(string $method, Closure $operation, string $message): void
    {
        [$database, $adapter] = $this->database();
        $cause = new RuntimeException('the engine failed');
        $adapter->verdicts[$method] = static fn (): never => throw $cause;

        $error = $this->attempt($database, $operation);

        $this->assertNotInstanceOf(RefusedException::class, $error);
        $this->assertTrue(
            $error === $cause || $error->getPrevious() === $cause,
            'The adapter error must reach the caller, as itself or as the cause: '.$error::class.': '.$error->getMessage(),
        );
    }

    public function testARefusedCollectionStoresNoDefinition(): void
    {
        [$database, $adapter] = $this->database();
        $adapter->verdicts['createCollection'] = static fn (): bool => false;

        $this->assertInstanceOf(RefusedException::class, $this->attempt($database, static fn (Database $database): Collection => $database->createCollection(Collection::create(id: 'drafts'))));
        $this->assertNull($database->findCollection('drafts'));
    }

    public function testARefusedCollectionDropKeepsItsDefinition(): void
    {
        [$database, $adapter] = $this->database();
        $adapter->verdicts['deleteCollection'] = static fn (): bool => false;

        $this->assertInstanceOf(RefusedException::class, $this->attempt($database, static fn (Database $database) => $database->deleteCollection('notes')));
        $this->assertNotNull($database->findCollection('notes'));
    }

    public function testColumnsCreatedBeforeARefusalInTheOneAtATimeFallbackAreDropped(): void
    {
        [$database, $adapter] = $this->database();
        $adapter->verdicts['createAttributes'] = static fn (): never => throw new DuplicateException('Attribute already exists');
        $calls = 0;
        $adapter->verdicts['createAttribute'] = static function () use (&$calls): ?bool {
            return ++$calls === 1 ? null : false;
        };
        $dropped = [];
        $adapter->verdicts['deleteAttribute'] = static function () use (&$dropped): ?bool {
            $dropped[] = true;

            return null;
        };

        $error = $this->attempt($database, static fn (Database $database): array => $database->createAttributes('books', [Attribute::string(key: 'summary', size: 64), Attribute::integer(key: 'pages')]));

        $this->assertInstanceOf(RefusedException::class, $error);
        $this->assertCount(1, $dropped, 'The column created before the refusal must be dropped');
        $this->assertNotContains('summary', \array_map(static fn (Attribute $attribute): string => $attribute->key, $database->getCollection('books')->attributes()));
    }

    public function testAColumnTheFallbackCannotDropIsLogged(): void
    {
        [$database, $adapter] = $this->database();
        $adapter->verdicts['createAttributes'] = static fn (): never => throw new DuplicateException('Attribute already exists');
        $calls = 0;
        $adapter->verdicts['createAttribute'] = static function () use (&$calls): ?bool {
            return ++$calls === 1 ? null : false;
        };
        $adapter->verdicts['deleteAttribute'] = static fn (): never => throw new DatabaseException('the engine is read-only');

        $log = StderrCapture::during(function () use ($database): void {
            $this->assertInstanceOf(RefusedException::class, $this->attempt($database, static fn (Database $database): array => $database->createAttributes('books', [Attribute::string(key: 'summary', size: 64), Attribute::integer(key: 'pages')])));
        });

        $this->assertStringContainsString('the engine is read-only', $log);
    }

    /**
     * @param  Closure(Database): mixed  $operation
     */
    private function attempt(Database $database, Closure $operation): Throwable
    {
        try {
            $operation($database);
        } catch (Throwable $error) {
            return $error;
        }

        $this->fail('The schema call must fail');
    }

    /**
     * @return array{Database, VerdictMemory}
     */
    private function database(): array
    {
        $adapter = new VerdictMemory();
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('refused')
            ->setNamespace('refused_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: 'books',
            attributes: [
                Attribute::string(key: 'title', size: 64, required: true),
                Attribute::string(key: 'label', size: 64),
            ],
            indexes: [Index::key(key: 'byTitle', attributes: ['title'])],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(id: 'authors', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions));
        $database->createCollection(Collection::create(id: 'notes', permissions: $permissions));
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        return [$database, $adapter];
    }
}
