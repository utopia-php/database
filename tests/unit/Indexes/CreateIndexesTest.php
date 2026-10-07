<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

final class CreateIndexesTest extends TestCase
{
    public function testCreatesEveryIndexAndReturnsThemAsStored(): void
    {
        $database = $this->database(new Memory());

        $created = $database->createIndexes('users', [
            Index::unique('by_email', ['email']),
            Index::key('by_name', ['name'], [64]),
        ]);

        $this->assertSame(['by_email', 'by_name'], \array_map(static fn (Index $index): string => $index->key, $created));
        $this->assertSame([null], $created[1]->lengths);
        $this->assertSame(
            \array_map(static fn (Index $index): array => $index->toDocument()->getArrayCopy(), $created),
            \array_map(static fn (Index $index): array => $index->toDocument()->getArrayCopy(), $database->getCollection('users')->indexes()),
        );

        $this->createUser($database, 'one', 'same@example.com');
        $this->expectException(DuplicateException::class);
        $this->createUser($database, 'two', 'same@example.com');
    }

    public function testAnEmptyListCreatesNothing(): void
    {
        $database = $this->database(new Memory());

        $this->assertSame([], $database->createIndexes('users', []));
        $this->assertSame([], $database->getCollection('users')->indexes());
    }

    public function testAnInvalidIndexCreatesNone(): void
    {
        $database = $this->database(new Memory());

        try {
            $database->createIndexes('users', [
                Index::unique('by_email', ['email']),
                Index::key('by_missing', ['missing']),
            ]);
            $this->fail('An index on a missing attribute was accepted');
        } catch (IndexException $error) {
            $this->assertSame('Invalid index attribute "missing" not found', $error->getMessage());
        }

        $this->assertNoIndexes($database);
    }

    public function testKeysRepeatedInTheBatchCreateNone(): void
    {
        $database = $this->database(new Memory());

        try {
            $database->createIndexes('users', [
                Index::unique('by_email', ['email']),
                Index::key('BY_EMAIL', ['name']),
            ]);
            $this->fail('A key repeated in the batch was accepted');
        } catch (DuplicateException) {
        }

        $this->assertNoIndexes($database);
    }

    public function testABatchOverTheIndexLimitCreatesNone(): void
    {
        $database = $this->database(new Memory());
        $indexes = [];
        for ($position = 0; $position < 65; $position++) {
            $indexes[] = Index::key('by_name_'.$position, ['name']);
        }

        try {
            $database->createIndexes('users', $indexes);
            $this->fail('A batch over the index limit was accepted');
        } catch (LimitException) {
        }

        $this->assertNoIndexes($database);
    }

    public function testAnEngineFailurePartWayDropsTheIndexesAlreadyCreated(): void
    {
        $adapter = new class () extends Memory {
            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
            {
                if ($index->key === 'broken') {
                    throw new DatabaseException('Engine refused the index');
                }

                return parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
            }
        };
        $database = $this->database($adapter);

        try {
            $database->createIndexes('users', [
                Index::unique('by_email', ['email']),
                Index::key('broken', ['name']),
            ]);
            $this->fail('The engine failure was swallowed');
        } catch (DatabaseException $error) {
            $this->assertSame('Engine refused the index', $error->getMessage());
        }

        $this->assertNoIndexes($database);
    }

    private function assertNoIndexes(Database $database): void
    {
        $this->assertSame([], $database->getCollection('users')->indexes());

        $this->createUser($database, 'one', 'same@example.com');
        $this->createUser($database, 'two', 'same@example.com');
        $this->assertSame(2, $database->count('users'));
    }

    private function createUser(Database $database, string $id, string $email): void
    {
        $database->createDocument('users', new Document([
            '$id' => $id,
            'email' => $email,
            'name' => $id,
        ]));
    }

    private function database(Memory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('create_indexes')
            ->setNamespace('create_indexes_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create('users', attributes: [
            Attribute::string('email', 128),
            Attribute::string('name', 64),
        ], permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));

        return $database;
    }
}
