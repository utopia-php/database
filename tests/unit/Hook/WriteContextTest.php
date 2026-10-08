<?php

namespace Tests\Unit\Hook;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\WriteContext;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Query;

final class WriteContextTest extends TestCase
{
    private const string COLLECTION = 'notes';

    public function testSkipPermissionsTellsWhetherTheUpdateKeepsThePermissions(): void
    {
        $database = $this->database();
        $recorder = new WriteContextTestRecorder();
        $database->addHook($recorder);
        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $database->updateDocument(self::COLLECTION, 'first', new Document(['body' => 'kept']));
        $database->updateDocument(self::COLLECTION, 'first', new Document([
            'body' => 'changed',
            '$permissions' => [Permission::read(Role::users())],
        ]));

        $this->assertSame([true, false], $recorder->skipPermissions);
    }

    public function testUpdateHandsTheHookTheIdTheDocumentWasStoredUnder(): void
    {
        $database = $this->database();
        $recorder = new WriteContextTestRecorder();
        $database->addHook($recorder);
        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $database->updateDocument(self::COLLECTION, 'first', new Document(['body' => 'kept']));

        $this->assertSame([['first', 'first']], $recorder->updates);
    }

    public function testBuilderReadsOnlyTheAdaptersTenantWhileRawBuilderReadsEveryTenant(): void
    {
        $database = $this->database(tenant: 1);
        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $recorder = new WriteContextTestRecorder();
        $database->addHook($recorder);
        $database->setTenant(2);
        $database->createCollection($this->notes());
        $database->createDocument(self::COLLECTION, $this->note('second', 'users'));

        $this->assertSame(
            [
                'scoped' => [['second', 'users', 2]],
                'raw' => [['first', 'any', 1], ['second', 'users', 2]],
            ],
            $recorder->permissionRows,
        );
    }

    public function testDecorateRowStoresTheTenantTheRowIsWrittenFor(): void
    {
        $database = $this->database(tenant: 3);
        $recorder = new WriteContextTestRecorder();
        $database->addHook($recorder);

        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $this->assertSame([[Storage::TENANT => 3]], $recorder->decorated);
    }

    public function testDecorateRowLeavesTheRowAsItIsWithoutSharedTables(): void
    {
        $database = $this->database();
        $recorder = new WriteContextTestRecorder();
        $database->addHook($recorder);

        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $this->assertSame([[]], $recorder->decorated);
    }

    public function testRunRemovesTheRowsItsStatementDeletes(): void
    {
        $database = $this->database();
        $database->addHook(new class () extends Interceptor {
            #[\Override]
            public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
            {
                $builder = $context->builder(Storage::permissionsTable($collection));
                $builder->filter([Query::equal(Storage::PERMISSIONS_DOCUMENT, ['first'])]);
                $context->run($builder->delete(), Event::PermissionsDelete);
            }
        });

        $database->createDocument(self::COLLECTION, $this->note('first', 'any'));

        $this->assertSame([], $database->find(self::COLLECTION));
        $this->assertSame(['first'], \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->getAuthorization()->skip(static fn (): array => $database->find(self::COLLECTION)),
        ));
    }

    private function database(int|string|null $tenant = null): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('write_context')
            ->setNamespace('write_context');
        if ($tenant !== null) {
            $database->setSharedTables(true)->setTenant($tenant);
        }
        $database->create();
        $database->addHook(new Permissions());
        $database->createCollection($this->notes());

        return $database;
    }

    private function notes(): Collection
    {
        return Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'body', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::update(Role::any())],
            documentSecurity: true,
        );
    }

    private function note(string $id, string $reader): Document
    {
        return new Document([
            '$id' => $id,
            'body' => $id,
            '$permissions' => [Permission::read($reader === 'any' ? Role::any() : Role::users())],
        ]);
    }
}
