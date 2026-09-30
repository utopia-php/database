<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PDO;
use Utopia\Database\PermissionType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

final class PermissionsTest extends TestCase
{
    public function testBatchUpdateParsesEachPermissionTypeOnce(): void
    {
        $adapter = $this->adapter();

        $this->assertTrue($adapter->createCollection('movies'));
        $collection = new Document(['$id' => 'movies']);
        $documents = $adapter->createDocuments($collection, [
            new Document(['$id' => 'first', '$permissions' => [Permission::read(Role::any())]]),
            new Document(['$id' => 'second', '$permissions' => [Permission::read(Role::any())]]),
        ]);
        $updates = new class ([
            '$permissions' => [
                Permission::read(Role::user('reader')),
                Permission::update(Role::user('editor')),
            ],
        ]) extends Document {
            public int $calls = 0;

            #[\Override]
            public function getPermissionsByType(PermissionType $type): array
            {
                $this->calls++;

                return parent::getPermissionsByType($type);
            }
        };

        $adapter->updateDocuments($collection, $updates, $documents);
        $this->assertSame(4, $updates->calls);
    }

    public function testUpdateKeepsPermissionRowsUnderTheStoredDocumentIdCasing(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $adapter = $this->adapterWithLegacyCasedPermissions($pdo);
        $collection = new Document(['$id' => 'movies']);

        $adapter->updateDocument($collection, 'CaseSensitive', new Document([
            '$id' => 'CaseSensitive',
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::create(Role::guests()),
                Permission::create(Role::guests()),
                Permission::read(Role::guests()),
            ],
        ]), false);

        $this->assertSame([
            ['_document' => 'caseSensitive', '_type' => 'create', '_permission' => 'any'],
            ['_document' => 'caseSensitive', '_type' => 'create', '_permission' => 'guests'],
            ['_document' => 'caseSensitive', '_type' => 'read', '_permission' => 'guests'],
        ], $this->permissionRows($pdo));
    }

    public function testBatchUpdateKeepsPermissionRowsUnderTheStoredDocumentIdCasing(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $adapter = $this->adapterWithLegacyCasedPermissions($pdo);
        $collection = new Document(['$id' => 'movies']);

        $adapter->updateDocuments($collection, new Document([
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::create(Role::users()),
                Permission::create(Role::users()),
                Permission::read(Role::guests()),
            ],
        ]), $adapter->find($collection));

        $this->assertSame([
            ['_document' => 'caseSensitive', '_type' => 'create', '_permission' => 'any'],
            ['_document' => 'caseSensitive', '_type' => 'create', '_permission' => 'users'],
            ['_document' => 'caseSensitive', '_type' => 'read', '_permission' => 'guests'],
        ], $this->permissionRows($pdo));
    }

    public function testCurrentPermissionsPrefersExactDocumentIdWhenBothCasingsExist(): void
    {
        $exact = [
            PermissionType::Create->value => ['guests'],
            PermissionType::Read->value => [],
            PermissionType::Update->value => [],
            PermissionType::Delete->value => [],
        ];
        $other = [
            PermissionType::Create->value => ['any'],
            PermissionType::Read->value => [],
            PermissionType::Update->value => [],
            PermissionType::Delete->value => [],
        ];

        /** @var array<string, list<string>> $current */
        $current = $this->invokeHook('currentPermissions', [
            [
                'CaseSensitive' => $exact,
                'caseSensitive' => $other,
            ],
            'CaseSensitive',
        ]);

        $this->assertSame(['guests'], $current[PermissionType::Create->value]);
    }

    public function testGroupPermissionRowsPopulatesBothRequestedCasings(): void
    {
        /** @var array<string, array<string, list<string>>> $map */
        $map = $this->invokeHook('groupPermissionRows', [
            ['CaseSensitive', 'caseSensitive'],
            [
                [
                    Storage::PERM_DOCUMENT => 'caseSensitive',
                    Storage::PERM_TYPE => PermissionType::Create->value,
                    Storage::PERM_PERMISSION => 'any',
                ],
            ],
        ]);

        $this->assertSame(['any'], $map['CaseSensitive'][PermissionType::Create->value]);
        $this->assertSame(['any'], $map['caseSensitive'][PermissionType::Create->value]);
    }

    public function testUpdateDoesNotInsertDuplicatePermissionRows(): void
    {
        $adapter = $this->adapter();
        $this->assertTrue($adapter->createCollection('movies'));
        $collection = new Document(['$id' => 'movies']);
        $adapter->createDocuments($collection, [
            new Document(['$id' => 'dupes', '$permissions' => [Permission::create(Role::any())]]),
        ]);

        $update = new class ([
            '$id' => 'dupes',
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::create(Role::guests()),
            ],
        ]) extends Document {
            #[\Override]
            public function getPermissionsByType(PermissionType $type): array
            {
                if ($type === PermissionType::Create) {
                    return ['any', 'guests', 'guests'];
                }

                return parent::getPermissionsByType($type);
            }
        };

        $adapter->updateDocument($collection, 'dupes', $update, false);

        $document = $adapter->getDocument($collection, 'dupes');
        $this->assertSame(['any', 'guests'], $document->getCreate());
    }

    public function testUpdateDoesNotDuplicatePermissionsWhenDocumentIdCasingDiffers(): void
    {
        $adapter = $this->adapter();
        $this->assertTrue($adapter->createCollection('movies'));
        $collection = new Document(['$id' => 'movies']);
        $adapter->createDocuments($collection, [
            new Document([
                '$id' => 'caseSensitive',
                '$permissions' => [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]),
        ]);

        $update = new Document([
            '$id' => 'CaseSensitive',
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::create(Role::guests()),
                Permission::create(Role::guests()),
                Permission::read(Role::any()),
                Permission::read(Role::guests()),
                Permission::read(Role::guests()),
            ],
        ]);

        $adapter->updateDocument($collection, 'caseSensitive', $update, false);

        $document = $adapter->getDocument($collection, 'CaseSensitive');
        $this->assertSame('CaseSensitive', $document->getId());
        $this->assertSame(['any', 'guests'], $document->getCreate());
        $this->assertSame(['any', 'guests'], $document->getRead());
    }

    public function testBatchUpdateDeduplicatesPermissionAdditions(): void
    {
        $this->expectNotToPerformAssertions();

        $adapter = $this->adapter();
        $adapter->createCollection('movies');
        $collection = new Document(['$id' => 'movies']);
        $documents = $adapter->createDocuments($collection, [
            new Document(['$id' => 'batch', '$permissions' => [Permission::create(Role::any())]]),
        ]);

        $updates = new class ([
            '$permissions' => [
                Permission::create(Role::any()),
                Permission::create(Role::guests()),
            ],
        ]) extends Document {
            #[\Override]
            public function getPermissionsByType(PermissionType $type): array
            {
                if ($type === PermissionType::Create) {
                    return ['any', 'guests', 'guests'];
                }

                return parent::getPermissionsByType($type);
            }
        };

        $adapter->updateDocuments($collection, $updates, $documents);
    }

    public function testUpdateMovesPermissionRowsToTheRenamedDocument(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $adapter = $this->adapter($pdo);
        $this->assertTrue($adapter->createCollection('movies'));
        $collection = new Document(['$id' => 'movies']);
        $permissions = [
            Permission::read(Role::user('alice')),
            Permission::update(Role::user('alice')),
        ];
        [$created] = $adapter->createDocuments($collection, [
            new Document(['$id' => 'before', '$permissions' => $permissions]),
        ]);

        $adapter->updateDocument($collection, 'before', new Document([
            '$id' => 'after',
            '$sequence' => $created->getSequence(),
            '$permissions' => $permissions,
        ]), false);

        $rows = $pdo->prepare('SELECT _document, _type, _permission FROM permissions_movies_perms ORDER BY _type');
        $rows->execute();

        $this->assertSame([
            ['_document' => 'after', '_type' => 'read', '_permission' => 'user:alice'],
            ['_document' => 'after', '_type' => 'update', '_permission' => 'user:alice'],
        ], $rows->fetchAll(\PDO::FETCH_ASSOC), 'The rows keyed by the old id are unreadable and must follow the document to its new id');
    }

    private function adapterWithLegacyCasedPermissions(PDO $pdo): SQLite
    {
        $adapter = $this->adapter($pdo);
        $this->assertTrue($adapter->createCollection('movies'));
        $adapter->createDocuments(new Document(['$id' => 'movies']), [
            new Document([
                '$id' => 'CaseSensitive',
                '$permissions' => [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]),
        ]);
        $pdo->exec("UPDATE permissions_movies_perms SET _document = 'caseSensitive'");

        return $adapter;
    }

    /**
     * @return array<mixed>
     */
    private function permissionRows(PDO $pdo): array
    {
        $rows = $pdo->prepare('SELECT _document, _type, _permission FROM permissions_movies_perms ORDER BY _type, _permission');
        $rows->execute();

        return $rows->fetchAll(\PDO::FETCH_ASSOC);
    }

    private function adapter(?PDO $pdo = null): SQLite
    {
        $adapter = new SQLite($pdo ?? new PDO('sqlite::memory:', null, null));
        $adapter->setNamespace('permissions');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->addWriteHook(new Permissions());

        return $adapter;
    }

    /**
     * @param  list<mixed>  $arguments
     */
    private function invokeHook(string $method, array $arguments = []): mixed
    {
        $hook = new Permissions();
        $reflection = new ReflectionMethod(Permissions::class, $method);

        return $reflection->invoke($hook, ...$arguments);
    }
}
