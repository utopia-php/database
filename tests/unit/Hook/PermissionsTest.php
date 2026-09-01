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

    public function testUniqueAdditionsRemovesDuplicateRoles(): void
    {
        $additions = $this->invokeHook('uniqueAdditions', [
            ['any', 'guests', 'guests', 'any'],
            ['any'],
        ]);

        $this->assertSame(['guests'], $additions);
    }

    public function testCurrentPermissionsLookupUsesStoredDocumentIdCase(): void
    {
        $stored = [
            PermissionType::Create->value => ['any'],
            PermissionType::Read->value => ['any'],
            PermissionType::Update->value => [],
            PermissionType::Delete->value => [],
        ];

        /** @var array<string, list<string>> $current */
        $current = $this->invokeHook('currentPermissions', [
            [
                'caseSensitive' => $stored,
            ],
            'CaseSensitive',
        ]);

        $this->assertSame(['any'], $current[PermissionType::Create->value]);
        $this->assertSame(['any'], $current[PermissionType::Read->value]);
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

    public function testGroupPermissionRowsMapsStoredDocumentIdToRequestedCasing(): void
    {
        /** @var array<string, array<string, list<string>>> $map */
        $map = $this->invokeHook('groupPermissionRows', [
            ['CaseSensitive'],
            [
                [
                    Storage::PERM_DOCUMENT => 'caseSensitive',
                    Storage::PERM_TYPE => PermissionType::Create->value,
                    Storage::PERM_PERMISSION => 'any',
                ],
                [
                    Storage::PERM_DOCUMENT => 'caseSensitive',
                    Storage::PERM_TYPE => PermissionType::Read->value,
                    Storage::PERM_PERMISSION => 'any',
                ],
            ],
        ]);

        $this->assertArrayHasKey('CaseSensitive', $map);
        $this->assertSame(['any'], $map['CaseSensitive'][PermissionType::Create->value]);
        $this->assertSame(['any'], $map['CaseSensitive'][PermissionType::Read->value]);
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

    public function testStoredDocumentIdsKeepsTableCasing(): void
    {
        /** @var array<string, string> $stored */
        $stored = $this->invokeHook('storedDocumentIds', [
            ['CaseSensitive', 'caseSensitive'],
            [
                [
                    Storage::PERM_DOCUMENT => 'caseSensitive',
                    Storage::PERM_TYPE => PermissionType::Create->value,
                    Storage::PERM_PERMISSION => 'any',
                ],
            ],
        ]);

        $this->assertSame('caseSensitive', $stored['CaseSensitive']);
        $this->assertSame('caseSensitive', $stored['caseSensitive']);
    }

    public function testPermissionDocumentIdUsesStoredCasing(): void
    {
        $this->assertSame(
            'caseSensitive',
            $this->invokeHook('permissionDocumentId', [
                'CaseSensitive',
                ['CaseSensitive' => 'caseSensitive'],
            ])
        );
        $this->assertSame(
            'new-id',
            $this->invokeHook('permissionDocumentId', [
                'new-id',
                ['old-id' => 'old-id'],
            ])
        );
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

        $document = $adapter->getDocument($collection, 'caseSensitive');
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

    private function adapter(): SQLite
    {
        $adapter = new SQLite(new PDO('sqlite::memory:', null, null));
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
