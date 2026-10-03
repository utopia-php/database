<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * Under tenant-per-document one shared table holds the documents of every tenant, and two
 * tenants can each hold a document with the same id. A permission write has to change the
 * `_perms` rows of the written document's tenant and no other, whichever tenant is selected.
 */
final class PermissionsTenantPerDocumentTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string DOCUMENT = 'note';

    private const string RENAMED = 'renamed';

    private const string COUNTER = 'views';

    private const int TENANT = 5;

    private const int OTHER_TENANT = 6;

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private PDO $pdo;

    private SQLite $adapter;

    private Authorization $authorization;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());

        $this->database = (new Database($this->adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('permissions_tenant_per_document')
            ->setNamespace('permissions_tenant_per_document')
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true)
            ->addHook(new Permissions());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'title', size: 64),
                Attribute::integer(key: self::COUNTER),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        ));

        foreach ([self::TENANT, self::OTHER_TENANT] as $tenant) {
            $this->database->createDocument(self::COLLECTION, $this->note($tenant, [self::ALICE, self::BOB]));
        }
    }

    public function testCreatingWithNoTenantSelectedStoresEachGrantUnderItsDocumentsTenant(): void
    {
        $this->assertSame($this->untouchedGrants(), $this->grants());
        $this->assertSame([self::TENANT => [self::DOCUMENT], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    public function testAnUpsertWithNoTenantSelectedRevokesTheGrantUnderTheDocumentsTenant(): void
    {
        $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], 'revoked')]);

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testAnUpsertWithIncreaseWithNoTenantSelectedRevokesTheGrantUnderTheDocumentsTenant(): void
    {
        $this->database->upsertDocumentsWithIncrease(
            self::COLLECTION,
            self::COUNTER,
            [$this->note(self::TENANT, [self::ALICE])->setAttribute(self::COUNTER, 1)],
        );

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testAnUpsertUnderAnotherTenantRevokesOnlyTheDocumentsOwnGrant(): void
    {
        $this->database->withTenant(
            self::OTHER_TENANT,
            fn (): int => $this->database->upsertDocuments(self::COLLECTION, [$this->note(self::TENANT, [self::ALICE], 'revoked')]),
        );

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testAnUpsertBatchAcrossTenantsRevokesOnlyWhereTheDocumentRevoked(): void
    {
        $this->database->upsertDocuments(self::COLLECTION, [
            $this->note(self::TENANT, [self::ALICE], 'revoked'),
            $this->note(self::OTHER_TENANT, [self::ALICE, self::BOB], 'retitled'),
        ]);

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testAnUpsertBatchScopesEveryRevokeToItsOwnDocumentsTenant(): void
    {
        $this->database->upsertDocuments(self::COLLECTION, [
            $this->note(self::TENANT, [self::ALICE], 'revoked'),
            $this->note(self::OTHER_TENANT, [self::BOB], 'revoked'),
        ]);

        $this->assertSame(
            [
                $this->grant(self::TENANT, self::ALICE),
                $this->grant(self::OTHER_TENANT, self::BOB),
            ],
            $this->grants(),
            'Each tenant revoked a different reader, so each must keep the grant it did not revoke',
        );
        $this->assertSame([self::TENANT => [self::DOCUMENT], self::OTHER_TENANT => []], $this->readableBy(self::ALICE));
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    public function testAnAdapterUpsertOfADocumentWithoutATenantRevokesUnderTheSelectedTenant(): void
    {
        $collection = $this->database->getCollection(self::COLLECTION);
        $stored = $this->authorization->skip(fn (): Document => $this->database->withTenant(
            self::TENANT,
            fn (): Document => $this->database->getDocument(self::COLLECTION, self::DOCUMENT),
        ));

        $this->database->withTenant(self::TENANT, fn (): array => $this->adapter->upsertDocuments($collection, '', [
            new Change($stored, new Document([
                '$id' => self::DOCUMENT,
                '$createdAt' => $stored->getCreatedAt(),
                '$updatedAt' => $stored->getUpdatedAt(),
                'title' => 'revoked',
                '$permissions' => [Permission::read(Role::user(self::ALICE))],
            ])),
        ]));

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testAnUpsertWithoutSharedTablesStillRevokesTheGrant(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $database = (new Database(new SQLite($pdo), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('permissions_not_shared')
            ->setNamespace('permissions_not_shared')
            ->addHook(new Permissions());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::update(Role::any())],
            documentSecurity: true,
        ));
        $database->createDocument(self::COLLECTION, $this->readers([self::ALICE, self::BOB])->setAttribute('$id', self::DOCUMENT));

        $database->upsertDocuments(self::COLLECTION, [$this->readers([self::ALICE])->setAttribute('$id', self::DOCUMENT)]);

        $statement = $pdo->query(
            'SELECT '.Storage::PERM_PERMISSION.' FROM `permissions_not_shared_'.Storage::permissionsTable(self::COLLECTION).'`',
        );
        $this->assertNotFalse($statement);
        $this->assertSame([Role::user(self::ALICE)->toString()], $statement->fetchAll(PDO::FETCH_COLUMN));
    }

    public function testAnUpdateUnderTheDocumentsTenantRevokesOnlyThatTenantsGrant(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): Document => $this->database->updateDocument(self::COLLECTION, self::DOCUMENT, $this->readers([self::ALICE])),
        );

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testABatchUpdateUnderTheDocumentsTenantRevokesOnlyThatTenantsGrant(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): int => $this->database->updateDocuments(self::COLLECTION, $this->readers([self::ALICE]), [Query::equal('$id', [self::DOCUMENT])]),
        );

        $this->assertBobRevokedOnlyUnderTheTenant();
    }

    public function testARenameUnderTheDocumentsTenantMovesOnlyThatTenantsGrants(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): Document => $this->database->updateDocument(
                self::COLLECTION,
                self::DOCUMENT,
                $this->readers([self::ALICE])->setAttribute('$id', self::RENAMED),
            ),
        );

        $this->assertSame(
            [
                $this->grant(self::TENANT, self::ALICE, self::RENAMED),
                $this->grant(self::OTHER_TENANT, self::ALICE),
                $this->grant(self::OTHER_TENANT, self::BOB),
            ],
            $this->grants(),
        );
        $this->assertSame([self::TENANT => [self::RENAMED], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::ALICE));
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    public function testADeleteUnderTheDocumentsTenantRemovesOnlyThatTenantsGrants(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): bool => $this->database->deleteDocument(self::COLLECTION, self::DOCUMENT),
        );

        $this->assertOnlyTheOtherTenantsGrantsRemain();
    }

    public function testABatchDeleteUnderTheDocumentsTenantRemovesOnlyThatTenantsGrants(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): int => $this->database->deleteDocuments(self::COLLECTION, [Query::equal('$id', [self::DOCUMENT])]),
        );

        $this->assertOnlyTheOtherTenantsGrantsRemain();
    }

    public function testAnUpdateWithNoTenantSelectedChangesNoGrant(): void
    {
        $updated = $this->database->updateDocument(self::COLLECTION, self::DOCUMENT, $this->readers([self::ALICE]));

        $this->assertTrue($updated->isEmpty(), 'With no tenant selected no tenant\'s document is found to update');
        $this->assertSame($this->untouchedGrants(), $this->grants());
    }

    public function testABatchUpdateWithNoTenantSelectedChangesNoGrant(): void
    {
        $updated = $this->database->updateDocuments(self::COLLECTION, $this->readers([self::ALICE]), [Query::equal('$id', [self::DOCUMENT])]);

        $this->assertSame(0, $updated);
        $this->assertSame($this->untouchedGrants(), $this->grants());
    }

    public function testADeleteWithNoTenantSelectedChangesNoGrant(): void
    {
        $this->assertFalse($this->database->deleteDocument(self::COLLECTION, self::DOCUMENT));
        $this->assertSame($this->untouchedGrants(), $this->grants());
    }

    public function testABatchDeleteWithNoTenantSelectedIsRejected(): void
    {
        try {
            $this->database->deleteDocuments(self::COLLECTION, [Query::equal('$id', [self::DOCUMENT])]);
            $this->fail('A batch delete with no tenant selected must be rejected');
        } catch (DatabaseException $exception) {
            $this->assertStringStartsWith('Missing tenant', $exception->getMessage());
        }

        $this->assertSame($this->untouchedGrants(), $this->grants());
    }

    private function assertBobRevokedOnlyUnderTheTenant(): void
    {
        $this->assertSame(
            [
                $this->grant(self::TENANT, self::ALICE),
                $this->grant(self::OTHER_TENANT, self::ALICE),
                $this->grant(self::OTHER_TENANT, self::BOB),
            ],
            $this->grants(),
            'Revoking bob on tenant 5\'s document must remove tenant 5\'s row and no other tenant\'s',
        );
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
        $this->assertSame([self::TENANT => [self::DOCUMENT], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::ALICE));
    }

    private function assertOnlyTheOtherTenantsGrantsRemain(): void
    {
        $this->assertSame(
            [
                $this->grant(self::OTHER_TENANT, self::ALICE),
                $this->grant(self::OTHER_TENANT, self::BOB),
            ],
            $this->grants(),
        );
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    /**
     * @param  list<string>  $readers
     */
    private function note(int $tenant, array $readers, string $title = 'first'): Document
    {
        return $this->readers($readers)
            ->setAttribute('$id', self::DOCUMENT)
            ->setAttribute('$tenant', $tenant)
            ->setAttribute('title', $title);
    }

    /**
     * @param  list<string>  $readers
     */
    private function readers(array $readers): Document
    {
        return new Document([
            '$permissions' => \array_map(
                static fn (string $reader): string => Permission::read(Role::user($reader)),
                $readers,
            ),
        ]);
    }

    /**
     * @return list<array{int, string, string, string}>
     */
    private function untouchedGrants(): array
    {
        return [
            $this->grant(self::TENANT, self::ALICE),
            $this->grant(self::TENANT, self::BOB),
            $this->grant(self::OTHER_TENANT, self::ALICE),
            $this->grant(self::OTHER_TENANT, self::BOB),
        ];
    }

    /**
     * @return array{int, string, string, string}
     */
    private function grant(int $tenant, string $reader, string $document = self::DOCUMENT): array
    {
        return [$tenant, $document, PermissionType::Read->value, Role::user($reader)->toString()];
    }

    /**
     * @return list<array{int, string, string, string}>
     */
    private function grants(): array
    {
        $columns = [Storage::TENANT, Storage::PERM_DOCUMENT, Storage::PERM_TYPE, Storage::PERM_PERMISSION];
        $statement = $this->pdo->query(
            'SELECT '.\implode(', ', $columns)
            .' FROM `permissions_tenant_per_document_'.Storage::permissionsTable(self::COLLECTION).'`'
            .' ORDER BY '.\implode(', ', $columns),
        );
        $this->assertNotFalse($statement);

        /** @var list<array{int, string, string, string}> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_NUM);

        return $rows;
    }

    /**
     * @return array<int, list<string>>
     */
    private function readableBy(string $reader): array
    {
        $roles = $this->authorization->getRoles();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($reader)->toString());

        try {
            $readable = [];
            foreach ([self::TENANT, self::OTHER_TENANT] as $tenant) {
                $readable[$tenant] = \array_values(\array_map(
                    static fn (Document $document): string => $document->getId(),
                    $this->database->withTenant($tenant, fn (): array => $this->database->find(self::COLLECTION)),
                ));
            }

            return $readable;
        } finally {
            $this->authorization->cleanRoles();
            foreach ($roles as $role) {
                $this->authorization->addRole($role);
            }
        }
    }
}
