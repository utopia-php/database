<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Memory files each permission entry under its document's tenant. Under tenant-per-document a
 * write reaches another tenant's document with the same id only through that tenant, so each
 * write must change the grants of the selected tenant's document and no other.
 */
final class MemoryPermissionsTenantPerDocumentTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string DOCUMENT = 'note';

    private const string RENAMED = 'renamed';

    private const int TENANT = 5;

    private const int OTHER_TENANT = 6;

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private Authorization $authorization;

    private Database $database;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());

        $this->database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('memory_permissions_tenant_per_document')
            ->setNamespace('memory_permissions_tenant_per_document')
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true);
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        ));

        foreach ([self::TENANT, self::OTHER_TENANT] as $tenant) {
            $this->database->createDocument(
                self::COLLECTION,
                $this->readers([self::ALICE, self::BOB])
                    ->setAttribute('$id', self::DOCUMENT)
                    ->setAttribute('$tenant', $tenant)
                    ->setAttribute('title', 'first'),
            );
        }
    }

    public function testCreatingWithNoTenantSelectedGrantsReadUnderEachDocumentsTenant(): void
    {
        $this->assertNothingRevoked();
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

        $this->assertSame([self::TENANT => [self::RENAMED], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::ALICE));
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    public function testADeleteUnderTheDocumentsTenantRemovesOnlyThatTenantsGrants(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): bool => $this->database->deleteDocument(self::COLLECTION, self::DOCUMENT),
        );

        $this->assertOnlyTheOtherTenantReadable();
    }

    public function testABatchDeleteUnderTheDocumentsTenantRemovesOnlyThatTenantsGrants(): void
    {
        $this->database->withTenant(
            self::TENANT,
            fn (): int => $this->database->deleteDocuments(self::COLLECTION, [Query::equal('$id', [self::DOCUMENT])]),
        );

        $this->assertOnlyTheOtherTenantReadable();
    }

    public function testAnUpdateWithNoTenantSelectedChangesNoGrant(): void
    {
        $updated = $this->database->updateDocument(self::COLLECTION, self::DOCUMENT, $this->readers([self::ALICE]));

        $this->assertTrue($updated->isEmpty(), 'With no tenant selected no tenant\'s document is found to update');
        $this->assertNothingRevoked();
    }

    public function testABatchUpdateWithNoTenantSelectedChangesNoGrant(): void
    {
        $this->assertSame(0, $this->database->updateDocuments(self::COLLECTION, $this->readers([self::ALICE]), [Query::equal('$id', [self::DOCUMENT])]));
        $this->assertNothingRevoked();
    }

    public function testADeleteWithNoTenantSelectedChangesNoGrant(): void
    {
        $this->assertFalse($this->database->deleteDocument(self::COLLECTION, self::DOCUMENT));
        $this->assertNothingRevoked();
    }

    public function testABatchDeleteWithNoTenantSelectedIsRejected(): void
    {
        try {
            $this->database->deleteDocuments(self::COLLECTION, [Query::equal('$id', [self::DOCUMENT])]);
            $this->fail('A batch delete with no tenant selected must be rejected');
        } catch (DatabaseException $exception) {
            $this->assertStringStartsWith('Missing tenant', $exception->getMessage());
        }

        $this->assertNothingRevoked();
    }

    private function assertNothingRevoked(): void
    {
        $everywhere = [self::TENANT => [self::DOCUMENT], self::OTHER_TENANT => [self::DOCUMENT]];

        $this->assertSame($everywhere, $this->readableBy(self::ALICE));
        $this->assertSame($everywhere, $this->readableBy(self::BOB));
    }

    private function assertBobRevokedOnlyUnderTheTenant(): void
    {
        $this->assertSame([self::TENANT => [self::DOCUMENT], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::ALICE));
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
    }

    private function assertOnlyTheOtherTenantReadable(): void
    {
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::ALICE));
        $this->assertSame([self::TENANT => [], self::OTHER_TENANT => [self::DOCUMENT]], $this->readableBy(self::BOB));
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
