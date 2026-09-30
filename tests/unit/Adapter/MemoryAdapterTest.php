<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class MemoryAdapterTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string DOCUMENT = 'note';

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private Authorization $authorization;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
    }

    public function testDeleteWithAnotherCasingRemovesTheGrants(): void
    {
        $database = $this->memory();
        $this->createNotes($database);
        $database->createDocument(self::COLLECTION, $this->note([self::ALICE]));

        $this->assertTrue($database->deleteDocument(self::COLLECTION, \strtoupper(self::DOCUMENT)));
        $database->createDocument(self::COLLECTION, $this->note([self::BOB]));

        $this->assertSame([], $this->readableBy($database, self::ALICE), 'A delete under another casing must remove the deleted document\'s grants');
        $this->assertSame([self::DOCUMENT], $this->readableBy($database, self::BOB));
    }

    public function testRevokeUnderOneTenantKeepsAnotherTenantsGrants(): void
    {
        $database = $this->sharedNotes();

        $database->withTenant(self::TENANT, fn (): Document => $database->updateDocument(self::COLLECTION, self::DOCUMENT, $this->readers([])));

        $this->assertSame([], $this->readableUnder($database, self::TENANT, self::ALICE));
        $this->assertSame([self::DOCUMENT], $this->readableUnder($database, self::OTHER_TENANT, self::ALICE), 'A revoke under one tenant must keep another tenant\'s grants');
        $this->assertSame([self::DOCUMENT], $this->readableUnder($database, self::OTHER_TENANT, self::BOB));

        $database->withTenant(self::OTHER_TENANT, fn (): Document => $database->updateDocument(self::COLLECTION, self::DOCUMENT, $this->readers([self::ALICE])));

        $this->assertSame([self::DOCUMENT], $this->readableUnder($database, self::OTHER_TENANT, self::ALICE));
        $this->assertSame([], $this->readableUnder($database, self::OTHER_TENANT, self::BOB), 'The second tenant\'s own revoke must remove its grant');
        $this->assertSame([], $this->readableUnder($database, self::TENANT, self::BOB));
    }

    public function testDeleteUnderOneTenantKeepsAnotherTenantsGrants(): void
    {
        $database = $this->sharedNotes();

        $database->withTenant(self::TENANT, fn (): bool => $database->deleteDocument(self::COLLECTION, self::DOCUMENT));

        $this->assertSame([self::DOCUMENT], $this->readableUnder($database, self::OTHER_TENANT, self::ALICE), 'A delete under one tenant must keep another tenant\'s grants');

        $database->withTenant(self::OTHER_TENANT, fn (): bool => $database->deleteDocument(self::COLLECTION, self::DOCUMENT));
        $database->withTenant(self::OTHER_TENANT, fn (): Document => $database->createDocument(self::COLLECTION, $this->note([self::BOB])));

        $this->assertSame([], $this->readableUnder($database, self::OTHER_TENANT, self::ALICE), 'The second tenant\'s own delete must remove its grants');
        $this->assertSame([self::DOCUMENT], $this->readableUnder($database, self::OTHER_TENANT, self::BOB));
        $this->assertSame([], $this->readableUnder($database, self::TENANT, self::BOB));
    }

    private function database(Adapter $adapter): Database
    {
        return (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('memory_adapter')
            ->setNamespace('memory_adapter_'.\uniqid());
    }

    private function memory(): Database
    {
        $database = $this->database(new Memory());
        $database->create();

        return $database;
    }

    private function sharedNotes(): Database
    {
        $database = $this->database(new Memory())
            ->setSharedTables(true)
            ->setTenant(null);
        $database->create();
        $this->createNotes($database);

        foreach ([self::TENANT, self::OTHER_TENANT] as $tenant) {
            $database->withTenant($tenant, fn (): Document => $database->createDocument(self::COLLECTION, $this->note([self::ALICE, self::BOB])));
        }

        return $database;
    }

    private function createNotes(Database $database): void
    {
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        ));
    }

    /**
     * @param  list<string>  $readers
     */
    private function note(array $readers): Document
    {
        return $this->readers($readers)
            ->setAttribute('$id', self::DOCUMENT)
            ->setAttribute('title', 'first');
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
     * @return list<string>
     */
    private function readableUnder(Database $database, int $tenant, string $reader): array
    {
        return $database->withTenant($tenant, fn (): array => $this->readableBy($database, $reader));
    }

    /**
     * @return list<string>
     */
    private function readableBy(Database $database, string $reader): array
    {
        $roles = $this->authorization->getRoles();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($reader)->toString());

        try {
            return \array_values(\array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find(self::COLLECTION),
            ));
        } finally {
            $this->authorization->cleanRoles();
            foreach ($roles as $role) {
                $this->authorization->addRole($role);
            }
        }
    }
}
