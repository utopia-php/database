<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Index;
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

    public function testRenamingKeepsItsUniqueValue(): void
    {
        $database = $this->memory();
        $database->createCollection(new Collection(
            id: 'users',
            attributes: [Attribute::string(key: 'email', size: 128)],
            permissions: $this->everyone(),
            documentSecurity: false,
        ));
        $database->createIndex('users', Index::unique(key: 'emailUnique', attributes: ['email'], lengths: [128]));
        $database->createDocument('users', new Document(['$id' => 'old', 'email' => 'a@example.test']));
        $database->createDocument('users', new Document(['$id' => 'other', 'email' => 'b@example.test']));

        $renamed = $database->updateDocument('users', 'old', new Document(['$id' => 'new', 'email' => 'a@example.test']));

        $this->assertSame('new', $renamed->getId());
        $this->assertTrue($database->getDocument('users', 'old')->isEmpty());
        $this->assertSame('a@example.test', $database->getDocument('users', 'new')->getAttribute('email'));

        try {
            $database->createDocument('users', new Document(['$id' => 'copy', 'email' => 'a@example.test']));
            $this->fail('The renamed document must still hold its unique value');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        $database->createDocument('users', new Document(['$id' => 'reuse', 'email' => 'c@example.test']));
        $database->updateDocument('users', 'reuse', new Document(['$id' => 'reused', 'email' => 'c@example.test']));
        $this->assertSame(['a@example.test', 'b@example.test', 'c@example.test'], $this->emails($database));
    }

    public function testSharedTablesListTenantlessCollections(): void
    {
        $listings = [];
        foreach (['memory' => new Memory(), 'sqlite' => new SQLite(new PDO('sqlite::memory:'))] as $name => $adapter) {
            $database = $this->database($adapter)
                ->setSharedTables(true)
                ->setTenant(null);
            $database->create();
            $database->addHook(new Permissions());
            $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
            $database->createCollection(new Collection(id: 'shared', attributes: [Attribute::string(key: 'name', size: 8)], permissions: $permissions));
            $database->setTenant(self::TENANT);
            $database->createCollection(new Collection(id: 'owned', attributes: [Attribute::string(key: 'name', size: 8)], permissions: $permissions));

            $identifiers = \array_map(static fn (Document $collection): string => $collection->getId(), $database->listCollections());
            \sort($identifiers);
            $listings[$name] = $identifiers;
        }

        $this->assertSame(['owned', 'shared'], $listings['sqlite']);
        $this->assertSame($listings['sqlite'], $listings['memory'], 'Memory must list the collections created without a tenant, as SQL does');
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
     * @return list<string>
     */
    private function everyone(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
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
    private function emails(Database $database): array
    {
        $emails = \array_map(
            static fn (Document $document): string => \is_string($email = $document->getAttribute('email')) ? $email : '',
            $database->find('users'),
        );
        \sort($emails);

        return $emails;
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
