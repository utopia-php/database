<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class TenantWriteGuardsTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string PER_DOCUMENT_WITHOUT_SHARING = 'Shared tables must be enabled if tenant per document is enabled.';

    private const string MISSING_DOCUMENT_TENANT = 'Missing tenant. Tenant must be set when tenant per document is enabled.';

    private const string MISSING_TENANT = 'Missing tenant. Tenant must be set when table sharing is enabled.';

    public function testTenantPerDocumentNeedsSharedTablesForEveryWrite(): void
    {
        $database = $this->database(shared: false);
        $database->setTenantPerDocument(true);

        $this->assertRefused(self::PER_DOCUMENT_WITHOUT_SHARING, fn (): mixed => $database->createDocument(self::COLLECTION, $this->note('a', 1)));
        $this->assertRefused(self::PER_DOCUMENT_WITHOUT_SHARING, fn (): mixed => $database->createDocuments(self::COLLECTION, [$this->note('a', 1)]));
        $this->assertRefused(self::PER_DOCUMENT_WITHOUT_SHARING, fn (): mixed => $database->upsertDocuments(self::COLLECTION, [$this->note('a', 1)]));

        $database->setTenantPerDocument(false);
        $this->assertSame(0, $database->count(self::COLLECTION));
    }

    public function testATenantPerDocumentWriteNeedsTheDocumentsTenant(): void
    {
        $database = $this->database(shared: true);
        $database->setTenantPerDocument(true);

        $this->assertRefused(self::MISSING_DOCUMENT_TENANT, fn (): mixed => $database->createDocument(self::COLLECTION, $this->note('a', null)));
        $this->assertRefused(self::MISSING_DOCUMENT_TENANT, fn (): mixed => $database->createDocuments(self::COLLECTION, [$this->note('b', 1), $this->note('c', null)]));
        $this->assertRefused(self::MISSING_DOCUMENT_TENANT, fn (): mixed => $database->upsertDocuments(self::COLLECTION, [$this->note('d', null)]));

        $this->assertSame(1, $database->createDocuments(self::COLLECTION, [$this->note('e', 1)]));
        $this->assertSame(1, $database->withTenant(1, fn (): Document => $database->getDocument(self::COLLECTION, 'e'))->getTenant());
    }

    public function testAnUpsertUnderAnotherTenantWritesThatTenantsOwnDocument(): void
    {
        $database = $this->database(shared: true);
        $database->setTenantPerDocument(true);
        $database->createDocument(self::COLLECTION, $this->note('shared', 1));

        $database->upsertDocument(self::COLLECTION, $this->note('shared', 2)->setAttribute('body', 'second'));

        $first = $database->withTenant(1, fn (): Document => $database->getDocument(self::COLLECTION, 'shared'));
        $second = $database->withTenant(2, fn (): Document => $database->getDocument(self::COLLECTION, 'shared'));
        $this->assertSame([1, 'shared'], [$first->getTenant(), $first->getAttribute('body')]);
        $this->assertSame([2, 'second'], [$second->getTenant(), $second->getAttribute('body')]);
    }

    public function testBulkWritesUnderSharedTablesNeedATenant(): void
    {
        $database = $this->database(shared: true);

        $this->assertRefused(self::MISSING_TENANT, fn (): mixed => $database->createDocuments(self::COLLECTION, [$this->note('a', null)]));
        $this->assertRefused(self::MISSING_TENANT, fn (): mixed => $database->upsertDocuments(self::COLLECTION, [$this->note('a', null)]));

        $database->setTenant(3);
        $this->assertSame(1, $database->createDocuments(self::COLLECTION, [$this->note('a', null)]));
        $this->assertSame(3, $database->getDocument(self::COLLECTION, 'a')->getTenant());
    }

    /**
     * @param  callable(): mixed  $write
     */
    private function assertRefused(string $message, callable $write): void
    {
        $error = null;
        try {
            $write();
        } catch (DatabaseException $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(DatabaseException::class, $error, 'the write must be refused');
        $this->assertSame($message, $error->getMessage());
    }

    private function note(string $id, ?int $tenant): Document
    {
        return new Document([
            Document::ID => $id,
            Document::TENANT => $tenant,
            Document::PERMISSIONS => [Permission::read(Role::any()), Permission::update(Role::any())],
            'body' => $id,
        ]);
    }

    private function database(bool $shared): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('tenants')
            ->setNamespace('tenants_'.\uniqid())
            ->setSharedTables($shared)
            ->setTenant(null);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'body', size: 32)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));

        return $database;
    }
}
