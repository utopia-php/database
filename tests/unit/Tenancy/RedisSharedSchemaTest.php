<?php

namespace Tests\Unit\Tenancy;

use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Adapter\InMemoryRedis;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

#[RequiresPhpExtension('redis')]
final class RedisSharedSchemaTest extends TestCase
{
    private const string COLLECTION = 'users';

    private const string NAMESPACE = 'shared';

    private InMemoryRedis $client;

    public function testEachTenantKeepsItsOwnSchemaRecord(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(2);
        $this->assertSame(['age', 'nick', 'title'], $this->recordKeys(2, 'attrs'));
        $this->assertSame(['byAge', 'byTitle'], $this->recordKeys(2, 'indexes'));
        $this->assertSame(['age', 'nick'], $this->recordKeys(1, 'attrs'));
        $this->assertSame(['byAge'], $this->recordKeys(1, 'indexes'));
        $this->assertSame(['title2'], $this->titles($database));

        $this->assertDuplicateTitleRefused($database, 'The unique index the later tenant declared on createCollection() must hold');
    }

    public function testRenamingAnAttributeLeavesTheOtherTenantAsItWas(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $this->assertTrue($database->renameAttribute(self::COLLECTION, 'age', 'years'));
        $this->assertSame(['years', 'nick'], $this->recordKeys(1, 'attrs'));
        $this->assertSame([['years']], $this->recordIndexAttributes(1));
        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'user')->getAttribute('years'));

        $database->setTenant(2);
        $this->assertSame(['age', 'nick', 'title'], $this->recordKeys(2, 'attrs'));
        $this->assertSame([['age'], ['title']], $this->recordIndexAttributes(2));
        $this->assertSame(20, $database->getDocument(self::COLLECTION, 'user')->getAttribute('age'));

        $this->assertTrue($database->renameAttribute(self::COLLECTION, 'age', 'years'));
        $this->assertSame(20, $database->getDocument(self::COLLECTION, 'user')->getAttribute('years'));
    }

    public function testARenamedUniqueAttributeStaysUnique(): void
    {
        $database = $this->createSharedDatabase();
        $database->setTenant(2);

        $database->renameAttribute(self::COLLECTION, 'title', 'headline');

        try {
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'other', 'age' => 1, 'nick' => 'x', 'headline' => 'title2']));
            $this->fail('The unique index must follow its attribute through a rename');
        } catch (DuplicateException) {
            $this->addToAssertionCount(1);
        }
    }

    public function testDeletingAnIndexLeavesTheOtherTenantsIndex(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $database->createIndex(self::COLLECTION, Index::unique(key: 'byTitle', attributes: ['nick']));
        $database->setTenant(2);
        $this->assertSame(['byAge', 'byTitle'], $this->recordKeys(2, 'indexes'));

        $database->setTenant(1);
        $this->assertTrue($database->deleteIndex(self::COLLECTION, 'byTitle'));
        $this->assertSame(['byAge'], $this->recordKeys(1, 'indexes'));

        $database->setTenant(2);
        $this->assertSame(['byAge', 'byTitle'], $this->recordKeys(2, 'indexes'));
        $this->assertDuplicateTitleRefused($database, 'Another tenant deleting its index of the same key must leave this tenant\'s index');
    }

    public function testDeletingAnAttributeLeavesTheOtherTenantsValues(): void
    {
        $database = $this->createSharedDatabase();

        $database->setTenant(1);
        $this->assertTrue($database->deleteAttribute(self::COLLECTION, 'nick'));
        $this->assertFalse($database->getDocument(self::COLLECTION, 'user')->offsetExists('nick'));

        $database->setTenant(2);
        $this->assertSame(['age', 'nick', 'title'], $this->recordKeys(2, 'attrs'));
        $this->assertSame('k2', $database->getDocument(self::COLLECTION, 'user')->getAttribute('nick'));
    }

    public function testATenantWithoutItsOwnRecordReadsTheCollectionWideRecord(): void
    {
        $database = $this->createSharedDatabase();
        $tenantKey = $this->recordKey(1);
        $sharedKey = 'utopia:'.self::NAMESPACE.':'.$database->getDatabase().':meta:'.self::COLLECTION;
        $this->client->hashes[$sharedKey] = $this->client->hashes[$tenantKey];
        unset($this->client->hashes[$tenantKey]);

        $database->setTenant(1);
        $this->assertSame(['user'], \array_map(fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)));

        $database->createIndex(self::COLLECTION, Index::key(key: 'byNick', attributes: ['nick']));
        $this->assertSame(['byAge', 'byNick'], $this->recordKeys(1, 'indexes'), 'The tenant\'s record starts from the collection-wide indexes');
        $this->assertSame(['indexes'], \array_keys($this->client->hashes[$tenantKey]), 'Only the field the tenant changed moves to its own record');
        $this->assertSame(['byAge'], \array_column(\json_decode($this->client->hashes[$sharedKey]['indexes'], true), 'key'), 'The collection-wide record stays as it was');
    }

    private function assertDuplicateTitleRefused(Database $database, string $message): void
    {
        try {
            $database->createDocument(self::COLLECTION, new Document(['$id' => 'other', 'age' => 1, 'nick' => 'x', 'title' => 'title2']));
            $this->fail($message);
        } catch (DuplicateException) {
            $this->addToAssertionCount(1);
        }
    }

    /**
     * @return list<string>
     */
    private function titles(Database $database): array
    {
        return \array_values(\array_map(
            fn (Document $document): string => (string) $document->getAttribute('title'),
            $database->find(self::COLLECTION),
        ));
    }

    private function recordKey(int $tenant): string
    {
        return 'utopia:'.self::NAMESPACE.':utopiaTests:meta:t:'.$tenant.':'.self::COLLECTION;
    }

    /**
     * @return list<string>
     */
    private function recordKeys(int $tenant, string $field): array
    {
        /** @var list<array{key: string}> $records */
        $records = \json_decode($this->client->hashes[$this->recordKey($tenant)][$field] ?? '[]', true);

        return \array_map(static fn (array $record): string => $record['key'], $records);
    }

    /**
     * @return list<list<string>>
     */
    private function recordIndexAttributes(int $tenant): array
    {
        /** @var list<array{attributes: list<string>}> $records */
        $records = \json_decode($this->client->hashes[$this->recordKey($tenant)]['indexes'] ?? '[]', true);

        return \array_map(static fn (array $record): array => $record['attributes'], $records);
    }

    private function createSharedDatabase(): Database
    {
        $this->client = new InMemoryRedis();
        $authorization = new Authorization();
        $authorization->disable();

        $database = new Database(new Redis($this->client), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables(true)
            ->setTenant(1);
        $database->create();

        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $attributes = [Attribute::integer(key: 'age'), Attribute::string(key: 'nick', size: 64)];
            $indexes = [Index::key(key: 'byAge', attributes: ['age'])];
            if ($tenant === 2) {
                $attributes[] = Attribute::string(key: 'title', size: 64);
                $indexes[] = Index::unique(key: 'byTitle', attributes: ['title']);
            }
            $database->createCollection(new Collection(
                id: self::COLLECTION,
                attributes: $attributes,
                indexes: $indexes,
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
                documentSecurity: false,
            ));
            $database->createDocument(self::COLLECTION, new Document(
                ['$id' => 'user', 'age' => $tenant * 10, 'nick' => 'k'.$tenant] + ($tenant === 2 ? ['title' => 'title2'] : []),
            ));
        }

        return $database;
    }
}
