<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\Document;

/**
 * `_tenant` is declared `INT(11) UNSIGNED`, so the engine reads "001" and "1"
 * as the same tenant and a query for tenant 1 returns rows written under
 * either. The PHP side has to agree with that: a scope comparison or a cache
 * key that told the two apart would claim a distinction the rows do not have,
 * and hand one tenant's cached row to a lookup the engine would have answered
 * with both.
 */
class TenantIdentityTest extends TestCase
{
    private function database(int|string|null $tenant): Database
    {
        $adapter = new MariaDB($this->createStub(PDO::class));
        $adapter->setDatabase('test');
        $adapter->setNamespace('test');
        $adapter->setSharedTables(true);
        $adapter->setTenant($tenant);

        return new Database($adapter, new Cache(new None()));
    }

    private function hashKey(int|string|null $tenant): string
    {
        [, , $hashKey] = $this->database($tenant)->getCacheKeys('col', 'doc1');

        return $hashKey;
    }

    public function testTheCacheKeyFollowsTheTenantTheColumnWillHold(): void
    {
        $this->assertSame(
            $this->hashKey(1),
            $this->hashKey('001'),
            'The engine stores "001" as 1 and returns those rows for tenant 1, so the cache key must not separate them',
        );
    }

    public function testAStringifiedTenantIsTheSameTenantAsItsInteger(): void
    {
        $this->assertSame($this->hashKey(1), $this->hashKey('1'));

        $adapter = new MariaDB($this->createStub(PDO::class));
        $adapter->setTenant('1');

        $this->assertSame(1, $adapter->getTenant(), 'A driver that stringifies the column must still compare equal to the integer tenant');
    }

    public function testADocumentTenantNormalisesTheSameWayTheAdapterDoes(): void
    {
        $this->assertSame(1, (new Document(['$tenant' => '1']))->getTenant());
        $this->assertSame(1, (new Document(['$tenant' => '001']))->getTenant());
    }

    public function testANonNumericTenantIsLeftAlone(): void
    {
        $adapter = new MariaDB($this->createStub(PDO::class));
        $adapter->setTenant('tenant-a');

        $this->assertSame('tenant-a', $adapter->getTenant());
    }
}
