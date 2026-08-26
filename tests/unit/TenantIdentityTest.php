<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\Document;

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

    public function testPaddedTenantDoesNotShareACacheKeyWithItsUnpaddedForm(): void
    {
        $this->assertNotSame(
            $this->hashKey('1'),
            $this->hashKey('001'),
            'Tenants "1" and "001" are distinct identifiers and must not share a cache key',
        );
    }

    public function testPaddedTenantDoesNotShareAScopeWithItsUnpaddedForm(): void
    {
        $padded = new MariaDB($this->createStub(PDO::class));
        $padded->setTenant('001');

        $unpadded = new MariaDB($this->createStub(PDO::class));
        $unpadded->setTenant('1');

        $this->assertNotSame(
            $unpadded->getTenant(),
            $padded->getTenant(),
            'Tenants "1" and "001" must not compare equal for shared-table scoping',
        );
    }

    public function testPaddedDocumentTenantKeepsItsIdentity(): void
    {
        $this->assertNotSame(
            (new Document(['$tenant' => '1']))->getTenant(),
            (new Document(['$tenant' => '001']))->getTenant(),
            'Documents tenanted "1" and "001" must not compare equal',
        );
    }

    public function testTenantBeyondIntegerRangeKeepsItsIdentity(): void
    {
        $beyond = '9223372036854775808';

        $adapter = new MariaDB($this->createStub(PDO::class));
        $adapter->setTenant($beyond);

        $this->assertSame($beyond, $adapter->getTenant());
    }

    public function testStringifiedTenantStillNormalisesToInt(): void
    {
        $adapter = new MariaDB($this->createStub(PDO::class));
        $adapter->setTenant('1');

        $this->assertSame(1, $adapter->getTenant(), 'PDO stringification must still compare equal to the integer tenant');
        $this->assertSame(1, (new Document(['$tenant' => '1']))->getTenant());
    }
}
