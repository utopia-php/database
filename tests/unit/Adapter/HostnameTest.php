<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\SQLite;

final class HostnameTest extends TestCase
{
    public function testAnSQLAdapterOverAPlainPDOReturnsTheHostnameItWasGiven(): void
    {
        $adapter = new MariaDB(self::createStub(PDO::class));

        $this->assertSame($adapter, $adapter->setHostname('db-1'));
        $this->assertSame('db-1', $adapter->hostname());

        $adapter->setHostname('db-2');
        $this->assertSame('db-2', $adapter->hostname());
    }

    public function testAnAdapterWithoutAHostnameReturnsAnEmptyOne(): void
    {
        $this->assertSame('', (new SQLite(new PDO('sqlite::memory:')))->hostname());
    }
}
