<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;

final class AddTableColumnTest extends TestCase
{
    public function testVectorOnMySQLTableThrowsDatabaseException(): void
    {
        $adapter = new MariaDB(self::createStub(PDO::class));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector columns are only supported on PostgreSQL');

        $adapter->createAttribute('movies', Attribute::vector(key: 'embedding', dimensions: 3));
    }
}
