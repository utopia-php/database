<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Exception as DatabaseException;

final class SQLitePrepareTest extends TestCase
{
    public function testExistsThrowsDatabaseExceptionWhenPrepareReturnsFalse(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn(false);

        $adapter = new SQLite($pdo);
        $adapter->setNamespace('namespace');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to prepare collection existence query');

        $adapter->exists('database', 'movies');
    }
}
