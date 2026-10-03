<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\Validator\Authorization;

final class SQLitePrepareTest extends TestCase
{
    public function testWrapperBackedCollectionAndDocumentStatementsExecute(): void
    {
        $adapter = new SQLite(new DatabasePDO('sqlite::memory:', null, null));
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $this->assertTrue($adapter->createCollection('movies'));

        $document = $adapter->createDocument(
            new Document(['$id' => 'movies']),
            new Document(['$id' => 'movie', '$permissions' => []]),
        );

        $this->assertSame('movie', $document->getId());
        $this->assertSame('movie', $adapter->getDocument(new Document(['$id' => 'movies']), 'movie')->getId());
    }

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
