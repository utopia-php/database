<?php

namespace Tests\Unit;

use Exception;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Query;
use Utopia\Database\Storage;

final class SQLGetDocumentTest extends TestCase
{
    public function testTranslatesExecuteTimeoutClosesCursorAndPreservesOriginalWhenCleanupFails(): void
    {
        $exception = $this->createTimeoutException();

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->once())
            ->method('bindValue')
            ->with(':'.Storage::UID, 'document', \PDO::PARAM_STR)
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('execute')
            ->willThrowException($exception);
        $statement->expects($this->never())
            ->method('fetch');
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willThrowException(new PDOException('Failed to close cursor'));

        $this->assertTimeout($this->createMySQL($statement), $exception);
    }

    public function testTranslatesFetchTimeoutAndClosesCursor(): void
    {
        $exception = $this->createTimeoutException();

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->once())
            ->method('bindValue')
            ->with(':'.Storage::UID, 'document', \PDO::PARAM_STR)
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('execute')
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('fetch')
            ->willThrowException($exception);
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willReturn(true);

        $this->assertTimeout($this->createMySQL($statement), $exception);
    }

    public function testUsesPostgresExecuteHook(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->once())
            ->method('bindValue')
            ->with(':'.Storage::UID, 'document', \PDO::PARAM_STR)
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('execute')
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('fetch')
            ->willReturn(false);
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willReturn(true);

        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($statement);
        $executed = [];
        $pdo->expects($this->exactly(2))
            ->method('exec')
            ->willReturnCallback(function (string $sql) use (&$executed): int {
                $executed[] = $sql;

                return 0;
            });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setTimeout(25);

        $document = $adapter->getDocument(
            new Document([Document::ID => 'collection']),
            'document'
        );

        $this->assertSame([], $document->getArrayCopy());
        $this->assertSame(["SET statement_timeout = '25ms'", 'RESET statement_timeout'], $executed);
    }

    public function testTranslatesBuilderFetchTimeoutAndClosesCursor(): void
    {
        $exception = $this->createTimeoutException();

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->once())
            ->method('execute')
            ->willReturn(true);
        $statement->expects($this->once())
            ->method('fetchAll')
            ->willThrowException($exception);
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willReturn(true);

        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($statement);

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        try {
            $adapter->getDocument(
                new Document([Document::ID => 'collection']),
                'document',
                [Query::select(['title'])]
            );
        } catch (TimeoutException $timeout) {
            $this->assertSame($exception, $timeout->getPrevious());

            return;
        }

        $this->fail('Expected a timeout exception.');
    }

    public function testAppliesTypedReadTransformOnFastAndBuilderPaths(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->method('bindValue')->willReturn(true);
        $statement->method('execute')->willReturn(true);
        $statement->method('fetch')->willReturn(false);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $queries = [];
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('prepare')
            ->willReturnCallback(function (string $sql) use (&$queries, $statement): \PDOStatement {
                $queries[] = $sql;

                return $statement;
            });

        $events = [];
        $transform = $this->createMock(Transform::class);
        $transform->expects($this->exactly(2))
            ->method('transform')
            ->willReturnCallback(function (\Utopia\Database\Event $event, string $query) use (&$events): string {
                $events[] = $event;

                return $query.' /* transformed */';
            });

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->addTransform('test', $transform);
        $collection = new Document([Document::ID => 'collection']);

        $adapter->getDocument($collection, 'fast');
        $adapter->getDocument($collection, 'builder', [Query::select(['title'])]);

        $this->assertSame([
            \Utopia\Database\Event::DocumentRead,
            \Utopia\Database\Event::DocumentRead,
        ], $events);
        $this->assertStringEndsWith('/* transformed */', $queries[0]);
        $this->assertStringEndsWith('/* transformed */', $queries[1]);
    }

    private function assertTimeout(MySQL $adapter, PDOException $exception): void
    {
        try {
            $adapter->getDocument(
                new Document([Document::ID => 'collection']),
                'document'
            );
        } catch (TimeoutException $timeout) {
            $this->assertSame($exception, $timeout->getPrevious());

            return;
        }

        $this->fail('Expected a timeout exception.');
    }

    private function createMySQL(\PDOStatement $statement): MySQL
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($statement);

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    private function createTimeoutException(): PDOException
    {
        $exception = new PDOException('Query execution was interrupted');
        $code = new ReflectionProperty(Exception::class, 'code');
        $code->setValue($exception, 'HY000');
        $exception->errorInfo = ['HY000', 3024, 'Query execution was interrupted'];

        return $exception;
    }
}
