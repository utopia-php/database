<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\WriteContext;

final class MariaDBCreateDocumentTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    private int $hookCalls = 0;

    public function testAnEmptyInsertIdIsAnError(): void
    {
        $adapter = $this->adapter(insertId: '');

        try {
            $adapter->createDocument($this->collection(), $this->document());
            $this->fail('A document without an insert id must not be returned as created');
        } catch (DatabaseException $error) {
            $this->assertSame('Error creating document empty "$sequence"', $error->getMessage());
        }

        $this->assertCount(1, $this->statements);
        $this->assertStringStartsWith('INSERT INTO', $this->statements[0]);
    }

    public function testAWriteHookFailureOtherThanAnOrphanedPermissionIsMappedAndNotRetried(): void
    {
        $adapter = $this->adapter(insertId: '12');
        $hook = $this->failingHook([$this->engineError('22001', 1406, 'Data too long for column \'_permission\' at row 1')]);
        $adapter->addWriteHook($hook);

        try {
            $adapter->createDocument($this->collection(), $this->document());
            $this->fail('A write hook failure must reach the caller');
        } catch (TruncateException $error) {
            $this->assertSame('Resize would result in data truncation', $error->getMessage());
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }

        $this->assertSame(1, $this->hookCalls);
        $this->assertCount(1, $this->statements);
    }

    public function testAnOrphanedPermissionIsClearedAndTheWriteHookRetried(): void
    {
        $adapter = $this->adapter(insertId: '12');
        $hook = $this->failingHook([$this->engineError('23000', 1062, 'Duplicate entry \'first-read-any\' for key \'_index1\'')]);
        $adapter->addWriteHook($hook);

        $created = $adapter->createDocument($this->collection(), $this->document());

        $this->assertSame('12', $created->getSequence());
        $this->assertSame(2, $this->hookCalls);
        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith('DELETE FROM', $this->statements[1]);
        $this->assertStringContainsString('_perms', $this->statements[1]);
    }

    private function collection(): Document
    {
        return new Document(['$id' => 'notes', 'attributes' => []]);
    }

    private function document(): Document
    {
        return new Document([
            '$id' => 'first',
            '$permissions' => ['read("any")'],
            '$createdAt' => '2026-09-30 00:00:00.000',
            '$updatedAt' => '2026-09-30 00:00:00.000',
            'body' => 'one',
        ]);
    }

    private function engineError(string $state, int $code, string $message): PDOException
    {
        $error = new class ('SQLSTATE[' . $state . ']: ' . $message, $state) extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }

    /**
     * @param list<PDOException> $failures
     */
    private function failingHook(array $failures): Interceptor
    {
        $record = function (): void {
            $this->hookCalls++;
        };

        return new class ($failures, $record) extends Interceptor {
            /**
             * @param list<PDOException> $failures
             */
            public function __construct(private array $failures, private readonly Closure $record)
            {
            }

            public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
            {
                ($this->record)();
                $failure = \array_shift($this->failures);
                if ($failure !== null) {
                    throw $failure;
                }
            }
        };
    }

    private function adapter(string $insertId): MariaDB
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn($insertId);

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
