<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Transform;
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class SQLTimeoutScopeTest extends TestCase
{
    public function testPublicReadTimeoutDoesNotAffectCollectionDelete(): void
    {
        $session = [];
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('exec')
            ->willReturnCallback(function (string $sql) use (&$session): int {
                $session[] = $sql;

                return 0;
            });

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([]);
        $statement->expects($this->once())->method('closeCursor')->willReturn(true);
        $pdo->expects($this->exactly(2))->method('prepare')->willReturn($statement);

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->setTimeout(25, Event::DocumentFind);

        $adapter->find(
            new Document(['$id' => 'movies']),
            orderAttributes: ['$sequence'],
        );
        $this->assertTrue($adapter->deleteCollection('movies'));

        $this->assertSame([
            'SET SESSION MAX_EXECUTION_TIME = 25',
            'SET SESSION MAX_EXECUTION_TIME = 0',
        ], $session);
    }

    public function testPublicStatementsUseExactBatchSchemaAndPermissionEvents(): void
    {
        $transform = new class () implements Transform {
            /** @var list<Event> */
            public array $events = [];

            public function transform(Event $event, string $query): string
            {
                $this->events[] = $event;

                return $query;
            }
        };

        $adapter = new SQLite(new DatabasePDO('sqlite::memory:', null, null));
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->addTransform('events', $transform);

        $this->assertTrue($adapter->createCollection('movies'));
        $this->assertContains(Event::CollectionCreate, $transform->events);
        $this->assertNotContains(Event::IndexCreate, $transform->events);
        $transform->events = [];

        $this->assertTrue($adapter->createAttributes('movies', [
            new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: false),
            new Attribute(key: 'label', type: ColumnType::String, size: 32, required: false),
        ]));
        $this->assertContains(Event::AttributesCreate, $transform->events);
        $this->assertNotContains(Event::AttributeCreate, $transform->events);
        $transform->events = [];

        $collection = new Document(['$id' => 'movies']);
        $created = new Document(['$id' => 'batch', 'score' => 1, 'label' => 'before', '$permissions' => []]);
        $adapter->createDocuments($collection, [$created]);
        $this->assertContains(Event::DocumentsCreate, $transform->events);
        $this->assertNotContains(Event::DocumentCreate, $transform->events);
        $created = $adapter->getDocument($collection, 'batch');
        $updated = new Document([
            '$id' => 'batch',
            '$sequence' => $created->getSequence(),
            'score' => 2,
            'label' => 'after',
            '$permissions' => [],
        ]);
        $transform->events = [];
        $adapter->upsertDocuments($collection, '', [new Change($created, $updated)]);
        $this->assertContains(Event::DocumentsUpsert, $transform->events);
        $this->assertNotContains(Event::DocumentCreate, $transform->events);
        $transform->events = [];
        $adapter->increaseDocumentAttribute('movies', 'batch', 'score', 1, '2026-08-13 00:00:00.000');
        $adapter->increaseDocumentAttribute('movies', 'batch', 'score', -1, '2026-08-13 00:00:00.000');
        $this->assertContains(Event::DocumentIncrease, $transform->events);
        $this->assertContains(Event::DocumentDecrease, $transform->events);

        $adapter->addWriteHook(new Permissions());
        $permissionDocument = $adapter->createDocument($collection, new Document([
            '$id' => 'permissioned',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $permissionDocument['$permissions'] = [Permission::read(Role::user('one'))];
        $adapter->updateDocument($collection, 'permissioned', $permissionDocument, false);
        $adapter->deleteDocument('movies', 'permissioned');

        foreach ([
            Event::PermissionsCreate,
            Event::PermissionsRead,
            Event::PermissionsDelete,
        ] as $event) {
            $this->assertContains($event, $transform->events);
        }
    }

    public function testMySQLGlobalTimeoutUsesOnlyTheMySQLSessionVariable(): void
    {
        $statements = [];
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('exec')
            ->willReturnCallback(function (string $sql) use (&$statements): int {
                $statements[] = $sql;

                return 0;
            });

        $adapter = new MySQL($pdo);
        $adapter->setTimeout(1000);
        $adapter->clearTimeout();

        $this->assertSame([
            'SET SESSION MAX_EXECUTION_TIME = 1000',
            'SET SESSION MAX_EXECUTION_TIME = 0',
        ], $statements);
    }

    public function testClearingOneScopePreservesTheGlobalTimeout(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))->method('exec')->willReturn(0);

        $adapter = new MariaDB($pdo);
        $adapter->setTimeout(1000);
        $adapter->setTimeout(25, Event::DocumentFind);

        $this->assertSame(25, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentCreate));

        $adapter->clearTimeout(Event::DocumentFind);

        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(1000, $adapter->getTimeout(Event::DocumentCreate));

        $adapter->clearTimeout();
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
    }

    /**
     * @param class-string<SQL&Feature\Timeouts> $adapterClass
     * @param list<string> $expected
     */
    #[DataProvider('adapters')]
    public function testReadScopeIsAppliedOnlyAroundMatchingOperations(string $adapterClass, array $expected): void
    {
        $statements = [];
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('exec')
            ->willReturnCallback(function (string $sql) use (&$statements): int {
                $statements[] = $sql;

                return 0;
            });

        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))->method('execute')->willReturn(true);

        $adapter = new $adapterClass($pdo);
        $adapter->setTimeout(25, Event::DocumentFind);
        $execute = new ReflectionMethod($adapter, 'execute');

        $this->assertTrue($execute->invoke($adapter, $statement, Event::DocumentFind));
        $this->assertTrue($execute->invoke($adapter, $statement, Event::DocumentCreate));
        $this->assertSame($expected, $statements);
    }

    /**
     * @return iterable<string, array{class-string<SQL&Feature\Timeouts>, list<string>}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [MariaDB::class, [
            'SET max_statement_time = 0.025000',
            'SET max_statement_time = 0.000000',
        ]];
        yield 'MySQL' => [MySQL::class, [
            'SET SESSION MAX_EXECUTION_TIME = 25',
            'SET SESSION MAX_EXECUTION_TIME = 0',
        ]];
        yield 'PostgreSQL' => [Postgres::class, [
            "SET statement_timeout = '25ms'",
            'RESET statement_timeout',
        ]];
    }
}
