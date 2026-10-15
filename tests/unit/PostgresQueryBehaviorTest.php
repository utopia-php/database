<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;

final class PostgresQueryBehaviorTest extends TestCase
{
    public function testPermissionHookFiltersTheRowJsonbColumn(): void
    {
        [$sql, $bindings] = $this->capturePermissionFilteredFind(['any', 'user:1']);

        $this->assertStringContainsString(
            'WHERE ("'.Storage::PERMISSIONS.'" @> ?::jsonb OR "'.Storage::PERMISSIONS.'" @> ?::jsonb)',
            $sql
        );
        $this->assertSame(['["read(\\"any\\")"]', '["read(\\"user:1\\")"]', 25], $bindings);
    }

    public function testPermissionHookRejectsEmptyRoles(): void
    {
        [$sql, $bindings] = $this->capturePermissionFilteredFind([]);

        $this->assertStringContainsString('WHERE 1 = 0', $sql);
        $this->assertSame([25], $bindings);
    }

    public function testCreateCollectionStoresJsonbPermissionsWithGinIndex(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->expects($this->exactly(2))
            ->method('execute')
            ->willReturn(true);

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

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $this->assertTrue($adapter->createCollection('movies'));
        $this->assertStringContainsString('"'.Storage::PERMISSIONS.'" JSONB', $queries[0]);
        $this->assertStringContainsString('USING GIN ("'.Storage::PERMISSIONS.'")', $queries[0]);
    }

    public function testVectorDistanceIsProjectedHydratedAndOrderedBeforeTieBreaker(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $bindings = [];
        $statement->expects($this->exactly(3))
            ->method('bindValue')
            ->willReturnCallback(function (int $position, mixed $value, int $type) use (&$bindings): bool {
                $bindings[] = [$position, $value, $type];

                return true;
            });
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([[
            Storage::UID => 'movie',
            Storage::SEQUENCE => 1,
            Storage::PERMISSIONS => '[]',
            Storage::DISTANCE => '0.25',
        ]]);
        $statement->expects($this->once())->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
                $sql = $query;

                return $statement;
            });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $documents = $adapter->find(
            new Document([Document::ID => 'movies']),
            [Query::vectorCosine('embedding', [1.0, 0.0, 0.0])],
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc]
        );

        $this->assertCount(1, $documents);
        $this->assertSame(0.25, $documents[0]->getAttribute(Document::DISTANCE));
        $this->assertMatchesRegularExpression('/SELECT \*, .*::text AS "'.Storage::DISTANCE.'"/', $sql);
        $this->assertStringContainsString('WHERE "table_main"."embedding" IS NOT NULL', $sql);
        $this->assertMatchesRegularExpression('/ORDER BY .*<=>.*\), "'.Storage::SEQUENCE.'" ASC/', $sql);
        $this->assertSame([
            [1, '[1,0,0]', \PDO::PARAM_STR],
            [2, '[1,0,0]', \PDO::PARAM_STR],
            [3, 25, \PDO::PARAM_INT],
        ], $bindings);
    }

    public function testVectorCursorComparesDistanceBeforeSequenceTieBreaker(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $bindings = [];
        $statement->method('bindValue')
            ->willReturnCallback(function (int $position, mixed $value, int $type) use (&$bindings): bool {
                $bindings[] = [$position, $value, $type];

                return true;
            });
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([]);
        $statement->expects($this->once())->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
                $sql = $query;

                return $statement;
            });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document([Document::ID => 'movies']),
            [Query::vectorCosine('embedding', [1.0, 0.0, 0.0])],
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
            cursor: [Document::DISTANCE => 0.25, Document::SEQUENCE => 17],
        );

        $this->assertStringContainsString(
            '"table_main"."embedding" IS NOT NULL AND ((("table_main"."embedding" <=> ?::vector)) > ? OR ((("table_main"."embedding" <=> ?::vector)) = ? AND "table_main"."'.Storage::SEQUENCE.'" > ?))',
            $sql,
        );
        $this->assertMatchesRegularExpression('/ORDER BY .*<=>.*\), "'.Storage::SEQUENCE.'" ASC/', $sql);
        $this->assertSame([
            [1, '[1,0,0]', \PDO::PARAM_STR],
            [2, '[1,0,0]', \PDO::PARAM_STR],
            [3, '0.25', \PDO::PARAM_STR],
            [4, '[1,0,0]', \PDO::PARAM_STR],
            [5, '0.25', \PDO::PARAM_STR],
            [6, 17, \PDO::PARAM_INT],
            [7, '[1,0,0]', \PDO::PARAM_STR],
            [8, 25, \PDO::PARAM_INT],
        ], $bindings);
    }

    public function testVectorCursorBeforeBindsRoundTripDistance(): void
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $bindings = [];
        $statement->method('bindValue')
            ->willReturnCallback(function (int $position, mixed $value, int $type) use (&$bindings): bool {
                $bindings[] = [$position, $value, $type];

                return true;
            });
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([]);
        $statement->expects($this->once())->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
                $sql = $query;

                return $statement;
            });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $distance = 0.015216441182063223;
        $adapter->find(
            new Document([Document::ID => 'movies']),
            [Query::vectorCosine('embedding', [1.0, 0.0, 0.0])],
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
            cursor: [Document::DISTANCE => $distance, Document::SEQUENCE => 17],
            cursorDirection: CursorDirection::Before,
        );

        $this->assertStringContainsString(
            '"table_main"."embedding" IS NOT NULL AND ((("table_main"."embedding" <=> ?::vector)) < ? OR ((("table_main"."embedding" <=> ?::vector)) = ? AND "table_main"."'.Storage::SEQUENCE.'" < ?))',
            $sql,
        );
        $this->assertMatchesRegularExpression('/ORDER BY .*<=>.*\) DESC, "'.Storage::SEQUENCE.'" DESC/', $sql);
        $this->assertSame(\json_encode($distance, JSON_THROW_ON_ERROR), $bindings[2][1]);
        $this->assertSame(\json_encode($distance, JSON_THROW_ON_ERROR), $bindings[4][1]);
    }

    public function testInvalidPowerArgumentIsTranslatedToLimitException(): void
    {
        $pdoException = new class ('zero raised to a negative power is undefined', '2201F') extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $pdoException->errorInfo = ['2201F', 7, 'zero raised to a negative power is undefined'];

        $statement = self::createStub(\PDOStatement::class);
        $statement->method('execute')->willThrowException($pdoException);
        $pdo = self::createStub(\PDO::class);
        $pdo->method('prepare')->willReturn($statement);

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        try {
            $adapter->updateDocuments(
                new Document([Document::ID => 'scores', 'attributes' => []]),
                new Document(['value' => Operator::power(-1)]),
                [new Document([Document::ID => 'first', Document::SEQUENCE => '1'])],
            );
            $this->fail('The update succeeded');
        } catch (LimitException $exception) {
            $this->assertSame($pdoException, $exception->getPrevious());
        }
    }

    /**
     * @param  list<string>  $roles
     * @return array{string, list<mixed>}
     */
    private function capturePermissionFilteredFind(array $roles): array
    {
        $bindings = [];
        $statement = self::createStub(\PDOStatement::class);
        $statement->method('bindValue')->willReturnCallback(function (int $position, mixed $value) use (&$bindings): bool {
            $bindings[] = $value;

            return true;
        });
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);

        $sql = '';
        $pdo = self::createStub(\PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
            $sql = $query;

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }
        $adapter->setAuthorization($authorization);

        $adapter->find(new Document([Document::ID => 'movies', 'documentSecurity' => true]));

        return [$sql, $bindings];
    }
}
