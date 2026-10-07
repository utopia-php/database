<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class OperatorLimitExactnessTest extends TestCase
{
    /** @var list<mixed> */
    private array $bound = [];

    /**
     * @return iterable<string, array{string, Operator, int|float}>
     */
    public static function limits(): iterable
    {
        yield 'increment past a whole float maximum' => ['high', Operator::increment(20, 9.0e18), 8999999999999999990];
        yield 'increment up to a whole float maximum' => ['high', Operator::increment(10, 9.0e18), 9000000000000000000];
        yield 'decrement past a whole float minimum' => ['low', Operator::decrement(20, -9.0e18), -8999999999999999990];
        yield 'decrement down to a whole float minimum' => ['low', Operator::decrement(10, -9.0e18), -9000000000000000000];
        yield 'multiply past a whole float maximum' => ['high', Operator::multiply(2, 9.0e18), 8999999999999999990];
        yield 'fractional maximum of a float' => ['ratio', Operator::increment(1, 2.25), 1.5];
        yield 'fractional maximum of a float not reached' => ['ratio', Operator::increment(0.5, 2.25), 2.0];
    }

    #[DataProvider('limits')]
    public function testAWholeNumberFloatLimitIsComparedExactly(string $attribute, Operator $operator, int|float $expected): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database->setDatabase('limits')->setNamespace('limits')->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'counters',
            attributes: [
                Attribute::bigInteger('high', default: 8999999999999999990),
                Attribute::bigInteger('low', default: -8999999999999999990),
                Attribute::double('ratio', default: 1.5),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument('counters', new Document(['$id' => 'existing']));

        $database->updateDocument('counters', 'existing', new Document([$attribute => clone $operator]));
        $database->upsertDocument('counters', new Document(['$id' => 'created', $attribute => clone $operator]));

        $this->assertSame($expected, $database->getDocument('counters', 'existing')->getAttribute($attribute), 'existing document');
        $this->assertSame($expected, $database->getDocument('counters', 'created')->getAttribute($attribute), 'new document');
    }

    /**
     * @return iterable<string, array{class-string<SQL>, Operator}>
     */
    public static function statements(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class, 'Postgres' => Postgres::class] as $engine => $class) {
            yield $engine . ' increment' => [$class, Operator::increment(1, 9.0e18)];
            yield $engine . ' multiply' => [$class, Operator::multiply(2, 9.0e18)];
            yield $engine . ' power' => [$class, Operator::power(2, 9.0e18)];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('statements')]
    public function testAWholeNumberFloatLimitIsBoundAsAnInteger(string $class, Operator $operator): void
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $adapter->updateDocuments(
            new Document(['$id' => 'counters', 'attributes' => []]),
            new Document(['high' => $operator]),
            [new Document(['$id' => 'first', '$sequence' => '1'])],
        );

        $this->assertContains(9000000000000000000, $this->bound);
        foreach ($this->bound as $value) {
            $this->assertIsNotFloat($value);
            $this->assertNotSame('9000000000000000000.00000000000000', $value);
        }
    }
}
