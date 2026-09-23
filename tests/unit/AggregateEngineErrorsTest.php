<?php

namespace Tests\Unit;

use Exception;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use stdClass;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class AggregateEngineErrorsTest extends TestCase
{
    private const string COLLECTION = 'readings';

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_engine_errors')
            ->setNamespace('aggregate_engine_errors_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'sensor', size: 20, required: true),
                Attribute::integer(key: 'value', required: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach ([['north', 6], ['north', 3], ['south', 5]] as [$sensor, $value]) {
            $database->createDocument(self::COLLECTION, new Document([
                'sensor' => $sensor,
                'value' => $value,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        return $database;
    }

    /**
     * @return array<string, array{0: list<Query>, 1: string}>
     */
    public static function missingAggregateProvider(): array
    {
        return [
            'stddev' => [[Query::stddev('value', 'result')], 'stddev'],
            'stddevPop' => [[Query::stddevPop('value', 'result')], 'stddevPop'],
            'stddevSamp' => [[Query::stddevSamp('value', 'result')], 'stddevSamp'],
            'variance' => [[Query::variance('value', 'result')], 'variance'],
            'varPop' => [[Query::varPop('value', 'result')], 'varPop'],
            'varSamp' => [[Query::varSamp('value', 'result')], 'varSamp'],
            'bitAnd' => [[Query::bitAnd('value', 'result')], 'bitAnd'],
            'bitOr' => [[Query::bitOr('value', 'result')], 'bitOr'],
            'bitXor' => [[Query::bitXor('value', 'result')], 'bitXor'],
            'stddev without an alias' => [[Query::stddev('value')], 'stddev'],
            'bitOr per group' => [[Query::bitOr('value', 'result'), Query::groupBy(['sensor'])], 'bitOr'],
            'variance next to a count' => [[Query::count('*', 'rows'), Query::variance('value', 'result')], 'variance'],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('missingAggregateProvider')]
    public function testSQLiteRejectsAnAggregateItHasNoFunctionFor(array $queries, string $method): void
    {
        $this->assertFailsWith(
            QueryException::class,
            'Aggregate '.$method.' is not supported by this adapter',
            fn () => $this->database()->find(self::COLLECTION, $queries),
        );
    }

    public function testSQLiteStillAnswersTheAggregatesItHasFunctionsFor(): void
    {
        $results = $this->database()->find(self::COLLECTION, [
            Query::count('*', 'rows'),
            Query::countDistinct('sensor', 'sensors'),
            Query::sum('value', 'total'),
            Query::avg('value', 'mean'),
            Query::min('value', 'least'),
            Query::max('value', 'most'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(3, $results[0]->getAttribute('rows'));
        $this->assertSame(2, $results[0]->getAttribute('sensors'));
        $this->assertSame(14, $results[0]->getAttribute('total'));
        $this->assertEqualsWithDelta(14 / 3, $results[0]->getAttribute('mean'), 1e-9);
        $this->assertSame(3, $results[0]->getAttribute('least'));
        $this->assertSame(6, $results[0]->getAttribute('most'));
    }

    /**
     * @return array<string, array{0: SQL, 1: bool}>
     */
    public static function adapterProvider(): array
    {
        return [
            'MariaDB' => [new MariaDB(new stdClass()), true],
            'MySQL' => [new MySQL(new stdClass()), true],
            'Postgres' => [new Postgres(new stdClass()), true],
            'SQLite' => [new SQLite(new PDO('sqlite::memory:')), false],
            'SQLite emulating MySQL' => [(new SQLite(new PDO('sqlite::memory:')))->setEmulateMySQL(true), false],
        ];
    }

    #[DataProvider('adapterProvider')]
    public function testOnlySQLiteLacksTheStatisticalAndBitwiseAggregates(SQL $adapter, bool $supported): void
    {
        $this->assertTrue($adapter->supports(Capability::Aggregations));
        $this->assertSame($supported, $adapter->supports(Capability::StatisticalAggregates));
        $this->assertSame($supported, $adapter->supports(Capability::BitwiseAggregates));
    }

    /**
     * @return array<string, array{0: SQL, 1: PDOException, 2: class-string<Throwable>, 3: string}>
     */
    public static function mappedEngineErrorProvider(): array
    {
        $unknownColumn = self::engineError('42S22', 1054, "SQLSTATE[42S22]: Column not found: 1054 Unknown column 'no_such_attribute' in 'WHERE'");
        $tooManyTables = self::engineError('HY000', 1116, 'SQLSTATE[HY000]: General error: 1116 Too many tables; MariaDB can only use 61 tables in a join');
        $noFulltextIndex = self::engineError('HY000', 1191, "SQLSTATE[HY000]: General error: 1191 Can't find FULLTEXT index matching the column list");
        $unknownDroppedColumn = self::engineError('42000', 1091, "SQLSTATE[42000]: Syntax error or access violation: 1091 Can't DROP COLUMN `score`; check that it exists");

        return [
            'MariaDB unknown column' => [new MariaDB(new stdClass()), $unknownColumn, NotFoundException::class, 'Attribute not found'],
            'MySQL unknown column' => [new MySQL(new stdClass()), $unknownColumn, NotFoundException::class, 'Attribute not found'],
            'MariaDB column that cannot be dropped' => [new MariaDB(new stdClass()), $unknownDroppedColumn, NotFoundException::class, 'Attribute not found'],
            'Postgres unknown column' => [
                new Postgres(new stdClass()),
                self::engineError('42703', 7, 'SQLSTATE[42703]: Undefined column: 7 ERROR:  column table_main.no_such_attribute does not exist'),
                NotFoundException::class,
                'Attribute not found',
            ],
            'MariaDB too many tables' => [new MariaDB(new stdClass()), $tooManyTables, QueryException::class, 'Too many tables in a join'],
            'MySQL too many tables' => [new MySQL(new stdClass()), $tooManyTables, QueryException::class, 'Too many tables in a join'],
            'MariaDB no fulltext index' => [new MariaDB(new stdClass()), $noFulltextIndex, QueryException::class, 'Searching requires a fulltext index on the searched attributes'],
            'MySQL no fulltext index' => [new MySQL(new stdClass()), $noFulltextIndex, QueryException::class, 'Searching requires a fulltext index on the searched attributes'],
        ];
    }

    /**
     * @param  class-string<Throwable>  $expected
     */
    #[DataProvider('mappedEngineErrorProvider')]
    public function testEngineErrorsAreMappedToLibraryExceptions(SQL $adapter, PDOException $error, string $expected, string $message): void
    {
        $processed = $this->process($adapter, $error);

        $this->assertInstanceOf($expected, $processed);
        $this->assertSame($message, $processed->getMessage());
        $this->assertSame($error, $processed->getPrevious());
    }

    private static function engineError(string $state, int $code, string $message): PDOException
    {
        $error = new PDOException($message);
        (new ReflectionProperty(Exception::class, 'code'))->setValue($error, $state);
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }

    private function process(SQL $adapter, PDOException $error): Throwable
    {
        $processed = (new ReflectionMethod($adapter, 'processException'))->invoke($adapter, $error);
        $this->assertInstanceOf(Throwable::class, $processed);

        return $processed;
    }

    /**
     * @param  class-string<Throwable>  $expected
     */
    private function assertFailsWith(string $expected, string $message, callable $call): void
    {
        $error = null;
        try {
            $call();
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf($expected, $error, $error === null ? 'the call succeeded' : $error::class.': '.$error->getMessage());
        $this->assertSame($message, $error->getMessage());
    }
}
