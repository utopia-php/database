<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
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
