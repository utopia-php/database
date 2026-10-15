<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * SQLite and the adapters that sum in PHP answer a double sum that overflowed with infinity, which SQLite sends as
 * `INF` when it stringifies. sum() refuses it with the Limit error MariaDB, MySQL and PostgreSQL raise for it.
 */
final class SumOverflowTest extends TestCase
{
    private const string COLLECTION = 'readings';

    /**
     * @return iterable<string, array{Closure(): Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:', null, null, [PDO::ATTR_STRINGIFY_FETCHES => false]))];
        yield 'sqlite stringified' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:', null, null, [PDO::ATTR_STRINGIFY_FETCHES => true]))];
        yield 'memory' => [static fn (): Adapter => new Memory()];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<float>  $readings
     */
    private function database(Closure $adapter, array $readings): Database
    {
        $database = new Database($adapter(), new Cache(new NoCache()));
        $database
            ->setDatabase('sum_overflow')
            ->setNamespace('sum_overflow_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::double(key: 'reading', required: true)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach ($readings as $reading) {
            $database->createDocument(self::COLLECTION, new Document([
                'reading' => $reading,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        return $database;
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testASumThatOverflowsIsRefused(Closure $adapter): void
    {
        $database = $this->database($adapter, [1e308, 1e308]);

        $this->expectException(LimitException::class);
        $database->sum(self::COLLECTION, 'reading');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testASumThatOverflowsUnderFiltersIsRefused(Closure $adapter): void
    {
        $database = $this->database($adapter, [1e308, 1e308, -1.0]);

        $this->expectException(LimitException::class);
        $database->sum(self::COLLECTION, 'reading', [Query::greaterThan('reading', 0)], 10);
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, list<Query>}>
     */
    public static function rows(): iterable
    {
        foreach (['sqlite' => false, 'sqlite stringified' => true] as $label => $stringify) {
            $adapter = static fn (): Adapter => new SQLite(new PDO('sqlite::memory:', null, null, [PDO::ATTR_STRINGIFY_FETCHES => $stringify]));
            yield "groupBy on {$label}" => [$adapter, [Query::count('*', 'n'), Query::groupBy(['reading'])]];
            yield "distinct on {$label}" => [$adapter, [Query::distinct(), Query::select(['reading'])]];
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<Query>  $queries
     */
    #[DataProvider('rows')]
    public function testASumOfGroupedOrDistinctRowsThatOverflowsIsRefused(Closure $adapter, array $queries): void
    {
        $database = $this->database($adapter, [1e308, 1e308, 9e307]);

        $this->expectException(LimitException::class);
        $database->sum(self::COLLECTION, 'reading', $queries);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<Query>  $queries
     */
    #[DataProvider('rows')]
    public function testTheLargestFiniteSumOfGroupedOrDistinctRowsIsStillAnswered(Closure $adapter, array $queries): void
    {
        $database = $this->database($adapter, [1e308, 1e308]);

        $this->assertSame(1e308, $database->sum(self::COLLECTION, 'reading', $queries));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheLargestFiniteSumIsStillAnswered(Closure $adapter): void
    {
        $database = $this->database($adapter, [1e308]);

        $this->assertSame(1e308, $database->sum(self::COLLECTION, 'reading'));
    }
}
