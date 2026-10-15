<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
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
 * SQLite answers a double aggregate that overflowed with infinity, which no caller can tell from a value, so it is
 * refused with the Limit error MariaDB, MySQL and PostgreSQL raise for it.
 */
final class AggregateOverflowTest extends TestCase
{
    private const string COLLECTION = 'readings';

    /**
     * @param  list<float>  $readings
     */
    private function database(bool $stringify, array $readings): Database
    {
        $pdo = new PDO('sqlite::memory:', null, null, [PDO::ATTR_STRINGIFY_FETCHES => $stringify]);
        $database = new Database(new SQLite($pdo), new Cache(new NoCache()));
        $database
            ->setDatabase('aggregate_overflow')
            ->setNamespace('aggregate_overflow_'.\uniqid())
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
     * @return iterable<string, array{bool, Query}>
     */
    public static function overflows(): iterable
    {
        foreach (['native' => false, 'stringified' => true] as $fetch => $stringify) {
            yield "sum {$fetch}" => [$stringify, Query::sum('reading', 'total')];
            yield "avg {$fetch}" => [$stringify, Query::avg('reading', 'total')];
            yield "default alias {$fetch}" => [$stringify, Query::sum('reading')];
        }
    }

    #[DataProvider('overflows')]
    public function testAnAggregateThatOverflowsIsRefused(bool $stringify, Query $aggregate): void
    {
        $database = $this->database($stringify, [1e308, 1e308]);

        $this->expectException(LimitException::class);
        $database->aggregate(self::COLLECTION, [$aggregate]);
    }

    #[DataProvider('overflows')]
    public function testAnAggregateThatOverflowsBelowTheRangeIsRefused(bool $stringify, Query $aggregate): void
    {
        $database = $this->database($stringify, [-1e308, -1e308]);

        $this->expectException(LimitException::class);
        $database->aggregate(self::COLLECTION, [$aggregate]);
    }

    public function testAnAggregateThatOverflowsInOneGroupIsRefused(): void
    {
        $database = $this->database(false, [1e308, 1e308, 1.0]);

        $this->expectException(LimitException::class);
        $database->aggregate(self::COLLECTION, [Query::sum('reading', 'total'), Query::groupBy(['reading'])]);
    }

    public function testTheLargestFiniteSumIsStillAnswered(): void
    {
        $database = $this->database(false, [1e308]);

        $rows = $database->aggregate(self::COLLECTION, [Query::sum('reading', 'total'), Query::avg('reading', 'mean')]);

        $this->assertSame(1e308, $rows[0]['total']);
        $this->assertSame(1e308, $rows[0]['mean']);
    }
}
