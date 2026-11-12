<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Mirror;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * Whether an adapter stores geometries is its Feature\Spatial answer, and a pool, a read/write pool, a database
 * profile and a mirror each give the answer of the adapter they stand for, so a consumer asks
 * hasFeature(Feature\Spatial::class) of whatever adapter it holds.
 */
final class SpatialFeatureTest extends TestCase
{
    /**
     * @return iterable<string, array{Adapter, bool}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [new MariaDB(new \stdClass()), true];
        yield 'MySQL' => [new MySQL(new \stdClass()), true];
        yield 'Postgres' => [new Postgres(new \stdClass()), true];
        yield 'SQLite' => [new SQLite(new PDO('sqlite::memory:')), false];
        yield 'MongoDB' => [self::mongo(), false];
        yield 'Memory' => [new Memory(), false];
        yield 'Redis' => [self::redis(), false];
        yield 'spatial adapter without the spatial axis order capability' => [
            new class (new \stdClass()) extends MySQL {
                /**
                 * @return array<Capability>
                 */
                #[\Override]
                public function capabilities(): array
                {
                    return \array_values(\array_filter(
                        parent::capabilities(),
                        static fn (Capability $capability): bool => $capability !== Capability::SpatialAxisOrder,
                    ));
                }
            },
            true,
        ];
        yield 'adapter with the spatial axis order capability but no spatial types' => [
            new class () extends Memory {
                /**
                 * @return array<Capability>
                 */
                #[\Override]
                public function capabilities(): array
                {
                    return [...parent::capabilities(), Capability::SpatialAxisOrder];
                }
            },
            false,
        ];
    }

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function everyAdapter(): iterable
    {
        foreach (self::adapters() as $name => [$adapter]) {
            yield $name => [$adapter];
        }
    }

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function nonSpatialAdapters(): iterable
    {
        yield 'SQLite' => [new SQLite(new PDO('sqlite::memory:'))];
        yield 'Memory' => [new Memory()];
    }

    #[DataProvider('adapters')]
    public function testTheAdapterSaysWhetherItStoresGeometries(Adapter $adapter, bool $spatial): void
    {
        $this->assertSame($spatial, $adapter->hasFeature(Feature\Spatial::class));
    }

    #[DataProvider('adapters')]
    public function testAPoolAnswersForTheAdapterItHolds(Adapter $adapter, bool $spatial): void
    {
        $pool = self::pool($adapter);

        $this->assertSame($spatial, $pool->hasFeature(Feature\Spatial::class));
        $this->assertSame($spatial, $pool->hasFeature(Feature\Spatial::class), 'A remembered answer must match the first');
        $this->assertSame($spatial, self::pool($adapter)->hasFeature(Feature\Spatial::class), 'Another handle over a fresh pool must answer alike');
    }

    #[DataProvider('adapters')]
    public function testAReadWritePoolAnswersForTheAdapterItHolds(Adapter $adapter, bool $spatial): void
    {
        $pool = self::readWritePool($adapter)->setSticky(false);

        $this->assertSame($spatial, $pool->hasFeature(Feature\Spatial::class));
        $this->assertSame($spatial, $pool->hasFeature(Feature\Spatial::class), 'A remembered answer must match the first');
    }

    #[DataProvider('adapters')]
    public function testADatabaseOverAPoolProfilesTheAdapterItHolds(Adapter $adapter, bool $spatial): void
    {
        $database = self::database(self::pool($adapter));

        $this->assertSame($spatial, $database->getAdapter()->hasFeature(Feature\Spatial::class));
        $this->assertSame($spatial, $database->profile()->hasFeature(Feature\Spatial::class));
    }

    #[DataProvider('adapters')]
    public function testAMirrorAnswersForItsSourceWhateverItsDestinationStores(Adapter $adapter, bool $spatial): void
    {
        $destination = $spatial ? new Memory() : new MariaDB(new \stdClass());
        $this->assertSame(! $spatial, $destination->hasFeature(Feature\Spatial::class));

        $mirrors = [
            'direct source' => new Mirror(self::database($adapter), self::database($destination)),
            'pooled source' => new Mirror(self::database(self::pool($adapter)), self::database(self::pool($destination))),
        ];

        foreach ($mirrors as $name => $mirror) {
            $this->assertSame($spatial, $mirror->getAdapter()->hasFeature(Feature\Spatial::class), $name);
            $this->assertSame($spatial, $mirror->profile()->hasFeature(Feature\Spatial::class), $name);
        }
    }

    /**
     * Every Feature and Capability a consumer checks on a pooled adapter, such as Relationships, Schemaless,
     * DefinedAttributes, Operators, Joins or IndexSpatialNull, is the answer of the adapter the pool holds.
     * Timeouts is left out: a pool holds timeouts itself whichever adapter it holds.
     */
    #[DataProvider('everyAdapter')]
    public function testEveryFeatureAndCapabilityThroughAPoolIsTheAdapters(Adapter $adapter): void
    {
        $proxies = [
            'pool' => self::pool($adapter),
            'read/write pool' => self::readWritePool($adapter)->setSticky(false),
        ];

        foreach ($proxies as $name => $proxy) {
            $profile = self::database($proxy)->profile();

            foreach (self::features() as $feature) {
                $this->assertSame($adapter->hasFeature($feature), $proxy->hasFeature($feature), "{$name}: {$feature}");
                $this->assertSame($adapter->hasFeature($feature), $profile->hasFeature($feature), "{$name} profile: {$feature}");
            }

            foreach (Capability::cases() as $capability) {
                $this->assertSame($adapter->supports($capability), $proxy->supports($capability), "{$name}: {$capability->name}");
                $this->assertSame($adapter->supports($capability), $profile->supports($capability), "{$name} profile: {$capability->name}");
            }
        }
    }

    #[DataProvider('nonSpatialAdapters')]
    public function testAPooledAdapterWithoutSpatialTypesRefusesAPointAttribute(Adapter $adapter): void
    {
        $database = self::database(self::pool($adapter));
        $database->setDatabase('spatial_feature')->setNamespace('spatial_feature_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: 'places'));

        $this->assertFalse($database->getAdapter()->hasFeature(Feature\Spatial::class));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Spatial attributes are not supported');
        $database->createAttribute('places', Attribute::point(key: 'location', required: true));
    }

    public function testAMirrorOverASourceWithoutSpatialTypesRefusesAPointAttribute(): void
    {
        $source = self::database(new Memory());
        $mirror = new Mirror($source, self::database(new MariaDB(new \stdClass())));
        $mirror->setDatabase('spatial_feature')->setNamespace('spatial_feature_'.\uniqid());
        $source->create();
        $source->createCollection(Collection::create(id: 'places'));

        $this->assertFalse($mirror->getAdapter()->hasFeature(Feature\Spatial::class));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Spatial attributes are not supported');
        $mirror->createAttribute('places', Attribute::point(key: 'location', required: true));
    }

    /**
     * @return list<class-string>
     */
    private static function features(): array
    {
        $directory = \dirname((string) (new ReflectionClass(Adapter::class))->getFileName()).'/Adapter/Feature';
        $features = [];

        foreach (\glob($directory.'/*.php') ?: [] as $file) {
            /** @var class-string $feature */
            $feature = 'Utopia\\Database\\Adapter\\Feature\\'.\basename($file, '.php');
            if ($feature !== Feature\Timeouts::class) {
                $features[] = $feature;
            }
        }

        return $features;
    }

    private static function mongo(): Mongo
    {
        return new class () extends Mongo {
            public function __construct()
            {
            }
        };
    }

    private static function redis(): Redis
    {
        return new class () extends Redis {
            public function __construct()
            {
            }
        };
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private static function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter> $connections */
        $connections = new UtopiaPool(new Stack(), 'spatial_feature', 1, static fn (): Adapter => $adapter, 1.0);

        return $connections;
    }

    private static function pool(Adapter $adapter): Pool
    {
        $pool = new Pool(self::connections($adapter));
        $pool->setAuthorization(new Authorization());

        return $pool;
    }

    private static function readWritePool(Adapter $adapter): ReadWritePool
    {
        $pool = new ReadWritePool(self::connections($adapter), self::connections($adapter));
        $pool->setAuthorization(new Authorization());

        return $pool;
    }

    private static function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization(new Authorization());

        return $database;
    }
}
