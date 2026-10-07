<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use ReflectionParameter;
use RuntimeException;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class FeatureContractTest extends TestCase
{
    private const string FEATURE_NAMESPACE = 'Utopia\\Database\\Adapter\\Feature\\';

    /**
     * Every adapter implements these, so they are abstract on Adapter rather than a Feature.
     */
    private const array MANDATORY = [
        'create',
        'exists',
        'collectionExists',
        'list',
        'delete',
        'createCollection',
        'deleteCollection',
        'analyzeCollection',
        'createAttribute',
        'createAttributes',
        'updateAttribute',
        'deleteAttribute',
        'renameAttribute',
        'getSchemaAttributes',
        'getSchemaIndexes',
        'createIndex',
        'deleteIndex',
        'renameIndex',
        'createDocument',
        'createDocuments',
        'getDocument',
        'updateDocument',
        'updateDocuments',
        'deleteDocument',
        'deleteDocuments',
        'increaseDocumentAttribute',
        'getSequences',
        'find',
        'count',
        'sum',
        'startTransaction',
        'commitTransaction',
        'rollbackTransaction',
        'getDriver',
    ];

    /**
     * Mandatory members that land with other wave-4 subtasks: update (W4-5), getColumnType (W4-2), limits (W4-3).
     * Abstract once declared.
     */
    private const array MANDATORY_LANDING = [
        'update',
        'getColumnType',
        'limits',
    ];

    /**
     * Members only SQL engines have; they live on Adapter\SQL, not on the contract every adapter implements.
     */
    private const array SQL_ONLY = [
        'quote',
        'execute',
    ];

    private const array REMOVED_FEATURES = [
        'Attributes',
        'Collections',
        'Databases',
        'Documents',
        'Indexes',
        'Transactions',
    ];

    private const array OPTIONAL_FEATURES = [
        'QueryBuilder',
        'RawQuery',
        'Relationships',
        'Spatial',
        'Timeouts',
        'Upserts',
    ];

    /**
     * Optional features other wave-4 subtasks add: Casting (W4-2), Connection (W4-4), Schemaless (W4-3).
     */
    private const array OPTIONAL_FEATURES_LANDING = [
        'Casting',
        'Connection',
        'Schemaless',
    ];

    /**
     * Features other wave-4 subtasks fold away: ColumnTypes, InternalCasting, SchemaAttributes, SchemaIndexes and
     * UTCCasting (W4-2), ConnectionId (W4-4).
     */
    private const array FEATURES_LEAVING = [
        'ColumnTypes',
        'ConnectionId',
        'InternalCasting',
        'SchemaAttributes',
        'SchemaIndexes',
        'UTCCasting',
    ];

    public function testEveryMandatoryMemberIsAbstractOnTheAdapter(): void
    {
        $adapter = new ReflectionClass(Adapter::class);
        $concrete = [];

        foreach (self::MANDATORY as $name) {
            if (! $adapter->hasMethod($name)) {
                $concrete[] = "{$name}() is not declared";

                continue;
            }

            $method = $adapter->getMethod($name);
            if (! $method->isAbstract() || ! $method->isPublic()) {
                $concrete[] = "{$name}() is not public and abstract";
            }
        }

        foreach (self::MANDATORY_LANDING as $name) {
            if ($adapter->hasMethod($name) && ! $adapter->getMethod($name)->isAbstract()) {
                $concrete[] = "{$name}() is declared but not abstract";
            }
        }

        $this->assertSame([], $concrete, 'The mandatory contract is declared once, abstract on Adapter');
    }

    public function testSqlOnlyMembersAreNotPartOfTheContract(): void
    {
        $adapter = new ReflectionClass(Adapter::class);

        foreach (self::SQL_ONLY as $name) {
            $this->assertFalse($adapter->hasMethod($name), "{$name}() belongs to Adapter\\SQL");
        }

        foreach (['getKeywords', 'getInternalIndexesKeys'] as $name) {
            if ($adapter->hasMethod($name)) {
                $this->assertFalse($adapter->getMethod($name)->isAbstract(), "{$name}() must not be required of an adapter without SQL");
            }
        }

        $this->assertSame([], (new Memory())->getKeywords());
        $this->assertSame([], (new Memory())->getInternalIndexesKeys());
    }

    public function testTheAdapterImplementsNoFeatureInterface(): void
    {
        $this->assertSame([], \class_implements(Adapter::class));

        foreach (self::REMOVED_FEATURES as $name) {
            $this->assertFalse(\interface_exists(self::FEATURE_NAMESPACE.$name), "Feature\\{$name} must be gone: its methods are abstract on Adapter");
        }
    }

    public function testFeatureHoldsOnlyOptionalInterfaces(): void
    {
        $present = self::features();
        $known = [...self::OPTIONAL_FEATURES, ...self::OPTIONAL_FEATURES_LANDING, ...self::FEATURES_LEAVING];

        $this->assertSame([], \array_values(\array_diff($present, $known)), 'Unexpected Feature interfaces');
        $this->assertSame([], \array_values(\array_diff(self::OPTIONAL_FEATURES, $present)), 'Missing optional Feature interfaces');
    }

    public function testNoOptionalFeatureRedeclaresAMandatoryMember(): void
    {
        $mandatory = \array_flip([...self::MANDATORY, ...self::MANDATORY_LANDING]);
        $overlaps = [];

        foreach (\array_diff(self::features(), self::FEATURES_LEAVING) as $name) {
            /** @var class-string $feature */
            $feature = self::FEATURE_NAMESPACE.$name;
            foreach ((new ReflectionClass($feature))->getMethods() as $method) {
                if (isset($mandatory[$method->getName()])) {
                    $overlaps[] = "Feature\\{$name}::{$method->getName()}()";
                }
            }
        }

        $this->assertSame([], $overlaps, 'A Feature holds optional behaviour only');
    }

    public function testPoolDeclaresEveryMandatoryMemberUnderTheSameParameterNames(): void
    {
        $pool = new ReflectionClass(Pool::class);
        $mismatched = [];

        foreach ((new ReflectionClass(Adapter::class))->getMethods(ReflectionMethod::IS_ABSTRACT) as $method) {
            if (! $method->isPublic()) {
                continue;
            }

            $name = $method->getName();
            $delegate = $pool->getMethod($name);
            if ($delegate->getDeclaringClass()->getName() !== Pool::class) {
                $mismatched[] = "{$name}() is not declared by Pool";

                continue;
            }

            $expected = self::parameterNames($method);
            $actual = self::parameterNames($delegate);
            if ($expected !== $actual) {
                $mismatched[] = "{$name}(".\implode(', ', $expected).') is Pool::'.$name.'('.\implode(', ', $actual).')';
            }
        }

        $this->assertSame([], $mismatched, 'Named arguments that work on an adapter must work on Pool');
    }

    public function testExistsAsksForTheDatabaseAndCollectionExistsForTheCollection(): void
    {
        foreach (['adapter' => new Memory(), 'pool' => $this->pool(new Memory())] as $name => $adapter) {
            $adapter->setNamespace('contract');
            $adapter->setDatabase('contract');

            $this->assertFalse($adapter->exists('contract'), $name);
            $this->assertFalse($adapter->collectionExists('contract', 'books'), $name);

            $adapter->create('contract');

            $this->assertTrue($adapter->exists('contract'), $name);
            $this->assertFalse($adapter->collectionExists('contract', 'books'), $name);
            $this->assertFalse($adapter->exists('elsewhere'), $name);

            $adapter->createCollection('books', [Attribute::string(key: 'title', size: 64)]);

            $this->assertTrue($adapter->collectionExists('contract', 'books'), $name);
            $this->assertFalse($adapter->collectionExists('elsewhere', 'books'), $name);
            $this->assertFalse($adapter->collectionExists('contract', 'authors'), $name);
        }
    }

    public function testDocumentWritesTakeTheCollectionDocument(): void
    {
        $adapter = new Memory();
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('contract');
        $adapter->setDatabase('contract');
        $adapter->create('contract');
        $adapter->createCollection('books', [Attribute::integer(key: 'reads', required: false)]);
        $books = Collection::create(id: 'books');

        $adapter->createDocument($books, new Document(['$id' => 'first', 'reads' => 1, '$permissions' => []]));
        $adapter->createDocument($books, new Document(['$id' => 'second', 'reads' => 1, '$permissions' => []]));

        [$first] = $adapter->getSequences($books, [new Document(['$id' => 'first'])]);
        $this->assertNotNull($first->getSequence());

        $this->assertTrue($adapter->increaseDocumentAttribute($books, 'first', 'reads', 4, '2026-01-01 00:00:00.000'));
        $this->assertSame(5, $adapter->getDocument($books, 'first')->getAttribute('reads'));

        $this->assertTrue($adapter->deleteDocument($books, 'first'));
        $this->assertTrue($adapter->getDocument($books, 'first')->isEmpty());

        $second = $adapter->getDocument($books, 'second');
        $this->assertSame(1, $adapter->deleteDocuments($books, [(string) $second->getSequence()], []));
        $this->assertTrue($adapter->getDocument($books, 'second')->isEmpty());
    }

    public function testAPoolOverAnEngineWithoutIntrospectionListsNoSchema(): void
    {
        $pool = $this->pool(new Memory());

        $this->assertSame([], $pool->getSchemaAttributes('books'));
        $this->assertSame([], $pool->getSchemaIndexes('books'));
    }

    public function testMemoryPoolHasFeatureDelegatesToInnerAdapter(): void
    {
        $pool = $this->pool(new Memory());

        $this->assertSame(false, $pool->hasFeature(Feature\Spatial::class));
        $this->assertSame(false, $pool->hasFeature(Feature\Upserts::class));
        $this->assertSame(true, $pool->hasFeature(Feature\Relationships::class));
    }

    /**
     * Pool holds timeouts as its own state and replays them onto each connection it borrows, so it answers for
     * the feature without a connection; the refusal of an engine without timeouts comes when one is applied.
     */
    public function testPoolAnswersForTimeoutsWithoutAConnection(): void
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willThrowException(new RuntimeException('no connection'));

        $pool = new Pool($connections);

        $this->assertTrue($pool->hasFeature(Feature\Timeouts::class));
    }

    /**
     * Pool carries every optional Feature method so it can forward whichever adapter the pool hands out, but it
     * implements none of the interfaces it forwards: a caller type-checking the facade would be told the pooled
     * engine supports something it does not. Support is answered by hasFeature(), and a call the inner adapter
     * cannot serve is refused at the facade.
     */
    public function testPoolRefusesAFeatureTheInnerAdapterLacks(): void
    {
        $forwarded = \array_diff(self::features(), ['Timeouts']);
        $implements = $this->interfaces(Pool::class);

        foreach ($forwarded as $name) {
            $this->assertArrayNotHasKey(self::FEATURE_NAMESPACE.$name, $implements, $name);
        }

        $pool = $this->pool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support upserts');
        $pool->upsertDocuments(new Document(['$id' => 'any']), '', []);
    }

    public function testRedisOffersUpsertsAndRelationships(): void
    {
        $redis = $this->adapters()['Redis'];

        $this->assertTrue($redis->hasFeature(Feature\Upserts::class));
        $this->assertTrue($redis->hasFeature(Feature\Relationships::class));
        $this->assertFalse($redis->hasFeature(Feature\Spatial::class));
        $this->assertFalse($redis->hasFeature(Feature\RawQuery::class));
    }

    public function testSQLiteOffersTheSqlFeaturesButNotSpatialOrTimeouts(): void
    {
        $sqlite = $this->adapters()['SQLite'];

        $this->assertTrue($sqlite->hasFeature(Feature\Upserts::class));
        $this->assertTrue($sqlite->hasFeature(Feature\Relationships::class));
        $this->assertTrue($sqlite->hasFeature(Feature\RawQuery::class));
        $this->assertTrue($sqlite->hasFeature(Feature\QueryBuilder::class));
        $this->assertFalse($sqlite->hasFeature(Feature\Spatial::class));
        $this->assertFalse($sqlite->hasFeature(Feature\Timeouts::class));
    }

    public function testMariaDBAndPostgresOfferSpatialAndTimeouts(): void
    {
        foreach (['MariaDB', 'MySQL', 'Postgres'] as $name) {
            $adapter = $this->adapters()[$name];

            $this->assertTrue($adapter->hasFeature(Feature\Spatial::class), $name);
            $this->assertTrue($adapter->hasFeature(Feature\Timeouts::class), $name);
        }
    }

    public function testEveryCapabilityIsDeclaredByAnAdapter(): void
    {
        $declared = [];
        foreach ($this->adapters() as $adapter) {
            foreach (Capability::cases() as $capability) {
                if ($adapter->supports($capability)) {
                    $declared[$capability->name] = true;
                }
            }
        }

        $undeclared = \array_values(\array_filter(
            \array_map(static fn (Capability $capability): string => $capability->name, Capability::cases()),
            static fn (string $name): bool => ! isset($declared[$name]),
        ));

        $this->assertSame([], $undeclared, 'Capabilities no adapter declares');
    }

    public function testSupportsAgreesWithHasFeatureWhereBothExist(): void
    {
        $disagreements = [];
        foreach ($this->adapters() as $name => $adapter) {
            $hasFeature = $adapter->hasFeature(...);
            foreach (Capability::cases() as $capability) {
                $feature = self::FEATURE_NAMESPACE.$capability->name;
                if (\interface_exists($feature) && $adapter->supports($capability) !== $hasFeature($feature)) {
                    $disagreements[] = "{$name}: Capability::{$capability->name}";
                }
            }
        }

        $this->assertSame([], $disagreements, 'supports() and hasFeature() disagree');
    }

    /**
     * @return list<string>
     */
    private static function features(): array
    {
        $directory = \dirname((string) (new ReflectionClass(Adapter::class))->getFileName()).'/Adapter/Feature';
        $features = \array_map(static fn (string $file): string => \basename($file, '.php'), \glob($directory.'/*.php') ?: []);
        \sort($features);

        return $features;
    }

    /**
     * @return list<string>
     */
    private static function parameterNames(ReflectionMethod $method): array
    {
        return \array_map(static fn (ReflectionParameter $parameter): string => $parameter->getName(), $method->getParameters());
    }

    /**
     * @return array<string, Adapter>
     */
    private function adapters(): array
    {
        $pdo = self::createStub(PDO::class);

        return [
            'MariaDB' => new MariaDB($pdo),
            'MySQL' => new MySQL($pdo),
            'Postgres' => new Postgres($pdo),
            'SQLite' => new SQLite(new PDO('sqlite::memory:')),
            'Memory' => new Memory(),
            'MongoDB' => new class () extends Mongo {
                public function __construct()
                {
                }
            },
            'Redis' => new class () extends Redis {
                public function __construct()
                {
                }
            },
        ];
    }

    /**
     * @param class-string $class
     * @return array<string, string>
     */
    private function interfaces(string $class): array
    {
        $implements = \class_implements($class);

        return $implements === false ? [] : $implements;
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
