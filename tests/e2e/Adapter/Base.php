<?php

namespace Tests\E2E\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use Tests\E2E\Adapter\Scopes\AggregationTests;
use Tests\E2E\Adapter\Scopes\AttributeTests;
use Tests\E2E\Adapter\Scopes\CollectionTests;
use Tests\E2E\Adapter\Scopes\CustomDocumentTypeTests;
use Tests\E2E\Adapter\Scopes\DatabaseTests;
use Tests\E2E\Adapter\Scopes\DocumentTests;
use Tests\E2E\Adapter\Scopes\GeneralTests;
use Tests\E2E\Adapter\Scopes\IndexTests;
use Tests\E2E\Adapter\Scopes\JoinComboTests;
use Tests\E2E\Adapter\Scopes\JoinTests;
use Tests\E2E\Adapter\Scopes\MetadataCacheTests;
use Tests\E2E\Adapter\Scopes\ObjectAttributeTests;
use Tests\E2E\Adapter\Scopes\OperatorTests;
use Tests\E2E\Adapter\Scopes\PermissionTests;
use Tests\E2E\Adapter\Scopes\RelationshipTests;
use Tests\E2E\Adapter\Scopes\SchemalessTests;
use Tests\E2E\Adapter\Scopes\SchemaReconciliationTests;
use Tests\E2E\Adapter\Scopes\SpatialTests;
use Tests\E2E\Adapter\Scopes\VectorTests;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Validator\Authorization;

\ini_set('memory_limit', '2048M');

abstract class Base extends TestCase
{
    use AggregationTests;
    use AttributeTests;
    use CollectionTests;
    use CustomDocumentTypeTests;
    use DatabaseTests;
    use DocumentTests;
    use GeneralTests;
    use IndexTests;
    use JoinComboTests;
    use JoinTests;
    use MetadataCacheTests;
    use ObjectAttributeTests;
    use OperatorTests;
    use PermissionTests;
    use RelationshipTests;
    use SchemaReconciliationTests;
    use SchemalessTests;
    use SpatialTests;
    use VectorTests;

    /**
     * @var array<int, mixed>
     */
    protected const array PDO_ATTRIBUTES = [
        PDO::ATTR_TIMEOUT => 3,
        PDO::ATTR_PERSISTENT => true,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_EMULATE_PREPARES => true,
        PDO::ATTR_STRINGIFY_FETCHES => true,
    ];

    protected static string $namespace;

    protected static ?Authorization $authorization = null;

    abstract protected function getDatabase(): Database;

    abstract protected function deleteColumn(string $collection, string $column): bool;

    abstract protected function deleteIndex(string $collection, string $index): bool;

    protected function setUp(): void
    {
        $this->testDatabase = 'utopiaTests_'.static::getTestToken();

        if (is_null(self::$authorization)) {
            self::$authorization = new Authorization();
        }

        self::$authorization->addRole('any');

        $this->getDatabase()
            ->removeHook(Relationships::class)->addHook(new Relationships())
            ->removeHook(Permissions::class)->addHook(new Permissions());
    }

    protected function tearDown(): void
    {
        self::$authorization?->reset();

    }

    protected string $testDatabase = 'utopiaTests';

    protected static function getTestToken(): string
    {
        return getenv('TEST_TOKEN') ?: getenv('UNIQUE_TEST_TOKEN') ?: (string) getmypid();
    }

    /**
     * Whether the tests run on one of the engines, for the few assertions that differ by engine. Test-only: a Pool
     * lends one adapter per call and setNamespace() hands that adapter back, which only a plain Pool allows; a
     * ReadWritePool would route setNamespace() as a write.
     *
     * @param  class-string<Adapter>  ...$engines
     */
    private function engineIs(string ...$engines): bool
    {
        $adapter = $this->getDatabase()->getAdapter();
        if ($adapter instanceof Pool) {
            $adapter = $adapter->delegate('setNamespace', [$adapter->getNamespace()]);
        }

        foreach ($engines as $engine) {
            if ($adapter instanceof $engine) {
                return true;
            }
        }

        return false;
    }

    /**
     * Engine properties the 7.x capabilities BoundaryInclusive, BatchOperations, AtomicTransactions,
     * CacheSkipOnFailure, BitwiseAggregates, MultiDimensionDistance, OptionalSpatial and POSIX declared; 8.0 deleted
     * them (DEC-15), so each reads the engine with the truth table those declarations had.
     */
    private function spatialIncludesBoundaries(): bool
    {
        return $this->engineIs(MariaDB::class, Postgres::class, SQLite::class, Memory::class) && ! $this->engineIs(MySQL::class);
    }

    private function supportsBulkWrites(): bool
    {
        return ! $this->engineIs(Mongo::class);
    }

    private function supportsAtomicTransactions(): bool
    {
        return $this->engineIs(SQL::class, Memory::class);
    }

    private function skipsCacheOnFailure(): bool
    {
        return $this->engineIs(SQL::class);
    }

    private function supportsBitwiseAggregates(): bool
    {
        return $this->engineIs(SQL::class) && ! $this->engineIs(SQLite::class);
    }

    private function supportsMultiDimensionDistance(): bool
    {
        return $this->engineIs(MySQL::class, Postgres::class);
    }

    private function supportsOptionalSpatial(): bool
    {
        return $this->engineIs(MariaDB::class) && ! $this->engineIs(MySQL::class);
    }

    private function usesPosixRegex(): bool
    {
        return $this->engineIs(Postgres::class);
    }
}
