<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\Schema\ColumnType;

final class LimitsTest extends TestCase
{
    private const array SQL_INTERNAL_INDEX_KEYS = [
        Storage::INDEX_PRIMARY,
        Storage::INDEX_CREATED_AT,
        Storage::INDEX_UPDATED_AT,
        Storage::INDEX_TENANT_ID,
    ];

    /**
     * @return array<string, array{Adapter, string}>
     */
    public static function sqlAdaptersWithTheirEarliestDate(): array
    {
        return [
            'MariaDB' => [new MariaDB(new stdClass()), '1000-01-01 00:00:00'],
            'MySQL' => [new MySQL(new stdClass()), '1000-01-01 00:00:00'],
            'Postgres' => [new Postgres(new stdClass()), '-4713-01-01 00:00:00'],
            'SQLite' => [new SQLite(new PDO('sqlite::memory:')), '1000-01-01 00:00:00'],
        ];
    }

    /**
     * @return array<string, array{Adapter}>
     */
    public static function sqlAdapters(): array
    {
        return \array_map(static fn (array $case): array => [$case[0]], self::sqlAdaptersWithTheirEarliestDate());
    }

    #[DataProvider('sqlAdaptersWithTheirEarliestDate')]
    public function testSQLAdaptersShareInnoDBLimits(Adapter $adapter, string $minDateTime): void
    {
        $limits = $adapter->limits();

        $this->assertSame(4294967295, $limits->string);
        $this->assertSame(16381, $limits->varchar);
        $this->assertSame(4294967295, $limits->integer);
        $this->assertSame(Database::MAX_BIG_INT, $limits->bigInteger);
        $this->assertSame(1017, $limits->attributes);
        $this->assertSame(64, $limits->indexes);
        $this->assertSame(\count(Database::internalAttributesFor(true)), $limits->defaultAttributes);
        $this->assertSame(\count(Database::INTERNAL_INDEXES), $limits->defaultIndexes);
        $this->assertSame(768, $limits->indexLength);
        $this->assertSame(36, $limits->uidLength);
        $this->assertSame(65535, $limits->documentSize);
        $this->assertSame($minDateTime, $limits->minDateTime->format('Y-m-d H:i:s'));
        $this->assertSame('9999-12-31 23:59:59', $limits->maxDateTime->format('Y-m-d H:i:s'));
        $this->assertSame(ColumnType::Integer, $limits->idType);
        $this->assertSame(self::SQL_INTERNAL_INDEX_KEYS, $limits->internalIndexKeys);
    }

    #[DataProvider('sqlAdapters')]
    public function testASharedTableSpendsAByteOfEachIndexKeyOnTheTenant(Adapter $adapter): void
    {
        $adapter->setSharedTables(true);
        $this->assertSame(767, $adapter->limits()->indexLength);

        $adapter->setSharedTables(false);
        $this->assertSame(768, $adapter->limits()->indexLength);
    }

    public function testSQLiteReservesItsOwnKeywords(): void
    {
        $mariadb = (new MariaDB(new stdClass()))->limits()->keywords;
        $sqlite = (new SQLite(new PDO('sqlite::memory:')))->limits()->keywords;

        $this->assertContains('SELECT', $mariadb);
        $this->assertNotContains('ABORT', $mariadb);
        $this->assertContains('ABORT', $sqlite);
        $this->assertSame($sqlite, \array_values($sqlite));
    }

    public function testMongoHasNoAttributeOrDocumentSizeCap(): void
    {
        $limits = $this->mongo()->limits();

        $this->assertSame(2147483647, $limits->string);
        $this->assertSame(2147483647, $limits->varchar);
        $this->assertSame(0, $limits->attributes);
        $this->assertSame(0, $limits->documentSize);
        $this->assertSame(64, $limits->indexes);
        $this->assertSame(1024, $limits->indexLength);
        $this->assertSame(255, $limits->uidLength);
        $this->assertSame('-9999-01-01 00:00:00', $limits->minDateTime->format('Y-m-d H:i:s'));
        $this->assertSame(ColumnType::Uuid7, $limits->idType);
        $this->assertSame([], $limits->keywords);
        $this->assertSame([], $limits->internalIndexKeys);
    }

    public function testMemoryKeepsAPositiveIndexLengthAndNoDocumentSizeCap(): void
    {
        $limits = (new Memory())->limits();

        $this->assertSame(1017, $limits->attributes);
        $this->assertSame(1024, $limits->indexLength);
        $this->assertSame(255, $limits->uidLength);
        $this->assertSame(0, $limits->documentSize);
        $this->assertSame('0001-01-01 00:00:00', $limits->minDateTime->format('Y-m-d H:i:s'));
        $this->assertSame(ColumnType::Integer, $limits->idType);
        $this->assertSame([], $limits->internalIndexKeys);
    }

    public function testAPoolAnswersWithTheLimitsOfTheAdapterItBorrows(): void
    {
        $connection = new MariaDB(new stdClass());
        $pool = new Pool(new UtopiaPool(new Stack(), 'limits', 1, static fn (): MariaDB => $connection, timeout: 0.0));
        $pool->setAuthorization(new Authorization());

        $this->assertSame(768, $pool->limits()->indexLength);
        $this->assertSame(ColumnType::Integer, $pool->limits()->idType);

        $pool->setSharedTables(true);

        $this->assertSame(767, $pool->limits()->indexLength, 'The pool asks again once shared tables change the limits');
    }

    public function testTheDatabaseForwardsCountOnlyTheAttributesAndIndexesACollectionDeclares(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $limits = $database->getAdapter()->limits();

        $this->assertSame($limits->attributes - $limits->defaultAttributes, $database->getLimitForAttributes());
        $this->assertSame($limits->indexes - $limits->defaultIndexes, $database->getLimitForIndexes());
        $this->assertSame(1024, $database->getMaxIndexLength());
        $this->assertSame(16381, $database->getMaxVarcharLength());
        $this->assertSame(255, $database->getMaxUidLength());
        $this->assertSame('0001-01-01', $database->getMinDateTime()->format('Y-m-d'));
        $this->assertSame('9999-12-31', $database->getMaxDateTime()->format('Y-m-d'));
        $this->assertSame(ColumnType::Integer, $database->getIdAttributeType());
    }

    public function testTheDatabaseReportsNoAttributeLimitWhenTheAdapterHasNone(): void
    {
        $database = new Database($this->mongo(), new Cache(new None()));

        $this->assertSame(0, $database->getLimitForAttributes());
        $this->assertSame(ColumnType::Uuid7, $database->getIdAttributeType());
    }

    private function mongo(): Mongo
    {
        return new class () extends Mongo {
            public function __construct()
            {
            }
        };
    }
}
