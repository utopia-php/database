<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class PoolDelegationTest extends TestCase
{
    /**
     * @return array<string, array{Closure(Pool): mixed, string}>
     */
    public static function featureCalls(): array
    {
        return [
            'raw queries' => [static fn (Pool $pool): mixed => $pool->rawQuery('SELECT 1'), 'Adapter does not support raw queries'],
            'query builder' => [static fn (Pool $pool): mixed => $pool->builder('books'), 'Adapter does not support query builder'],
            'schema builder' => [static fn (Pool $pool): mixed => $pool->schema(), 'Adapter does not support query builder'],
            'spatial decoding' => [static fn (Pool $pool): mixed => $pool->decode('', ColumnType::Point), 'Adapter does not support spatial'],
            'casting before a write' => [static fn (Pool $pool): mixed => $pool->castBefore(new Document(), new Document()), 'Adapter does not support casting'],
            'casting after a read' => [static fn (Pool $pool): mixed => $pool->castAfter(new Document(), [new Document()]), 'Adapter does not support casting'],
            'datetime casting' => [static fn (Pool $pool): mixed => $pool->castDatetime('2026-01-01'), 'Adapter does not support casting'],
            'upsert' => [static fn (Pool $pool): mixed => $pool->upsertDocument(new Document(), new Change(new Document(), new Document())), 'Adapter does not support upserts'],
            'connection' => [static fn (Pool $pool): mixed => $pool->id(), 'Adapter does not support connections'],
            'relationships' => [static fn (Pool $pool): mixed => $pool->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', key: 'author')), 'Adapter does not support relationships'],
        ];
    }

    /**
     * @param  Closure(Pool): mixed  $call
     */
    #[DataProvider('featureCalls')]
    public function testEachMissingFeatureNamesItself(Closure $call, string $message): void
    {
        /** @var Adapter&Stub $adapter */
        $adapter = self::createStub(Adapter::class);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage($message);
        $call($this->pool($adapter));
    }

    public function testMetadataEntriesReachTheBorrowedAdapter(): void
    {
        $adapter = new Memory();
        $adapter->setMetadata('stale', 'entry');
        $pool = $this->pool($adapter);
        $pool->setMetadata('request', 'r-1');

        $this->assertSame([], $pool->list());
        $this->assertSame(['request' => 'r-1'], $adapter->getMetadata());
    }

    public function testTheIntrospectedIndexTypeIsTheBorrowedAdapters(): void
    {
        $pool = $this->pool(new Postgres(new stdClass()));

        $this->assertSame(IndexType::Key, $pool->getSchemaIndexType(IndexType::Fulltext));
    }

    public function testDirectTransactionCallsReachTheBorrowedAdapter(): void
    {
        $adapter = new Memory();
        $pool = $this->pool($adapter);

        $this->assertTrue($pool->startTransaction());
        $this->assertTrue($adapter->inTransaction());
        $this->assertTrue($pool->commitTransaction());
        $this->assertFalse($adapter->inTransaction());
        $this->assertFalse($pool->commitTransaction(), 'A commit without a transaction reports false, as the adapter does');

        $this->assertTrue($pool->startTransaction());
        $this->assertTrue($pool->rollbackTransaction());
        $this->assertFalse($adapter->inTransaction());
        $this->assertFalse($pool->rollbackTransaction());
    }

    public function testQueryBuilderReadsTheRowsOfTheBorrowedAdapter(): void
    {
        $database = new Database($this->pool(new SQLite(new PDO('sqlite::memory:'))), new Cache(new NoCache()));
        $database
            ->setDatabase('library')
            ->setNamespace('library')
            ->setAuthorization(new Authorization());

        $rows = $database->getAuthorization()->skip(static function () use ($database): array|int {
            $database->create();
            $database->createCollection(Collection::create(
                id: 'books',
                attributes: [Attribute::string('title', size: 64)],
                documentSecurity: false,
            ));
            $database->createDocument('books', new Document(['$id' => 'dune', 'title' => 'Dune']));
            $database->createDocument('books', new Document(['$id' => 'emma', 'title' => 'Emma']));

            return $database->query($database->from('books')->select(['title'])->filter([Query::equal('$id', ['emma'])]));
        });

        $this->assertIsArray($rows);
        $this->assertSame(['Emma'], \array_map(static fn (Document $row): mixed => $row->getAttribute('title'), $rows));
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
