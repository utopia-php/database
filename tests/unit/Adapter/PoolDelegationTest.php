<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolDelegationTest extends TestCase
{
    /**
     * @return array<string, array{Closure(Pool): mixed, string}>
     */
    public static function featureCalls(): array
    {
        return [
            'raw queries' => [static fn (Pool $pool): mixed => $pool->rawQuery('SELECT 1'), 'Adapter does not support raw queries'],
            'query builder' => [static fn (Pool $pool): mixed => $pool->getBuilder('books'), 'Adapter does not support query builder'],
            'column types' => [static fn (Pool $pool): mixed => $pool->getColumnType('string', 32), 'Adapter does not support column types'],
            'spatial' => [static fn (Pool $pool): mixed => $pool->decodePoint(''), 'Adapter does not support spatial'],
            'internal casting' => [static fn (Pool $pool): mixed => $pool->castingBefore(new Document(), new Document()), 'Adapter does not support internal casting'],
            'UTC casting' => [static fn (Pool $pool): mixed => $pool->setUTCDatetime('2026-01-01'), 'Adapter does not support UTC casting'],
            'connection id' => [static fn (Pool $pool): mixed => $pool->getConnectionId(), 'Adapter does not support connection id'],
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

    public function testDebugEntriesReachTheBorrowedAdapter(): void
    {
        $adapter = new Memory();
        $adapter->setDebug('stale', 'entry');
        $pool = $this->pool($adapter);
        $pool->setDebug('request', 'r-1');

        $this->assertTrue($pool->ping());
        $this->assertSame(['request' => 'r-1'], $adapter->getDebug());
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

            return $database->execute($database->from('books')->select(['title'])->filter([Query::equal('$id', ['emma'])]));
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
