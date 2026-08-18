<?php

namespace Tests\Unit;

use Exception;
use PDOException;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\OrderDirection;

#[AllowMockObjectsWithoutExpectations]
final class SQLFindTest extends TestCase
{
    #[DataProvider('paths')]
    public function testFetchFailureIsTranslatedAndWinsOverCloseFailure(bool $fast): void
    {
        $timeout = $this->createTimeoutException();
        $statement = $this->statement();
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willThrowException($timeout);
        $statement->expects($this->once())
            ->method('closeCursor')
            ->willThrowException(new PDOException('Failed to close cursor'));

        try {
            $this->find($this->adapter($statement), $fast);
        } catch (TimeoutException $exception) {
            $this->assertSame($timeout, $exception->getPrevious());

            return;
        }

        $this->fail('Expected a timeout exception.');
    }

    #[DataProvider('paths')]
    public function testCloseFailureIsTranslated(bool $fast): void
    {
        $close = new PDOException('Lost connection while closing cursor');
        $statement = $this->statement();
        $statement->expects($this->once())->method('execute')->willReturn(true);
        $statement->expects($this->once())->method('fetchAll')->willReturn([]);
        $statement->expects($this->once())->method('closeCursor')->willThrowException($close);

        try {
            $this->find($this->adapter($statement), $fast);
        } catch (PDOException $exception) {
            $this->assertSame($close, $exception);

            return;
        }

        $this->fail('Expected a close cursor exception.');
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function paths(): iterable
    {
        yield 'fast path' => [true];
        yield 'builder path' => [false];
    }

    public function testJoinDocumentSecurityLookupMatchesRemappedPhysicalIds(): void
    {
        $adapter = new MySQL($this->getMockBuilder(\PDO::class)->disableOriginalConstructor()->getMock());
        $adapter->setDatabase('appwrite');
        $adapter->setNamespace('_5');

        $method = new ReflectionMethod(MySQL::class, 'joinDocumentSecurityEnabled');

        $map = ['database_1_collection_2' => false];
        $this->assertFalse($method->invoke($adapter, $map, 'database_1_collection_2'));
        $this->assertFalse($method->invoke($adapter, $map, 'appwrite._5_database_1_collection_2'));
        $this->assertFalse($method->invoke($adapter, $map, '_5_database_1_collection_2'));

        $this->assertTrue($method->invoke($adapter, $map, 'database_1_collection_9'));
        $this->assertTrue($method->invoke($adapter, [], 'appwrite._5_database_1_collection_2'));
        $this->assertTrue($method->invoke($adapter, ['database_1_collection_2' => true], 'appwrite._5_database_1_collection_2'));
    }

    public function testJoinWithoutSelectProjectsQualifiedStars(): void
    {
        $sql = $this->captureFindSql([
            Query::leftJoin('orders', '$id', 'customerId'),
        ]);

        $this->assertQualifiedJoinStars($sql);
        $this->assertStringContainsString('LEFT JOIN', $sql);
    }

    public function testEmulatesFullOuterJoinWithOuterLimit(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertQualifiedJoinStars($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
    }

    public function testEmulatesFullOuterJoinOrderByIsUnambiguousAfterUnion(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql);
    }

    public function testEmulatesFullOuterJoinOrderByMatchesProjectedUnionOutput(): void
    {
        $sql = $this->captureFindSql(
            [
                Query::fullOuterJoin('orders', '$id', 'customerId'),
                Query::select(['name']),
            ],
            limit: 2,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql);
    }

    public function testEmulatesFullOuterJoinOrderByHandlesMultipleAttributes(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 2,
            orderAttributes: ['name', Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc, OrderDirection::Asc],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql, expectedTerms: 2);
    }

    public function testEmulatesFullOuterJoinStripsOrderAliasesFromDocuments(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                '_uid' => 'doc1',
                '_id' => 1,
                '_permissions' => '[]',
                '_createdAt' => '2020-01-01 00:00:00.000',
                '_updatedAt' => '2020-01-01 00:00:00.000',
                'name' => 'Alice',
                'foj_ord_0' => 1,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [Query::fullOuterJoin('orders', '$id', 'customerId')],
            limit: 1,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('doc1', $results[0]->getId());
        $this->assertSame(false, $results[0]->isSet('foj_ord_0'));
    }

    public function testEmulatesFullOuterJoinRemapsQualifiedUnionColumns(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                'table_main._uid' => 'doc1',
                'table_main._id' => '1',
                'table_main._permissions' => '[]',
                'table_main._createdAt' => '2020-01-01 00:00:00.000',
                'table_main._updatedAt' => '2020-01-01 00:00:00.000',
                'table_main.name' => 'Alice',
                'orders._uid' => 'order1',
                'orders._permissions' => '["read"]',
                'foj_ord_0' => 1,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [
                Query::fullOuterJoin('orders', '$id', 'customerId'),
                Query::select(['name']),
            ],
            limit: 1,
            orderAttributes: [Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('doc1', $results[0]->getId());
        $this->assertSame('1', $results[0]->getSequence());
        $this->assertSame('Alice', $results[0]->getAttribute('name'));
        $this->assertSame(false, $results[0]->isSet('foj_ord_0'));
        $this->assertSame(false, $results[0]->isSet('table_main._uid'));
        $this->assertSame('order1', $results[0]->getAttribute('orders.$id'));
        $this->assertSame(['read'], $results[0]->getAttribute('orders.$permissions'));
        $this->assertSame(false, $results[0]->isSet('orders._uid'));
    }

    public function testRemapRowPreservesSelectedJoinIdentity(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                '_uid' => 'hm1',
                '_id' => 1,
                '_permissions' => '[]',
                '_createdAt' => '2020-01-01 00:00:00.000',
                '_updatedAt' => '2020-01-01 00:00:00.000',
                'name' => 'Main',
                'alpha.$id' => 'peer-a',
                'beta.$id' => 'peer-b',
                'alpha.label' => 'alpha-one',
                'beta.label' => 'beta-key',
                'alpha.score' => 11,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [
                Query::join('peers', '$id', 'mainId', '=', 'alpha'),
                Query::join('peers', 'peerKey', '$id', '=', 'beta'),
                Query::select(['name', 'alpha.$id', 'beta.$id', 'alpha.label', 'beta.label', 'alpha.score']),
            ],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('hm1', $results[0]->getId());
        $this->assertSame('peer-a', $results[0]->getAttribute('alpha.$id'));
        $this->assertSame('peer-b', $results[0]->getAttribute('beta.$id'));
        $this->assertSame('alpha-one', $results[0]->getAttribute('alpha.label'));
        $this->assertSame('beta-key', $results[0]->getAttribute('beta.label'));
        $this->assertNotSame('peer-a', $results[0]->getId());
        $this->assertNotSame('peer-b', $results[0]->getId());
    }

    public function testRemapRowDoesNotCopyJoinIdentityOntoMain(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                'table_main._uid' => 'hm1',
                'table_main._id' => '1',
                'table_main._permissions' => '[]',
                'table_main._createdAt' => '2020-01-01 00:00:00.000',
                'table_main._updatedAt' => '2020-01-01 00:00:00.000',
                'table_main.name' => 'Main',
                'twin.$id' => 'hm1',
                'twin.name' => 'Main',
                'twin.$permissions' => '["read(\"any\")"]',
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [
                Query::join('collection', '$id', '$id', '=', 'twin'),
                Query::select(['name', 'twin.$id', 'twin.name', 'twin.$permissions']),
            ],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('hm1', $results[0]->getId());
        $this->assertSame('hm1', $results[0]->getAttribute('twin.$id'));
        $this->assertSame('Main', $results[0]->getAttribute('twin.name'));
        $this->assertSame(['read("any")'], $results[0]->getAttribute('twin.$permissions'));
    }

    public function testRemapRowMapsMainAliasIdentityWhenJoinIdentityIsSelected(): void
    {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([
            [
                'table_main._uid' => '',
                'table_main._id' => null,
                'table_main._permissions' => '[]',
                'table_main._createdAt' => null,
                'table_main._updatedAt' => null,
                'table_main.name' => null,
                'tail.score' => 7,
            ],
        ]);
        $statement->method('closeCursor')->willReturn(true);

        $results = $this->adapter($statement)->find(
            new Document(['$id' => 'collection']),
            [
                Query::rightJoin('tail', '$id', 'mainId', '=', 'tail'),
                Query::select(['name', 'tail.score']),
            ],
        );

        $this->assertSame(1, \count($results));
        $this->assertSame('', $results[0]->getId());
        $this->assertSame(7, $results[0]->getAttribute('tail.score'));
    }

    public function testDistinctSelectIdProjectsMainUid(): void
    {
        $sql = $this->captureFindSql([
            Query::distinct(),
            Query::join('orders', '$id', 'customerId', '=', 'b'),
            Query::select(['$id', 'name', 'b.label']),
        ]);

        $this->assertMatchesRegularExpression('/SELECT\s+DISTINCT/i', $sql);
        $this->assertStringContainsString('`table_main`.`_uid`', $sql);
        $this->assertStringContainsString('`b`.`label` AS `b.label`', $sql);
    }

    public function testSelectedJoinIdentityIsProjectedAsQualifiedInternal(): void
    {
        $sql = $this->captureFindSql([
            Query::join('peers', '$id', 'mainId', '=', 'alpha'),
            Query::select(['name', 'alpha.$id', 'alpha.label']),
        ]);

        $this->assertStringContainsString('`alpha`.`_uid` AS `alpha._uid`', $sql);
        $this->assertStringContainsString('`alpha`.`label` AS `alpha.label`', $sql);
        $this->assertStringNotContainsString('`alpha.$id`', $sql);
        $this->assertStringContainsString('`table_main`.`_uid`', $sql);
    }

    public function testJoinSideSearchRelevanceUsesJoinAlias(): void
    {
        $sql = $this->captureFindSql([
            Query::leftJoin('meta', '$id', 'mainId', '=', 'meta'),
            Query::search('meta.body', 'needle'),
        ]);

        $this->assertStringContainsString('`meta`.`body`', $sql);
        $this->assertStringNotContainsString('metabody', $sql);
        $this->assertStringNotContainsString('`table_main`.`metabody`', $sql);
    }

    public function testSqliteJoinSideSearchQuotesJoinAlias(): void
    {
        $adapter = new SQLite(new \PDO('sqlite::memory:'));
        $adapter->setNamespace('namespace');
        $compile = new ReflectionMethod(SQLite::class, 'compileAdapterFilter');

        $compiled = $compile->invoke(
            $adapter,
            Query::search('meta.body', 'needle'),
            'jh_m',
            Query::DEFAULT_ALIAS,
        );

        $this->assertIsArray($compiled);
        $this->assertArrayHasKey('expression', $compiled);
        $this->assertIsString($compiled['expression']);
        $expression = $compiled['expression'];
        $this->assertStringContainsString('`meta`.`body`', $expression);
        $this->assertStringContainsString('LIKE', $expression);
        $this->assertStringNotContainsString('metabody', $expression);
        $this->assertStringNotContainsString('`table_main`.`metabody`', $expression);
        $this->assertStringNotContainsString(Storage::SEQUENCE, $expression);

        $main = $compile->invoke(
            $adapter,
            Query::search('body', 'needle'),
            'jh_m',
            Query::DEFAULT_ALIAS,
        );

        $this->assertIsArray($main);
        $this->assertArrayHasKey('expression', $main);
        $this->assertIsString($main['expression']);
        $this->assertStringContainsString('`table_main`.`body`', $main['expression']);
    }

    public function testEmulatesFullOuterJoinCursorAfterUsesJoinQualifiedOrder(): void
    {
        $sql = $this->captureFindSql(
            [Query::fullOuterJoin('meta', '$id', 'mainId', '=', 'meta')],
            limit: 1,
            orderAttributes: ['meta.score', Document::SEQUENCE],
            orderTypes: [OrderDirection::Asc, OrderDirection::Asc],
            cursor: [
                'meta.score' => 10,
                Document::SEQUENCE => '5',
            ],
        );

        $this->assertEmulatedFullOuterJoin($sql);
        $this->assertSame(1, $this->countLimitsAfterUnion($sql), $sql);
        $this->assertUnambiguousUnionOrderBy($sql, expectedTerms: 2);
        $this->assertGreaterThanOrEqual(2, \preg_match_all('/`meta`\.`score`\s*>/i', $sql));
        $this->assertDoesNotMatchRegularExpression(
            '/ORDER BY\s+`score`\b/i',
            $sql,
        );
    }

    public function testQualifyDottedAttributeKeepsNestedObjectPaths(): void
    {
        $adapter = new MySQL($this->getMockBuilder(\PDO::class)->disableOriginalConstructor()->getMock());
        $method = new ReflectionMethod(MySQL::class, 'qualifyDottedAttribute');

        $aliasSet = [
            Query::DEFAULT_ALIAS => true,
            'orders' => true,
        ];
        $mainAttributes = [
            'meta.score' => true,
        ];

        $this->assertSame(
            'metascore',
            $method->invoke($adapter, 'meta.score', $aliasSet, $mainAttributes)
        );
        $this->assertSame(
            'orders.email',
            $method->invoke($adapter, 'orders.email', $aliasSet, $mainAttributes)
        );
        $this->assertSame(
            'orders._uid',
            $method->invoke($adapter, 'orders.$id', $aliasSet, $mainAttributes)
        );
        $this->assertSame(
            'profile.user.email',
            $method->invoke($adapter, 'profile.user.email', $aliasSet, $mainAttributes)
        );
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     * @param  array<string, mixed>  $cursor
     */
    private function captureFindSql(
        array $queries,
        ?int $limit = 25,
        array $orderAttributes = [],
        array $orderTypes = [],
        array $cursor = [],
    ): string {
        $statement = $this->statement();
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('closeCursor')->willReturn(true);

        $sql = '';
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturnCallback(function (string $query) use (&$sql, $statement): \PDOStatement {
                $sql = $query;

                return $statement;
            });

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document(['$id' => 'collection']),
            $queries,
            limit: $limit,
            orderAttributes: $orderAttributes,
            orderTypes: $orderTypes,
            cursor: $cursor,
        );

        $this->assertNotSame('', $sql);

        return $sql;
    }

    private function assertEmulatedFullOuterJoin(string $sql): void
    {
        $this->assertStringContainsString('UNION ALL', $sql);
        $this->assertSame(0, \preg_match_all('/UNION(?! ALL)/i', $sql), $sql);
        $this->assertStringContainsString('LEFT JOIN', $sql);
        $this->assertStringContainsString('RIGHT JOIN', $sql);
        $this->assertStringContainsString('IS NULL', $sql);
        $this->assertStringNotContainsString('FULL OUTER JOIN', $sql);
        $this->assertDoesNotMatchRegularExpression('/FROM\s*\(\s*SELECT\s+\*/i', $sql);
    }

    private function assertQualifiedJoinStars(string $sql, string $quote = '`', string $joinAlias = 'j0'): void
    {
        $this->assertStringContainsString($quote.$joinAlias.$quote.'.*', $sql);
        $this->assertStringContainsString($quote.'table_main'.$quote.'.*', $sql);
        $this->assertDoesNotMatchRegularExpression('/SELECT\s+\*(?:\s|,|$)/i', $sql);
    }

    private function countLimitsAfterUnion(string $sql): int
    {
        $unionPosition = \stripos($sql, 'UNION');
        $this->assertNotFalse($unionPosition);

        $limitMatches = \preg_match_all('/\bLIMIT\s+(?:2|\?)/i', $sql, $matches, PREG_OFFSET_CAPTURE);
        $this->assertNotFalse($limitMatches);
        foreach ($matches[0] as $match) {
            $this->assertGreaterThan($unionPosition, $match[1], $sql);
        }

        return $limitMatches;
    }

    private function assertUnambiguousUnionOrderBy(string $sql, int $expectedTerms = 1): void
    {
        $unionPosition = \stripos($sql, 'UNION');
        $this->assertNotFalse($unionPosition);

        $afterUnion = \substr($sql, $unionPosition);
        $this->assertMatchesRegularExpression('/ORDER BY/i', $afterUnion, $sql);

        $this->assertDoesNotMatchRegularExpression(
            '/ORDER BY\s+`_id`\b/i',
            $afterUnion,
            $sql,
        );

        $aliasMatches = \preg_match_all('/`foj_ord_\d+`/', $afterUnion);
        $positionalMatches = \preg_match_all('/ORDER BY\s+\d+/i', $afterUnion);

        $this->assertTrue(
            $aliasMatches >= $expectedTerms || $positionalMatches === 1,
            $sql,
        );

        if ($aliasMatches >= $expectedTerms) {
            $orderByPosition = \stripos($afterUnion, 'ORDER BY');
            $this->assertNotFalse($orderByPosition);
            $selectSql = \substr($sql, 0, $unionPosition + $orderByPosition);
            for ($index = 0; $index < $expectedTerms; $index++) {
                $this->assertStringContainsString('foj_ord_'.$index, $selectSql, $sql);
                $this->assertStringContainsString('`foj_ord_'.$index.'`', $afterUnion, $sql);
            }
        }

        $this->assertDoesNotMatchRegularExpression(
            '/ORDER BY\s+`table_main`\.`_id`/i',
            $afterUnion,
            $sql,
        );
    }

    private function find(MySQL $adapter, bool $fast): void
    {
        $adapter->find(
            new Document(['$id' => 'collection']),
            $fast ? [] : [Query::equal('title', ['value'])],
            orderAttributes: ['$sequence'],
            orderTypes: [OrderDirection::Asc],
        );
    }

    private function adapter(\PDOStatement $statement): MySQL
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->once())->method('prepare')->willReturn($statement);

        $adapter = new MySQL($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        return $adapter;
    }

    private function statement(): \PDOStatement&MockObject
    {
        $statement = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement->method('bindValue')->willReturn(true);

        return $statement;
    }

    private function createTimeoutException(): PDOException
    {
        $exception = new PDOException('Query execution was interrupted');
        $code = new ReflectionProperty(Exception::class, 'code');
        $code->setValue($exception, 'HY000');
        $exception->errorInfo = ['HY000', 3024, 'Query execution was interrupted'];

        return $exception;
    }
}
