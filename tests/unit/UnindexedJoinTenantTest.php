<?php

namespace Tests\Unit;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Under shared tables every index but `_uid`'s and the primary key leads with `_tenant`, so MySQL can look a joined
 * table's rows up by the tenant alone. For a join no index serves it does, once per row the join pairs, reading the
 * tenant's whole table each time. Such a join compares its tenant as a range, which MySQL reads once; a join an
 * index serves keeps the equality its index lookup starts with.
 */
final class UnindexedJoinTenantTest extends TestCase
{
    private const string NAMESPACE = 'tenancy';

    private const string CUSTOMERS = 'customers';

    private const string LABELS = 'labels';

    private const int TENANT = 7;

    private const string READER = 'reader';

    /**
     * The attributes the labels collection's key and unique indexes lead with.
     */
    private const array LABEL_INDEXED = ['code'];

    private string $sql = '';

    /**
     * @var list<mixed>
     */
    private array $bindings = [];

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function unindexed(): iterable
    {
        yield 'an inner join on an unindexed attribute' => [Query::join(self::LABELS, 'named', [Query::on('name', 'name')]), 'named'];
        yield 'a left join on an unindexed attribute' => [Query::leftJoin(self::LABELS, 'named', [Query::on('name', 'name')]), 'named'];
        yield 'an inner join compared other than for equality' => [Query::join(self::LABELS, 'named', [Query::on('name', 'code', '>')]), 'named'];
    }

    #[DataProvider('unindexed')]
    public function testJoinNoIndexServesMatchesItsTenantAsARange(Query $join, string $alias): void
    {
        $this->find(new MySQL($this->pdo()), [$join]);

        $this->assertSame(1, \substr_count($this->sql, $this->range($alias)), 'The tenant of a join no index serves is a range: '.$this->sql);
        $this->assertStringNotContainsString($this->equality($alias), $this->sql);
        $this->assertSame(2, \count(\array_keys($this->bindings, self::TENANT, true)) - $this->equalities(), 'The range binds the tenant at both ends: '.$this->sql);
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function indexed(): iterable
    {
        yield 'a join on an indexed attribute' => [Query::join(self::LABELS, 'coded', [Query::on('name', 'code')]), 'coded'];
        yield 'a join on the joined $id' => [Query::join(self::LABELS, 'byId', [Query::on('name', '$id')]), 'byId'];
        yield 'a join on the joined $sequence' => [Query::join(self::LABELS, 'bySequence', [Query::on('$sequence', '$sequence')]), 'bySequence'];
        yield 'a join on the joined $createdAt' => [Query::join(self::LABELS, 'byCreation', [Query::on('$createdAt', '$createdAt')]), 'byCreation'];
        yield 'a right join on an unindexed attribute' => [Query::rightJoin(self::LABELS, 'named', [Query::on('name', 'name')]), 'named'];
    }

    #[DataProvider('indexed')]
    public function testJoinAnIndexServesKeepsItsTenantEquality(Query $join, string $alias): void
    {
        $this->find(new MySQL($this->pdo()), [$join]);

        $this->assertStringContainsString($this->equality($alias), $this->sql);
        $this->assertStringNotContainsString($this->range($alias), $this->sql);
    }

    /**
     * @return iterable<string, array{Query, Query}>
     */
    public static function chained(): iterable
    {
        $named = [Query::on('name', 'name')];
        $coded = [Query::on('named.code', 'code')];

        yield 'a left join then a left join' => [Query::leftJoin(self::LABELS, 'named', $named), Query::leftJoin(self::LABELS, 'coded', $coded)];
        yield 'an inner join then a left join' => [Query::join(self::LABELS, 'named', $named), Query::leftJoin(self::LABELS, 'coded', $coded)];
        yield 'a left join then an inner join' => [Query::leftJoin(self::LABELS, 'named', $named), Query::join(self::LABELS, 'coded', $coded)];
        yield 'an inner join then an inner join' => [Query::join(self::LABELS, 'named', $named), Query::join(self::LABELS, 'coded', $coded)];
    }

    #[DataProvider('chained')]
    public function testLaterJoinComparingAnIndexedColumnLeavesTheEarlierJoinUnserved(Query $named, Query $coded): void
    {
        $this->find(new MySQL($this->pdo()), [$named, $coded]);

        $this->assertStringContainsString($this->range('named'), $this->sql, 'Only the ON of named reaches named, and it compares no indexed column: '.$this->sql);
        $this->assertStringNotContainsString($this->equality('named'), $this->sql);
        $this->assertStringContainsString($this->equality('coded'), $this->sql);
    }

    public function testEveryJoinedTableOfAMixedReadIsKeptToTheTenantAndChecked(): void
    {
        $this->find(new MySQL($this->pdo()), [
            Query::join(self::CUSTOMERS, 'peer', [Query::on('$id', '$id')]),
            Query::join(self::LABELS, 'named', [Query::on('name', 'name')]),
            Query::leftJoin(self::LABELS, 'coded', [Query::on('name', 'code')]),
        ]);

        $this->assertStringContainsString($this->equality('peer'), $this->sql);
        $this->assertStringContainsString($this->range('named'), $this->sql);
        $this->assertStringContainsString($this->equality('coded'), $this->sql);
        $this->assertStringContainsString('`table_main`._tenant IN (?)', $this->sql, 'The main table keeps its equality');

        foreach (['table_main', 'peer', 'named', 'coded'] as $alias) {
            $this->assertMatchesRegularExpression(
                '/`'.$alias.'`\.`_uid` IN \(SELECT (\/\*\+ NO_SEMIJOIN\(\) \*\/ )?_document FROM `[^`]+`\.`[^`]+_perms` WHERE _permission IN \([?, ]+\) AND _type = \? AND _tenant IN \(\?\)\)/',
                $this->sql,
                'The permission check of '.$alias.' compares its own `_uid` within the tenant: '.$this->sql,
            );
        }
    }

    public function testCountOfAJoinNoIndexServesMatchesItsTenantAsARange(): void
    {
        $adapter = new MySQL($this->pdo());
        $this->configure($adapter, true);

        $adapter->count($this->collection(), [Query::join(self::LABELS, 'named', [Query::on('name', 'name')])]);

        $this->assertStringContainsString($this->range('named'), $this->sql);
    }

    public function testJoinOfACollectionWithoutDescribedIndexesKeepsItsTenantEquality(): void
    {
        $adapter = new MySQL($this->pdo());
        $this->configure($adapter, true);

        $adapter->find(
            new Document(['$id' => self::CUSTOMERS, 'documentSecurity' => true]),
            [Query::join(self::LABELS, 'named', [Query::on('name', 'name')])],
            limit: 25,
        );

        $this->assertStringContainsString($this->equality('named'), $this->sql);
    }

    public function testMariaDBKeepsTheTenantEquality(): void
    {
        $this->find(new MariaDB($this->pdo()), [Query::join(self::LABELS, 'named', [Query::on('name', 'name')])]);

        $this->assertStringContainsString($this->equality('named'), $this->sql);
        $this->assertStringNotContainsString($this->range('named'), $this->sql);
    }

    public function testPlainTablesHaveNoTenantCondition(): void
    {
        $adapter = new MySQL($this->pdo());
        $this->configure($adapter, false);

        $adapter->find($this->collection(), [Query::join(self::LABELS, 'named', [Query::on('name', 'name')])], limit: 25);

        $this->assertStringNotContainsString('_tenant', $this->sql);
    }

    /**
     * @param  list<Query>  $queries
     */
    private function find(MariaDB $adapter, array $queries): void
    {
        $this->configure($adapter, true);

        $adapter->find($this->collection(), $queries, limit: 25);

        $this->assertNotSame('', $this->sql);
    }

    private function collection(): Document
    {
        return new Document([
            '$id' => self::CUSTOMERS,
            'documentSecurity' => true,
            Database::JOIN_DOCUMENT_SECURITY => [self::CUSTOMERS => true, self::LABELS => true],
            Database::JOIN_INDEXED => [self::CUSTOMERS => [], self::LABELS => self::LABEL_INDEXED],
        ]);
    }

    private function configure(MariaDB $adapter, bool $shared): void
    {
        $adapter->setDatabase(self::NAMESPACE);
        $adapter->setNamespace(self::NAMESPACE);
        $adapter->setSharedTables($shared);
        $adapter->setTenant($shared ? self::TENANT : null);
        $authorization = new Authorization();
        $authorization->addRole(Role::user(self::READER)->toString());
        $adapter->setAuthorization($authorization);
    }

    private function pdo(): PDO
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('bindValue')->willReturnCallback(function (int|string $position, mixed $value): bool {
            $this->bindings[] = $value;

            return true;
        });
        $statement->method('execute')->willReturn(true);
        $statement->method('fetchAll')->willReturn([]);
        $statement->method('fetch')->willReturn(false);
        $statement->method('closeCursor')->willReturn(true);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->sql = $query;
            $this->bindings = [];

            return $statement;
        });

        return $pdo;
    }

    private function range(string $alias): string
    {
        return '(`'.$alias.'`._tenant >= ? AND `'.$alias.'`._tenant <= ?)';
    }

    private function equality(string $alias): string
    {
        return '`'.$alias.'`._tenant IN (?)';
    }

    /**
     * How many tenant equalities the statement binds.
     */
    private function equalities(): int
    {
        return \substr_count($this->sql, '_tenant IN (?)');
    }
}
