<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\OrderDirection;

final class FullOuterJoinRandomOrderTest extends TestCase
{
    /**
     * @return iterable<string, array{bool}>
     */
    public static function joins(): iterable
    {
        yield 'emulated full outer join' => [false];
        yield 'native full outer join' => [true];
    }

    #[DataProvider('joins')]
    public function testAFullOuterJoinInRandomOrderReturnsEveryRow(bool $native): void
    {
        $database = new Database($native ? new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')) : new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database->setDatabase('random_order')->setNamespace('random_order')->setAuthorization(new Authorization());
        $database->create();
        foreach (['customers' => [Attribute::string('name', size: 16)], 'notes' => [Attribute::string('customerId', size: 16), Attribute::string('body', size: 16)]] as $id => $attributes) {
            $database->createCollection(new Collection(id: $id, attributes: $attributes, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        }
        foreach (['c1', 'c2', 'c3'] as $customer) {
            $database->createDocument('customers', new Document(['$id' => $customer, 'name' => $customer]));
        }
        foreach (['n1' => 'c1', 'n2' => 'c1', 'n3' => 'c2', 'n4' => 'cx'] as $note => $customer) {
            $database->createDocument('notes', new Document(['$id' => $note, 'customerId' => $customer, 'body' => $note]));
        }
        $join = Query::fullOuterJoin('notes', '$id', 'customerId', '=', 'note');
        $select = Query::select(['name', 'note.body']);

        $this->assertSame(
            $this->rows($database->find('customers', [$join, $select])),
            $this->rows($database->find('customers', [$join, $select, Query::orderRandom(), Query::limit(100)])),
        );
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>}>
     */
    public static function emulatingEngines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('emulatingEngines')]
    public function testARandomOrderOverAnEmulatedFullOuterJoinOrdersTheUnionAsATable(string $class): void
    {
        $statements = [];
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use (&$statements): PDOStatement {
            $statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('closeCursor')->willReturn(true);
            $statement->method('bindValue')->willReturn(true);

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);

        $adapter->find(
            new Document(['$id' => 'customers']),
            [Query::fullOuterJoin('notes', '$id', 'customerId', '=', 'note')],
            limit: 10,
            orderAttributes: [''],
            orderTypes: [OrderDirection::Random],
        );

        $this->assertCount(1, $statements);
        $this->assertStringStartsWith('SELECT * FROM ((SELECT ', $statements[0]);
        $this->assertStringContainsString(' UNION ALL (SELECT ', $statements[0]);
        $this->assertStringEndsWith(') AS `foj_rows` ORDER BY RAND() LIMIT ?', $statements[0]);
    }

    /**
     * @param array<Document> $documents
     * @return list<string>
     */
    private function rows(array $documents): array
    {
        $rows = \array_map(static fn (Document $document): string => \json_encode([$document->getAttribute('name'), $document->getAttribute('note.body')], JSON_THROW_ON_ERROR), $documents);
        \sort($rows);

        return $rows;
    }
}
