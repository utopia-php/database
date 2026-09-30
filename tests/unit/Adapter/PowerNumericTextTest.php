<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class PowerNumericTextTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /** @var list<mixed> */
    private array $bound = [];

    /**
     * @return iterable<string, array{string, list<mixed>}>
     */
    public static function exponents(): iterable
    {
        yield 'integer text' => ['2', [2]];
        yield 'decimal text' => ['0.5', [0.5]];
        yield 'negative text' => ['-1', [-1]];
        yield 'exponent text' => ['1e1', [10.0]];
        yield 'non-numeric text' => ['two', ['two']];
    }

    /**
     * @param list<mixed> $values
     */
    #[DataProvider('exponents')]
    public function testANumericTextExponentIsReadAsANumber(string $exponent, array $values): void
    {
        $this->assertSame($values, Operator::power($exponent)->getValues());
        $this->assertSame($values[0], Operator::power($exponent)->getValue());
        $this->assertSame($values, Operator::parse((string) \json_encode(['method' => 'power', 'attribute' => 'count', 'values' => [$exponent]]))->getValues());
    }

    public function testTheLimitOfAPowerIsLeftAsGiven(): void
    {
        $this->assertSame([2, '18446744073709551615'], Operator::power('2', '18446744073709551615')->getValues());
        $this->assertSame(['method' => 'power', 'attribute' => '', 'values' => ['2']], Operator::power('2')->toArray());
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function databases(): iterable
    {
        yield 'SQLite' => ['SQLite'];
        yield 'Memory' => ['Memory'];
    }

    #[DataProvider('databases')]
    public function testANumericTextExponentIsAppliedAndNonNumericTextRefused(string $engine): void
    {
        $database = $this->database($engine === 'SQLite' ? new SQLite(new PDO('sqlite::memory:')) : new Memory());

        $updated = $database->updateDocument('items', 'first', new Document([
            'count' => Operator::power('2'),
            'ratio' => Operator::power('2'),
        ]));
        $this->assertSame(9, $updated->getAttribute('count'));
        $this->assertSame(2.25, $updated->getAttribute('ratio'));

        $updated = $database->updateDocument('items', 'first', new Document(['count' => Operator::power('2', '50')]));
        $this->assertSame(9, $updated->getAttribute('count'));

        if ($database->getAdapter()->supports(Capability::Upserts)) {
            $database->upsertDocument('items', new Document(['$id' => 'created', 'count' => Operator::power('2')]));
            $this->assertSame(16, $database->getDocument('items', 'created')->getAttribute('count'));
        }

        try {
            $database->updateDocument('items', 'first', new Document(['count' => Operator::power('two')]));
            $this->fail('A non-numeric exponent must be refused');
        } catch (StructureException $error) {
            $this->assertStringContainsString('value must be numeric', $error->getMessage());
        }

        $this->assertSame(9, $database->getDocument('items', 'first')->getAttribute('count'));
    }

    public function testRedisAppliesANumericTextExponent(): void
    {
        $adapter = new class ($this->createStub(Redis::class)) extends RedisAdapter {
            public function applied(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };

        $this->assertSame(9, $adapter->applied(3, Operator::power('2')));
        $this->assertSame(3, $adapter->applied(3, Operator::power('2', '5')));
    }

    /**
     * @return iterable<string, array{class-string<SQL>}>
     */
    public static function engines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
        yield 'Postgres' => [Postgres::class];
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('engines')]
    public function testANumericTextExponentIsSentAsANumber(string $class): void
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $adapter->updateDocuments(
            new Document(['$id' => 'items', 'attributes' => []]),
            new Document(['count' => Operator::power('2')]),
            [new Document(['$id' => 'first', '$sequence' => '1'])],
        );

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('POWER(', $this->statements[0]);
        $this->assertContains(2, $this->bound);
        $this->assertNotContains('2', $this->bound);
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database->setDatabase('power_text')->setNamespace('power_text')->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(new Collection(
            id: 'items',
            attributes: [Attribute::integer('count', default: 4), Attribute::float('ratio')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument('items', new Document(['$id' => 'first', 'count' => 3, 'ratio' => 1.5]));

        return $database;
    }
}
