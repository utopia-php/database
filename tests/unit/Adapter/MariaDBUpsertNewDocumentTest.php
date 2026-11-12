<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Database\Operator;

final class MariaDBUpsertNewDocumentTest extends TestCase
{
    private const string UNSIGNED_DEFAULT = '18446744073709551610';

    /** @var list<mixed> */
    private array $bound = [];

    /**
     * @return iterable<string, array{Operator, int|string}>
     */
    public static function unsignedOperators(): iterable
    {
        yield 'increment beyond the native integer' => [Operator::increment(3), '18446744073709551613'];
        yield 'increment past the limit keeps the default' => [Operator::increment(10, '18446744073709551615'), self::UNSIGNED_DEFAULT];
        yield 'decrement back into the native integer' => [Operator::decrement('18446744073709551600'), 10];
    }

    #[DataProvider('unsignedOperators')]
    public function testAnUnsignedOperatorOnANewDocumentIsComputedExactly(Operator $operator, int|string $expected): void
    {
        $collection = new Document([
            '$id' => 'counters',
            'attributes' => [new Document(['$id' => 'counter', 'type' => 'bigint', 'signed' => false, 'default' => self::UNSIGNED_DEFAULT])],
        ]);

        $this->adapter()->upsertDocuments($collection, [
            new Change(new Document(), new Document(['$id' => 'created', '$permissions' => [], 'counter' => $operator])),
        ]);

        $this->assertContains($expected, $this->bound);
    }

    private function adapter(): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('closeCursor')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
