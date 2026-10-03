<?php

namespace Tests\Unit\Adapter;

use ArrayObject;
use LogicException;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Change;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

/**
 * Which bound values each write wraps in ST_GeomFromText on the engines the host cannot run.
 * Only a column the collection declares as point, linestring or polygon is spatial; a string
 * column holding text that reads like WKT is bound as the text itself.
 */
final class SpatialBindingTest extends TestCase
{
    private const string POSITION = 'POINT(3 4)';

    private const string GEOMETRY_FUNCTION = 'ST_GeomFromText(';

    /**
     * @return iterable<string, array{class-string<SQL>, string, string}>
     */
    public static function writes(): iterable
    {
        $engines = ['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class, 'Postgres' => Postgres::class];
        $operations = ['createDocument', 'createDocuments', 'updateDocument', 'updateDocuments', 'upsertDocuments'];
        $answers = [
            'point' => 'POINT(1 2)',
            'point with trailing text' => 'POINT(1 2) is my answer',
            'linestring' => 'LINESTRING(0 0,1 1)',
            'polygon' => 'POLYGON((0 0,1 1,1 0,0 0))',
            'lowercase point' => 'point (1 2)',
        ];

        foreach ($engines as $engineName => $engine) {
            foreach ($operations as $operation) {
                foreach ($answers as $answerName => $answer) {
                    yield $engineName.' '.$operation.' '.$answerName => [$engine, $operation, $answer];
                }
            }
        }
    }

    /**
     * @param  class-string<SQL>  $engine
     */
    #[DataProvider('writes')]
    public function testStringColumnHoldingWktIsBoundAsText(string $engine, string $operation, string $answer): void
    {
        $bindings = $this->write($engine, $operation, [$answer, 'plain text']);

        $this->assertContains([$answer, false], $bindings, 'The string column is bound as the text it holds');
        $this->assertNotContains([$answer, true], $bindings, 'The string column is not wrapped in '.self::GEOMETRY_FUNCTION);
        $this->assertNotContains(['plain text', true], $bindings, 'Another document\'s plain text in the same column is not wrapped');
    }

    /**
     * @param  class-string<SQL>  $engine
     */
    #[DataProvider('writes')]
    public function testSpatialColumnIsWrappedInGeomFromText(string $engine, string $operation, string $answer): void
    {
        $bindings = $this->write($engine, $operation, [$answer]);

        $this->assertContains([self::POSITION, true], $bindings);
        $this->assertNotContains([self::POSITION, false], $bindings);
    }

    /**
     * @param  class-string<SQL>  $engine
     * @param  list<string>  $answers
     * @return list<array{mixed, bool}> Each bound value paired with whether its placeholder sits inside ST_GeomFromText
     */
    private function write(string $engine, string $operation, array $answers): array
    {
        $bindings = new ArrayObject();
        $adapter = new $engine($this->pdo($bindings));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $collection = new Document([
            '$id' => 'answers',
            'attributes' => [
                new Document(['$id' => 'answer', 'key' => 'answer', 'type' => ColumnType::String->value, 'size' => 255]),
                new Document(['$id' => 'position', 'key' => 'position', 'type' => ColumnType::Point->value]),
            ],
        ]);

        $documents = [];
        foreach ($answers as $index => $answer) {
            $documents[] = new Document([
                '$id' => 'document'.$index,
                '$sequence' => (string) ($index + 1),
                '$permissions' => [],
                '$createdAt' => '2026-09-30 00:00:00.000',
                '$updatedAt' => '2026-09-30 00:00:00.000',
                'answer' => $answer,
                'position' => self::POSITION,
            ]);
        }

        match ($operation) {
            'createDocument' => $adapter->createDocument($collection, $documents[0]),
            'createDocuments' => $adapter->createDocuments($collection, $documents),
            'updateDocument' => $adapter->updateDocument($collection, $documents[0]->getId(), $documents[0], true),
            'updateDocuments' => $adapter->updateDocuments(
                $collection,
                new Document(['answer' => $answers[0], 'position' => self::POSITION]),
                $documents,
            ),
            'upsertDocuments' => $adapter->upsertDocuments(
                $collection,
                '',
                \array_map(static fn (Document $document): Change => new Change(new Document(), $document), $documents),
            ),
            default => throw new LogicException('Unknown write operation: '.$operation),
        };

        return $bindings->getArrayCopy();
    }

    /**
     * @param  ArrayObject<int, array{mixed, bool}>  $bindings
     */
    private function pdo(ArrayObject $bindings): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('lastInsertId')->willReturn('1');
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($bindings): PDOStatement {
            $wrapped = $this->placeholdersInsideGeometryFunction($query);
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('rowCount')->willReturn(1);
            $statement->method('bindValue')->willReturnCallback(
                static function (int|string $position, mixed $value) use ($bindings, $wrapped): bool {
                    $bindings->append([$value, $wrapped[(int) $position - 1] ?? false]);

                    return true;
                },
            );

            return $statement;
        });

        return $pdo;
    }

    /**
     * @return list<bool>
     */
    private function placeholdersInsideGeometryFunction(string $query): array
    {
        $wrapped = [];
        $offset = 0;
        while (($position = \strpos($query, '?', $offset)) !== false) {
            $wrapped[] = \str_ends_with(\substr($query, 0, $position), self::GEOMETRY_FUNCTION);
            $offset = $position + 1;
        }

        return $wrapped;
    }
}
