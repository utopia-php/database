<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;

final class MariaDBGeometryWriteTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /** @var list<mixed> */
    private array $bound = [];

    /**
     * @return iterable<string, array{string, array<mixed>, string}>
     */
    public static function geometries(): iterable
    {
        yield 'point' => ['location', [1.5, -2], 'POINT(1.5 -2)'];
        yield 'linestring' => ['route', [[0, 0], [1, 2], [3, 4]], 'LINESTRING(0 0, 1 2, 3 4)'];
        yield 'polygon' => ['area', [[[0, 0], [1, 0], [1, 1], [0, 0]]], 'POLYGON((0 0, 1 0, 1 1, 0 0))'];
        yield 'polygon with a hole' => [
            'area',
            [[[0, 0], [4, 0], [4, 4], [0, 0]], [[1, 1], [2, 1], [2, 2], [1, 1]]],
            'POLYGON((0 0, 4 0, 4 4, 0 0), (1 1, 2 1, 2 2, 1 1))',
        ];
    }

    /**
     * @param array<mixed> $geometry
     */
    #[DataProvider('geometries')]
    public function testAGeometryArrayIsWrittenAsWellKnownText(string $attribute, array $geometry, string $text): void
    {
        $this->adapter()->createDocument($this->collection(), $this->document($attribute, $geometry));

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('ST_GeomFromText(', $this->statements[0]);
        $this->assertContains($text, $this->bound);
    }

    /**
     * @return iterable<string, array{string, array<mixed>, string}>
     */
    public static function malformedGeometries(): iterable
    {
        yield 'empty' => ['area', [], 'Unrecognized geometry array format'];
        yield 'keyed' => ['area', ['x' => 1, 'y' => 2], 'Unrecognized geometry array format'];
        yield 'single coordinate' => ['area', [['a']], 'Unrecognized geometry array format'];
        yield 'word in a line' => ['route', [[0, 0], ['x', 1]], 'Invalid point format in geometry array'];
        yield 'three coordinates in a line' => ['route', [[0, 0], [1, 2, 3]], 'Invalid point format in geometry array'];
        yield 'word in a ring' => ['area', [[[0, 0], ['x', 1]]], 'Invalid point format in polygon ring'];
        yield 'ring that is not a list' => ['area', [[[0, 0], [1, 1]], 5], 'Invalid ring format in polygon geometry'];
    }

    /**
     * @param array<mixed> $geometry
     */
    #[DataProvider('malformedGeometries')]
    public function testAMalformedGeometryArrayIsRefusedBeforeAStatementIsSent(string $attribute, array $geometry, string $message): void
    {
        try {
            $this->adapter()->createDocument($this->collection(), $this->document($attribute, $geometry));
            $this->fail('A malformed geometry must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    private function collection(): Document
    {
        return new Document([
            '$id' => 'places',
            'attributes' => [
                new Document(['$id' => 'location', 'key' => 'location', 'type' => 'point']),
                new Document(['$id' => 'route', 'key' => 'route', 'type' => 'linestring']),
                new Document(['$id' => 'area', 'key' => 'area', 'type' => 'polygon']),
            ],
        ]);
    }

    /**
     * @param array<mixed> $geometry
     */
    private function document(string $attribute, array $geometry): Document
    {
        return new Document([
            '$id' => 'place',
            '$permissions' => [],
            '$createdAt' => '2026-09-30 00:00:00.000',
            '$updatedAt' => '2026-09-30 00:00:00.000',
            $attribute => $geometry,
        ]);
    }

    private function adapter(): MariaDB
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (int|string $parameter, mixed $value): bool {
                $this->bound[] = $value;

                return true;
            });

            return $statement;
        });
        $pdo->method('lastInsertId')->willReturn('1');

        $adapter = new MariaDB($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
