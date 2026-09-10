<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use stdClass;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\MySQL as MySQLSchema;
use Utopia\Query\Schema\PostgreSQL as PostgreSQLSchema;
use Utopia\Query\Schema\Table;

final class AddTableColumnTest extends TestCase
{
    public function testVectorOnMySQLTableThrowsDatabaseException(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector columns are only supported on PostgreSQL');

        $this->addTableColumn((new MySQLSchema())->table('t'), 'embedding', ColumnType::Vector, 3);
    }

    public function testVectorOnPostgreSQLTableSetsTypeAndDimensions(): void
    {
        $size = 4;
        $column = $this->addTableColumn((new PostgreSQLSchema())->table('t'), 'embedding', ColumnType::Vector, $size);

        $this->assertSame(ColumnType::Vector, $column->type);
        $this->assertSame($size, $column->dimensions);
    }

    public function testBigSerialOnMySQLTableIsAutoIncrement(): void
    {
        $column = $this->addTableColumn((new MySQLSchema())->table('t'), 'seq', ColumnType::BigSerial, 0);

        $this->assertSame(ColumnType::BigSerial, $column->type);
        $this->assertSame(true, $column->isAutoIncrement);
    }

    public function testBigSerialOnPostgreSQLTableIsAutoIncrement(): void
    {
        $column = $this->addTableColumn((new PostgreSQLSchema())->table('t'), 'seq', ColumnType::BigSerial, 0);

        $this->assertSame(ColumnType::BigSerial, $column->type);
        $this->assertSame(true, $column->isAutoIncrement);
    }

    public function testBigSerialOnBaseTableThrowsDatabaseException(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Serial columns are not supported on this dialect');

        $this->addTableColumn(new Table(), 'seq', ColumnType::BigSerial, 0);
    }

    private function addTableColumn(Table $table, string $id, ColumnType $type, int $size): Column
    {
        $adapter = new MariaDB(new stdClass());
        $method = new ReflectionMethod(SQL::class, 'addTableColumn');
        $column = $method->invoke($adapter, $table, $id, $type, $size);

        if (! $column instanceof Column) {
            $this->fail('addTableColumn did not return a Column');
        }

        return $column;
    }
}
