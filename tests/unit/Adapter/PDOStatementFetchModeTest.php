<?php

namespace Tests\Unit\Adapter;

use PDOException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\PDO;
use Utopia\Database\PDOStatement;

final class PDOStatementFetchModeTest extends TestCase
{
    private const string QUERY = 'SELECT 7 AS value, 8 AS other';

    public function testFetchModeIsKeptAcrossAReconnect(): void
    {
        $statement = new PDOStatement(new PDO('sqlite::memory:', null, null), $this->lostStatement(), self::QUERY);
        $statement->setFetchMode(\PDO::FETCH_NUM);

        $this->assertTrue($statement->execute());
        $this->assertSame([7, 8], $statement->fetch());
    }

    public function testFetchModeArgumentsAreKeptAcrossAReconnect(): void
    {
        $statement = new PDOStatement(new PDO('sqlite::memory:', null, null), $this->lostStatement(), self::QUERY);
        $statement->setFetchMode(\PDO::FETCH_COLUMN, 1);

        $this->assertTrue($statement->execute());
        $this->assertSame([8], $statement->fetchAll());
    }

    public function testWithoutAFetchModeTheReconnectedStatementUsesTheDefault(): void
    {
        $statement = new PDOStatement(new PDO('sqlite::memory:', null, null), $this->lostStatement(), self::QUERY);

        $this->assertTrue($statement->execute());
        $this->assertSame(['value' => 7, 0 => 7, 'other' => 8, 1 => 8], $statement->fetch());
    }

    private function lostStatement(): \PDOStatement
    {
        $lost = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $lost->method('setFetchMode')->willReturn(true);
        $lost->expects($this->once())
            ->method('execute')
            ->willThrowException(new PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away'));

        return $lost;
    }
}
