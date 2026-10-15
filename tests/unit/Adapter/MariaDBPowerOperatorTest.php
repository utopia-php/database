<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Document;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;

final class MariaDBPowerOperatorTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<MariaDB>, mixed}>
     */
    public static function nonNumericExponents(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class] as $engine => $class) {
            yield $engine . ' word' => [$class, 'two'];
            yield $engine . ' boolean' => [$class, true];
            yield $engine . ' list' => [$class, [2]];
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('nonNumericExponents')]
    public function testPowerWithANonNumericExponentIsRefusedBeforeAStatementIsSent(string $class, mixed $exponent): void
    {
        $adapter = $this->adapter($class);

        try {
            $adapter->updateDocuments(
                new Document(['$id' => 'scores', 'attributes' => []]),
                new Document(['value' => new Operator(OperatorType::Power, 'value', [$exponent])]),
                [new Document(['$id' => 'first', '$sequence' => '1'])],
            );
            $this->fail('A power exponent that is not a number must be refused');
        } catch (OperatorException $error) {
            $this->assertSame('Power exponent must be numeric', $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>, int|float}>
     */
    public static function numericExponents(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'MySQL' => MySQL::class] as $engine => $class) {
            yield $engine . ' integer' => [$class, 2];
            yield $engine . ' float' => [$class, 0.5];
        }
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('numericExponents')]
    public function testPowerWithANumericExponentIsSentAsPower(string $class, int|float $exponent): void
    {
        $adapter = $this->adapter($class);

        $adapter->updateDocuments(
            new Document(['$id' => 'scores', 'attributes' => []]),
            new Document(['value' => Operator::power($exponent)]),
            [new Document(['$id' => 'first', '$sequence' => '1'])],
        );

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('POWER(COALESCE(`value`, 0)', $this->statements[0]);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    private function adapter(string $class): MariaDB
    {
        $statement = $this->createStub(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        $statement->method('rowCount')->willReturn(1);

        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statement): PDOStatement {
            $this->statements[] = $query;

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
