<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;

final class PostgresStatementTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /** @var list<string> */
    private array $sessionStatements = [];

    /**
     * @return iterable<string, array{int, Event}>
     */
    public static function nonPositiveTimeouts(): iterable
    {
        yield 'zero' => [0, Event::All];
        yield 'negative' => [-1, Event::All];
        yield 'zero for one event' => [0, Event::DocumentFind];
    }

    #[DataProvider('nonPositiveTimeouts')]
    public function testNonPositiveTimeoutIsRejected(int $milliseconds, Event $event): void
    {
        $adapter = $this->adapter();
        $adapter->setTimeout(400, Event::DocumentFind);

        try {
            $adapter->setTimeout($milliseconds, $event);
            $this->fail('A timeout that is not positive must be rejected');
        } catch (DatabaseException $error) {
            $this->assertSame('Timeout must be greater than 0', $error->getMessage());
        }

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(400, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame([], $this->sessionStatements);
    }

    public function testAFailedTimeoutResetAfterASuccessfulStatementReachesTheCaller(): void
    {
        $reset = new PDOException('reset failed');
        $adapter = $this->adapter(resetFailure: $reset);
        $adapter->setTimeout(250);

        $error = null;
        try {
            $adapter->rawQuery('SELECT 1');
        } catch (Throwable $caught) {
            $error = $caught;
        }
        $this->assertSame($reset, $error, 'A timeout left on the session must not be hidden');

        $this->assertSame(["SET statement_timeout = '250ms'", 'RESET statement_timeout'], $this->sessionStatements);
    }

    public function testAFailedStatementKeepsItsOwnErrorWhenTheTimeoutResetAlsoFails(): void
    {
        $adapter = $this->adapter(resetFailure: new PDOException('reset failed'), statementFailure: $this->engineError('22003', 'integer out of range'));
        $adapter->setTimeout(250);

        try {
            $adapter->rawQuery('SELECT 1');
            $this->fail('The failed statement must reach the caller');
        } catch (LimitException $error) {
            $this->assertSame('Numeric value out of range', $error->getMessage());
        }

        $this->assertSame(["SET statement_timeout = '250ms'", 'RESET statement_timeout'], $this->sessionStatements);
    }

    /**
     * @return iterable<string, array{string, string, string}>
     */
    public static function limitErrors(): iterable
    {
        yield 'numeric out of range' => ['22003', 'integer out of range', 'Numeric value out of range'];
        yield 'datetime overflow' => ['22008', 'timestamp out of range', 'Datetime field overflow'];
    }

    #[DataProvider('limitErrors')]
    public function testAnOutOfRangeWriteIsALimitError(string $state, string $message, string $expected): void
    {
        $adapter = $this->adapter(statementFailure: $this->engineError($state, $message));

        try {
            $adapter->updateDocuments(
                new Document(['$id' => 'scores', 'attributes' => []]),
                new Document(['value' => Operator::increment(1)]),
                [new Document(['$id' => 'first', '$sequence' => '1'])],
            );
            $this->fail('An out-of-range write must be refused');
        } catch (LimitException $error) {
            $this->assertSame($expected, $error->getMessage());
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }
    }

    /**
     * @return iterable<string, array{mixed}>
     */
    public static function nonNumericExponents(): iterable
    {
        yield 'word' => ['two'];
        yield 'boolean' => [true];
        yield 'list' => [[2]];
    }

    #[DataProvider('nonNumericExponents')]
    public function testPowerWithANonNumericExponentIsRefusedBeforeAStatementIsSent(mixed $exponent): void
    {
        try {
            $this->adapter()->updateDocuments(
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

    public function testPowerWithANumericExponentIsSentAsPower(): void
    {
        $this->adapter()->updateDocuments(
            new Document(['$id' => 'scores', 'attributes' => []]),
            new Document(['value' => Operator::power(3)]),
            [new Document(['$id' => 'first', '$sequence' => '1'])],
        );

        $this->assertCount(1, $this->statements);
        $this->assertStringContainsString('POWER(', $this->statements[0]);
    }

    private function engineError(string $state, string $message): PDOException
    {
        $error = new class ('SQLSTATE[' . $state . ']: ' . $message, $state) extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, 7, $message];

        return $error;
    }

    private function adapter(?PDOException $resetFailure = null, ?PDOException $statementFailure = null): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($statementFailure): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('rowCount')->willReturn(1);
            if ($statementFailure === null) {
                $statement->method('execute')->willReturn(true);
            } else {
                $statement->method('execute')->willThrowException($statementFailure);
            }

            return $statement;
        });
        $pdo->method('exec')->willReturnCallback(function (string $statement) use ($resetFailure): int {
            $this->sessionStatements[] = $statement;
            if ($resetFailure !== null && $statement === 'RESET statement_timeout') {
                throw $resetFailure;
            }

            return 0;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
