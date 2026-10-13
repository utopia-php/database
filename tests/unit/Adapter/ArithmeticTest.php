<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Redis as RedisClient;
use stdClass;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Arithmetic;
use Utopia\Database\Arithmetic\Rounding;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Pools\Pool as UtopiaPool;

final class ArithmeticTest extends TestCase
{
    /**
     * @return iterable<string, array{callable(): Adapter, list<mixed>}>
     */
    public static function adapters(): iterable
    {
        $mariaDB = [Rounding::HalfEven, Rounding::HalfEven, Rounding::HalfAwayFromZero, Rounding::HalfEven, false, [], []];

        yield 'MariaDB' => [static fn (): Adapter => new MariaDB(new stdClass()), $mariaDB];
        yield 'MySQL' => [static fn (): Adapter => new MySQL(new stdClass()), $mariaDB];
        yield 'SQLite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:')), $mariaDB];
        yield 'Memory' => [static fn (): Adapter => new Memory(), $mariaDB];
        yield 'Redis' => [static fn (): Adapter => new Redis(new RedisClient()), $mariaDB];
        yield 'PostgreSQL' => [static fn (): Adapter => new Postgres(new stdClass()), [
            Rounding::HalfEven,
            Rounding::HalfAwayFromZero,
            Rounding::HalfAwayFromZero,
            Rounding::HalfEven,
            true,
            [OperatorType::Multiply, OperatorType::Divide, OperatorType::Modulo],
            [OperatorType::Increment, OperatorType::Decrement, OperatorType::Multiply, OperatorType::Divide],
        ]];
        yield 'MongoDB' => [self::mongo(...), [Rounding::TowardZero, Rounding::TowardZero, Rounding::TowardZero, Rounding::TowardZero, false, [], []]];
    }

    /**
     * @param  callable(): Adapter  $adapter
     * @param  list<mixed>  $expected
     */
    #[DataProvider('adapters')]
    public function testEachAdapterDeclaresHowItsEngineStoredAFractionalResult(callable $adapter, array $expected): void
    {
        $this->assertSame($expected, self::describe($adapter()->arithmetic()));
    }

    /**
     * @param  callable(): Adapter  $adapter
     * @param  list<mixed>  $expected
     */
    #[DataProvider('adapters')]
    public function testAPoolAnswersForItsPooledAdapter(callable $adapter, array $expected): void
    {
        $pool = new Pool($this->connections($adapter()));
        $pool->setAuthorization(new Authorization());

        $this->assertSame($expected, self::describe($pool->arithmetic()));
        $this->assertSame($expected, self::describe($pool->arithmetic()));
    }

    /**
     * @param  callable(): Adapter  $adapter
     * @param  list<mixed>  $expected
     */
    #[DataProvider('adapters')]
    public function testAReadWritePoolAnswersForItsPooledAdapter(callable $adapter, array $expected): void
    {
        $pool = new ReadWritePool($this->connections($adapter()), $this->connections($adapter()));
        $pool->setAuthorization(new Authorization());

        $this->assertSame($expected, self::describe($pool->arithmetic()));
    }

    /**
     * @return iterable<string, array{OperatorType, bool, Rounding}>
     */
    public static function methods(): iterable
    {
        yield 'a power' => [OperatorType::Power, false, Rounding::TowardZero];
        yield 'a power to a whole exponent' => [OperatorType::Power, true, Rounding::TowardZero];
        yield 'a division by a whole number' => [OperatorType::Divide, true, Rounding::HalfAwayFromZero];
        yield 'a division by a fraction' => [OperatorType::Divide, false, Rounding::HalfEven];
        yield 'a multiplication by a whole number' => [OperatorType::Multiply, true, Rounding::HalfEven];
        yield 'an increment' => [OperatorType::Increment, false, Rounding::HalfEven];
        yield 'a modulo' => [OperatorType::Modulo, false, Rounding::HalfEven];
    }

    #[DataProvider('methods')]
    public function testAnOperatorIsRoundedByItsMethodAndOperand(OperatorType $method, bool $whole, Rounding $expected): void
    {
        $arithmetic = new Arithmetic(
            counter: Rounding::TowardZero,
            operators: Rounding::HalfEven,
            division: Rounding::HalfAwayFromZero,
            power: Rounding::TowardZero,
        );

        $this->assertSame($expected, $arithmetic->rounding($method, $whole));
    }

    /**
     * @return list<mixed>
     */
    private static function describe(Arithmetic $arithmetic): array
    {
        return [
            $arithmetic->counter,
            $arithmetic->operators,
            $arithmetic->division,
            $arithmetic->power,
            $arithmetic->refusesCounterFractions,
            $arithmetic->exact,
            $arithmetic->refusedWithoutLimit,
        ];
    }

    private static function mongo(): Mongo
    {
        return new Mongo(new class () extends Client {
            public function __construct()
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }
        });
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return $connections;
    }
}
