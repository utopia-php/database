<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\BigInt;
use Utopia\Mongo\Client;
use Utopia\Query\Schema\ColumnType;

final class BigIntegerTest extends TestCase
{
    public function testUnsignedBoundsAndArithmeticStayExact(): void
    {
        $bounds = Attribute::bigInteger(key: 'total', signed: false)->bounds();

        $this->assertNotNull($bounds);
        $this->assertSame('18446744073709551615', $bounds->max);
        $this->assertSame('9223372036854775808', BigInt::add(PHP_INT_MAX, 1));
        $this->assertSame(PHP_INT_MAX, BigInt::subtract('9223372036854775808', 1));
        $this->assertSame('18446744073709551615', BigInt::add('18446744073709551614', 1));
        $this->assertSame('18446744073709551616', BigInt::add(BigInt::UNSIGNED_MAX, 1));
        $this->assertSame('18446744073709551614', BigInt::subtract(BigInt::UNSIGNED_MAX, 1));
        $this->assertSame('18446744073709551614', BigInt::multiply('9223372036854775807', 2));
        $this->assertSame(1, BigInt::modulo(BigInt::UNSIGNED_MAX, 2));
    }

    public function testSqlColumnTypesMapBigIntegerAndLegacyMetadata(): void
    {
        $mariaDB = new MariaDB(new \stdClass());
        $postgres = new Postgres(new \stdClass());

        $legacy = Attribute::fromDocument(new Document([
            '$id' => 'total',
            'key' => 'total',
            'type' => 'bigint',
            'size' => 9999,
        ]));

        $this->assertSame('BIGINT', $mariaDB->getColumnType(Attribute::bigInteger(key: 'total')));
        $this->assertSame('BIGINT', $mariaDB->getColumnType($legacy));
        $this->assertSame('BIGINT UNSIGNED', $mariaDB->getColumnType(Attribute::bigInteger(key: 'total', signed: false)));
        $this->assertSame('BIGINT', $postgres->getColumnType(Attribute::bigInteger(key: 'total')));
        $this->assertSame('BIGINT', $postgres->getColumnType($legacy));
    }

    public function testCastingNormalizesLegacyBigIntegerWithoutPrecisionLoss(): void
    {
        /** @var Adapter&Stub $adapter */
        $adapter = self::createStub(Adapter::class);
        $adapter->method('hasFeature')->willReturn(false);
        $database = new Database($adapter, new Cache(new None()));
        $collection = new Document([
            'attributes' => [
                [
                    '$id' => 'signed',
                    'type' => 'bigint',
                    'array' => false,
                    'signed' => true,
                ],
                [
                    '$id' => 'unsigned',
                    'type' => ColumnType::BigInteger->value,
                    'array' => false,
                    'signed' => false,
                ],
            ],
        ]);
        $document = new Document([
            'signed' => '9223372036854775807',
            'unsigned' => '18446744073709551615',
        ]);

        $result = $database->casting($collection, $document);

        $this->assertSame(PHP_INT_MAX, $result->getAttribute('signed'));
        $this->assertSame('18446744073709551615', $result->getAttribute('unsigned'));
    }

    public function testSQLiteCreatesAndReadsBigIntegerColumn(): void
    {
        $adapter = new SQLite(new \PDO('sqlite::memory:'));
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $collection = new Document([
            '$id' => 'bigints',
            'attributes' => [new Document([
                '$id' => 'value',
                'type' => ColumnType::BigInteger->value,
                'array' => false,
            ])],
        ]);

        $this->assertTrue($adapter->createCollection('bigints', [
            Attribute::bigInteger(key: 'value'),
        ]));
        $adapter->createDocument($collection, new Document([
            '$id' => 'maximum',
            '$permissions' => [],
            'value' => PHP_INT_MAX,
        ]));

        $stored = $adapter->getDocument($collection, 'maximum')->getAttribute('value');
        $this->assertTrue(\is_int($stored) || \is_string($stored));
        $this->assertSame((string) PHP_INT_MAX, (string) $stored);
    }

    public function testMemoryOperatorPreservesUnsignedIntegerStrings(): void
    {
        $adapter = new class () extends Memory {
            public function apply(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };

        $this->assertSame(
            '9223372036854775808',
            $adapter->apply(PHP_INT_MAX, Operator::increment(1)),
        );
        $this->assertSame(
            '18446744073709551615',
            $adapter->apply('18446744073709551614', Operator::increment(1)),
        );
        $this->assertSame(
            PHP_INT_MAX,
            $adapter->apply('9223372036854775808', Operator::decrement(1)),
        );
    }

    public function testMemoryAndRedisOperatorsLeaveValuesUnchangedWhenBoundsAreCrossed(): void
    {
        $memory = new class () extends Memory {
            public function apply(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };
        $redis = new class (self::createStub(\Redis::class)) extends RedisAdapter {
            public function apply(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };

        $cases = [
            [10, Operator::increment(100, 50), 10],
            [5.0, Operator::decrement(10, 0), 5.0],
            [10, Operator::multiply(10, 75), 10],
            [100.0, Operator::divide(-4, -10), 100.0],
            [20.0, Operator::divide(-2, -50), -10.0],
            [80.0, Operator::multiply(0.5, 50), 40.0],
            [52.0, Operator::increment(-5, 50), 47.0],
            [10, Operator::increment(5, 15), 15],
            [10, Operator::decrement(5, 5), 5],
            [-10.0, Operator::multiply(-2, 50), 20.0],
            [5.0, Operator::power(3, 100), 5.0],
            [100.0, Operator::power(0.5, 50), 10.0],
            [-4.0, Operator::power(2, 20), 16.0],
            [-2.0, Operator::power(3, 100), -8.0],
            [0.0, Operator::power(-1, 100), 0.0],
            [-4.0, Operator::power(0.5, 100), -4.0],
            [PHP_INT_MAX, Operator::increment(2, PHP_INT_MAX), PHP_INT_MAX],
            [PHP_INT_MAX, Operator::increment(1, BigInt::UNSIGNED_MAX), '9223372036854775808'],
        ];

        foreach ([$memory, $redis] as $adapter) {
            foreach ($cases as [$current, $operator, $expected]) {
                $this->assertSame($expected, $adapter->apply($current, $operator));
            }
        }
    }

    public function testMemoryAndRedisRejectUnboundedInvalidPowers(): void
    {
        $adapters = [
            new class () extends Memory {
                public function apply(mixed $current, Operator $operator): mixed
                {
                    return $this->applyOperator($current, $operator);
                }
            },
            new class (self::createStub(\Redis::class)) extends RedisAdapter {
                public function apply(mixed $current, Operator $operator): mixed
                {
                    return $this->applyOperator($current, $operator);
                }
            },
        ];

        foreach ($adapters as $adapter) {
            try {
                $adapter->apply(0.0, Operator::power(-1));
                $this->fail('Expected invalid power to throw');
            } catch (LimitException $exception) {
                $this->assertSame('Value out of range', $exception->getMessage());
            }
        }
    }

    public function testSqlBuilderPreservesUnsignedIntegerBindings(): void
    {
        $adapter = new class (new \stdClass()) extends MariaDB {
            /**
             * @return array{expression: string, bindings: list<mixed>}
             */
            public function expression(Operator $operator): array
            {
                return $this->getOperatorBuilderExpression('value', $operator);
            }
        };

        $result = $adapter->expression(Operator::increment(1, BigInt::UNSIGNED_MAX));

        $this->assertStringContainsString('CASE', $result['expression']);
        $this->assertSame([BigInt::UNSIGNED_MAX, 1, 1], $result['bindings']);
    }

    public function testMongoRejectsUnsignedArithmeticBeforeBsonCoercion(): void
    {
        $adapter = new Mongo(new class () extends Client {
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

        $this->expectException(TypeException::class);
        $this->expectExceptionMessage('outside the signed 64-bit integer range');
        $adapter->increaseDocumentAttribute('collection', 'document', 'value', BigInt::UNSIGNED_MAX, '2026-01-01T00:00:00.000+00:00');
    }

    public function testMemoryAndRedisHonourAFloatBoundOnTheExactBigIntegerPath(): void
    {
        $cases = [
            'increment above a float max' => [PHP_INT_MAX - 5, Operator::increment(10, 9.0e18), PHP_INT_MAX - 5],
            'increment above a whole float max' => [PHP_INT_MAX, Operator::increment(1, 100.0), PHP_INT_MAX],
            'increment onto a float max' => [PHP_INT_MAX, Operator::increment(1, 9223372036854775808.0), '9223372036854775808'],
            'increment below a float max' => [PHP_INT_MAX, Operator::increment(1, 1.0e19), '9223372036854775808'],
            'increment below a whole string max' => [PHP_INT_MAX, Operator::increment(1, '9223372036854775808.0'), '9223372036854775808'],
            'decrement below a float min' => [PHP_INT_MIN, Operator::decrement(1, -9.0e18), PHP_INT_MIN],
            'decrement above a float min' => [PHP_INT_MIN, Operator::decrement(1, -1.0e19), '-9223372036854775809'],
            'multiply above a float max' => [PHP_INT_MAX, Operator::multiply(2, 1.5e19), PHP_INT_MAX],
            'unbounded increment' => [PHP_INT_MAX, Operator::increment(1), '9223372036854775808'],
        ];

        foreach (['memory' => self::memoryOperators(), 'redis' => self::redisOperators()] as $name => $apply) {
            foreach ($cases as $case => [$current, $operator, $expected]) {
                $this->assertSame($expected, $apply($current, $operator), "{$name}: {$case}");
            }
        }
    }

    public function testMemoryAndRedisRefuseAFractionalBoundOnTheExactBigIntegerPath(): void
    {
        $cases = [
            'float max' => [Operator::increment(1, 102.4), 'Cannot apply increment operator: max/min limit must be a whole number, got 102.4'],
            'string max' => [Operator::increment(1, '9223372036854775808.5'), 'Cannot apply increment operator: max/min limit must be a whole number, got 9223372036854775808.5'],
            'float min' => [Operator::decrement(1, -0.5), 'Cannot apply decrement operator: max/min limit must be a whole number, got -0.5'],
        ];

        foreach (['memory' => self::memoryOperators(), 'redis' => self::redisOperators()] as $name => $apply) {
            foreach ($cases as $case => [$operator, $message]) {
                $current = $operator->getMethod() === OperatorType::Decrement ? PHP_INT_MIN : PHP_INT_MAX;
                try {
                    $apply($current, $operator);
                    $this->fail("{$name}: {$case} must be refused");
                } catch (OperatorException $exception) {
                    $this->assertSame($message, $exception->getMessage(), "{$name}: {$case}");
                }
            }
        }
    }

    public function testRedisOperatorPreservesUnsignedIntegerStrings(): void
    {
        $apply = self::redisOperators();

        $this->assertSame('9223372036854775808', $apply(PHP_INT_MAX, Operator::increment(1)));
        $this->assertSame('18446744073709551615', $apply('18446744073709551614', Operator::increment(1)));
        $this->assertSame(PHP_INT_MAX, $apply('9223372036854775808', Operator::decrement(1)));
    }

    public function testRedisKeepsTheStoredValueWhereAnOperatorCannotApply(): void
    {
        $apply = self::redisOperators();

        $this->assertSame(10.0, $apply(10.0, Operator::power(400, 1000)));
        $this->assertSame(10, $apply(10, new Operator(OperatorType::Divide, 'value', [0])));
        $this->assertSame(10, $apply(10, new Operator(OperatorType::Modulo, 'value', [0])));

        try {
            $apply(10.0, Operator::power(400));
            $this->fail('An unbounded power that overflows must throw');
        } catch (LimitException $exception) {
            $this->assertSame('Value out of range', $exception->getMessage());
        }
    }

    public function testMemoryKeepsTheStoredValueWhenABoundedPowerOverflows(): void
    {
        $apply = self::memoryOperators();

        $this->assertSame(10.0, $apply(10.0, Operator::power(400, 1000)));

        try {
            $apply(10.0, Operator::power(400));
            $this->fail('An unbounded power that overflows must throw');
        } catch (LimitException $exception) {
            $this->assertSame('Value out of range', $exception->getMessage());
        }
    }

    public function testMemoryOperatorsCoerceOperandsAndKeepUnparsableDates(): void
    {
        $apply = self::memoryOperators();

        $this->assertSame(3, $apply(1, Operator::increment('2')));
        $this->assertSame(3.75, $apply(1.5, Operator::multiply('2.5')));
        $this->assertSame('ab', $apply('ab', Operator::stringConcat(['x'])));
        $this->assertSame('y', $apply(['x'], Operator::stringConcat('y')));
        $this->assertSame('not-a-date', $apply('not-a-date', Operator::dateAddDays(1)));
        $this->assertSame('not-a-date', $apply('not-a-date', Operator::dateSubDays(1)));
    }

    public function testRedisKeepsAnUnparsableDate(): void
    {
        $apply = self::redisOperators();

        $this->assertSame('not-a-date', $apply('not-a-date', Operator::dateAddDays(1)));
        $this->assertSame('not-a-date', $apply('not-a-date', Operator::dateSubDays(1)));
    }

    /**
     * @return Closure(mixed, Operator): mixed
     */
    private static function memoryOperators(): Closure
    {
        $adapter = new class () extends Memory {
            public function apply(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };

        return $adapter->apply(...);
    }

    /**
     * @return Closure(mixed, Operator): mixed
     */
    private static function redisOperators(): Closure
    {
        $adapter = new class (self::createStub(\Redis::class)) extends RedisAdapter {
            public function apply(mixed $current, Operator $operator): mixed
            {
                return $this->applyOperator($current, $operator);
            }
        };

        return $adapter->apply(...);
    }
}
