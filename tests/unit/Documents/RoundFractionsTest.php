<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Tests\Unit\Support\FractionalEngine;
use Tests\Unit\Support\FractionalResults;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Arithmetic;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Mirror;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\BigInt;
use Utopia\Mongo\Client;

use function Swoole\Coroutine\run;

final class RoundFractionsTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string DOCUMENT = 'meter';

    /**
     * @return iterable<string, array{int|float|string|null, int|string|null, int|string|null}>
     */
    public static function integerBounds(): iterable
    {
        yield 'fractional' => [10.5, 10, 11];
        yield 'negative fractional' => [-2.5, -3, -2];
        yield 'just under a whole number' => [9.999, 9, 10];
        yield 'whole float' => [10.0, 10, 10];
        yield 'integer' => [10, 10, 10];
        yield 'fractional text' => ['10.5', 10, 11];
        yield 'integer text' => ['10', '10', '10'];
        yield 'beyond the native integer range' => [1.0e19, '10000000000000000000', '10000000000000000000'];
        yield 'fractional text beyond float precision' => ['9007199254740993.5', 9007199254740993, 9007199254740994];
        yield 'negative fractional text beyond float precision' => ['-9007199254740993.5', -9007199254740994, -9007199254740993];
        yield 'negative fractional text' => ['-5.5', -6, -5];
        yield 'negative fraction of one text' => ['-0.5', -1, 0];
        yield 'fraction of one text' => ['0.5', 0, 1];
        yield 'whole decimal text' => ['10.000', 10, 10];
        yield 'leading zeros text' => ['0000000000000000000005.5', 5, 6];
        yield 'fractional text above the native integer range' => ['9223372036854775807.5', \PHP_INT_MAX, '9223372036854775808'];
        yield 'fractional text below the native integer range' => ['-9223372036854775808.5', '-9223372036854775809', \PHP_INT_MIN];
        yield 'fractional text at the unsigned limit' => ['18446744073709551615.9', '18446744073709551615', '18446744073709551616'];
        yield 'exponent text' => ['1e3', 1000, 1000];
        yield 'capital exponent text' => ['1E3', 1000, 1000];
        yield 'fractional exponent text' => ['1.25e1', 12, 13];
        yield 'negative fractional exponent text' => ['-1.25e1', -13, -12];
        yield 'negative exponent text' => ['1e-3', 0, 1];
        yield 'negative value with a negative exponent text' => ['-1e-3', -1, 0];
        yield 'vanishing exponent text' => ['1e-999999999', 0, 1];
        yield 'negative value with a vanishing exponent text' => ['-1e-999999999', -1, 0];
        yield 'exponent beyond the native integer range text' => ['5e-9223372036854775808', 0, 1];
        yield 'zero exponent text beyond float precision' => ['9007199254740993.5e0', 9007199254740993, 9007199254740994];
        yield 'padded fractional text' => [' 10.5 ', 10, 11];
        yield 'fractional text padded with form feed and vertical tab' => ["\f10.5\v", 10, 11];
        yield 'signed fractional text' => ['+10.5', 10, 11];
        yield 'fraction without a whole part text' => ['.5', 0, 1];
        yield 'whole part without a fraction text' => ['5.', 5, 5];
        yield 'negative zero text' => ['-0.0', 0, 0];
        yield 'zero with a huge exponent text' => ['0e999999999', 0, 0];
        yield 'zero decimal with an exponent text' => ['0.000e50', 0, 0];
        yield 'negative zero with an exponent text' => ['-0e5', 0, 0];
        yield 'no bound' => [null, null, null];
    }

    #[DataProvider('integerBounds')]
    public function testAWholeChangeIsHeldAtTheWholeNumberAFractionalBoundRoundsTo(int|float|string|null $bound, int|string|null $maximum, int|string|null $minimum): void
    {
        $database = $this->database();

        if ($bound === null || $maximum === null || $minimum === null) {
            $this->create($database, ['big' => 5]);
            $this->assertSame(6, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, $bound)->getAttribute('big'));
            $this->assertSame(5, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, $bound)->getAttribute('big'));

            return;
        }

        $this->assertHeldAtMaximum($database, $bound, $maximum);
        $this->assertHeldAtMinimum($database, $bound, $minimum);
    }

    public function testAnIntegerAttributeRoundsItsBoundsLikeABigInteger(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 9], 'up');
        $this->create($database, ['count' => 12], 'down');

        $this->assertSame(10, $database->increaseDocumentAttribute(self::COLLECTION, 'up', 'count', 1, 10.5)->getAttribute('count'));
        $this->assertLimited(fn () => $database->increaseDocumentAttribute(self::COLLECTION, 'up', 'count', 1, 10.5));
        $this->assertSame(11, $database->decreaseDocumentAttribute(self::COLLECTION, 'down', 'count', 1, 10.5)->getAttribute('count'));
        $this->assertLimited(fn () => $database->decreaseDocumentAttribute(self::COLLECTION, 'down', 'count', 1, 10.5));

        $this->assertSame(10, $this->stored($database, 'count', 'up'));
        $this->assertSame(11, $this->stored($database, 'count', 'down'));
    }

    /**
     * @return iterable<string, array{string, class-string<Throwable>|null}>
     */
    public static function unroundedAttributes(): iterable
    {
        yield 'double' => ['ratio', null];
        yield 'integer array' => ['scores', TypeException::class];
        yield 'undeclared' => ['missing', NotFoundException::class];
    }

    /**
     * @param  class-string<Throwable>|null  $refusal
     */
    #[DataProvider('unroundedAttributes')]
    public function testOtherAttributesKeepTheirBoundsAndChangeValues(string $attribute, ?string $refusal): void
    {
        $database = $this->database();
        $this->create($database, ['ratio' => 5.25]);

        if ($refusal !== null) {
            $this->expectException($refusal);
        }

        $this->assertSame(6.75, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, 1.5, 6.8)->getAttribute($attribute));
        $this->assertSame(5.25, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, 1.5, 5.2)->getAttribute($attribute));
        $this->assertSame(5.25, $this->stored($database, $attribute));
    }

    public function testBoundsTheLibraryRefusesAnywayArePassedOnUnchanged(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 5]);

        $this->assertRefused('Max must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, \INF));
        $this->assertRefused('Min must be an integer.', fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 'ten'));
        $this->assertSame(5, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function nonNumericBounds(): iterable
    {
        yield 'empty' => [''];
        yield 'exponent without digits' => ['1e'];
        yield 'hexadecimal' => ['0x1A'];
        yield 'digit separators' => ['1_000'];
        yield 'two decimal points' => ['1.2.3'];
        yield 'two signs' => ['--5'];
    }

    #[DataProvider('nonNumericBounds')]
    public function testNonNumericTextBoundsArePassedOnUnchanged(string $bound): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 5]);

        $this->assertRefused('Max must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, $bound));
        $this->assertRefused('Min must be an integer.', fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, $bound));
        $this->assertRefused('Max must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5, $bound));
        $this->assertSame(5, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{int|float|string, int|null}>
     */
    public static function integerChanges(): iterable
    {
        yield 'integer' => [2, 7];
        yield 'whole float' => [2.0, 7];
        yield 'integer text' => ['2', 7];
        yield 'fractional' => [1.5, 6];
        yield 'fractional text' => ['1.5', 6];
        yield 'infinite' => [\INF, null];
    }

    #[DataProvider('integerChanges')]
    public function testAnIntegerAttributeStoresEveryFiniteChangeAsAWholeNumber(int|float|string $value, ?int $stored): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 5]);

        if ($stored === null) {
            $this->assertRefused('Change value must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', $value));
            $this->assertSame(5, $this->stored($database, 'count'));

            return;
        }

        $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', $value);

        $this->assertSame($stored, $this->stored($database, 'count'));
    }

    public function testAWholeChangeValueOnAnIntegerAttributeIsPassedAsAnInteger(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 8]);

        $this->assertSame(10, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 2)->getAttribute('count'));
        $this->assertSame(12, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', '2')->getAttribute('count'));
        $this->assertSame(12, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{string, int|null, string|null, float|null, int|float|null}>
     */
    public static function metadataAttributes(): iterable
    {
        yield 'integer' => ['count', 5, null, 6.5, 6];
        yield 'big integer as stored' => ['big', 5, null, 6.5, 6];
        yield 'double' => ['ratio', 5, null, 6.5, 6.5];
        yield 'integer array' => ['scores', null, 'Attribute value must be numeric.', null, null];
        yield 'undeclared' => ['missing', null, null, 1.5, 1.5];
    }

    #[DataProvider('metadataAttributes')]
    public function testCollectionMetadataDeclaresTheIntegerAttributesWithoutDefinedAttributes(string $attribute, ?int $start, ?string $refusal, ?float $returned, int|float|null $stored): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes,
                ));
            }
        });
        $this->create($database, $start === null ? [] : [$attribute => $start]);
        $increase = fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, 1.5, 10.5);

        if ($refusal !== null) {
            $this->assertRefused($refusal, $increase);

            return;
        }

        $this->assertSame($returned, $increase()->getAttribute($attribute));
        $this->assertSame($stored, $this->stored($database, $attribute), 'Only a declared single-valued integer stores the change rounded');
    }

    public function testAMissingCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->increaseDocumentAttribute('missing', self::DOCUMENT, 'count', 1.5, 10.5);
    }

    /**
     * MariaDB and MySQL on database 7.4.1: the exact result is returned and the column stores it rounded half to
     * even.
     *
     * @return iterable<string, array{bool, string, int, float, int|float|null, float, int}>
     */
    public static function fractionalChanges(): iterable
    {
        yield 'increase rounds a half down to even' => [true, 'count', 5, 1.5, null, 6.5, 6];
        yield 'increase rounds a half up to even' => [true, 'count', 6, 1.5, null, 7.5, 8];
        yield 'increase rounds below a half down' => [true, 'count', 5, 1.4, null, 6.4, 6];
        yield 'increase rounds above a half up' => [true, 'count', 5, 1.6, null, 6.6, 7];
        yield 'increase rounds a negative half to even' => [true, 'count', -10, 2.5, null, -7.5, -8];
        yield 'increase by a half that rounds back to the value' => [true, 'count', 4, 0.5, null, 4.5, 4];
        yield 'increase by a half that rounds up' => [true, 'count', 5, 0.5, null, 5.5, 6];
        yield 'increase within a fractional maximum' => [true, 'count', 5, 1.5, 6.5, 6.5, 6];
        yield 'increase within a whole maximum' => [true, 'count', 5, 1.5, 7, 6.5, 6];
        yield 'increase a big integer' => [true, 'big', 5, 2.5, null, 7.5, 8];
        yield 'decrease rounds a half up to even' => [false, 'count', 5, 1.5, null, 3.5, 4];
        yield 'decrease rounds a half down to even' => [false, 'count', 6, 1.5, null, 4.5, 4];
        yield 'decrease by less than a half keeps the value' => [false, 'count', 5, 0.4, null, 4.6, 5];
        yield 'decrease within a fractional minimum' => [false, 'count', 5, 1.5, 3.5, 3.5, 4];
        yield 'a whole float change returns a float' => [true, 'count', 8, 2.0, null, 10.0, 10];
    }

    #[DataProvider('fractionalChanges')]
    public function testAFractionalChangeOnAnIntegerMatchesTheEngine(bool $increase, string $attribute, int $start, float $value, int|float|null $bound, float $returned, int $stored): void
    {
        $database = $this->database();
        $this->create($database, [$attribute => $start]);

        $document = $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value, $bound)
            : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value, $bound);

        $this->assertSame($returned, $document->getAttribute($attribute), 'The exact result of a fractional change is returned');
        $this->assertSame($stored, $this->stored($database, $attribute), 'The engine stored the change rounded half to even');
    }

    /**
     * @return iterable<string, array{bool, int}>
     */
    public static function fractionalChangesPastTheirBound(): iterable
    {
        yield 'increase past the maximum' => [true, 6];
        yield 'decrease past the minimum' => [false, 4];
    }

    #[DataProvider('fractionalChangesPastTheirBound')]
    public function testAFractionalChangePastItsBoundIsRefusedAndRolledBack(bool $increase, int $bound): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 5]);

        try {
            $increase
                ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5, $bound)
                : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5, $bound);
            $this->fail('A fractional change past its bound must be refused');
        } catch (LimitException $exception) {
            $this->assertSame('Attribute value exceeds '.($increase ? 'maximum' : 'minimum').' limit: '.$bound, $exception->getMessage());
        }

        $this->assertSame(5, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{bool, int|string, int|string|null}>
     */
    public static function boundsBeyondFloatPrecision(): iterable
    {
        yield 'an increase past a maximum' => [true, 9007199254740993, null];
        yield 'an increase past a maximum given as text' => [true, '9007199254740993', null];
        yield 'a decrease past a minimum' => [false, 9007199254740993, null];
        yield 'an increase within a maximum' => [true, 9007199254740994, 9007199254740994];
        yield 'a decrease within a minimum' => [false, 9007199254740992, 9007199254740992];
    }

    #[DataProvider('boundsBeyondFloatPrecision')]
    public function testAFractionalChangeIsHeldToABoundBeyondFloatPrecisionExactly(bool $increase, int|string $bound, int|string|null $stored): void
    {
        $database = $this->database();
        $this->create($database, ['big' => 9007199254740993]);
        $change = fn (): Document => $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 0.5, $bound)
            : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 0.5, $bound);

        if ($stored === null) {
            $this->assertLimited($change);
            $this->assertSame(9007199254740993, $this->stored($database, 'big'));

            return;
        }

        $change();
        $this->assertSame($stored, $this->stored($database, 'big'));
    }

    /**
     * @return iterable<string, array{FractionalEngine}>
     */
    public static function engines(): iterable
    {
        foreach (FractionalEngine::cases() as $engine) {
            yield $engine->name => [$engine];
        }
    }

    #[DataProvider('engines')]
    public function testAWholeChangeWrittenWithAnExponentChangesByItsValue(FractionalEngine $engine): void
    {
        $database = $this->database(self::engine($engine));
        $this->create($database, ['count' => 5]);

        $this->assertSame(1005.0, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', '1e3', '2e3')->getAttribute('count'));
        $this->assertSame(980.0, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', '2.50e1')->getAttribute('count'));
        $this->assertSame(980, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{FractionalEngine, Operator}>
     */
    public static function nonFiniteOperators(): iterable
    {
        foreach (FractionalEngine::cases() as $engine) {
            yield "{$engine->name}: an infinite factor under a fractional maximum" => [$engine, Operator::multiply(\INF, 10.5)];
            yield "{$engine->name}: a fractional factor under a not-a-number maximum" => [$engine, Operator::multiply(1.5, \NAN)];
            yield "{$engine->name}: a fractional increment under an infinite maximum" => [$engine, Operator::increment(1.5, \INF)];
            yield "{$engine->name}: a fractional decrement above a negative infinite minimum" => [$engine, Operator::decrement(1.5, -\INF)];
        }
    }

    #[DataProvider('nonFiniteOperators')]
    public function testANonFiniteOperandOrLimitIsRefused(FractionalEngine $engine, Operator $operator): void
    {
        $database = $this->database(self::engine($engine));
        $this->create($database, ['count' => 10]);

        $this->assertStructureRefused(fn () => $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['count' => $operator])));
        $this->assertStructureRefused(fn () => $database->updateDocuments(self::COLLECTION, new Document(['count' => clone $operator])));
        $this->assertSame(10, $this->stored($database, 'count'));
    }

    public function testAFractionalChangeOnAMissingDocumentIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->increaseDocumentAttribute(self::COLLECTION, 'missing', 'count', 1.5);
    }

    /**
     * @return iterable<string, array{int|float}>
     */
    public static function nonPositiveChanges(): iterable
    {
        yield 'zero' => [0];
        yield 'negative' => [-1];
        yield 'negative fraction' => [-1.5];
    }

    #[DataProvider('nonPositiveChanges')]
    public function testANonPositiveChangeIsATypeError(int|float $value): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 5]);

        $this->assertRefused('Value must be numeric and greater than 0', fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', $value));
        $this->assertSame(5, $this->stored($database, 'count'));
    }

    public function testAFractionalChangeOnADoubleIsPassedThrough(): void
    {
        $database = $this->database();
        $this->create($database, ['ratio' => 5.25]);

        $this->assertSame(6.75, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 1.5)->getAttribute('ratio'));
        $this->assertSame(6.75, $this->stored($database, 'ratio'));
    }

    public function testAFractionalMaximumOnAnIntegerBoundsTheIncrementAtItsWholePart(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 8]);

        $this->assertSame(10, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 2, 10.5)->getAttribute('count'));
        $this->assertLimited(fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 10.5));
        $this->assertSame(10, $this->stored($database, 'count'));
    }

    public function testAFractionalMinimumOnAnIntegerBoundsTheDecrementAtItsWholePart(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 10]);

        $this->assertSame(3, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 7, 2.5)->getAttribute('count'));
        $this->assertLimited(fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 2.5));
        $this->assertSame(3, $this->stored($database, 'count'));
    }

    public function testWithoutTheModeTheLibraryRefusesTheFractionalBoundItself(): void
    {
        $database = $this->database(round: false);
        $this->create($database, ['count' => 8]);

        $this->assertRefused('Max must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 10.5));
        $this->assertSame(8, $this->stored($database, 'count'));
    }

    /**
     * @return iterable<string, array{int, float, bool, int}>
     */
    public static function fractionalChangesBeyondFloatPrecision(): iterable
    {
        yield 'increase rounds half to even' => [9007199254740993, 1.5, true, 9007199254740994];
        yield 'increase rounds up past the half' => [9007199254740993, 1.75, true, 9007199254740995];
        yield 'increase rounds down below the half' => [9007199254740993, 1.25, true, 9007199254740994];
        yield 'decrease rounds half to even' => [9007199254740995, 1.5, false, 9007199254740994];
        yield 'decrease rounds down past the half' => [9007199254740995, 1.75, false, 9007199254740993];
        yield 'decrease rounds up below the half' => [9007199254740995, 1.25, false, 9007199254740994];
    }

    #[DataProvider('fractionalChangesBeyondFloatPrecision')]
    public function testAFractionalChangeOnABigIntegerBeyondFloatPrecisionStoresTheExactRoundedResult(int $before, float $change, bool $increase, int $expected): void
    {
        $database = $this->database();
        $this->create($database, ['big' => $before]);

        $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', $change)
            : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', $change);

        $this->assertSame($expected, $this->stored($database, 'big'));
    }

    public function testAFractionalTextMaximumBeyondFloatPrecisionBoundsTheIncrementAtItsWholePart(): void
    {
        $database = $this->database();
        $this->create($database, ['big' => 9007199254740992]);

        $this->assertSame(9007199254740993, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '9007199254740993.5')->getAttribute('big'));
        $this->assertLimited(fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '9007199254740993.5'));
        $this->assertSame(9007199254740993, $this->stored($database, 'big'));
    }

    public function testAFractionalTextMinimumBeyondFloatPrecisionBoundsTheDecrementAtItsWholePart(): void
    {
        $database = $this->database();
        $this->create($database, ['big' => -9007199254740992]);

        $this->assertSame(-9007199254740993, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '-9007199254740993.5')->getAttribute('big'));
        $this->assertLimited(fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '-9007199254740993.5'));
        $this->assertSame(-9007199254740993, $this->stored($database, 'big'));
    }

    public function testAMaximumTextBeyondTheIntegerRangeAllowsTheIncrement(): void
    {
        $database = $this->database();
        $this->create($database, ['big' => 5]);

        $this->assertSame(6, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '1e400')->getAttribute('big'));
    }

    public function testAMaximumTextBelowTheIntegerRangeRefusesTheIncrement(): void
    {
        $database = $this->database();
        $this->create($database, ['big' => 5]);

        $this->assertLimited(fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 1, '-1e400'));
        $this->assertSame(5, $this->stored($database, 'big'));
    }

    public function testAMinimumTextWithAHugeExponentAllowsTheDecrement(): void
    {
        $database = $this->database();
        $this->create($database, ['big' => 5]);

        $this->assertSame(-5, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', 10, '-1e999999999')->getAttribute('big'));
    }

    /**
     * @return iterable<string, array{FractionalEngine, bool, int, float, int|float|null, int|null}>
     */
    public static function counters(): iterable
    {
        return FractionalResults::counters();
    }

    #[DataProvider('counters')]
    public function testAFractionalCounterChangeStoresWhatTheEngineStored(FractionalEngine $engine, bool $increase, int $start, float $value, int|float|null $bound, ?int $stored): void
    {
        $database = $this->database(self::engine($engine));
        $this->create($database, ['big' => $start]);
        $change = fn (): Document => $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', $value, $bound)
            : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'big', $value, $bound);

        if ($stored === null) {
            $this->assertRefused('Change value must be an integer.', $change);
            $this->assertSame($start, $this->stored($database, 'big'));

            return;
        }

        $this->assertSame($increase ? $start + $value : $start - $value, $change()->getAttribute('big'));
        $this->assertSame($stored, $this->stored($database, 'big'));
    }

    /**
     * PostgreSQL on 7.4.1 compared the result with a bound in PHP first, then bound the bound next to an integer
     * column, which it read as an integer and refused when fractional.
     *
     * @return iterable<string, array{bool, int|float, int|float, string|null}>
     */
    public static function postgresBounds(): iterable
    {
        yield 'a whole change within a fractional maximum' => [true, 2, 10.5, 'Max must be an integer.'];
        yield 'a whole change past a fractional maximum' => [true, 3, 10.5, null];
        yield 'a whole change within a fractional minimum' => [false, 2, 5.5, 'Min must be an integer.'];
        yield 'a whole change past a fractional minimum' => [false, 3, 5.5, null];
        yield 'a fractional change past a whole maximum' => [true, 2.5, 10, null];
        yield 'a fractional change past a whole minimum' => [false, 2.5, 6, null];
        yield 'a fractional change within a fractional maximum' => [true, 1.5, 10.5, 'Change value must be an integer.'];
    }

    #[DataProvider('postgresBounds')]
    public function testPostgresRefusesAFractionalBoundAfterTheLimitCheck(bool $increase, int|float $value, int|float $bound, ?string $refusal): void
    {
        $database = $this->database(self::engine(FractionalEngine::PostgreSQL));
        $this->create($database, ['count' => 8]);
        $change = fn (): Document => $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', $value, $bound)
            : $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', $value, $bound);

        $refusal === null ? $this->assertLimited($change) : $this->assertRefused($refusal, $change);
        $this->assertSame(8, $this->stored($database, 'count'));
    }

    public function testPostgresStoresAWholeChangeUnderAWholeBound(): void
    {
        $database = $this->database(self::engine(FractionalEngine::PostgreSQL));
        $this->create($database, ['count' => 8]);

        $this->assertSame(10.0, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 2.0, 10)->getAttribute('count'));
        $this->assertSame(7, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 3, 7)->getAttribute('count'));
        $this->assertSame(7, $this->stored($database, 'count'));
    }

    public function testAChangeThatRoundsBackToTheValueStillWritesAndFiresItsEvents(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 4]);
        $before = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();
        $recorder = new class () implements Lifecycle {
            /** @var list<Event> */
            public array $events = [];

            #[\Override]
            public function handle(Domain $event): void
            {
                $this->events[] = $event->event;
            }
        };
        $database->addHook($recorder);

        $document = $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 0.5);

        $this->assertSame([Event::DocumentPurge, Event::DocumentIncrease], $recorder->events);
        $this->assertSame(4.5, $document->getAttribute('count'));
        $this->assertSame(4, $this->stored($database, 'count'));
        $this->assertNotSame($before, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt(), 'The write still bumps $updatedAt');
    }

    /**
     * @return iterable<string, array{FractionalEngine, int, OperatorType, list<int|float>, int|null}>
     */
    public static function operators(): iterable
    {
        return FractionalResults::operators();
    }

    /**
     * @param  list<int|float>  $values
     */
    #[DataProvider('operators')]
    public function testAFractionalOperatorOnUpdateStoresWhatTheEngineStored(FractionalEngine $engine, int $stored, OperatorType $method, array $values, ?int $expected): void
    {
        $database = $this->database(self::engine($engine));
        $this->create($database, ['count' => $stored]);
        $update = fn (): Document => $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['count' => new Operator($method, 'count', $values)]));

        if ($expected === null) {
            $this->assertStructureRefused($update);
            $this->assertSame($stored, $this->stored($database, 'count'));

            return;
        }

        $this->assertSame($expected, $update()->getAttribute('count'));
        $this->assertSame($expected, $this->stored($database, 'count'));
    }

    public function testFractionalItemsAddedToAnIntegerArrayAreCutTowardZero(): void
    {
        $database = $this->database();
        $this->create($database, ['scores' => [1]]);

        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['scores' => Operator::arrayAppend([2.7, -3.5])]));
        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['scores' => Operator::arrayPrepend(['0.9'])]));
        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['scores' => Operator::arrayInsert(1, 4.2)]));

        $this->assertSame([0, 4, 1, 2, -3], $this->stored($database, 'scores'));
    }

    public function testABulkUpdateRoundsEachDocumentFromItsOwnStoredValue(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 10, 'scores' => []], 'first');
        $this->create($database, ['count' => 11, 'scores' => []], 'second');
        $this->create($database, ['count' => 12, 'scores' => []], 'third');
        $updated = [];

        $count = $database->updateDocuments(
            self::COLLECTION,
            new Document(['count' => Operator::increment(1.5), 'scores' => Operator::arrayAppend([7.9])]),
            onNext: function (Document $document) use (&$updated): void {
                $updated[$document->getId()] = $document->getAttribute('count');
            },
        );

        $this->assertSame(3, $count);
        $this->assertSame(['first' => 12, 'second' => 12, 'third' => 14], $updated);
        $this->assertSame(12, $this->stored($database, 'count', 'first'));
        $this->assertSame(12, $this->stored($database, 'count', 'second'));
        $this->assertSame(14, $this->stored($database, 'count', 'third'));
        $this->assertSame([7], $this->stored($database, 'scores', 'third'));
    }

    public function testAnUpsertAcrossTenantsRoundsEachDocumentFromItsOwnTenantsValue(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = (new Database(self::sqlite(), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('round_fractions')
            ->setNamespace('round_fractions_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true)
            ->setRoundFractions(true);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'count')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        foreach ([5 => 10, 6 => 11] as $tenant => $count) {
            $database->createDocument(self::COLLECTION, new Document([Document::ID => self::DOCUMENT, Document::TENANT => $tenant, 'count' => $count]));
        }

        $database->upsertDocuments(self::COLLECTION, [
            new Document([Document::ID => self::DOCUMENT, Document::TENANT => 5, 'count' => Operator::increment(1.5)]),
            new Document([Document::ID => self::DOCUMENT, Document::TENANT => 6, 'count' => Operator::increment(1.5)]),
        ]);

        $this->assertSame(12, $database->withTenant(5, fn (): mixed => $this->stored($database, 'count')));
        $this->assertSame(12, $database->withTenant(6, fn (): mixed => $this->stored($database, 'count')));
    }

    public function testABulkUpdateWithAFractionalLimitHoldsEachDocumentAtItsLimit(): void
    {
        $database = $this->database();
        $this->create($database, ['count' => 10], 'first');
        $this->create($database, ['count' => 12], 'second');

        $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(2, 13.9)]), [Query::orderAsc('count')]);

        $this->assertSame(12, $this->stored($database, 'count', 'first'));
        $this->assertSame(12, $this->stored($database, 'count', 'second'));
    }

    public function testABulkUpdateTheEngineRefusedWritesNothing(): void
    {
        $database = $this->database(self::engine(FractionalEngine::PostgreSQL));
        $this->create($database, ['count' => 10], 'first');
        $this->create($database, ['count' => 11], 'second');

        $this->assertStructureRefused(fn () => $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(1.5)])));

        $this->assertSame(10, $this->stored($database, 'count', 'first'));
        $this->assertSame(11, $this->stored($database, 'count', 'second'));
    }

    public function testAnUpsertRoundsAStoredDocumentFromItsValueAndANewOneFromZero(): void
    {
        $database = $this->database(self::sqlite());
        $this->create($database, ['count' => 10], 'first');
        $this->create($database, ['count' => 11], 'second');

        $upserted = $database->upsertDocument(self::COLLECTION, new Document(['$id' => 'first', 'count' => Operator::increment(1.5)]));
        $count = $database->upsertDocuments(self::COLLECTION, [
            new Document(['$id' => 'second', 'count' => Operator::increment(1.5)]),
            new Document(['$id' => 'third', 'count' => Operator::increment(2.5), 'scores' => Operator::arrayAppend([3.9])]),
        ]);

        $this->assertSame(2, $count);
        $this->assertSame(12, $upserted->getAttribute('count'));
        $this->assertSame(12, $this->stored($database, 'count', 'first'));
        $this->assertSame(12, $this->stored($database, 'count', 'second'));
        $this->assertSame(2, $this->stored($database, 'count', 'third'));
    }

    public function testAnUpsertTheEngineRefusedWritesNothing(): void
    {
        $database = $this->database(self::sqlite(self::arithmetic(FractionalEngine::PostgreSQL)));
        $this->create($database, ['count' => 10], 'first');

        $this->assertStructureRefused(fn () => $database->upsertDocuments(self::COLLECTION, [
            new Document(['$id' => 'second', 'count' => 4]),
            new Document(['$id' => 'first', 'count' => Operator::increment(1.5)]),
        ]));

        $this->assertSame(10, $this->stored($database, 'count', 'first'));
        $this->assertTrue($database->getDocument(self::COLLECTION, 'second')->isEmpty());
    }

    public function testWithoutTheModeEveryFractionIsRefusedAsBefore(): void
    {
        $database = $this->database(self::sqlite(), round: false);
        $this->create($database, ['count' => 10, 'scores' => []]);

        $this->assertRefused('Change value must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5));
        $this->assertRefused('Min must be an integer.', fn () => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 2.5));
        $this->assertRefused('Change value must be an integer.', fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 2.0));
        $this->assertStructureRefused(fn () => $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['count' => Operator::increment(1.5)])));
        $this->assertStructureRefused(fn () => $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['count' => Operator::increment(1, 10.5)])));
        $this->assertStructureRefused(fn () => $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['scores' => Operator::arrayAppend([1.5])])));
        $this->assertStructureRefused(fn () => $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::multiply(1.5)])));
        $this->assertStructureRefused(fn () => $database->upsertDocuments(self::COLLECTION, [new Document(['$id' => self::DOCUMENT, 'count' => Operator::increment(1.5)])]));

        $this->assertSame(10, $this->stored($database, 'count'));
        $this->assertSame([], $this->stored($database, 'scores'));
    }

    public function testTheModeIsScopedAndOffByDefault(): void
    {
        $database = $this->database(round: false);

        $this->assertFalse($database->isRoundingFractions());
        $this->assertTrue($database->withRoundFractions(true, $database->isRoundingFractions(...)));
        $this->assertFalse($database->isRoundingFractions());
        $this->assertTrue($database->setRoundFractions(true)->isRoundingFractions());
        $this->assertFalse($database->withRoundFractions(false, $database->isRoundingFractions(...)));
    }

    public function testAMirrorSetsTheModeOnItsSourceAndDestination(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source, $destination);

        $this->assertSame([true, true, true], $mirror->withRoundFractions(true, fn (): array => [
            $mirror->isRoundingFractions(),
            $source->isRoundingFractions(),
            $destination->isRoundingFractions(),
        ]));
        $this->assertFalse($source->isRoundingFractions());

        $mirror->setRoundFractions(true);

        $this->assertTrue($mirror->isRoundingFractions());
        $this->assertTrue($source->isRoundingFractions());
        $this->assertTrue($destination->isRoundingFractions());
    }

    public function testASnapshotCarriesTheModeIntoAChildCoroutine(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine-scoped toggles');
        }

        $database = $this->database(round: false);
        $this->create($database, ['count' => 5]);
        $outcomes = null;
        $hookFlags = Runtime::getHookFlags();

        try {
            run(function () use ($database, &$outcomes): void {
                $released = new Channel(1);
                $results = new Channel(1);

                $database->withRoundFractions(true, function () use ($database, $released, $results): void {
                    $snapshot = $database->snapshot();

                    Coroutine::create(function () use ($database, $snapshot, $released, $results): void {
                        $released->pop();
                        $results->push([
                            'unscoped' => $this->refusal(fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5)),
                            'scoped' => $database->withSnapshot($snapshot, fn (): ?string => $this->refusal(fn () => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5))),
                        ]);
                    });
                });

                $released->push(true);
                $outcomes = $results->pop(5);
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        $this->assertSame(['unscoped' => 'Change value must be an integer.', 'scoped' => null], $outcomes);
        $this->assertSame(6, $this->stored($database, 'count'), 'Only the change under the snapshot rounded 5 + 1.5');
        $this->assertFalse($database->isRoundingFractions());
    }

    private function database(?Adapter $adapter = null, bool $round = true): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = (new Database($adapter ?? self::engine(FractionalEngine::MariaDB), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('round_fractions')
            ->setNamespace('round_fractions_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
                Attribute::double(key: 'ratio'),
                Attribute::integer(key: 'scores', array: true),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));

        return $database->setRoundFractions($round);
    }

    private static function engine(FractionalEngine $engine): Memory
    {
        return new class (self::arithmetic($engine)) extends Memory {
            public function __construct(private readonly Arithmetic $declared)
            {
                parent::__construct();
            }

            #[\Override]
            public function arithmetic(): Arithmetic
            {
                return $this->declared;
            }
        };
    }

    private static function sqlite(?Arithmetic $arithmetic = null): SQLite
    {
        return new class (new PDO('sqlite::memory:'), $arithmetic ?? self::arithmetic(FractionalEngine::MariaDB)) extends SQLite {
            public function __construct(PDO $pdo, private readonly Arithmetic $declared)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function arithmetic(): Arithmetic
            {
                return $this->declared;
            }
        };
    }

    private static function arithmetic(FractionalEngine $engine): Arithmetic
    {
        return match ($engine) {
            FractionalEngine::MariaDB => (new MariaDB(new stdClass()))->arithmetic(),
            FractionalEngine::Memory => (new Memory())->arithmetic(),
            FractionalEngine::PostgreSQL => (new Postgres(new stdClass()))->arithmetic(),
            FractionalEngine::MongoDB => (new Mongo(new class () extends Client {
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
            }))->arithmetic(),
        };
    }

    /**
     * @param  array<string, mixed>  $values
     */
    private function create(Database $database, array $values, string $id = self::DOCUMENT): void
    {
        $database->createDocument(self::COLLECTION, new Document([Document::ID => $id, ...$values]));
    }

    private function stored(Database $database, string $attribute, string $id = self::DOCUMENT): mixed
    {
        return $database->getDocument(self::COLLECTION, $id)->getAttribute($attribute);
    }

    private function assertHeldAtMaximum(Database $database, int|float|string $bound, int|string $maximum): void
    {
        $increase = fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'maximum', 'big', 1, $bound);

        if (BigInt::compare($maximum, \PHP_INT_MAX) > 0) {
            $this->create($database, ['big' => 5], 'maximum');
            $this->assertSame(6, $increase()->getAttribute('big'));

            return;
        }

        if (BigInt::compare($maximum, \PHP_INT_MIN) <= 0) {
            $this->create($database, ['big' => 5], 'maximum');
            $this->assertLimited($increase);
            $this->assertSame(5, $this->stored($database, 'big', 'maximum'));

            return;
        }

        $this->create($database, ['big' => BigInt::subtract($maximum, 1)], 'maximum');
        $this->assertSame(BigInt::toNative($maximum), $increase()->getAttribute('big'));
        $this->assertLimited($increase);
        $this->assertSame(BigInt::toNative($maximum), $this->stored($database, 'big', 'maximum'));
    }

    private function assertHeldAtMinimum(Database $database, int|float|string $bound, int|string $minimum): void
    {
        $decrease = fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'minimum', 'big', 1, $bound);

        if (BigInt::compare($minimum, \PHP_INT_MIN) < 0) {
            $this->create($database, ['big' => 5], 'minimum');
            $this->assertSame(4, $decrease()->getAttribute('big'));

            return;
        }

        if (BigInt::compare($minimum, \PHP_INT_MAX) >= 0) {
            $this->create($database, ['big' => 5], 'minimum');
            $this->assertLimited($decrease);
            $this->assertSame(5, $this->stored($database, 'big', 'minimum'));

            return;
        }

        $this->create($database, ['big' => BigInt::add($minimum, 1)], 'minimum');
        $this->assertSame(BigInt::toNative($minimum), $decrease()->getAttribute('big'));
        $this->assertLimited($decrease);
        $this->assertSame(BigInt::toNative($minimum), $this->stored($database, 'big', 'minimum'));
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function assertLimited(callable $call): void
    {
        try {
            $call();
            $this->fail('A change past its bound must be refused');
        } catch (LimitException) {
            $this->addToAssertionCount(1);
        }
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function assertRefused(string $message, callable $call): void
    {
        $this->assertSame($message, $this->refusal($call));
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function assertStructureRefused(callable $call): void
    {
        try {
            $call();
            $this->fail('The write must be refused as an invalid structure');
        } catch (StructureException) {
            $this->addToAssertionCount(1);
        }
    }

    /**
     * @param  callable(): mixed  $call
     */
    private function refusal(callable $call): ?string
    {
        try {
            $call();
        } catch (TypeException $exception) {
            return $exception->getMessage();
        }

        return null;
    }
}
