<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Support\Engine;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class IntegerRoundingTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string DOCUMENT = 'counter';

    /**
     * @return iterable<string, array{Engine}>
     */
    public static function engines(): iterable
    {
        foreach (Engine::cases() as $engine) {
            yield $engine->value => [$engine];
        }
    }

    #[DataProvider('engines')]
    public function testAnAmountIsRoundedHalfAwayFromZero(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(102, $this->increase($database, 'count', 1.5)->getAttribute('count'));
        $this->assertSame(105, $this->increase($database, 'count', 2.5)->getAttribute('count'));
        $this->assertSame(106, $this->increase($database, 'count', '0.5')->getAttribute('count'));
        $this->assertSame(107, $this->increase($database, 'count', 1.49)->getAttribute('count'));
        $this->assertSame(109, $this->increase($database, 'count', 2.0)->getAttribute('count'));
        $this->assertSame(106, $this->decrease($database, 'count', 2.5)->getAttribute('count'));
        $this->assertSame(104, $this->decrease($database, 'count', '1.5')->getAttribute('count'));
        $this->assertSame(103, $this->decrease($database, 'count', 0.5)->getAttribute('count'));
        $this->assertSame(103, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testAnAmountBelowAHalfLeavesTheDocumentUnchanged(Engine $engine): void
    {
        $database = $this->database($engine);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();

        $this->assertSame(100, $this->increase($database, 'count', 0.4)->getAttribute('count'));
        $this->assertSame(100, $this->decrease($database, 'count', '0.49')->getAttribute('count'));
        $this->assertSame(100, $this->increase($database, 'count', 0.4, 50)->getAttribute('count'));

        $stored = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(100, $stored->getAttribute('count'));
        $this->assertSame($updatedAt, $stored->getUpdatedAt());
    }

    #[DataProvider('engines')]
    public function testBigIntegerTextIsRoundedExactly(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(9007199254740994, $this->increase($database, 'big', '9007199254740993.5')->getAttribute('big'));
        $this->assertSame(9007199254740995, $this->increase($database, 'big', 1, '9007199254740995.9')->getAttribute('big'));
        $this->assertLimited('Attribute value exceeds maximum limit: 9007199254740995', fn () => $this->increase($database, 'big', 1, '9007199254740995.9'));
        $this->assertSame(0, $this->decrease($database, 'big', '9007199254740994.5')->getAttribute('big'));
        $this->assertSame(-9007199254740995, $this->decrease($database, 'big', '9007199254740994.5', '-9007199254740995.2')->getAttribute('big'));

        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['big' => \PHP_INT_MAX - 1]));
        $this->assertSame(\PHP_INT_MAX, $this->increase($database, 'big', '0.5')->getAttribute('big'));
        $this->assertSame(\PHP_INT_MAX, $this->stored($database, 'big'));
    }

    #[DataProvider('engines')]
    public function testAMaximumIsFlooredAndAMinimumCeiled(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(101, $this->increase($database, 'count', 1, 101.9)->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds maximum limit: 101', fn () => $this->increase($database, 'count', 1, 101.9));
        $this->assertLimited('Attribute value exceeds maximum limit: 101', fn () => $this->increase($database, 'count', 1, '101.5'));
        $this->assertSame(100, $this->decrease($database, 'count', 1, 99.1)->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds minimum limit: 100', fn () => $this->decrease($database, 'count', 1, 99.1));
        $this->assertLimited('Attribute value exceeds minimum limit: 100', fn () => $this->decrease($database, 'count', 1, '99.5'));

        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['count' => 1]));
        $this->assertSame(0, $this->decrease($database, 'count', 1, -0.5)->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds minimum limit: 0', fn () => $this->decrease($database, 'count', 1, -0.5));
        $this->assertLimited('Attribute value exceeds maximum limit: -1', fn () => $this->increase($database, 'count', 1, -0.5));
        $this->assertSame(0, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testWholeBoundsAreComparedExactly(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(101, $this->increase($database, 'count', 1, 101.0)->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds maximum limit: 101', fn () => $this->increase($database, 'count', 1, '101.00'));
        $this->assertSame(102, $this->increase($database, 'count', 1, 9.0e18)->getAttribute('count'));
        $this->assertSame(101, $this->decrease($database, 'count', 1, -9.0e18)->getAttribute('count'));
        $this->assertSame(102, $this->increase($database, 'count', 1, 1.0e19)->getAttribute('count'));
        $this->assertSame(101, $this->decrease($database, 'count', 1, '-5.0')->getAttribute('count'));
        $this->assertSame(101, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testWhatIsNotAFiniteNumberIsStillRefused(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertRefused('Max must be an integer.', fn () => $this->increase($database, 'count', 1, 'ten'));
        $this->assertRefused('Min must be an integer.', fn () => $this->decrease($database, 'count', 1, \NAN));
        $this->assertRefused('Change value must be an integer.', fn () => $this->increase($database, 'count', \INF));
        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testPlainDecimalTextIsRounded(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(102, $this->increase($database, 'count', ' 1.5')->getAttribute('count'));
        $this->assertSame(104, $this->increase($database, 'count', '1.5 ')->getAttribute('count'));
        $this->assertSame(106, $this->increase($database, 'count', '+1.5')->getAttribute('count'));
        $this->assertSame(107, $this->increase($database, 'count', '.5')->getAttribute('count'));
        $this->assertSame(108, $this->increase($database, 'count', '1.')->getAttribute('count'));
        $this->assertSame(107, $this->decrease($database, 'count', "\t.5\n")->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds maximum limit: 107', fn () => $this->increase($database, 'count', 1, ' 107.9 '));
        $this->assertSame(106, $this->decrease($database, 'count', 1, '-.5')->getAttribute('count'));
        $this->assertSame(107, $this->update($database, ['count' => Operator::increment('+.5', ' 107.9')])->getAttribute('count'));
        $this->assertSame(107, $this->stored($database, 'count'));
    }

    public function testAMillionDigitAmountIsRefusedWithoutDelay(): void
    {
        $database = $this->database(Engine::SQLite);
        $amount = \str_repeat('1', 1000000).'.5';
        $start = \hrtime(true);

        $this->assertLimited('Attribute value exceeds maximum limit: 2147483647', fn () => $this->increase($database, 'count', $amount));

        $this->assertLessThan(2.0, (\hrtime(true) - $start) / 1e9, 'Rounding a long amount must take time linear in its length');
        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testTextWithAnExponentIsRefused(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertRefused('Change value must be an integer.', fn () => $this->increase($database, 'count', '1.5e2'));
        $this->assertRefused('Change value must be an integer.', fn () => $this->decrease($database, 'count', '15e-1'));
        $this->assertRefused('Max must be an integer.', fn () => $this->increase($database, 'count', 1, '1.5e2'));
        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testAnOperatorAmountGivenAsTextIsRounded(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(99, $this->update($database, ['count' => Operator::increment('-0.5')])->getAttribute('count'));
        $this->assertSame(100, $this->update($database, ['count' => Operator::decrement('-0.5')])->getAttribute('count'));
        $this->assertSame(99, $this->update($database, ['count' => Operator::decrement(' .5')])->getAttribute('count'));
        $this->assertSame(99, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testACounterChangeThatRoundsToNothingFiresNoEvent(Engine $engine): void
    {
        $database = $this->database($engine);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $this->increase($database, 'count', 0.4);
        $this->decrease($database, 'count', '0.49');

        $this->assertSame([], $recorder->received(Event::DocumentIncrease));
        $this->assertSame([], $recorder->received(Event::DocumentDecrease));
        $this->assertSame([], $recorder->received(Event::DocumentUpdate));
        $this->assertSame($updatedAt, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt());
    }

    #[DataProvider('engines')]
    public function testAnOperatorThatRoundsToNothingIsNoChange(Engine $engine): void
    {
        $database = $this->database($engine);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();

        $this->assertSame(100, $this->update($database, ['count' => Operator::increment(0.4)])->getAttribute('count'));
        $this->assertSame(100, $this->update($database, ['count' => Operator::decrement('-0.4', 100.5)])->getAttribute('count'));
        $this->assertSame($updatedAt, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt());

        $sameValue = new RecordingLifecycle();
        $database->addHook($sameValue);
        $this->update($database, ['count' => 100]);

        $roundedToNothing = new RecordingLifecycle();
        $database->addHook($roundedToNothing);
        $this->update($database, ['count' => Operator::increment(0.4)]);

        $this->assertSame(\array_slice($sameValue->getEvents(), 0, \count($roundedToNothing->getEvents())), $roundedToNothing->getEvents());
        $this->assertSame($updatedAt, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt());
    }

    #[DataProvider('engines')]
    public function testUpdateDocumentsWithOnlyAnOperatorThatRoundsToNothingUpdatesNothing(Engine $engine): void
    {
        $database = $this->database($engine);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $this->assertSame(0, $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(0.4)])));
        $this->assertSame(1, $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::decrement(0.4), 'big' => 7])));

        $this->assertCount(1, $recorder->received(Event::DocumentsUpdate));
        $this->assertSame(100, $this->stored($database, 'count'));
        $this->assertSame(7, $this->stored($database, 'big'));
    }

    #[DataProvider('engines')]
    public function testUpdatesWithAnOperatorThatRoundsToNothingUpdateNothingWhenGivenAgain(Engine $engine): void
    {
        $database = $this->database($engine);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();
        $updates = new Document(['count' => Operator::increment(0.4)]);
        \usleep(2000);

        $this->assertSame(0, $database->updateDocuments(self::COLLECTION, $updates));
        $this->assertSame(0, $database->updateDocuments(self::COLLECTION, $updates));

        $this->assertSame($updatedAt, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt());
    }

    public function testAnUpsertWithAnOperatorThatRoundsToNothingIsNoChange(): void
    {
        $database = $this->database(Engine::SQLite);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $upserted = $database->upsertDocument(self::COLLECTION, new Document([
            Document::ID => self::DOCUMENT,
            'count' => Operator::increment(0.4),
            'big' => 0,
            'ratio' => 1.5,
            'counts' => [1],
        ]));
        $count = $database->upsertDocuments(self::COLLECTION, [new Document([
            Document::ID => self::DOCUMENT,
            'count' => Operator::decrement(0.4, 100.5),
            'big' => 0,
            'ratio' => 1.5,
            'counts' => [1],
        ])]);

        $this->assertSame(100, $upserted->getAttribute('count'));
        $this->assertSame(0, $count);
        $this->assertSame([], $recorder->received(Event::DocumentUpsert));
        $this->assertSame($updatedAt, $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt());
    }

    public function testAnUpsertThatCreatesADocumentTakesAnAmountThatRoundsToNothingAsZero(): void
    {
        $database = $this->database(Engine::SQLite);

        $database->upsertDocuments(self::COLLECTION, [
            new Document([Document::ID => 'rounded', 'count' => Operator::increment(0.4)]),
            new Document([Document::ID => 'zero', 'count' => Operator::increment(0)]),
        ]);

        $this->assertSame($this->stored($database, 'count', 'zero'), $this->stored($database, 'count', 'rounded'));
    }

    #[DataProvider('engines')]
    public function testFractionalItemsAddedToAnIntegerArrayAreRefused(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertStructureRefused(fn () => $this->update($database, ['counts' => [1, 2.5]]));
        $this->assertStructureRefused(fn () => $this->update($database, ['counts' => Operator::arrayAppend([2.5])]));
        $this->assertSame([1], $this->stored($database, 'counts'));
    }

    #[DataProvider('engines')]
    public function testUpdateDocumentRoundsIncrementAndDecrementOperators(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(102, $this->update($database, ['count' => Operator::increment(1.5)])->getAttribute('count'));
        $this->assertSame(99, $this->update($database, ['count' => Operator::decrement('2.5')])->getAttribute('count'));
        $this->assertSame(96, $this->update($database, ['count' => Operator::increment(-2.5)])->getAttribute('count'));
        $this->assertSame(96, $this->update($database, ['count' => Operator::increment(0.4)])->getAttribute('count'));
        $this->assertSame(98, $this->update($database, ['count' => Operator::increment(2, 98.7)])->getAttribute('count'));
        $this->assertSame(98, $this->update($database, ['count' => Operator::increment(1, 98.7)])->getAttribute('count'));
        $this->assertSame(98, $this->update($database, ['count' => Operator::decrement(4, '94.2')])->getAttribute('count'));
        $this->assertSame(95, $this->update($database, ['count' => Operator::decrement(3, '94.2')])->getAttribute('count'));
        $this->assertSame(9007199254740994, $this->update($database, ['big' => Operator::increment('9007199254740993.5')])->getAttribute('big'));
        $this->assertSame(95, $this->stored($database, 'count'));
        $this->assertSame(9007199254740994, $this->stored($database, 'big'));
    }

    #[DataProvider('engines')]
    public function testUpdateDocumentsRoundsIncrementAndDecrementOperators(Engine $engine): void
    {
        $database = $this->database($engine);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'second', 'count' => 10, 'big' => 0, 'ratio' => 0.0]));

        $this->assertSame(2, $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(2.5)])));
        $this->assertSame(103, $this->stored($database, 'count'));
        $this->assertSame(13, $this->stored($database, 'count', 'second'));

        $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(7, 20.9)]));
        $this->assertSame(103, $this->stored($database, 'count'));
        $this->assertSame(20, $this->stored($database, 'count', 'second'));

        $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::decrement(0.5, 15.5)]));
        $this->assertSame(102, $this->stored($database, 'count'));
        $this->assertSame(19, $this->stored($database, 'count', 'second'));
    }

    public function testUpsertsRoundIncrementAndDecrementOperators(): void
    {
        $database = $this->database(Engine::SQLite);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'second', 'count' => 10, 'big' => 0, 'ratio' => 0.0]));

        $upserted = $database->upsertDocument(self::COLLECTION, new Document([Document::ID => self::DOCUMENT, 'count' => Operator::increment(1.5)]));
        $count = $database->upsertDocuments(self::COLLECTION, [
            new Document([Document::ID => self::DOCUMENT, 'count' => Operator::decrement(2.5, 98.5)]),
            new Document([Document::ID => 'second', 'count' => Operator::increment('1.5', '12.7')]),
        ]);

        $this->assertSame(102, $upserted->getAttribute('count'));
        $this->assertSame(2, $count);
        $this->assertSame(99, $this->stored($database, 'count'));
        $this->assertSame(12, $this->stored($database, 'count', 'second'));
    }

    /**
     * @return iterable<string, array{Engine, Operator, string}>
     */
    public static function refusedOperators(): iterable
    {
        foreach ([Engine::Memory, Engine::SQLite] as $engine) {
            yield "{$engine->value} multiply" => [$engine, Operator::multiply(1.5), 'multiply'];
            yield "{$engine->value} divide" => [$engine, Operator::divide(2.5), 'divide'];
            yield "{$engine->value} modulo" => [$engine, Operator::modulo(1.5), 'modulo'];
            yield "{$engine->value} power" => [$engine, Operator::power(0.5), 'power'];
        }
    }

    #[DataProvider('refusedOperators')]
    public function testMultiplyDivideModuloOrPowerByAFractionOnAnIntegerIsRefused(Engine $engine, Operator $operator, string $method): void
    {
        $database = $this->database($engine);

        try {
            $this->update($database, ['count' => $operator]);
            $this->fail("A fractional {$method} on an integer attribute was accepted");
        } catch (StructureException $error) {
            $this->assertSame("Invalid document structure: Cannot apply {$method} operator: value must be numeric, got double", $error->getMessage());
        }

        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('engines')]
    public function testFloatAttributesAreNotRounded(Engine $engine): void
    {
        $database = $this->database($engine);

        $this->assertSame(2.75, $this->increase($database, 'ratio', 1.25)->getAttribute('ratio'));
        $this->assertSame(2.25, $this->decrease($database, 'ratio', 0.5, 2.25)->getAttribute('ratio'));
        $this->assertLimited('Attribute value exceeds maximum limit: 2.5', fn () => $this->increase($database, 'ratio', 0.5, 2.5));
        $this->assertSame(2.75, $this->update($database, ['ratio' => Operator::increment(0.5)])->getAttribute('ratio'));
        $this->assertSame(3.0, $this->update($database, ['ratio' => Operator::increment(0.25, 3.25)])->getAttribute('ratio'));
        $this->assertSame(3.0, $this->update($database, ['ratio' => Operator::increment(0.5, 3.25)])->getAttribute('ratio'));
        $this->assertSame(1.5, $this->update($database, ['ratio' => Operator::multiply(0.5)])->getAttribute('ratio'));
        $this->assertSame(1.5, $this->stored($database, 'ratio'));
    }

    private function database(Engine $engine): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = (new Database($engine->adapter(), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('integer_rounding')
            ->setNamespace('integer_rounding_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
                Attribute::double(key: 'ratio'),
                Attribute::integer(key: 'counts', array: true),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document([
            Document::ID => self::DOCUMENT,
            'count' => 100,
            'big' => 0,
            'ratio' => 1.5,
            'counts' => [1],
        ]));

        return $database;
    }

    private function increase(Database $database, string $attribute, int|float|string $value, int|float|string|null $max = null): Document
    {
        return $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value, $max);
    }

    private function decrease(Database $database, string $attribute, int|float|string $value, int|float|string|null $min = null): Document
    {
        return $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value, $min);
    }

    /**
     * @param  array<string, mixed>  $changes
     */
    private function update(Database $database, array $changes): Document
    {
        return $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document($changes));
    }

    private function stored(Database $database, string $attribute, string $id = self::DOCUMENT): mixed
    {
        return $database->getDocument(self::COLLECTION, $id)->getAttribute($attribute);
    }

    private function assertLimited(string $message, callable $change): void
    {
        try {
            $change();
            $this->fail("The change was not limited: {$message}");
        } catch (LimitException $error) {
            $this->assertSame($message, $error->getMessage());
        }
    }

    private function assertStructureRefused(callable $change): void
    {
        try {
            $change();
            $this->fail('The change was not refused');
        } catch (StructureException $error) {
            $this->assertStringStartsWith('Invalid document structure: ', $error->getMessage());
        }
    }

    private function assertRefused(string $message, callable $change): void
    {
        try {
            $change();
            $this->fail("The change was not refused: {$message}");
        } catch (TypeException $error) {
            $this->assertSame($message, $error->getMessage());
        }
    }
}
