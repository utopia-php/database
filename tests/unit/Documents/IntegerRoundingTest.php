<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
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

    private const string MEMORY = 'memory';

    private const string SCHEMALESS = 'schemaless memory';

    private const string SQLITE = 'sqlite';

    /**
     * @return iterable<string, array{string}>
     */
    public static function adapters(): iterable
    {
        yield self::MEMORY => [self::MEMORY];
        yield self::SCHEMALESS => [self::SCHEMALESS];
        yield self::SQLITE => [self::SQLITE];
    }

    #[DataProvider('adapters')]
    public function testAFractionalAmountIsRoundedHalfAwayFromZero(string $adapter): void
    {
        $database = $this->database($adapter);

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

    #[DataProvider('adapters')]
    public function testAnAmountBelowAHalfLeavesTheDocumentUnchanged(string $adapter): void
    {
        $database = $this->database($adapter);
        $updatedAt = $database->getDocument(self::COLLECTION, self::DOCUMENT)->getUpdatedAt();

        $this->assertSame(100, $this->increase($database, 'count', 0.4)->getAttribute('count'));
        $this->assertSame(100, $this->decrease($database, 'count', '0.49')->getAttribute('count'));
        $this->assertSame(100, $this->increase($database, 'count', 0.4, 50)->getAttribute('count'));

        $stored = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(100, $stored->getAttribute('count'));
        $this->assertSame($updatedAt, $stored->getUpdatedAt());
    }

    #[DataProvider('adapters')]
    public function testBigIntegerTextIsRoundedExactly(string $adapter): void
    {
        $database = $this->database($adapter);

        $this->assertSame(9007199254740994, $this->increase($database, 'big', '9007199254740993.5')->getAttribute('big'));
        $this->assertSame(9007199254740995, $this->increase($database, 'big', 1, '9007199254740995.9')->getAttribute('big'));
        $this->assertLimited('Attribute value exceeds maximum limit: 9007199254740995', fn () => $this->increase($database, 'big', 1, '9007199254740995.9'));
        $this->assertSame(0, $this->decrease($database, 'big', '9007199254740994.5')->getAttribute('big'));
        $this->assertSame(-9007199254740995, $this->decrease($database, 'big', '9007199254740994.5', '-9007199254740995.2')->getAttribute('big'));

        $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document(['big' => \PHP_INT_MAX - 1]));
        $this->assertSame(\PHP_INT_MAX, $this->increase($database, 'big', '0.5')->getAttribute('big'));
        $this->assertSame(\PHP_INT_MAX, $this->stored($database, 'big'));
    }

    #[DataProvider('adapters')]
    public function testAFractionalMaximumIsFlooredAndAFractionalMinimumCeiled(string $adapter): void
    {
        $database = $this->database($adapter);

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

    #[DataProvider('adapters')]
    public function testWholeBoundsAreComparedExactly(string $adapter): void
    {
        $database = $this->database($adapter);

        $this->assertSame(101, $this->increase($database, 'count', 1, 101.0)->getAttribute('count'));
        $this->assertLimited('Attribute value exceeds maximum limit: 101', fn () => $this->increase($database, 'count', 1, '101.00'));
        $this->assertSame(102, $this->increase($database, 'count', 1, 9.0e18)->getAttribute('count'));
        $this->assertSame(101, $this->decrease($database, 'count', 1, -9.0e18)->getAttribute('count'));
        $this->assertSame(102, $this->increase($database, 'count', 1, 1.0e19)->getAttribute('count'));
        $this->assertSame(101, $this->decrease($database, 'count', 1, '-5.0')->getAttribute('count'));
        $this->assertSame(101, $this->stored($database, 'count'));
    }

    #[DataProvider('adapters')]
    public function testWhatIsNotAFiniteNumberIsStillRefused(string $adapter): void
    {
        $database = $this->database($adapter);

        $this->assertRefused('Max must be an integer.', fn () => $this->increase($database, 'count', 1, 'ten'));
        $this->assertRefused('Min must be an integer.', fn () => $this->decrease($database, 'count', 1, \NAN));
        $this->assertRefused('Change value must be an integer.', fn () => $this->increase($database, 'count', \INF));
        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('adapters')]
    public function testUpdateDocumentRoundsIncrementAndDecrementOperators(string $adapter): void
    {
        $database = $this->database($adapter);

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

    #[DataProvider('adapters')]
    public function testUpdateDocumentsRoundsIncrementAndDecrementOperators(string $adapter): void
    {
        $database = $this->database($adapter);
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
        $database = $this->database(self::SQLITE);
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
     * @return iterable<string, array{string, Operator, string}>
     */
    public static function refusedOperators(): iterable
    {
        foreach ([self::MEMORY, self::SQLITE] as $adapter) {
            yield "{$adapter} multiply" => [$adapter, Operator::multiply(1.5), 'multiply'];
            yield "{$adapter} divide" => [$adapter, Operator::divide(2.5), 'divide'];
            yield "{$adapter} modulo" => [$adapter, Operator::modulo(1.5), 'modulo'];
            yield "{$adapter} power" => [$adapter, Operator::power(0.5), 'power'];
        }
    }

    #[DataProvider('refusedOperators')]
    public function testAFractionalMultiplyDivideModuloOrPowerOnAnIntegerIsRefused(string $adapter, Operator $operator, string $method): void
    {
        $database = $this->database($adapter);

        try {
            $this->update($database, ['count' => $operator]);
            $this->fail("A fractional {$method} on an integer attribute was accepted");
        } catch (StructureException $error) {
            $this->assertSame("Invalid document structure: Cannot apply {$method} operator: value must be numeric, got double", $error->getMessage());
        }

        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('adapters')]
    public function testFloatAttributesAreNotRounded(string $adapter): void
    {
        $database = $this->database($adapter);

        $this->assertSame(2.75, $this->increase($database, 'ratio', 1.25)->getAttribute('ratio'));
        $this->assertSame(2.25, $this->decrease($database, 'ratio', 0.5, 2.25)->getAttribute('ratio'));
        $this->assertLimited('Attribute value exceeds maximum limit: 2.5', fn () => $this->increase($database, 'ratio', 0.5, 2.5));
        $this->assertSame(2.75, $this->update($database, ['ratio' => Operator::increment(0.5)])->getAttribute('ratio'));
        $this->assertSame(3.0, $this->update($database, ['ratio' => Operator::increment(0.25, 3.25)])->getAttribute('ratio'));
        $this->assertSame(3.0, $this->update($database, ['ratio' => Operator::increment(0.5, 3.25)])->getAttribute('ratio'));
        $this->assertSame(1.5, $this->update($database, ['ratio' => Operator::multiply(0.5)])->getAttribute('ratio'));
        $this->assertSame(1.5, $this->stored($database, 'ratio'));
    }

    private function database(string $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = (new Database(self::adapter($adapter), new Cache(new None())))
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
        ]));

        return $database;
    }

    private static function adapter(string $adapter): Adapter
    {
        return match ($adapter) {
            self::MEMORY => new Memory(),
            self::SCHEMALESS => new class () extends Memory {
                #[\Override]
                public function capabilities(): array
                {
                    return \array_values(\array_filter(
                        parent::capabilities(),
                        static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes,
                    ));
                }
            },
            self::SQLITE => new SQLite(new PDO('sqlite::memory:')),
        };
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
