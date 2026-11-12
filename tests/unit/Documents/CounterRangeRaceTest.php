<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Documents\Interleaving\Memory;
use Tests\Unit\Documents\Interleaving\Standalone;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Format;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Range;

/**
 * An increase or decrease holds a counter to its column's range in the write itself: when another write moves the
 * counter after it was read, the write that would pass the range changes nothing and is refused.
 */
final class CounterRangeRaceTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string INTEGER_RANGE = 'raceIntegerRange';

    private const string DOUBLE_RANGE = 'raceDoubleRange';

    #[\Override]
    public static function setUpBeforeClass(): void
    {
        Structure::addFormat(self::INTEGER_RANGE, static fn (array $attribute): Range => self::range($attribute, Range::TYPE_INTEGER), ColumnType::Integer);
        Structure::addFormat(self::DOUBLE_RANGE, static fn (array $attribute): Range => self::range($attribute, Range::TYPE_FLOAT), ColumnType::Double);
    }

    #[\Override]
    public static function tearDownAfterClass(): void
    {
        Structure::removeFormat(self::INTEGER_RANGE);
        Structure::removeFormat(self::DOUBLE_RANGE);
    }

    /**
     * @return iterable<string, array{Closure(): (Memory|Standalone)}>
     */
    public static function adapters(): iterable
    {
        yield 'memory' => [static fn (): Memory => new Memory()];
        yield 'without transactions' => [static fn (): Standalone => new Standalone()];
    }

    /**
     * @param  Closure(): (Memory|Standalone)  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnIncreaseThatAnotherIncreaseCarriedPastTheColumnMaximumIsRefused(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter(), ['b' => 9, 'f' => 2.0]);
        $engine->beforeIncrease = $this->concurrently($engine, 'b', 1);

        try {
            $database->increaseDocumentAttribute(self::COLLECTION, 'row', 'b', 1);
            $this->fail('An increase past the column maximum another increase reached was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds maximum limit: 10', $error->getMessage());
        }

        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'row')->getAttribute('b'));
    }

    /**
     * @param  Closure(): (Memory|Standalone)  $adapter
     */
    #[DataProvider('adapters')]
    public function testADecreaseThatAnotherDecreaseCarriedPastTheColumnMinimumIsRefused(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter(), ['b' => 1, 'f' => -1.0]);
        $engine->beforeIncrease = $this->concurrently($engine, 'f', -0.5);

        try {
            $database->decreaseDocumentAttribute(self::COLLECTION, 'row', 'f', 0.25);
            $this->fail('A decrease past the column minimum another decrease reached was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds minimum limit: -1.5', $error->getMessage());
        }

        $this->assertSame(-1.5, $database->getDocument(self::COLLECTION, 'row')->getAttribute('f'));
    }

    /**
     * @param  Closure(): (Memory|Standalone)  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnIncreaseThatAnotherIncreaseCarriedPastTheGivenMaximumIsRefused(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter(), ['b' => 0, 'f' => 0.0]);
        $engine->beforeIncrease = $this->concurrently($engine, 'n', 5);

        try {
            $database->increaseDocumentAttribute(self::COLLECTION, 'row', 'n', 3, 6);
            $this->fail('An increase past the given maximum another increase reached was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds maximum limit: 6', $error->getMessage());
        }

        $this->assertSame(5, $database->getDocument(self::COLLECTION, 'row')->getAttribute('n'));
    }

    /**
     * @param  Closure(): (Memory|Standalone)  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnIncreaseThatStillFitsAfterAnotherIncreaseIsApplied(Closure $adapter): void
    {
        [$database, $engine] = $this->database($adapter(), ['b' => 7, 'f' => 2.0]);
        $engine->beforeIncrease = $this->concurrently($engine, 'b', 1);

        $database->increaseDocumentAttribute(self::COLLECTION, 'row', 'b', 2);

        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'row')->getAttribute('b'));
    }

    /**
     * @return Closure(Document, string): void
     */
    private function concurrently(Memory|Standalone $engine, string $attribute, int|float $change): Closure
    {
        return static function (Document $collection, string $id) use ($engine, $attribute, $change): void {
            $write = static fn (): bool => $engine->increaseDocumentAttribute($collection, $id, $attribute, $change, DateTime::now());
            if ($engine instanceof Memory) {
                $engine->detached($write);

                return;
            }
            $write();
        };
    }

    /**
     * @param  array<string, int|float>  $values
     * @return array{Database, Memory|Standalone}
     */
    private function database(Memory|Standalone $adapter, array $values): array
    {
        $authorization = new Authorization();
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('race')
            ->setNamespace('race_'.\uniqid());
        $database->create();

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'b', format: new Format(self::INTEGER_RANGE, ['min' => 0, 'max' => 10])),
                Attribute::double(key: 'f', format: new Format(self::DOUBLE_RANGE, ['min' => -1.5, 'max' => 2.5])),
                Attribute::integer(key: 'n'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'row', 'n' => 0, ...$values]));

        return [$database, $adapter];
    }

    /**
     * @param  array<string, mixed>  $attribute
     */
    private static function range(array $attribute, string $type): Range
    {
        /** @var array{min?: int|float, max?: int|float} $options */
        $options = $attribute['formatOptions'] ?? [];

        return new Range($options['min'] ?? -INF, $options['max'] ?? INF, $type);
    }
}
