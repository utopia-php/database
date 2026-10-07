<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class SQLiteUpsertIntegerOverflowTest extends TestCase
{
    private const string NAMESPACE = 'upsert_overflow';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'counters',
            attributes: [
                Attribute::bigInteger('high', default: PHP_INT_MAX - 5),
                Attribute::bigInteger('low', default: PHP_INT_MIN + 5),
                Attribute::string('digits', size: 64, default: '9223372036854775807'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
    }

    /**
     * @return iterable<string, array{string, Operator}>
     */
    public static function overflows(): iterable
    {
        yield 'increment past the maximum' => ['high', Operator::increment(10)];
        yield 'multiply past the maximum' => ['high', Operator::multiply(2)];
        yield 'decrement past the minimum' => ['low', Operator::decrement(10)];
    }

    #[DataProvider('overflows')]
    public function testAnOperatorThatOverflowsANewDocumentIsRefused(string $attribute, Operator $operator): void
    {
        try {
            $this->database->upsertDocument('counters', new Document(['$id' => 'created', $attribute => $operator]));
            $this->fail('A value outside the integer range must be refused');
        } catch (LimitException $error) {
            $this->assertSame('Value out of range', $error->getMessage());
        }

        $this->assertTrue($this->database->getDocument('counters', 'created')->isEmpty());
    }

    public function testAnOperatorWithinTheRangeIsStored(): void
    {
        $this->database->upsertDocument('counters', new Document(['$id' => 'created', 'high' => Operator::increment(5)]));

        $this->assertSame(PHP_INT_MAX, $this->database->getDocument('counters', 'created')->getAttribute('high'));
    }

    public function testAStringThatSpellsALargeNumberIsNotRefused(): void
    {
        $this->database->upsertDocument('counters', new Document(['$id' => 'created', 'digits' => Operator::stringConcat('0')]));

        $this->assertSame('92233720368547758070', $this->database->getDocument('counters', 'created')->getAttribute('digits'));
    }
}
