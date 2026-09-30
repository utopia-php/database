<?php

namespace Tests\Unit\Adapter;

use Closure;
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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class UpsertNewDocumentParityTest extends TestCase
{
    private const string NAMESPACE = 'upsert_parity';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: 'items',
            attributes: [
                Attribute::integer('count', default: 7),
                Attribute::integer('numbers', array: true, default: [1, 2, 2, 3, 5]),
                Attribute::string('words', size: 16, array: true, default: ['a', 'b', 'b']),
                Attribute::integer('empty', array: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
    }

    /**
     * @return iterable<string, array{string, Closure(): Operator, mixed}>
     */
    public static function operators(): iterable
    {
        yield 'increment within its maximum' => ['count', static fn (): Operator => Operator::increment(3, 10), 10];
        yield 'increment past its maximum' => ['count', static fn (): Operator => Operator::increment(5, 10), 7];
        yield 'decrement past its minimum' => ['count', static fn (): Operator => Operator::decrement(5, 5), 7];
        yield 'multiply past its maximum' => ['count', static fn (): Operator => Operator::multiply(3, 20), 7];
        yield 'divide past its minimum' => ['count', static fn (): Operator => Operator::divide(2, 5), 7];
        yield 'power past its maximum' => ['count', static fn (): Operator => Operator::power(2, 40), 7];
        yield 'power within its maximum' => ['count', static fn (): Operator => Operator::power(2, 49), 49];
        yield 'filter greater than' => ['numbers', static fn (): Operator => Operator::arrayFilter('greaterThan', 2), [3, 5]];
        yield 'filter greater than or equal' => ['numbers', static fn (): Operator => Operator::arrayFilter('greaterThanEqual', 3), [3, 5]];
        yield 'filter less than' => ['numbers', static fn (): Operator => Operator::arrayFilter('lessThan', 3), [1, 2, 2]];
        yield 'filter less than or equal' => ['numbers', static fn (): Operator => Operator::arrayFilter('lessThanEqual', 2), [1, 2, 2]];
        yield 'filter equal' => ['numbers', static fn (): Operator => Operator::arrayFilter('equal', 2), [2, 2]];
        yield 'filter not equal' => ['numbers', static fn (): Operator => Operator::arrayFilter('notEqual', 2), [1, 3, 5]];
        yield 'filter null' => ['numbers', static fn (): Operator => Operator::arrayFilter('isNull'), []];
        yield 'filter not null' => ['numbers', static fn (): Operator => Operator::arrayFilter('isNotNull'), [1, 2, 2, 3, 5]];
        yield 'filter equal string' => ['words', static fn (): Operator => Operator::arrayFilter('equal', 'b'), ['b', 'b']];
        yield 'filter a missing array' => ['empty', static fn (): Operator => Operator::arrayFilter('greaterThan', 0), []];
    }

    /**
     * @param Closure(): Operator $operator
     */
    #[DataProvider('operators')]
    public function testANewDocumentGetsWhatAnExistingDocumentWithTheSameValueGets(string $attribute, Closure $operator, mixed $expected): void
    {
        $this->database->createDocument('items', new Document(['$id' => 'existing']));

        $this->database->upsertDocuments('items', [
            new Document(['$id' => 'existing', $attribute => $operator()]),
            new Document(['$id' => 'created', $attribute => $operator()]),
        ]);

        $this->assertSame($expected, $this->database->getDocument('items', 'existing')->getAttribute($attribute), 'existing document');
        $this->assertSame($expected, $this->database->getDocument('items', 'created')->getAttribute($attribute), 'new document');
    }
}
