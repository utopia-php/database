<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class SQLiteUpsertArrayOperatorLimitTest extends TestCase
{
    private const string NAMESPACE = 'array_limit';

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new SQLite(new PDO('sqlite::memory:'));
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'lists',
            attributes: [Attribute::integer('numbers', array: true)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $this->database->createDocument('lists', new Document(['$id' => 'first', 'numbers' => [1, 2, 3]]));
    }

    /**
     * @return iterable<string, array{callable(list<int>): Operator}>
     */
    public static function arrayOperators(): iterable
    {
        yield 'append' => [Operator::arrayAppend(...)];
        yield 'prepend' => [Operator::arrayPrepend(...)];
        yield 'intersect' => [Operator::arrayIntersect(...)];
        yield 'diff' => [Operator::arrayDiff(...)];
    }

    /**
     * @param callable(list<int>): Operator $operator
     */
    #[DataProvider('arrayOperators')]
    public function testAnOversizedArrayOperandIsRefusedAndLeavesTheArray(callable $operator): void
    {
        $size = Operator::MAX_ARRAY_OPERATOR_SIZE + 1;

        try {
            $this->upsert($operator(\range(1, $size)));
            $this->fail('An array operand above the limit must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame(
                'Array size ' . $size . ' exceeds maximum allowed size of ' . Operator::MAX_ARRAY_OPERATOR_SIZE . ' for array operations',
                $error->getMessage(),
            );
        }

        $this->assertSame([1, 2, 3], $this->database->getDocument('lists', 'first')->getAttribute('numbers'));
    }

    public function testAnArrayOperandAtTheLimitIsApplied(): void
    {
        $this->upsert(Operator::arrayAppend(\array_fill(0, Operator::MAX_ARRAY_OPERATOR_SIZE, 9)));

        $numbers = $this->database->getDocument('lists', 'first')->getAttribute('numbers');
        $this->assertIsArray($numbers);
        $this->assertCount(Operator::MAX_ARRAY_OPERATOR_SIZE + 3, $numbers);
    }

    private function upsert(Operator $operator): void
    {
        $existing = $this->database->getDocument('lists', 'first');

        $this->adapter->upsertDocuments(
            $this->database->getCollection('lists'),
            '',
            [new Change($existing, new Document([
                '$id' => 'first',
                '$permissions' => [],
                '$createdAt' => $existing->getCreatedAt(),
                '$updatedAt' => $existing->getUpdatedAt(),
                'numbers' => $operator,
            ]))],
        );
    }
}
