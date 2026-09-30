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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class SQLiteUpsertArrayRemoveTest extends TestCase
{
    private const string NAMESPACE = 'upsert_array_remove';

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
                Attribute::integer('numbers', array: true),
                Attribute::float('ratios', array: true),
                Attribute::string('words', size: 16, array: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
    }

    /**
     * @return iterable<string, array{string, list<int|float|string>, int|float|string, list<int|float|string>}>
     */
    public static function removals(): iterable
    {
        yield 'an integer' => ['numbers', [1, 2, 2, 3], 2, [1, 3]];
        yield 'a float' => ['ratios', [0.5, 1.5, 0.5], 0.5, [1.5]];
        yield 'a string' => ['words', ['a', 'b', 'b'], 'b', ['a']];
        yield 'a numeric string' => ['words', ['1', '2', '2'], '2', ['1']];
    }

    /**
     * @param list<int|float|string> $stored
     * @param list<int|float|string> $expected
     */
    #[DataProvider('removals')]
    public function testAnUpsertOfAnExistingDocumentRemovesTheElementAsAnUpdateDoes(string $attribute, array $stored, int|float|string $removed, array $expected): void
    {
        $this->database->createDocument('items', new Document(['$id' => 'upserted', $attribute => $stored]));
        $this->database->createDocument('items', new Document(['$id' => 'updated', $attribute => $stored]));

        $this->database->upsertDocument('items', new Document(['$id' => 'upserted', $attribute => Operator::arrayRemove($removed)]));
        $this->database->updateDocument('items', 'updated', new Document([$attribute => Operator::arrayRemove($removed)]));

        $this->assertSame($expected, $this->database->getDocument('items', 'updated')->getAttribute($attribute));
        $this->assertSame($expected, $this->database->getDocument('items', 'upserted')->getAttribute($attribute));
    }
}
