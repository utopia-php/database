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
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class SQLArrayWritesTest extends TestCase
{
    private const string NAMESPACE = 'array_writes';

    private PDO $pdo;

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'items',
            attributes: [
                Attribute::string('tags', size: 16, array: true),
                Attribute::integer('numbers', array: true, default: [1, 2, 2, 3]),
                Attribute::string('words', size: 16, array: true, default: ['a', 'b', 'b']),
                Attribute::boolean('active'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
    }

    public function testCreatedDocumentsKeepTheirArrays(): void
    {
        $this->database->createDocuments('items', [
            new Document(['$id' => 'first', 'tags' => ['red', 'blue'], 'numbers' => [7, 8]]),
            new Document(['$id' => 'second', 'tags' => ['green'], 'numbers' => []]),
        ]);

        $this->assertSame(['red', 'blue'], $this->database->getDocument('items', 'first')->getAttribute('tags'));
        $this->assertSame([7, 8], $this->database->getDocument('items', 'first')->getAttribute('numbers'));
        $this->assertSame(['green'], $this->database->getDocument('items', 'second')->getAttribute('tags'));
        $this->assertSame([], $this->database->getDocument('items', 'second')->getAttribute('numbers'));
        $this->assertSame(['first'], $this->ids($this->database->find('items', [Query::contains('tags', ['blue'])])));
        $this->assertSame(['["red","blue"]', '["green"]'], $this->stored('tags'));
    }

    public function testUpdatedDocumentsStoreArraysAsJsonAndBooleansAsIntegers(): void
    {
        $this->database->createDocuments('items', [
            new Document(['$id' => 'first', 'tags' => ['old'], 'active' => false]),
            new Document(['$id' => 'second', 'active' => false]),
        ]);

        $this->assertSame(2, $this->database->updateDocuments('items', new Document(['tags' => ['a', 'b'], 'active' => true])));

        foreach (['first', 'second'] as $id) {
            $document = $this->database->getDocument('items', $id);
            $this->assertSame(['a', 'b'], $document->getAttribute('tags'));
            $this->assertTrue($document->getAttribute('active'));
        }
        $this->assertSame(['["a","b"]', '["a","b"]'], $this->stored('tags'));
        $this->assertSame([1, 1], $this->stored('active'));
    }

    public function testAnUpdateWithNothingToSetChangesNothing(): void
    {
        $this->database->createDocument('items', new Document(['$id' => 'first', 'tags' => ['kept']]));
        $document = $this->database->getDocument('items', 'first');

        $this->assertSame(0, $this->adapter->updateDocuments($this->database->getCollection('items'), new Document([]), [$document]));

        $this->assertSame(['["kept"]'], $this->stored('tags'));
    }

    /**
     * @return iterable<string, array{string, Operator, list<int|string>}>
     */
    public static function newDocumentArrayOperators(): iterable
    {
        yield 'unique integers' => ['numbers', Operator::arrayUnique(), [1, 2, 3]];
        yield 'remove an integer' => ['numbers', Operator::arrayRemove(2), [1, 3]];
        yield 'intersect integers' => ['numbers', Operator::arrayIntersect([2, 3]), [2, 2, 3]];
        yield 'diff integers' => ['numbers', Operator::arrayDiff([1]), [2, 2, 3]];
        yield 'unique strings' => ['words', Operator::arrayUnique(), ['a', 'b']];
        yield 'remove a string' => ['words', Operator::arrayRemove('b'), ['a']];
        yield 'intersect strings' => ['words', Operator::arrayIntersect(['b']), ['b', 'b']];
        yield 'diff strings' => ['words', Operator::arrayDiff(['a']), ['b', 'b']];
    }

    /**
     * @param list<int|string> $expected
     */
    #[DataProvider('newDocumentArrayOperators')]
    public function testAnArrayOperatorOnANewDocumentKeepsTheElementTypes(string $attribute, Operator $operator, array $expected): void
    {
        $this->database->upsertDocument('items', new Document(['$id' => 'created', $attribute => $operator]));

        $this->assertSame($expected, $this->database->getDocument('items', 'created')->getAttribute($attribute));
    }

    public function testANestedOperandMatchesNoElementOfANewDocument(): void
    {
        $this->database->setValidation(false);

        $this->database->upsertDocument('items', new Document(['$id' => 'created', 'numbers' => Operator::arrayIntersect([[2]])]));

        $this->assertSame([], $this->database->getDocument('items', 'created')->getAttribute('numbers'));
    }

    /**
     * @param array<Document> $documents
     * @return array<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), $documents);
    }

    /**
     * @return list<mixed>
     */
    private function stored(string $column): array
    {
        $statement = $this->pdo->query('SELECT `' . $column . '` FROM `' . self::NAMESPACE . '_items` ORDER BY _id');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        /** @var list<mixed> */
        return $statement->fetchAll(PDO::FETCH_COLUMN);
    }
}
