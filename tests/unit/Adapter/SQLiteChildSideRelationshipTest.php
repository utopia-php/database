<?php

namespace Tests\Unit\Adapter;

use PDO;
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
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;

final class SQLiteChildSideRelationshipTest extends TestCase
{
    private const string NAMESPACE = 'child_side';

    private PDO $pdo;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->database = new Database(new SQLite($this->pdo), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Relationships($this->database));
        $this->database->create();

        foreach (['authors', 'books'] as $collection) {
            $this->database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                documentSecurity: false,
            ));
        }
    }

    public function testAOneToManyKeyRenamedFromTheChildSideRenamesItsColumn(): void
    {
        $this->database->createRelationship(new Relationship(
            collection: 'authors',
            relatedCollection: 'books',
            type: RelationType::OneToMany,
            twoWay: true,
            key: 'books',
            twoWayKey: 'author',
        ));

        $this->assertTrue($this->database->updateRelationship('books', 'author', newKey: 'writer'));

        $this->assertContains('writer', $this->columns('books'));
        $this->assertNotContains('author', $this->columns('books'));

        $this->database->createDocument('authors', new Document([
            '$id' => 'herbert',
            'name' => 'Herbert',
            'books' => [new Document(['$id' => 'dune', 'name' => 'Dune'])],
        ]));

        $this->assertSame('herbert', $this->relatedId($this->database->getDocument('books', 'dune')->getAttribute('writer')));
        $this->assertSame(['dune'], $this->relatedIds($this->database->getDocument('authors', 'herbert')->getAttribute('books')));
    }

    public function testAManyToOneTwoWayKeyRenamedFromTheChildSideRenamesTheParentColumn(): void
    {
        $this->database->createRelationship(new Relationship(
            collection: 'books',
            relatedCollection: 'authors',
            type: RelationType::ManyToOne,
            twoWay: true,
            key: 'author',
            twoWayKey: 'books',
        ));

        $this->assertTrue($this->database->updateRelationship('authors', 'books', newTwoWayKey: 'writer'));

        $this->assertContains('writer', $this->columns('books'));
        $this->assertNotContains('author', $this->columns('books'));

        $this->database->createDocument('books', new Document([
            '$id' => 'dune',
            'name' => 'Dune',
            'writer' => new Document(['$id' => 'herbert', 'name' => 'Herbert']),
        ]));

        $this->assertSame('herbert', $this->relatedId($this->database->getDocument('books', 'dune')->getAttribute('writer')));
        $this->assertSame(['dune'], $this->relatedIds($this->database->getDocument('authors', 'herbert')->getAttribute('books')));
    }

    /**
     * @return list<string>
     */
    private function columns(string $collection): array
    {
        $statement = $this->pdo->query('PRAGMA table_info(`' . self::NAMESPACE . '_' . $collection . '`)');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        $columns = [];
        foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
            if (\is_array($row) && \is_string($row['name'] ?? null)) {
                $columns[] = $row['name'];
            }
        }

        return $columns;
    }

    private function relatedId(mixed $related): string
    {
        $this->assertInstanceOf(Document::class, $related);

        return $related->getId();
    }

    /**
     * @return list<string>
     */
    private function relatedIds(mixed $related): array
    {
        $this->assertIsArray($related);

        return \array_values(\array_map($this->relatedId(...), $related));
    }
}
