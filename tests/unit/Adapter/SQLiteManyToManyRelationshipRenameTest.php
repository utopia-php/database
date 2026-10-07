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
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Validator\Authorization;

final class SQLiteManyToManyRelationshipRenameTest extends TestCase
{
    private const string NAMESPACE = 'many_to_many_rename';

    private Database $database;

    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization($authorization);
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships($this->database));
        $this->database->create();

        foreach (['books', 'authors'] as $collection) {
            $this->database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                documentSecurity: false,
            ));
        }
    }

    public function testAOneWayTwoWayKeyRenameKeepsExistingAndNewRelations(): void
    {
        $this->createRelationship(twoWay: false);
        $this->createBook('dune', ['herbert']);

        $this->database->updateRelationship('books', 'authors', new RelationshipUpdate(twoWayKey: 'works'));

        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'authors'));

        $this->createBook('emma', ['austen', 'herbert']);

        $this->assertSame(['austen', 'herbert'], $this->relatedIds('books', 'emma', 'authors'));
        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'authors'));
    }

    public function testAOneWayKeyRenameKeepsExistingAndNewRelations(): void
    {
        $this->createRelationship(twoWay: false);
        $this->createBook('dune', ['herbert']);

        $this->database->updateRelationship('books', 'authors', new RelationshipUpdate(key: 'writers'));

        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'writers'));

        $this->database->createDocument('books', new Document([
            '$id' => 'emma',
            'name' => 'emma',
            'writers' => [new Document(['$id' => 'austen', 'name' => 'austen'])],
        ]));

        $this->assertSame(['austen'], $this->relatedIds('books', 'emma', 'writers'));
    }

    public function testAOneWayRenameOfBothKeysKeepsExistingAndNewRelations(): void
    {
        $this->createRelationship(twoWay: false);
        $this->createBook('dune', ['herbert']);

        $this->database->updateRelationship('books', 'authors', new RelationshipUpdate(key: 'writers', twoWayKey: 'works'));

        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'writers'));

        $this->database->createDocument('books', new Document([
            '$id' => 'emma',
            'name' => 'emma',
            'writers' => [new Document(['$id' => 'austen', 'name' => 'austen'])],
        ]));

        $this->assertSame(['austen'], $this->relatedIds('books', 'emma', 'writers'));
    }

    public function testATwoWayTwoWayKeyRenameKeepsBothSidesRelated(): void
    {
        $this->createRelationship(twoWay: true);
        $this->createBook('dune', ['herbert']);

        $this->database->updateRelationship('books', 'authors', new RelationshipUpdate(twoWayKey: 'works'));

        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'authors'));
        $this->assertSame(['dune'], $this->relatedIds('authors', 'herbert', 'works'));

        $this->createBook('emma', ['herbert']);

        $this->assertSame(['dune', 'emma'], $this->relatedIds('authors', 'herbert', 'works'));
    }

    public function testATwoWayKeyRenameFromTheChildSideKeepsBothSidesRelated(): void
    {
        $this->createRelationship(twoWay: true);
        $this->createBook('dune', ['herbert']);

        $this->database->updateRelationship('authors', 'books', new RelationshipUpdate(key: 'works'));

        $this->assertSame(['dune'], $this->relatedIds('authors', 'herbert', 'works'));
        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'authors'));

        $this->createBook('emma', ['herbert']);

        $this->assertSame(['dune', 'emma'], $this->relatedIds('authors', 'herbert', 'works'));
    }

    public function testATwoWayKeyRenameToAnExistingRelatedAttributeIsRejected(): void
    {
        $this->createRelationship(twoWay: false);
        $this->createBook('dune', ['herbert']);

        try {
            $this->database->updateRelationship('books', 'authors', new RelationshipUpdate(twoWayKey: 'name'));
            $this->fail('Renaming the two-way key onto an existing attribute should be rejected');
        } catch (DuplicateException $error) {
            $this->assertSame('Related attribute already exists', $error->getMessage());
        }

        $this->assertSame(['herbert'], $this->relatedIds('books', 'dune', 'authors'));
    }

    private function createRelationship(bool $twoWay): void
    {
        $this->database->createRelationship('books', Relationship::manyToMany(
            relatedCollection: 'authors',
            twoWay: $twoWay,
            key: 'authors',
            twoWayKey: 'books',
        ));
    }

    /**
     * @param list<string> $authors
     */
    private function createBook(string $id, array $authors): void
    {
        $this->database->createDocument('books', new Document([
            '$id' => $id,
            'name' => $id,
            'authors' => \array_map(
                fn (string $author): Document|string => $this->database->getDocument('authors', $author)->isEmpty()
                    ? new Document(['$id' => $author, 'name' => $author])
                    : $author,
                $authors,
            ),
        ]));
    }

    /**
     * @return list<string>
     */
    private function relatedIds(string $collection, string $id, string $key): array
    {
        $related = $this->database->getDocument($collection, $id)->getAttribute($key);
        $this->assertIsArray($related);

        $ids = [];
        foreach ($related as $document) {
            $this->assertInstanceOf(Document::class, $document);
            $ids[] = $document->getId();
        }
        \sort($ids);

        return $ids;
    }
}
