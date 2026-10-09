<?php

namespace Tests\Unit\Relationships;

use PDO;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class RelationshipRenameRecoveryTest extends TestCase
{
    private const string NAMESPACE = 'rename_recovery';

    private PDO $pdo;

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $this->pdo = new PDO('sqlite::memory:');
        $this->database = new Database(new SQLite($this->pdo), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization($authorization);
        $this->database->addHook(new Relationships());
        $this->database->create();

        foreach (['books', 'authors'] as $collection) {
            $this->database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
                documentSecurity: false,
            ));
        }
    }

    public function testAnOrphanColumnUnderTheNewKeyIsNotTakenForAnEarlierRename(): void
    {
        $this->database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', key: 'author'));
        $this->database->createDocument('authors', new Document(['$id' => 'herbert', 'name' => 'Herbert']));
        $this->database->createDocument('books', new Document(['$id' => 'dune', 'name' => 'Dune', 'author' => 'herbert']));
        $this->addColumn('books', 'writer');

        $this->assertRenameFails('books', 'author', new RelationshipUpdate(key: 'writer'));

        $this->assertSame(['author', 'name'], $this->attributeKeys('books'));
        $author = $this->database->getDocument('books', 'dune')->getAttribute('author');
        $this->assertInstanceOf(Document::class, $author);
        $this->assertSame('herbert', $author->getId());
    }

    public function testATwoWayKeyRenameTheRelatedTableRejectsIsNotTakenForAnEarlierRename(): void
    {
        $this->database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'book'));
        $this->addColumn('authors', 'novel');

        $this->assertRenameFails('books', 'author', new RelationshipUpdate(twoWayKey: 'novel'));

        $this->assertSame(['book', 'name'], $this->attributeKeys('authors'));
    }

    public function testARetryCompletesARenameThatMovedOnlyOneOfItsColumns(): void
    {
        $this->database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'book'));
        $this->database->createDocument('authors', new Document(['$id' => 'herbert', 'name' => 'Herbert']));
        $this->database->createDocument('books', new Document(['$id' => 'dune', 'name' => 'Dune', 'author' => 'herbert']));
        $this->addColumn('authors', 'novel');
        $update = new RelationshipUpdate(key: 'writer', twoWayKey: 'novel');

        $this->assertRenameFails('books', 'author', $update);
        $this->assertSame(['author', 'name'], $this->attributeKeys('books'));

        $this->pdo->exec('ALTER TABLE `'.self::NAMESPACE.'_authors` DROP COLUMN `novel`');
        $this->database->updateRelationship('books', 'author', $update);

        $this->assertSame(['name', 'writer'], $this->attributeKeys('books'));
        $this->assertSame(['name', 'novel'], $this->attributeKeys('authors'));
        $writer = $this->database->getDocument('books', 'dune')->getAttribute('writer');
        $this->assertInstanceOf(Document::class, $writer);
        $this->assertSame('herbert', $writer->getId());
        $novel = $this->database->getDocument('authors', 'herbert')->getAttribute('novel');
        $this->assertInstanceOf(Document::class, $novel);
        $this->assertSame('dune', $novel->getId());
    }

    private function addColumn(string $collection, string $column): void
    {
        $this->pdo->exec('ALTER TABLE `'.self::NAMESPACE."_{$collection}` ADD COLUMN `{$column}` VARCHAR(255)");
    }

    private function assertRenameFails(string $collection, string $key, RelationshipUpdate $update): void
    {
        try {
            $this->database->updateRelationship($collection, $key, $update);
        } catch (Throwable) {
            return;
        }

        $this->fail('A rename the engine rejected was reported as done');
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(string $collection): array
    {
        $keys = \array_map(static fn (Attribute $attribute): string => $attribute->key, $this->database->getCollection($collection)->attributes());
        \sort($keys);

        return $keys;
    }
}
