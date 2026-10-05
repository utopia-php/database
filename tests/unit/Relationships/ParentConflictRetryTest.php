<?php

namespace Tests\Unit\Relationships;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\RelationshipSQLite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Contention;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;

/**
 * A lock conflict on writing a document, after its related documents were related, rolls the whole attempt back and
 * the transaction runs it again: the retried attempt must relate the documents it was given once more.
 */
final class ParentConflictRetryTest extends TestCase
{
    /**
     * @return iterable<string, array{bool}>
     */
    public static function modes(): iterable
    {
        yield 'prepared' => [true];
        yield 'one by one' => [false];
    }

    #[DataProvider('modes')]
    public function testARetriedCreateKeepsItsNewRelatedDocuments(bool $prepare): void
    {
        $database = $this->database($prepare, 'createDocument');

        $database->createDocument('parents', new Document([
            '$id' => 'p1',
            'name' => 'p1',
            'children' => [new Document([
                '$id' => 'c1',
                'name' => 'c1',
                'toys' => [new Document(['$id' => 't1', 'name' => 't1'])],
            ])],
        ]));

        $this->assertSame([['p1', 'p1']], $this->stored($database, 'parents', 'name'));
        $this->assertSame([['c1', 'p1']], $this->stored($database, 'children', 'parent'));
        $this->assertSame([['t1', 'c1']], $this->stored($database, 'toys', 'child'));
    }

    #[DataProvider('modes')]
    public function testARetriedCreateKeepsItsRelatedDocumentIds(bool $prepare): void
    {
        $database = $this->database($prepare, 'createDocument');
        $database->createDocument('children', new Document(['$id' => 'c1', 'name' => 'c1']));

        $database->createDocument('parents', new Document(['$id' => 'p1', 'name' => 'p1', 'children' => ['c1']]));

        $this->assertSame([['p1', 'p1']], $this->stored($database, 'parents', 'name'));
        $this->assertSame([['c1', 'p1']], $this->stored($database, 'children', 'parent'));
    }

    #[DataProvider('modes')]
    public function testARetriedUpdateKeepsItsNewRelatedDocuments(bool $prepare): void
    {
        $database = $this->database($prepare, 'updateDocument');
        $database->createDocument('parents', new Document(['$id' => 'p1', 'name' => 'p1']));

        $database->updateDocument('parents', 'p1', new Document([
            'name' => 'renamed',
            'children' => [new Document([
                '$id' => 'c1',
                'name' => 'c1',
                'toys' => [new Document(['$id' => 't1', 'name' => 't1'])],
            ])],
        ]));

        $this->assertSame([['p1', 'renamed']], $this->stored($database, 'parents', 'name'));
        $this->assertSame([['c1', 'p1']], $this->stored($database, 'children', 'parent'));
        $this->assertSame([['t1', 'c1']], $this->stored($database, 'toys', 'child'));
    }

    /**
     * A database whose first write of the given operation to the parents collection meets a lock conflict.
     */
    private function database(bool $prepare, string $operation): Database
    {
        $adapter = new class (new PDO('sqlite::memory:'), $operation) extends RelationshipSQLite {
            private bool $conflicted = false;

            public function __construct(PDO $pdo, private readonly string $operation)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                $this->conflict('createDocument', $collection);

                return parent::createDocument($collection, $document);
            }

            #[\Override]
            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                $this->conflict('updateDocument', $collection);

                return parent::updateDocument($collection, $id, $document, $skipPermissions);
            }

            private function conflict(string $operation, Document $collection): void
            {
                if ($this->conflicted || $operation !== $this->operation || $collection->getId() !== 'parents') {
                    return;
                }

                $this->conflicted = true;
                $this->getPDO()->exec('ROLLBACK');

                throw new Contention('Deadlock found when trying to get lock');
            }
        };

        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('parent_conflict')->setNamespace('conflict');
        $database->create();
        $database->addHook(new Relationships($database, prepare: $prepare));

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        foreach (['parents', 'children', 'toys'] as $collection) {
            $database->createCollection(new Collection(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions, documentSecurity: false));
        }
        $database->createRelationship(Relationship::oneToMany(collection: 'parents', relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));
        $database->createRelationship(Relationship::oneToMany(collection: 'children', relatedCollection: 'toys', twoWay: true, key: 'toys', twoWayKey: 'child'));

        return $database;
    }

    /**
     * @return list<array{string, mixed}>
     */
    private function stored(Database $database, string $collection, string $attribute): array
    {
        return \array_values(\array_map(
            static fn (Document $document): array => [$document->getId(), $document->getAttribute($attribute)],
            $database->skipRelationships(static fn (): array => $database->find($collection)),
        ));
    }
}
