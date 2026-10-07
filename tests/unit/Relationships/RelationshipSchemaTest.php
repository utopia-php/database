<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Schema\Index as SchemaIndex;
use Utopia\Database\Validator\Authorization;

final class RelationshipSchemaTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenamingARelationshipWhoseIndexIsGoneKeepsTheOldKey(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $database->deleteIndex('books', '_index_author');

        try {
            $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer'));
            $this->fail('a relationship whose index is gone cannot be renamed');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship indexes for 'author': Index not found", $error->getMessage());
            $this->assertInstanceOf(NotFoundException::class, $error->getPrevious());
        }

        $keys = $this->attributeKeys($database, 'books');
        $this->assertContains('author', $keys);
        $this->assertNotContains('writer', $keys);
        $this->assertSame('author', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    public function testIndexesOfTheMetadataCollectionCannotBeRenamed(): void
    {
        $database = $this->database(new Memory());
        $before = $this->indexKeys($database, Database::METADATA);

        try {
            $database->renameIndex(Database::METADATA, '_key_title', 'renamed');
            $this->fail('the metadata collection\'s indexes must not be changed');
        } catch (NotFoundException $error) {
            $this->assertSame('Index not found', $error->getMessage());
        }

        $this->assertSame($before, $this->indexKeys($database, Database::METADATA));

        try {
            $database->renameIndex('books', 'missing', 'renamed');
            $this->fail('an unknown index cannot be changed');
        } catch (NotFoundException $error) {
            $this->assertSame('Index not found', $error->getMessage());
        }
    }

    public function testAFailedDefinitionUpdateReversesTheSchemaRename(): void
    {
        $renames = [];
        $failure = new RuntimeException('the related definition could not be written');
        $adapter = $this->memory([
            'updateRelationship' => static function (Relationship $relationship, ?string $newKey, ?string $newTwoWayKey) use (&$renames): ?bool {
                $renames[] = "{$relationship->key}->{$newKey}";

                return null;
            },
        ]);
        $database = $this->intercepting($adapter, attributeMeta: static function (string $collection, string $id) use ($failure): void {
            if ($collection === 'authors' && $id === 'books') {
                throw $failure;
            }
        });
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer'));
            $this->fail('a failed definition update must fail the rename');
        } catch (DatabaseException $error) {
            $this->assertSame($failure, $error->getPrevious());
        }

        $this->assertSame(['author->writer', 'writer->author'], $renames);
        $this->assertContains('author', $this->attributeKeys($database, 'books'), 'the definition that was written is restored with the schema');
        $this->assertNotContains('writer', $this->attributeKeys($database, 'books'));
        $this->assertSame('books', $this->relationship($database, 'books', 'author')->twoWayKey);
        $this->assertSame('author', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    public function testAFailedJunctionDefinitionUpdateRestoresBothSides(): void
    {
        $failure = new RuntimeException('the junction definition could not be written');
        $database = $this->intercepting(new Memory(), attributeMeta: static function (string $collection, string $id) use ($failure): void {
            if (\str_starts_with($collection, '_') && $id === 'writers') {
                throw $failure;
            }
        });
        $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));

        try {
            $database->updateRelationship('books', 'writers', new RelationshipUpdate(key: 'authors_of', twoWayKey: 'written'));
            $this->fail('a failed junction definition update must fail the rename');
        } catch (DatabaseException $error) {
            $this->assertSame($failure, $error->getPrevious());
        }

        $this->assertContains('writers', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('authors_of', $this->attributeKeys($database, 'books'));
        $this->assertContains('works', $this->attributeKeys($database, 'authors'));
        $this->assertNotContains('written', $this->attributeKeys($database, 'authors'));
        $this->assertSame('works', $this->relationship($database, 'books', 'writers')->twoWayKey);
        $this->assertSame('writers', $this->relationship($database, 'authors', 'works')->twoWayKey);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAFailedSecondIndexRenameReversesTheFirstAndTheDefinitions(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
        $database->deleteIndex('authors', '_index_owner');
        $physical = $this->schemaIndexIds($database, 'books');

        try {
            $database->updateRelationship('books', 'library', new RelationshipUpdate(key: 'shelf', twoWayKey: 'keeper'));
            $this->fail('a rename whose second index is gone must fail');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship indexes for 'library': Index not found", $error->getMessage());
        }

        $this->assertSame(['library'], $this->indexAttributes($database, 'books', '_index_library'));
        $this->assertNull($this->index($database, 'books', '_index_shelf'));
        $this->assertContains('library', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('shelf', $this->attributeKeys($database, 'books'));
        $this->assertContains('owner', $this->attributeKeys($database, 'authors'));
        $this->assertNotContains('keeper', $this->attributeKeys($database, 'authors'));
        $this->assertSame($physical, $this->schemaIndexIds($database, 'books'), 'the physical index is back under its old name');
    }

    public function testRelationshipSchemaChangesNeedTheRelationshipsFeature(): void
    {
        $database = new Database($this->createStub(Adapter::class), new Cache(new None()));

        foreach ([
            static fn (): Relationship => $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors')),
            static fn (): Relationship => $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer')),
            static fn () => $database->deleteRelationship('books', 'author'),
        ] as $change) {
            try {
                $change();
                $this->fail('an adapter without relationships must refuse the change');
            } catch (DatabaseException $error) {
                $this->assertSame('Adapter does not support relationships', $error->getMessage());
            }
        }
    }

    public function testAnAdapterThatDoesNotCreateTheRelationshipFailsTheCreate(): void
    {
        $adapter = $this->memory([
            'createRelationship' => static fn (): bool => false,
            'deleteCollection' => static fn (string $id): never => throw new RuntimeException("cannot drop {$id}"),
        ]);
        $database = $this->database($adapter);

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame('Failed to create relationship', $error->getMessage());
        $this->assertStringContainsString('Failed to cleanup junction collection', $log);
        $this->assertNotContains('writers', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('works', $this->attributeKeys($database, 'authors'));
    }

    public function testARelationshipOnlyInTheSchemaIsAdopted(): void
    {
        $database = $this->database($this->memory([
            'createRelationship' => static fn (): never => throw new DuplicateException('Relationship already exists in the schema'),
        ]));

        $this->assertSame('author', $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'))->key);
        $this->assertContains('author', $this->attributeKeys($database, 'books'));
        $this->assertContains('books', $this->attributeKeys($database, 'authors'));

        $database->createDocument('authors', new Document([Document::ID => 'ada', 'name' => 'Ada']));
        $database->createDocument('books', new Document([Document::ID => 'notes', 'title' => 'Notes', 'author' => 'ada']));
        $author = $database->getDocument('books', 'notes')->getAttribute('author');
        $this->assertInstanceOf(Document::class, $author);
        $this->assertSame('ada', $author->getId());
    }

    public function testAFailedDefinitionWriteRollsBackAndLogsTheFailedCleanups(): void
    {
        $adapter = $this->memory([
            'deleteRelationship' => static fn (): never => throw new RuntimeException('cannot drop the relationship'),
            'deleteCollection' => static fn (string $id): never => throw new RuntimeException("cannot drop {$id}"),
        ]);
        $database = $this->intercepting($adapter, update: static function (string $collection, string $id, Document $document): void {
            if ($collection === Database::METADATA && $id === 'books' && \in_array('writers', self::keysOf($document), true)) {
                throw new RuntimeException('the definition could not be written');
            }
        });

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertStringStartsWith('Failed to create relationship: ', $error->getMessage());
        $this->assertStringContainsString("Failed to cleanup relationship 'writers': ", $log);
        $this->assertStringContainsString('Failed to cleanup junction collection', $log);
        $this->assertNotContains('writers', $this->attributeKeys($database, 'books'));
    }

    public function testAFailedIndexRollsBackAndLogsTheFailedIndexAndDefinitionCleanups(): void
    {
        $adapter = $this->memory([
            'createIndex' => static fn (string $collection, Index $index): ?bool => $index->key === '_index_owner' ? throw new RuntimeException('cannot index the owner') : null,
            'deleteIndex' => static fn (string $collection, string $id): never => throw new RuntimeException("cannot drop {$id}"),
        ]);
        /** @var bool $armed */
        $armed = false;
        $database = $this->intercepting($adapter, update: static function (string $collection, string $id, Document $document) use (&$armed): void {
            if ($armed && $collection === Database::METADATA && $id === 'books' && ! \in_array('library', self::keysOf($document), true)) {
                throw new RuntimeException('the definitions could not be removed');
            }
        });
        $armed = true;

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame('Failed to create relationship indexes: cannot index the owner', $error->getMessage());
        $this->assertStringContainsString("Failed to cleanup index '_index_library'", $log);
        $this->assertStringContainsString("Failed to cleanup metadata for relationship 'library'", $log);
    }

    public function testAFailedIndexWhoseRelationshipCleanupFailsIsLogged(): void
    {
        $database = $this->database($this->memory([
            'createIndex' => static fn (string $collection, Index $index): ?bool => $index->key === '_index_owner' ? throw new RuntimeException('cannot index the owner') : null,
            'deleteRelationship' => static fn (): never => throw new RuntimeException('cannot drop the relationship'),
        ]));

        $error = null;
        $log = StderrCapture::during(function () use ($database, &$error): void {
            try {
                $database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
            } catch (DatabaseException $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame('Failed to create relationship indexes: cannot index the owner', $error->getMessage());
        $this->assertStringContainsString("Failed to cleanup relationship 'library': ", $log);
        $this->assertNotContains('library', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('owner', $this->attributeKeys($database, 'authors'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnUpdateWithoutChangesIsAcceptedAndAnUnknownRelationshipIsNotFound(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $before = $this->relationship($database, 'books', 'author');

        $this->assertSame($before->toDocument()->getArrayCopy(), $database->updateRelationship('books', 'author', new RelationshipUpdate())->toDocument()->getArrayCopy());
        $this->assertSame($before->toDocument()->getArrayCopy(), $this->relationship($database, 'books', 'author')->toDocument()->getArrayCopy());

        try {
            $database->updateRelationship('books', 'missing', new RelationshipUpdate(key: 'other'));
            $this->fail('an unknown relationship cannot be updated');
        } catch (NotFoundException $error) {
            $this->assertSame('Relationship not found', $error->getMessage());
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenamingFromTheChildSideOfAOneToManyRenamesItsIndex(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('authors', Relationship::oneToMany(relatedCollection: 'books', twoWay: true, key: 'books', twoWayKey: 'author'));
        $database->createDocument('authors', new Document([Document::ID => 'ada', 'name' => 'Ada']));
        $database->createDocument('books', new Document([Document::ID => 'notes', 'title' => 'Notes', 'author' => 'ada']));

        $this->assertSame('writer', $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer'))->key);

        $this->assertSame(['writer'], $this->indexAttributes($database, 'books', '_index_writer'));
        $this->assertNull($this->index($database, 'books', '_index_author'));
        $this->assertSame(['notes'], \array_map(
            static fn (Document $book): string => $book->getId(),
            $database->find('books', [Query::equal('writer', ['ada'])]),
        ));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenamingTheParentKeyFromTheChildSideOfAManyToOneRenamesItsIndex(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->assertSame('writer', $database->updateRelationship('authors', 'books', new RelationshipUpdate(twoWayKey: 'writer'))->twoWayKey);

        $this->assertSame(['writer'], $this->indexAttributes($database, 'books', '_index_writer'));
        $this->assertNull($this->index($database, 'books', '_index_author'));
        $this->assertSame('writer', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnAdapterThatDoesNotUpdateTheRelationshipFailsTheUpdate(Closure $adapter): void
    {
        $database = $this->database($this->refusingUpdates($adapter()));
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer'));
            $this->fail('an adapter that does not update the relationship must fail the update');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship 'author': Failed to update relationship", $error->getMessage());
        }

        $this->assertContains('author', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('writer', $this->attributeKeys($database, 'books'));
    }

    public function testARenameTheSchemaAlreadyAppliedIsCompleted(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
            {
                parent::updateRelationship($collection, $relationship, $side, $update);

                throw new RuntimeException('the connection dropped after the rename');
            }
        };
        $database = $this->database($adapter);
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->assertSame('writer', $database->updateRelationship('books', 'author', new RelationshipUpdate(key: 'writer'))->key);

        $this->assertContains('writer', $this->attributeKeys($database, 'books'));
        $this->assertSame('writer', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAFailedJunctionIndexRenameRestoresTheJunctionDefinitions(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $junction = $this->junction($database);
        $database->deleteIndex($junction, '_index_writers');

        try {
            $database->updateRelationship('books', 'writers', new RelationshipUpdate(key: 'authors_of', twoWayKey: 'written'));
            $this->fail('a rename whose junction index is gone must fail');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship indexes for 'writers': Index not found", $error->getMessage());
        }

        $this->assertEqualsCanonicalizing(['writers', 'works'], $this->attributeKeys($database, $junction));
        $this->assertContains('writers', $this->attributeKeys($database, 'books'));
        $this->assertContains('works', $this->attributeKeys($database, 'authors'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testARelationshipWhoseColumnIsGoneIsStillDeleted(Closure $adapter): void
    {
        $inner = $adapter();
        $database = $this->database($this->missingRelationships($inner));
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $database->deleteRelationship('books', 'author');

        $this->assertNotContains('author', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('books', $this->attributeKeys($database, 'authors'));
    }

    public function testAnAdapterThatDoesNotDeleteTheRelationshipFailsTheDelete(): void
    {
        $database = $this->database($this->memory(['deleteRelationship' => static fn (): bool => false]));
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->deleteRelationship('books', 'author');
            $this->fail('an adapter that does not drop the relationship must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to delete relationship', $error->getMessage());
        }

        $this->assertContains('author', $this->attributeKeys($database, 'books'));
    }

    public function testAFailedDefinitionWriteOnDeleteKeepsItsErrorWhenTheRollbackFails(): void
    {
        $failure = new RuntimeException('the definitions could not be written');
        /** @var bool $armed */
        $armed = false;
        $adapter = $this->memory([
            'createRelationship' => static function () use (&$armed): ?bool {
                if ($armed) {
                    throw new RuntimeException('the relationship could not be recreated');
                }

                return null;
            },
        ]);
        $database = $this->intercepting($adapter, update: static function (string $collection, string $id, Document $document) use (&$armed, $failure): void {
            if ($armed && $collection === Database::METADATA && $id === 'books' && ! \in_array('writers', self::keysOf($document), true)) {
                throw $failure;
            }
        }, create: static function (string $collection) use (&$armed): void {
            if ($armed && $collection === Database::METADATA) {
                throw new RuntimeException('the junction definition could not be restored');
            }
        });
        $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $armed = true;

        try {
            $database->deleteRelationship('books', 'writers');
            $this->fail('a failed definition write must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata after retries for relationship deletion 'writers': the definitions could not be written", $error->getMessage());
            $this->assertSame($failure, $error->getPrevious());
        }

        /** @var bool $armed */
        $armed = false;
        $this->assertContains('writers', $this->attributeKeys($database, 'books'));
    }

    public function testAFailedDefinitionWriteOnDeleteKeepsItsErrorWhenTheIndexesCannotBeRestored(): void
    {
        $failure = new RuntimeException('the definitions could not be written');
        /** @var bool $armed */
        $armed = false;
        $adapter = $this->memory([
            'createIndex' => static function (string $collection, Index $index) use (&$armed): ?bool {
                if ($armed) {
                    throw new RuntimeException('the index could not be restored');
                }

                return null;
            },
        ]);
        $database = $this->intercepting($adapter, update: static function (string $collection, string $id, Document $document) use (&$armed, $failure): void {
            if ($armed && $collection === Database::METADATA && $id === 'books' && ! \in_array('author', self::keysOf($document), true)) {
                throw $failure;
            }
        });
        $database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $armed = true;

        try {
            $database->deleteRelationship('books', 'author');
            $this->fail('a failed definition write must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata after retries for relationship deletion 'author': the definitions could not be written", $error->getMessage());
            $this->assertSame($failure, $error->getPrevious());
        }

        /** @var bool $armed */
        $armed = false;
        $this->assertContains('author', $this->attributeKeys($database, 'books'));
    }

    public function testAFailedJunctionPurgeRestoresEveryDefinitionItCanAndReversesTheSchemaRename(): void
    {
        $renames = [];
        $adapter = $this->memory([
            'updateRelationship' => static function (Relationship $relationship, ?string $newKey) use (&$renames): ?bool {
                $renames[] = "{$relationship->key}->{$newKey}";

                return \count($renames) > 1 ? throw new RuntimeException('the schema rename could not be reversed') : null;
            },
        ]);
        $failure = new RuntimeException('the junction cache could not be purged');
        $database = new class ($adapter, new Cache(new None()), $failure) extends Database {
            public bool $armed = false;

            private int $junctionPurges = 0;

            public function __construct(Adapter $adapter, Cache $cache, private readonly RuntimeException $failure)
            {
                parent::__construct($adapter, $cache);
            }

            public function purgeCachedCollection(string $collection): void
            {
                if ($this->armed && \str_starts_with($collection, '_') && \in_array(++$this->junctionPurges, [3, 4, 5], true)) {
                    throw $this->failure;
                }

                parent::purgeCachedCollection($collection);
            }

            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                if ($this->armed && $collection === self::METADATA && $id === 'books' && RelationshipSchemaTest::replacedAttribute($this, $id, $document) === 'authors_of') {
                    throw new RuntimeException('the definition could not be restored');
                }

                return parent::updateDocument($collection, $id, $document);
            }
        };
        $this->prepare($database);
        $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $junction = $this->junction($database);
        $database->armed = true;

        try {
            $database->updateRelationship('books', 'writers', new RelationshipUpdate(key: 'authors_of', twoWayKey: 'written'));
            $this->fail('a failed junction purge must fail the rename');
        } catch (RuntimeException $error) {
            $this->assertSame($failure, $error);
        }

        $database->armed = false;
        $this->assertSame(['writers->authors_of', 'authors_of->writers'], $renames, 'the schema rename is reversed, and its failure is not reported over the purge failure');
        $this->assertEqualsCanonicalizing(['writers', 'works'], $this->attributeKeys($database, $junction));
        $this->assertContains('works', $this->attributeKeys($database, 'authors'));
        $this->assertNotContains('written', $this->attributeKeys($database, 'authors'));
        $this->assertContains('authors_of', $this->attributeKeys($database, 'books'), 'the one restore that failed leaves its definition; the others still run');
    }

    public function testAFailedJunctionIndexRenameKeepsItsErrorWhenEveryRollbackStepFails(): void
    {
        $renames = [];
        $adapter = $this->memory([
            'updateRelationship' => static function (Relationship $relationship, ?string $newKey) use (&$renames): ?bool {
                $renames[] = "{$relationship->key}->{$newKey}";

                return \count($renames) > 1 ? throw new RuntimeException('the schema rename could not be reversed') : null;
            },
        ]);
        $database = new class ($adapter, new Cache(new None())) extends Database {
            public bool $armed = false;

            /** @var list<string> */
            public array $rollbacks = [];

            public function renameIndex(string $collection, string $old, string $new): void
            {
                if ($this->armed && $new === '_index_writers') {
                    $this->rollbacks[] = "index {$old}->{$new}";

                    throw new RuntimeException('the index rename could not be reversed');
                }

                parent::renameIndex($collection, $old, $new);
            }

            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                $replaced = $this->armed && $collection === self::METADATA ? RelationshipSchemaTest::replacedAttribute($this, $id, $document) : null;
                if ($replaced !== null && \in_array($replaced, ['authors_of', 'written'], true)) {
                    $step = (\str_starts_with($id, '_') ? 'junction' : $id).' '.$replaced;
                    if (\end($this->rollbacks) !== $step) {
                        $this->rollbacks[] = $step;
                    }

                    throw new RuntimeException('the definition could not be restored');
                }

                return parent::updateDocument($collection, $id, $document);
            }
        };
        $this->prepare($database);
        $database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $database->deleteIndex($this->junction($database), '_index_works');
        $database->armed = true;

        try {
            $database->updateRelationship('books', 'writers', new RelationshipUpdate(key: 'authors_of', twoWayKey: 'written'));
            $this->fail('a rename whose second junction index is gone must fail');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship indexes for 'writers': Index not found", $error->getMessage());
            $this->assertInstanceOf(NotFoundException::class, $error->getPrevious());
        }

        $this->assertSame(['writers->authors_of', 'authors_of->writers'], $renames);
        $this->assertSame([
            'index _index_authors_of->_index_writers',
            'books authors_of',
            'authors written',
            'junction authors_of',
            'junction written',
        ], $database->rollbacks, 'every rollback step runs although each one fails');
    }

    /**
     * @return list<string>
     */
    private function schemaIndexIds(Database $database, string $collection): array
    {
        $ids = \array_map(static fn (SchemaIndex $index): string => $index->name, $database->getSchemaIndexes($collection));
        \sort($ids);

        return $ids;
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->attributes($database, $collection),
        );
    }

    private function relationship(Database $database, string $collection, string $key): Relationship
    {
        foreach ($this->attributes($database, $collection) as $attribute) {
            if ($attribute->key === $key && $attribute->relationship !== null) {
                return $attribute->relationship;
            }
        }

        $this->fail("{$collection} has no relationship {$key}");
    }

    /**
     * The key of the one stored attribute $definition replaces in the stored definition of $collection, or null when
     * the write adds, removes or changes no attribute.
     */
    public static function replacedAttribute(Database $database, string $collection, Document $definition): ?string
    {
        $stored = self::attributeDefinitions($database->silent(fn (): Document => $database->getDocument(Database::METADATA, $collection)));
        $written = self::attributeDefinitions($definition);

        if (\count($stored) !== \count($written)) {
            return null;
        }

        foreach ($stored as $key => $attribute) {
            if (($written[$key] ?? null) !== $attribute) {
                return $key;
            }
        }

        return null;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private static function attributeDefinitions(Document $definition): array
    {
        $definitions = [];
        foreach (Collection::fromDocument($definition)->attributes() as $attribute) {
            $definitions[$attribute->key] = $attribute->toDocument()->getArrayCopy();
        }

        return $definitions;
    }

    /**
     * @return list<Attribute>
     */
    private function attributes(Database $database, string $collection): array
    {
        return $database->getCollection($collection)->attributes();
    }

    private function database(Adapter $adapter): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())));
    }

    private function prepare(Database $database): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_schema')
            ->setNamespace('relationship_schema_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(Collection::create(id: 'books', attributes: [Attribute::string(key: 'title', size: 64)], permissions: $permissions));
        $database->createCollection(Collection::create(id: 'authors', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions));

        return $database;
    }

    /**
     * @param  array<string, Closure>  $overrides
     */
    private function memory(array $overrides): Memory
    {
        return new class ($overrides) extends Memory {
            /**
             * @param  array<string, Closure>  $overrides
             */
            public function __construct(private readonly array $overrides)
            {
                parent::__construct();
            }

            public function createRelationship(string $collection, Relationship $relationship): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::createRelationship($collection, $relationship);
            }

            public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship, $update->key, $update->twoWayKey]) ?? parent::updateRelationship($collection, $relationship, $side, $update);
            }

            public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::deleteRelationship($collection, $relationship, $side);
            }

            public function deleteCollection(string $id): bool
            {
                if (! \str_starts_with($id, '_')) {
                    return parent::deleteCollection($id);
                }

                return $this->intercept(__FUNCTION__, [$id]) ?? parent::deleteCollection($id);
            }

            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
            {
                return $this->intercept(__FUNCTION__, [$collection, $index]) ?? parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
            }

            public function deleteIndex(string $collection, string $id): bool
            {
                return $this->intercept(__FUNCTION__, [$collection, $id]) ?? parent::deleteIndex($collection, $id);
            }

            /**
             * @param  list<mixed>  $arguments
             */
            private function intercept(string $method, array $arguments): ?bool
            {
                $override = $this->overrides[$method] ?? null;
                $result = $override === null ? null : $override(...$arguments);

                return \is_bool($result) ? $result : null;
            }
        };
    }

    /**
     * @param  (Closure(string, string, Document): void)|null  $update
     * @param  (Closure(string, string): void)|null  $attributeMeta
     * @param  (Closure(string): void)|null  $create
     */
    private function intercepting(Adapter $adapter, ?Closure $update = null, ?Closure $attributeMeta = null, ?Closure $create = null): Database
    {
        $database = new class ($adapter, new Cache(new None()), $update, $attributeMeta, $create) extends Database {
            public function __construct(
                Adapter $adapter,
                Cache $cache,
                private readonly ?Closure $update,
                private readonly ?Closure $attributeMeta,
                private readonly ?Closure $create,
            ) {
                parent::__construct($adapter, $cache);
            }

            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                if ($this->update !== null) {
                    ($this->update)($collection, $id, $document);
                }

                $attributeMeta = $this->attributeMeta;
                $replaced = $attributeMeta === null || $collection !== self::METADATA ? null : RelationshipSchemaTest::replacedAttribute($this, $id, $document);
                if ($attributeMeta !== null && $replaced !== null) {
                    $attributeMeta($id, $replaced);
                }

                return parent::updateDocument($collection, $id, $document);
            }

            public function createDocument(string $collection, Document $document): Document
            {
                if ($this->create !== null) {
                    ($this->create)($collection);
                }

                return parent::createDocument($collection, $document);
            }
        };

        return $this->prepare($database);
    }

    private function index(Database $database, string $collection, string $key): ?Index
    {
        foreach ($database->getCollection($collection)->indexes() as $index) {
            if ($index->key === $key) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @return array<string>
     */
    private function indexAttributes(Database $database, string $collection, string $key): array
    {
        $index = $this->index($database, $collection, $key);
        $this->assertNotNull($index, "{$collection} has no index {$key}");

        return $index->attributes;
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database, string $collection): array
    {
        return \array_map(static fn (Index $index): string => $index->key, $database->getCollection($collection)->indexes());
    }

    /**
     * @return list<string>
     */
    private static function keysOf(Document $document): array
    {
        /** @var array<Document|array<string, mixed>> $attributes */
        $attributes = $document->getAttribute('attributes', []);
        $keys = [];
        foreach ($attributes as $attribute) {
            $key = $attribute instanceof Document ? $attribute->getAttribute('key', $attribute->getId()) : ($attribute['key'] ?? $attribute[Document::ID] ?? '');
            $keys[] = \is_string($key) ? $key : '';
        }

        return $keys;
    }

    private function junction(Database $database): string
    {
        return '_'.$database->getCollection('books')->getSequence().'_'.$database->getCollection('authors')->getSequence();
    }

    private function refusingUpdates(Adapter $adapter): Adapter
    {
        if ($adapter instanceof SQLite) {
            return new class (new PDO('sqlite::memory:')) extends SQLite {
                public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool
                {
                    return false;
                }
            };
        }

        return $this->memory(['updateRelationship' => static fn (): bool => false]);
    }

    private function missingRelationships(Adapter $adapter): Adapter
    {
        if ($adapter instanceof SQLite) {
            return new class (new PDO('sqlite::memory:')) extends SQLite {
                public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool
                {
                    throw new NotFoundException('Relationship not found in the schema');
                }
            };
        }

        return $this->memory(['deleteRelationship' => static fn (): never => throw new NotFoundException('Relationship not found in the schema')]);
    }
}
