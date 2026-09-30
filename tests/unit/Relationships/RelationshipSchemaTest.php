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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $database->deleteIndex('books', '_index_author');

        try {
            $database->updateRelationship('books', 'author', newKey: 'writer');
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

    public function testIndexMetadataOfTheMetadataCollectionCannotBeUpdated(): void
    {
        $database = new class (new Memory(), new Cache(new None())) extends Database {
            public function renameIndexAttributes(string $collection, string $id): Index
            {
                return $this->updateIndexMeta($collection, $id, static function (Index $index): void {
                    $index->setAttribute('attributes', ['changed']);
                });
            }
        };
        $this->prepare($database);

        try {
            $database->renameIndexAttributes(Database::METADATA, '_key_title');
            $this->fail('the metadata collection\'s indexes must not be changed');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot update metadata indexes', $error->getMessage());
        }

        try {
            $database->renameIndexAttributes('books', 'missing');
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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->updateRelationship('books', 'author', newKey: 'writer');
            $this->fail('a failed definition update must fail the rename');
        } catch (RuntimeException $error) {
            $this->assertSame($failure, $error);
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
        $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));

        try {
            $database->updateRelationship('books', 'writers', newKey: 'authors_of', newTwoWayKey: 'written');
            $this->fail('a failed junction definition update must fail the rename');
        } catch (RuntimeException $error) {
            $this->assertSame($failure, $error);
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
        $database->createRelationship(Relationship::oneToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
        $database->deleteIndex('authors', '_index_owner');
        $physical = $this->schemaIndexIds($database, 'books');

        try {
            $database->updateRelationship('books', 'library', newKey: 'shelf', newTwoWayKey: 'keeper');
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
            static fn (): bool => $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors')),
            static fn (): bool => $database->updateRelationship('books', 'author', newKey: 'writer'),
            static fn (): bool => $database->deleteRelationship('books', 'author'),
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
                $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
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

        $this->assertTrue($database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books')));
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
                $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
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
                $database->createRelationship(Relationship::oneToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
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
                $database->createRelationship(Relationship::oneToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'library', twoWayKey: 'owner'));
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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $before = $this->relationship($database, 'books', 'author');

        $this->assertTrue($database->updateRelationship('books', 'author'));
        $this->assertEquals($before, $this->relationship($database, 'books', 'author'));

        try {
            $database->updateRelationship('books', 'missing', newKey: 'other');
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
        $database->createRelationship(Relationship::oneToMany(collection: 'authors', relatedCollection: 'books', twoWay: true, key: 'books', twoWayKey: 'author'));
        $database->createDocument('authors', new Document([Document::ID => 'ada', 'name' => 'Ada']));
        $database->createDocument('books', new Document([Document::ID => 'notes', 'title' => 'Notes', 'author' => 'ada']));

        $this->assertTrue($database->updateRelationship('books', 'author', newKey: 'writer'));

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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->assertTrue($database->updateRelationship('authors', 'books', newTwoWayKey: 'writer'));

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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->updateRelationship('books', 'author', newKey: 'writer');
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
            public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
            {
                parent::updateRelationship($relationship, $newKey, $newTwoWayKey);

                throw new RuntimeException('the connection dropped after the rename');
            }
        };
        $database = $this->database($adapter);
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->assertTrue($database->updateRelationship('books', 'author', newKey: 'writer'));

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
        $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $junction = $this->junction($database);
        $database->deleteIndex($junction, '_index_writers');

        try {
            $database->updateRelationship('books', 'writers', newKey: 'authors_of', newTwoWayKey: 'written');
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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        $this->assertTrue($database->deleteRelationship('books', 'author'));

        $this->assertNotContains('author', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('books', $this->attributeKeys($database, 'authors'));
    }

    public function testAnAdapterThatDoesNotDeleteTheRelationshipFailsTheDelete(): void
    {
        $database = $this->database($this->memory(['deleteRelationship' => static fn (): bool => false]));
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

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
        $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));
        $armed = true;

        try {
            $database->deleteRelationship('books', 'writers');
            $this->fail('a failed definition write must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata after retries for relationship deletion 'writers': the definitions could not be written", $error->getMessage());
            $this->assertSame($failure, $error->getPrevious());
        }

        $armed = false;
        $this->assertContains('writers', $this->attributeKeys($database, 'books'));
    }

    public function testAFailedDefinitionWriteOnDeleteKeepsItsErrorWhenTheIndexesCannotBeRestored(): void
    {
        $failure = new RuntimeException('the definitions could not be written');
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
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $armed = true;

        try {
            $database->deleteRelationship('books', 'author');
            $this->fail('a failed definition write must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to persist metadata after retries for relationship deletion 'author': the definitions could not be written", $error->getMessage());
            $this->assertSame($failure, $error->getPrevious());
        }

        $armed = false;
        $this->assertContains('author', $this->attributeKeys($database, 'books'));
    }

    /**
     * @return list<string>
     */
    private function schemaIndexIds(Database $database, string $collection): array
    {
        $ids = \array_map(static fn (Document $index): string => $index->getId(), $database->getSchemaIndexes($collection));
        \sort($ids);

        return $ids;
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_values(\array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->attributes($database, $collection),
        ));
    }

    private function relationship(Database $database, string $collection, string $key): Relationship
    {
        foreach ($this->attributes($database, $collection) as $attribute) {
            if ($attribute->key === $key) {
                return Relationship::fromArray(['collection' => $collection] + $attribute->getArrayCopy());
            }
        }

        $this->fail("{$collection} has no relationship {$key}");
    }

    /**
     * @return array<Attribute>
     */
    private function attributes(Database $database, string $collection): array
    {
        /** @var array<Attribute> $attributes */
        $attributes = $database->getCollection($collection)->getAttribute('attributes', []);

        return $attributes;
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
        $database->createCollection(new Collection(id: 'books', attributes: [Attribute::string(key: 'title', size: 64)], permissions: $permissions));
        $database->createCollection(new Collection(id: 'authors', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions));

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

            public function createRelationship(Relationship $relationship): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::createRelationship($relationship);
            }

            public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship, $newKey, $newTwoWayKey]) ?? parent::updateRelationship($relationship, $newKey, $newTwoWayKey);
            }

            public function deleteRelationship(Relationship $relationship): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::deleteRelationship($relationship);
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

                return parent::updateDocument($collection, $id, $document);
            }

            public function createDocument(string $collection, Document $document): Document
            {
                if ($this->create !== null) {
                    ($this->create)($collection);
                }

                return parent::createDocument($collection, $document);
            }

            protected function updateAttributeMeta(string $collection, string $id, callable $updateCallback, bool $triggerEvent = true): Attribute
            {
                if ($this->attributeMeta !== null) {
                    ($this->attributeMeta)($collection, $id);
                }

                return parent::updateAttributeMeta($collection, $id, $updateCallback, $triggerEvent);
            }
        };

        return $this->prepare($database);
    }

    private function index(Database $database, string $collection, string $key): ?Index
    {
        /** @var array<Index> $indexes */
        $indexes = $database->getCollection($collection)->getAttribute('indexes', []);
        foreach ($indexes as $index) {
            if ($index->key === $key) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @return list<string>
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
                public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
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
                public function deleteRelationship(Relationship $relationship): bool
                {
                    throw new NotFoundException('Relationship not found in the schema');
                }
            };
        }

        return $this->memory(['deleteRelationship' => static fn (): never => throw new NotFoundException('Relationship not found in the schema')]);
    }
}
